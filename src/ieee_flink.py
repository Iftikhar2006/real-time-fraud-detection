import json
import os
import sys
import time

import redis

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.datastream.checkpoint_config import (
    ExternalizedCheckpointRetention
)
PROJECT_DIR = os.path.expanduser("~/fraud-detection")
sys.path.insert(0, PROJECT_DIR)

from src.inference import load_predictor, predict_message
from src.behaviour_engine import BehaviourEngine
from src.risk_engine import RiskEngine
from src.mongo_store import MongoStore


class FraudInference(MapFunction):

    def open(self, runtime_context):
        # Per-stage performance instrumentation.
        self.timing_count = 0
        self.timing_totals = {}
        self.timing_max = {}
        os.chdir(PROJECT_DIR)

        print("Loading LightGBM model...")

        self.model, self.metadata, self.categories = load_predictor()

        print("LightGBM model loaded successfully.")

        print("Connecting to Redis...")

        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_connect_timeout=5
        )

        self.redis_client.ping()

        print("Redis connected successfully.")

        print("Connecting to MongoDB...")

        self.mongo_store = MongoStore()

        print("MongoDB connected successfully.")

        self.redis_prefix = os.getenv("REDIS_PREFIX", "fraud:ieee:")
        self.behaviour_engine = BehaviourEngine(self.redis_client)

        self.risk_engine = RiskEngine(
            threshold=self.metadata["threshold"]
        )

        print("Behaviour Engine and Risk Engine initialized.")

    def record_timing(self, stage, duration):
        self.timing_totals[stage] = (
            self.timing_totals.get(stage, 0.0) + duration
        )

        self.timing_max[stage] = max(
            self.timing_max.get(stage, 0.0),
            duration
        )


    def map(self, value):
        total_start = time.perf_counter()

        # Step 1: JSON parsing.
        start = time.perf_counter()
        message = json.loads(value)
        self.record_timing("json_parse", time.perf_counter() - start)

        # Step 2: ML prediction.
        start = time.perf_counter()
        prediction = predict_message(
            message,
            self.model,
            self.metadata,
            self.categories
        )
        self.record_timing("ml_inference", time.perf_counter() - start)

        # Step 3: Historical behavioural analysis.
        start = time.perf_counter()
        behaviour = self.behaviour_engine.process(message)
        self.record_timing("behaviour_engine", time.perf_counter() - start)

        # Step 4: Risk evaluation.
        start = time.perf_counter()
        risk = self.risk_engine.evaluate(prediction, behaviour)
        self.record_timing("risk_engine", time.perf_counter() - start)

        # Step 5: Combine results.
        result = {
            **prediction,
            "behaviour": behaviour,
            **risk
        }

        transaction_id = result["TransactionID"]

        # Step 6: Redis transaction storage.
        start = time.perf_counter()

        redis_key = f"{self.redis_prefix}transaction:{transaction_id}"

        self.redis_client.set(redis_key, json.dumps(result))

        self.redis_client.sadd(
            f"{self.redis_prefix}transaction_ids",
            str(transaction_id)
        )

        # Step 7: Redis alert storage.
        if result["review_recommended"]:
            alert_key = f"{self.redis_prefix}alert:{transaction_id}"

            self.redis_client.set(alert_key, json.dumps(result))

            self.redis_client.sadd(
                f"{self.redis_prefix}alert_ids",
                str(transaction_id)
            )
        else:
            alert_key = f"{self.redis_prefix}alert:{transaction_id}"

            self.redis_client.delete(alert_key)
            self.redis_client.srem(
                f"{self.redis_prefix}alert_ids",
                str(transaction_id)
            )

        self.record_timing("redis_storage", time.perf_counter() - start)

        # Step 8: MongoDB persistence.
        start = time.perf_counter()
        self.mongo_store.save_result(result)
        self.record_timing("mongodb_persistence", time.perf_counter() - start)

        # Step 9: Output serialization.
        start = time.perf_counter()
        output = json.dumps(result)
        self.record_timing("output_serialization", time.perf_counter() - start)

        total_duration = time.perf_counter() - total_start
        self.record_timing("total_processing", total_duration)

        self.timing_count += 1

        # Print one summary for every 100 processed transactions.
        if self.timing_count % 100 == 0:
            __import__('builtins').print(
                f"\n========== PERFORMANCE SUMMARY "
                f"({self.timing_count} TRANSACTIONS) =========="
            )

            for stage, total in self.timing_totals.items():
                average_ms = (total / self.timing_count) * 1000
                maximum_ms = self.timing_max[stage] * 1000

                __import__('builtins').print(
                    f"{stage}: average={average_ms:.3f} ms, "
                    f"maximum={maximum_ms:.3f} ms"
                )

            __import__('builtins').print("============================================")

        return output

    def close(self):
        if hasattr(self, "redis_client"):
            self.redis_client.close()
            print("Redis connection closed.")

        if hasattr(self, "mongo_store"):
            self.mongo_store.close()


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    # Persistent filesystem-backed checkpoints.
    flink_config = env._j_stream_execution_environment.getConfiguration()

    flink_config.setString(
        "execution.checkpointing.storage",
        "filesystem"
    )

    flink_config.setString(
        "execution.checkpointing.dir",
        os.getenv("FLINK_CHECKPOINT_DIR", "file:///home/iftiiii/flink-checkpoints")
    )

    # At-least-once recovery every 30 seconds.
    env.enable_checkpointing(
        30000,
        CheckpointingMode.AT_LEAST_ONCE
    )

    env.set_parallelism(1)

    env.add_python_file(
        os.path.join(PROJECT_DIR, "src_bundle.zip")
    )
    # Retain externalized checkpoints when the job is cancelled.
    checkpoint_config = env.get_checkpoint_config()

    checkpoint_config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )
    print(
        "CHECKPOINT RETENTION MODE:",
        checkpoint_config.get_externalized_checkpoint_retention(),
        flush=True
    )

    print(
        "CHECKPOINTING ENABLED:",
        env.get_checkpoint_config().is_checkpointing_enabled(),
        flush=True
    )

    # Retry failed jobs up to 3 times, waiting 10 seconds between attempts.
    flink_config.setString("restart-strategy.type", "fixed-delay")
    flink_config.setString("restart-strategy.fixed-delay.attempts", "3")
    flink_config.setString("restart-strategy.fixed-delay.delay", "10 s")
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .set_topics(os.getenv("KAFKA_TOPIC", "ieee_transactions"))
        .set_group_id(os.getenv("KAFKA_GROUP_ID", "ieee-fraud-inference-mongodb-v1"))
        .set_starting_offsets(
            KafkaOffsetsInitializer(
                (
                    lambda offsets: (
                        offsets.earliest()
                        if os.getenv("KAFKA_STARTING_OFFSETS", "committed").lower() == "earliest"
                        else offsets.committedOffsets()
                    )
                )(
                    __import__(
                        'pyflink.java_gateway',
                        fromlist=['get_gateway']
                    ).get_gateway().jvm
                    .org.apache.flink.connector.kafka.source.enumerator.initializer
                    .OffsetsInitializer
                )
            )
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    transactions = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "IEEE-CIS Kafka Source"
    )

    predictions = transactions.map(FraudInference())

    predictions.print()

    env.execute("IEEE-CIS Complete Fraud Detection Pipeline")


if __name__ == "__main__":
    main()
