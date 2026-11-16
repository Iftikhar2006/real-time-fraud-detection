import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction


class ParseTransaction(MapFunction):
    def map(self, value):
        try:
            transaction = json.loads(value)
            return json.dumps({
                "transaction_id": transaction.get("transaction_id"),
                "user_id": transaction.get("user_id"),
                "amount": transaction.get("amount"),
                "billing_region": transaction.get("billing_region"),
                "device_info": transaction.get("device_info"),
            })
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "Invalid transaction JSON"})


env = StreamExecutionEnvironment.get_execution_environment()

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("localhost:9092")
    .set_topics("transactions")
    .set_group_id("flink-test1")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

transactions = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Transactions"
)

transactions.map(ParseTransaction()).print()

env.execute("Kafka to Flink Test 1")
