
import redis
import json
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction


class ParseTransaction(MapFunction):
    def map(self, value):
        try:
            transaction = json.loads(value)

            return {
                "transaction_id": transaction.get("transaction_id"),
                "timestamp": transaction.get("timestamp"),
                "user_id": transaction.get("user_id"),
                "amount": transaction.get("amount"),
                "card_id": transaction.get("card_id"),
                "card_type": transaction.get("card_type"),
                "card_category": transaction.get("card_category"),
                "billing_region": transaction.get("billing_region"),
                "billing_country": transaction.get("billing_country"),
                "device_type": transaction.get("device_type"),
                "device_info": transaction.get("device_info"),
                "transaction_hour": transaction.get("transaction_hour"),
                "transaction_day": transaction.get("transaction_day"),
            }
        except (json.JSONDecodeError, TypeError):
            return {"error": "Invalid transaction JSON"}


class UserStatistics(KeyedProcessFunction):
    def open(self, runtime_context):
        self.redis_client = redis.Redis(
            host="127.0.0.1",
            port=6379,
            decode_responses=True
        )
        self.redis_client.ping()

    def process_element(self, transaction, ctx):
        user_id = transaction["user_id"]
        amount = float(transaction.get("amount") or 0.0)
        timestamp = transaction.get("timestamp")
        key = f"fraud:user:{user_id}"

        previous = self.redis_client.hgetall(key)

        count = int(previous.get("count", 0))
        total = float(previous.get("total_amount", 0.0))
        previous_average = total / count if count > 0 else 0.0

        previous_amount = previous.get("last_amount")
        previous_timestamp = previous.get("last_timestamp")
        previous_device = previous.get("last_device")
        previous_region = previous.get("last_region")

        time_since_previous = None
        if timestamp and previous_timestamp:
            try:
                current_time = datetime.fromisoformat(timestamp)
                last_time = datetime.fromisoformat(previous_timestamp)
                time_since_previous = (
                    current_time - last_time
                ).total_seconds()
            except (ValueError, TypeError):
                pass

        amount_deviation = (
            amount - previous_average
            if count > 0 else None
        )

        device_changed = (
            previous_device is not None
            and transaction.get("device_info") != previous_device
        )

        location_changed = (
            previous_region is not None
            and transaction.get("billing_region") != previous_region
        )

        count += 1
        total += amount
        average = total / count

        self.redis_client.hset(
            key,
            mapping={
                "count": count,
                "total_amount": total,
                "last_amount": amount,
                "last_timestamp": timestamp or "",
                "last_device": transaction.get("device_info") or "",
                "last_region": transaction.get("billing_region") or "",
            }
        )

        result = dict(transaction)
        result["user_transaction_count"] = count
        result["user_average_amount"] = round(average, 2)
        result["previous_average_amount"] = round(previous_average, 2)
        result["previous_transaction_amount"] = (
            float(previous_amount) if previous_amount is not None else None
        )
        result["time_since_previous_seconds"] = time_since_previous
        result["amount_deviation"] = (
            round(amount_deviation, 2)
            if amount_deviation is not None else None
        )
        result["device_changed"] = device_changed
        result["location_changed"] = location_changed

        yield result


env = StreamExecutionEnvironment.get_execution_environment()

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("127.0.0.1:9092")
    .set_topics("transactions")
    .set_group_id("flink-fraud-test")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

transactions = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Transactions",
)

parsed_transactions = transactions.map(ParseTransaction())

user_statistics = (
    parsed_transactions
    .filter(
        lambda transaction:
        "user_id" in transaction and transaction.get("amount") is not None
    )
    .key_by(lambda transaction: transaction["user_id"])
    .process(UserStatistics())
)

user_statistics.print()

env.execute("User Transaction Statistics")
