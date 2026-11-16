
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
            return json.dumps(transaction, indent=2)
        except (json.JSONDecodeError, TypeError):
            return "Invalid transaction JSON: " + str(value)


env = StreamExecutionEnvironment.get_execution_environment()

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("127.0.0.1:9092")
    .set_topics("transactions")
    .set_group_id("flink-simple-test")
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

parsed_transactions.print()

env.execute("Kafka Flink Connectivity Test")
