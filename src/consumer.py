import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="fraud-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)


print("Waiting for transactions...\n")

for message in consumer:

    transaction = message.value

    print("Received:")
    print(transaction)
    print("-" * 80)