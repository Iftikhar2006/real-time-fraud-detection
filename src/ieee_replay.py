import argparse
import json
import time
from datetime import datetime, timezone

import joblib
import pandas as pd
from kafka import KafkaProducer

from data_loader import load_data


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ieee_transactions"
METADATA_PATH = "models/model_metadata.json"


def json_safe(value):
    """Convert pandas/numpy values into JSON-compatible values."""
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.limit <= 0 or args.interval < 0:
        raise ValueError("Limit must be positive and interval non-negative.")

    print("Loading model metadata...")

    with open(METADATA_PATH) as f:
        metadata = json.load(f)

    features = metadata["features"]

    print("Loading IEEE-CIS dataset...")
    data, _ = load_data()

    print("Preparing replay records...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all"
    )

    try:
        count = min(args.limit, len(data))

        for index in range(count):
            row = data.iloc[index]

            feature_values = {
                feature: json_safe(row[feature])
                for feature in features
            }

            message = {
                "metadata": {
                    "TransactionID": int(row["TransactionID"]),
                    "isFraud": int(row["isFraud"]),
                    "replay_timestamp": datetime.now(
                        timezone.utc
                    ).isoformat()
                },
                "features": feature_values
            }

            producer.send(TOPIC, message)

            print(
                f"Sent TransactionID={row['TransactionID']} "
                f"| Ground truth={row['isFraud']}"
            )

            if index < count - 1:
                time.sleep(args.interval)

        producer.flush()
        print(f"\nReplay completed: {count} records sent.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
