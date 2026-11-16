import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

from data_loader import DATA_DIR


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ieee_transactions")
METADATA_PATH = "models/model_metadata.json"


def json_safe(value):
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def load_selected_data(start_index, limit, features, chunk_size):
    transaction_path = DATA_DIR / "train_transaction.csv"
    identity_path = DATA_DIR / "train_identity.csv"

    if not transaction_path.exists() or not identity_path.exists():
        raise FileNotFoundError("IEEE-CIS training CSV files not found.")

    print("Inspecting CSV headers...", flush=True)

    transaction_columns = pd.read_csv(
        transaction_path, nrows=0
    ).columns.tolist()

    identity_columns = pd.read_csv(
        identity_path, nrows=0
    ).columns.tolist()

    transaction_features = [
        feature for feature in features
        if feature in transaction_columns
    ]

    identity_features = [
        feature for feature in features
        if feature in identity_columns
    ]

    available_features = set(
        transaction_features + identity_features
    )

    missing_features = set(features) - available_features

    if missing_features:
        raise ValueError(
            f"Features missing from both CSV files: "
            f"{sorted(missing_features)}"
        )

    transaction_usecols = list(dict.fromkeys(
        ["TransactionID", "isFraud"] + transaction_features
    ))

    identity_usecols = list(dict.fromkeys(
        ["TransactionID"] + identity_features
    ))

    print(
        f"Reading transaction data in chunks of {chunk_size} rows...",
        flush=True
    )

    selected_parts = []
    current_index = 0
    target_end = start_index + limit

    for chunk in pd.read_csv(
        transaction_path,
        usecols=transaction_usecols,
        chunksize=chunk_size,
        low_memory=False
    ):
        chunk_start = current_index
        chunk_end = chunk_start + len(chunk)

        if chunk_end > start_index and chunk_start < target_end:
            local_start = max(0, start_index - chunk_start)
            local_end = min(len(chunk), target_end - chunk_start)

            selected_parts.append(
                chunk.iloc[local_start:local_end].copy()
            )

        current_index = chunk_end

        if current_index >= target_end:
            break

    if not selected_parts:
        raise ValueError(
            f"No transaction rows found from index {start_index}."
        )

    selected = pd.concat(selected_parts, ignore_index=True)

    print(
        f"Selected {len(selected)} transaction rows. "
        f"Scanning identity data...",
        flush=True
    )

    selected_ids = set(selected["TransactionID"].tolist())
    identity_parts = []

    for chunk in pd.read_csv(
        identity_path,
        usecols=identity_usecols,
        chunksize=chunk_size,
        low_memory=False
    ):
        matching = chunk[
            chunk["TransactionID"].isin(selected_ids)
        ]

        if not matching.empty:
            identity_parts.append(matching)

    if identity_parts:
        identity = pd.concat(identity_parts, ignore_index=True)
    else:
        identity = pd.DataFrame(columns=identity_usecols)
        identity["TransactionID"] = pd.Series(
            dtype=selected["TransactionID"].dtype
        )

    if identity["TransactionID"].duplicated().any():
        raise ValueError(
            "Duplicate TransactionID values found in identity data."
        )

    print("Joining selected transaction and identity rows...", flush=True)

    selected = selected.merge(
        identity,
        on="TransactionID",
        how="left",
        validate="one_to_one",
        sort=False
    )

    missing_after_join = set(features) - set(selected.columns)

    if missing_after_join:
        raise ValueError(
            f"Model features missing after join: "
            f"{sorted(missing_after_join)}"
        )

    return selected


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start-index", type=int, required=True)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--chunk-size", type=int, default=10000)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and validate records without sending to Kafka."
    )

    args = parser.parse_args()

    if (
        args.start_index < 0
        or args.limit <= 0
        or args.interval < 0
        or args.chunk_size <= 0
    ):
        raise ValueError("Invalid replay arguments.")

    print("Loading model metadata...", flush=True)

    with open(METADATA_PATH) as f:
        metadata = json.load(f)

    features = metadata["features"]

    print("Loading only the requested dataset range...", flush=True)

    selected = load_selected_data(
        args.start_index,
        args.limit,
        features,
        args.chunk_size
    )

    fraud_count = int(selected["isFraud"].sum())

    print("\nReplay summary:")
    print("Start index:", args.start_index)
    print("End index:", args.start_index + len(selected) - 1)
    print("Total records:", len(selected))
    print("Fraud records:", fraud_count)
    print("Legitimate records:", len(selected) - fraud_count)
    print("Model features per record:", len(features))
    print("Dry run:", args.dry_run)

    if args.dry_run:
        print("\nDRY RUN PASSED. No records were sent to Kafka.")
        return

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all"
    )

    try:
        for position, (_, row) in enumerate(selected.iterrows()):
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
                f"Sent {position + 1}/{len(selected)} "
                f"| ID={int(row['TransactionID'])} "
                f"| Fraud={int(row['isFraud'])}",
                flush=True
            )

            if position < len(selected) - 1:
                time.sleep(args.interval)

        producer.flush()
        print("\nReplay completed successfully.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
