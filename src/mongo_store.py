import json
import os

from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING


class MongoStore:

    def __init__(self):

        uri = os.getenv(
            "MONGODB_URI",
            "mongodb://localhost:27017/"
        )

        self.client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )

        # Verify the connection immediately.
        self.client.admin.command("ping")

        database = os.getenv("MONGODB_DATABASE", "fraud_detection")
        self.db = self.client[database]

        self.transactions = self.db["transactions"]
        self.alerts = self.db["alerts"]

        # Indexes for dashboard queries and filtering.
        self.transactions.create_index(
            [("risk_level", ASCENDING)]
        )

        self.transactions.create_index(
            [("replay_timestamp", ASCENDING)]
        )

        self.transactions.create_index(
            [("ground_truth", ASCENDING)]
        )

        self.alerts.create_index(
            [("processed_at", ASCENDING)]
        )

        print("MongoDB connected.")
        print("Collections and indexes initialized.")

    def save_result(self, result):

        # Convert the result into BSON-compatible basic Python values.
        document = json.loads(
            json.dumps(result, default=str)
        )

        transaction_id = str(document["TransactionID"])

        # Use TransactionID as the unique MongoDB document ID.
        document["_id"] = transaction_id

        document["processed_at"] = datetime.now(timezone.utc)

        # Idempotent upsert: repeated processing updates the same record.
        self.transactions.replace_one(
            {"_id": transaction_id},
            document,
            upsert=True
        )

        # Maintain a separate collection containing only active alerts.
        if document.get("review_recommended", False):

            self.alerts.replace_one(
                {"_id": transaction_id},
                document,
                upsert=True
            )

        else:

            self.alerts.delete_one(
                {"_id": transaction_id}
            )

        return transaction_id

    def close(self):

        self.client.close()
        print("MongoDB connection closed.")
