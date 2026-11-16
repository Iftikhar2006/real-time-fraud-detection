import os

import streamlit as st
from pymongo import MongoClient, DESCENDING


# Fixed database mapping: do not replace with a UI-controlled selector.
DATABASES = {
    "demo": "fraud_detection_demo",
    "historical": "fraud_detection",
}


@st.cache_resource
def get_mongo_client():
    uri = os.getenv(
        "MONGODB_URI",
        "mongodb://localhost:27017/"
    )

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=3000
    )

    client.admin.command("ping")
    return client


def get_database(environment):
    if environment not in DATABASES:
        raise ValueError(
            "Environment must be 'demo' or 'historical'."
        )

    client = get_mongo_client()
    return client[DATABASES[environment]]


@st.cache_data(ttl=5)
def get_summary(environment):
    db = get_database(environment)

    total = db.transactions.count_documents({})
    predicted_fraud = db.transactions.count_documents(
        {"predicted_fraud": 1}
    )
    actual_fraud = db.transactions.count_documents(
        {"ground_truth": 1}
    )
    high_risk = db.transactions.count_documents(
        {"risk_level": "HIGH"}
    )
    alerts = db.alerts.count_documents({})

    return {
        "total": total,
        "predicted_fraud": predicted_fraud,
        "actual_fraud": actual_fraud,
        "high_risk": high_risk,
        "alerts": alerts,
        "normal": total - high_risk,
    }


@st.cache_data(ttl=5)
def get_transactions(environment, risk_filter="ALL", limit=100):
    db = get_database(environment)

    query = {}

    if risk_filter != "ALL":
        query["risk_level"] = risk_filter

    return list(
        db.transactions.find(
            query,
            {
                "_id": 0,
                "TransactionID": 1,
                "fraud_probability": 1,
                "predicted_fraud": 1,
                "ground_truth": 1,
                "risk_level": 1,
                "review_recommended": 1,
                "processed_at": 1,
                "replay_timestamp": 1,
                "behavioural_flags": 1,
                "behaviour": 1,
            }
        )
        .sort("processed_at", DESCENDING)
        .limit(limit)
    )


@st.cache_data(ttl=5)
def get_alerts(environment, limit=100):
    db = get_database(environment)

    return list(
        db.alerts.find(
            {},
            {
                "_id": 0,
                "TransactionID": 1,
                "fraud_probability": 1,
                "risk_level": 1,
                "ground_truth": 1,
                "processed_at": 1,
                "replay_timestamp": 1,
                "behavioural_flags": 1,
                "behaviour": 1,
            }
        )
        .sort("processed_at", DESCENDING)
        .limit(limit)
    )
