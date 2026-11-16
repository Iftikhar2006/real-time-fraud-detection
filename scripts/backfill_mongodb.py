import json
import redis
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.mongo_store import MongoStore

r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)

store = MongoStore()

try:
    transaction_ids = sorted(
        r.smembers("fraud:ieee:transaction_ids"),
        key=int
    )

    print("Redis transactions found:", len(transaction_ids))

    migrated = 0
    missing = []

    for transaction_id in transaction_ids:

        raw = r.get(
            f"fraud:ieee:transaction:{transaction_id}"
        )

        if raw is None:
            missing.append(transaction_id)
            continue

        result = json.loads(raw)

        store.save_result(result)
        migrated += 1

        if migrated % 50 == 0:
            print("Migrated:", migrated)

    print("\n========== BACKFILL SUMMARY ==========")
    print("Migrated:", migrated)
    print("Missing Redis records:", len(missing))

    if missing:
        print("Missing IDs:", missing[:10])

    print(
        "MongoDB transactions:",
        store.transactions.count_documents({})
    )

    print(
        "MongoDB alerts:",
        store.alerts.count_documents({})
    )

    print(
        "MongoDB HIGH-risk transactions:",
        store.transactions.count_documents(
            {"risk_level": "HIGH"}
        )
    )

finally:
    store.close()
