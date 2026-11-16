
import hashlib
import json
import os

WINDOW_10M = 600
WINDOW_1H = 3600


class BehaviourEngine:

    def __init__(self, redis_client):
        self.redis = redis_client

    def process(self, message):

        metadata = message["metadata"]
        features = message["features"]

        transaction_id = str(metadata["TransactionID"])

        # Six-field anonymized card signature.
        card_fields = [
            features.get(f"card{i}")
            for i in range(1, 7)
        ]

        # No reliable history key if any card field is missing.
        if any(value is None for value in card_fields):
            return {
                "history_available": False,
                "reason": "incomplete_card_signature"
            }

        signature = json.dumps(
            card_fields,
            separators=(",", ":"),
            default=str
        )

        signature_hash = hashlib.sha256(
            signature.encode()
        ).hexdigest()

        prefix = os.getenv("REDIS_PREFIX", "fraud:ieee:")
        state_key = f"{prefix}behaviour:card:{signature_hash}"

        processed_key = f"{prefix}behaviour:processed:{transaction_id}"

        # Prevent duplicate transactions from updating history twice.
        cached = self.redis.get(processed_key)

        if cached:
            result = json.loads(cached)
            result["duplicate"] = True
            return result

        dt = float(features["TransactionDT"])
        amount = float(features["TransactionAmt"])

        raw_state = self.redis.get(state_key)

        if raw_state:
            state = json.loads(raw_state)
        else:
            state = {
                "count": 0,
                "total": 0.0,
                "last_dt": None,
                "recent": []
            }

        previous_count = state["count"]

        previous_average = (
            state["total"] / previous_count
            if previous_count else None
        )

        time_since_previous = (
            dt - state["last_dt"]
            if state["last_dt"] is not None else None
        )

        amount_ratio = (
            amount / previous_average
            if previous_average and previous_average > 0
            else None
        )

        # Keep only transactions from the previous hour.
        recent = [
            event for event in state["recent"]
            if 0 <= dt - event[0] <= WINDOW_1H
        ]

        recent_10m = [
            event for event in recent
            if dt - event[0] <= WINDOW_10M
        ]

        result = {
            "history_available": True,
            "duplicate": False,
            "previous_count": previous_count,
            "previous_average": previous_average,
            "time_since_previous": time_since_previous,
            "amount_ratio": amount_ratio,
            "recent_count_10m": len(recent_10m),
            "recent_spend_10m": sum(
                event[1] for event in recent_10m
            ),
            "recent_count_1h": len(recent),
            "recent_spend_1h": sum(
                event[1] for event in recent
            )
        }

        # Update history AFTER calculating the current features.
        recent.append([dt, amount])

        state["count"] += 1
        state["total"] += amount
        state["last_dt"] = dt
        state["recent"] = recent

        # Save state and result together in a Redis transaction.
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.set(state_key, json.dumps(state))
            pipe.set(processed_key, json.dumps(result))
            pipe.execute()

        return result
