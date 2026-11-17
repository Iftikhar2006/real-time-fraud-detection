import json
import os
import unittest
from unittest.mock import patch

import numpy as np

from src.behaviour_engine import BehaviourEngine
from src.risk_engine import RiskEngine
from src.inference import predict_message
from src.mongo_store import MongoStore


# ---------------- FAKE REDIS ----------------

class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.operations = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def set(self, key, value):
        self.operations.append((key, value))
        return self

    def execute(self):
        for key, value in self.operations:
            self.redis.set(key, value)


class FakeRedis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def pipeline(self, transaction=True):
        return FakePipeline(self)


def transaction(tx_id, dt, amount):
    return {
        "metadata": {"TransactionID": tx_id},
        "features": {
            "card1": 2755,
            "card2": 404.0,
            "card3": 150.0,
            "card4": "mastercard",
            "card5": 102.0,
            "card6": "credit",
            "TransactionDT": dt,
            "TransactionAmt": amount
        }
    }


# ---------------- FAKE MONGODB ----------------

class FakeCollection:
    def __init__(self):
        self.documents = {}

    def replace_one(self, query, document, upsert=False):
        self.documents[query["_id"]] = document

    def delete_one(self, query):
        self.documents.pop(query["_id"], None)

    def create_index(self, *args, **kwargs):
        pass


class FakeBooster:
    def __init__(self, probability):
        self.probability = probability
        self.received_matrix = None

    def predict(self, matrix):
        self.received_matrix = matrix
        return [self.probability]


class FakeModel:
    def __init__(self, probability):
        self.booster_ = FakeBooster(probability)


# ---------------- BEHAVIOUR ENGINE TESTS ----------------

class TestBehaviourEngine(unittest.TestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.engine = BehaviourEngine(self.redis)
        self.env = patch.dict(os.environ, {"REDIS_PREFIX": "test:"})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def test_first_transaction_has_no_previous_history(self):
        result = self.engine.process(transaction("T1", 100, 100))

        self.assertTrue(result["history_available"])
        self.assertEqual(result["previous_count"], 0)
        self.assertIsNone(result["previous_average"])

    def test_previous_average_and_amount_ratio(self):
        self.engine.process(transaction("T1", 100, 100))
        result = self.engine.process(transaction("T2", 130, 400))

        self.assertEqual(result["previous_count"], 1)
        self.assertEqual(result["previous_average"], 100)
        self.assertEqual(result["amount_ratio"], 4)

    def test_recent_transaction_window(self):
        self.engine.process(transaction("T1", 100, 100))
        result = self.engine.process(transaction("T2", 130, 50))

        self.assertEqual(result["time_since_previous"], 30)
        self.assertEqual(result["recent_count_10m"], 1)
        self.assertEqual(result["recent_spend_10m"], 100)

    def test_old_transaction_removed_from_recent_window(self):
        self.engine.process(transaction("T1", 100, 100))
        result = self.engine.process(transaction("T2", 4100, 50))

        self.assertEqual(result["previous_count"], 1)
        self.assertEqual(result["recent_count_1h"], 0)

    def test_incomplete_card_signature(self):
        message = transaction("T1", 100, 100)
        message["features"]["card3"] = None

        result = self.engine.process(message)

        self.assertFalse(result["history_available"])
        self.assertEqual(result["reason"], "incomplete_card_signature")

    def test_duplicate_does_not_update_history(self):
        message = transaction("T1", 100, 100)

        self.engine.process(message)
        duplicate = self.engine.process(message)

        self.assertTrue(duplicate["duplicate"])

        state_keys = [
            key for key in self.redis.data
            if ":behaviour:card:" in key
        ]

        state = json.loads(self.redis.get(state_keys[0]))
        self.assertEqual(state["count"], 1)
        self.assertEqual(state["total"], 100)


# ---------------- RISK ENGINE TESTS ----------------

class TestRiskEngine(unittest.TestCase):

    def setUp(self):
        self.engine = RiskEngine(threshold=0.5)

    def prediction(self, probability):
        return {
            "TransactionID": "T1",
            "fraud_probability": probability,
            "ground_truth": 1
        }

    def test_high_probability_recommends_review(self):
        result = self.engine.evaluate(
            self.prediction(0.8),
            {"history_available": True}
        )

        self.assertEqual(result["predicted_fraud"], 1)
        self.assertEqual(result["risk_level"], "HIGH")
        self.assertTrue(result["review_recommended"])

    def test_low_probability_is_normal(self):
        result = self.engine.evaluate(
            self.prediction(0.2),
            {"history_available": True}
        )

        self.assertEqual(result["predicted_fraud"], 0)
        self.assertEqual(result["risk_level"], "NORMAL")
        self.assertFalse(result["review_recommended"])

    def test_threshold_boundary(self):
        result = self.engine.evaluate(
            self.prediction(0.5),
            {"history_available": True}
        )

        self.assertEqual(result["predicted_fraud"], 1)

    def test_behavioural_flags(self):
        result = self.engine.evaluate(
            self.prediction(0.3),
            {
                "history_available": True,
                "amount_ratio": 4,
                "time_since_previous": 30
            }
        )

        self.assertIn(
            "AMOUNT_3X_PREVIOUS_AVERAGE",
            result["behavioural_flags"]
        )
        self.assertIn(
            "TRANSACTION_WITHIN_60_SECONDS",
            result["behavioural_flags"]
        )

    def test_missing_history_flag(self):
        result = self.engine.evaluate(
            self.prediction(0.2),
            {"history_available": False}
        )

        self.assertIn(
            "BEHAVIOURAL_HISTORY_UNAVAILABLE",
            result["behavioural_flags"]
        )


# ---------------- INFERENCE TESTS ----------------

class TestInference(unittest.TestCase):

    def setUp(self):
        self.metadata = {
            "features": ["TransactionAmt", "ProductCD"],
            "threshold": 0.5
        }

        self.message = {
            "metadata": {
                "TransactionID": 123,
                "isFraud": 1,
                "replay_timestamp": "2026-11-17T00:00:00Z"
            },
            "features": {
                "TransactionAmt": "250.5",
                "ProductCD": "W"
            }
        }

    def test_prediction_output(self):
        model = FakeModel(0.8)

        result = predict_message(
            self.message,
            model,
            self.metadata,
            {"ProductCD": {"W": 0}}
        )

        self.assertEqual(result["TransactionID"], 123)
        self.assertEqual(result["fraud_probability"], 0.8)
        self.assertEqual(result["predicted_fraud"], 1)
        self.assertEqual(result["ground_truth"], 1)

    def test_numeric_feature_conversion(self):
        model = FakeModel(0.2)

        predict_message(
            self.message,
            model,
            self.metadata,
            {"ProductCD": {"W": 0}}
        )

        matrix = model.booster_.received_matrix

        self.assertEqual(matrix.dtype, np.float64)
        self.assertEqual(matrix[0][0], 250.5)
        self.assertEqual(matrix[0][1], 0.0)

    def test_missing_feature_raises_error(self):
        message = {
            "metadata": {"TransactionID": 1},
            "features": {"TransactionAmt": 100}
        }

        with self.assertRaises(ValueError):
            predict_message(
                message,
                FakeModel(0.2),
                self.metadata,
                {"ProductCD": {"W": 0}}
            )


# ---------------- MONGODB PERSISTENCE TESTS ----------------

class TestMongoStore(unittest.TestCase):

    def setUp(self):
        self.store = MongoStore.__new__(MongoStore)
        self.store.transactions = FakeCollection()
        self.store.alerts = FakeCollection()

    def test_normal_transaction_is_saved(self):
        result = {
            "TransactionID": 101,
            "fraud_probability": 0.1,
            "review_recommended": False
        }

        saved_id = self.store.save_result(result)

        self.assertEqual(saved_id, "101")
        self.assertIn("101", self.store.transactions.documents)
        self.assertNotIn("101", self.store.alerts.documents)

    def test_fraud_transaction_creates_alert(self):
        result = {
            "TransactionID": 102,
            "fraud_probability": 0.9,
            "review_recommended": True
        }

        self.store.save_result(result)

        self.assertIn("102", self.store.transactions.documents)
        self.assertIn("102", self.store.alerts.documents)

    def test_normal_update_removes_old_alert(self):
        self.store.alerts.documents["103"] = {"_id": "103"}

        self.store.save_result({
            "TransactionID": 103,
            "fraud_probability": 0.1,
            "review_recommended": False
        })

        self.assertNotIn("103", self.store.alerts.documents)

    def test_repeated_save_uses_same_document_id(self):
        result = {
            "TransactionID": 104,
            "fraud_probability": 0.9,
            "review_recommended": True
        }

        self.store.save_result(result)
        self.store.save_result(result)

        self.assertEqual(len(self.store.transactions.documents), 1)
        self.assertEqual(len(self.store.alerts.documents), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
