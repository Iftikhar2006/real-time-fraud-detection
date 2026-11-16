import json
import joblib
import numpy as np
import pandas as pd


MODEL_PATH = "models/lightgbm_fraud_model.joblib"
METADATA_PATH = "models/model_metadata.json"


def load_predictor():
    """Load model and precompute categorical encodings."""

    model = joblib.load(MODEL_PATH)

    with open(METADATA_PATH) as f:
        metadata = json.load(f)

    categories = dict(zip(
        metadata["categorical_features"],
        model.booster_.pandas_categorical
    ))

    # Precompute category-to-integer mappings once,
    # rather than reconstructing pandas objects per transaction.
    category_maps = {
        column: {
            value: index
            for index, value in enumerate(categories[column])
        }
        for column in metadata["categorical_features"]
    }

    return model, metadata, category_maps


def predict_message(message, model, metadata, categories):
    """Predict fraud using a NumPy feature matrix."""

    features = metadata["features"]
    received = message["features"]

    missing = set(features) - set(received)
    if missing:
        raise ValueError(
            f"Missing model features: {sorted(missing)}"
        )

    values = []

    for feature in features:
        value = received[feature]

        if feature in categories:
            if value is None or pd.isna(value):
                values.append(np.nan)
            else:
                code = categories[feature].get(value, -1)
                values.append(
                    float(code) if code >= 0 else np.nan
                )
        else:
            converted = pd.to_numeric(
                value,
                errors="coerce"
            )
            values.append(
                np.nan if pd.isna(converted) else float(converted)
            )

    # Preserve the exact feature order from model metadata.
    matrix = np.asarray([values], dtype=np.float64)

    probability = float(
        model.booster_.predict(matrix)[0]
    )

    threshold = metadata["threshold"]

    return {
        "TransactionID": message["metadata"]["TransactionID"],
        "fraud_probability": probability,
        "predicted_fraud": int(probability >= threshold),
        "ground_truth": message["metadata"].get("isFraud"),
        "replay_timestamp": message["metadata"].get("replay_timestamp")
    }
