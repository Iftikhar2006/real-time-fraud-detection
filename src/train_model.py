import json
import gc
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
)

from data_loader import load_data


MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

RANDOM_STATE = 42


def main():
    print("\n===== LOADING DATA =====")

    data, categorical_features = load_data()

    y = data.pop("isFraud")
    data.drop(columns=["TransactionID"], inplace=True)

    X = data
    feature_names = X.columns.tolist()

    print("\n===== TRAIN / VALIDATION SPLIT =====")

    train_idx, valid_idx = train_test_split(
        np.arange(len(y)),
        test_size=0.20,
        random_state=RANDOM_STATE,
        stratify=y
    )

    X_train = X.iloc[train_idx]
    X_valid = X.iloc[valid_idx]
    y_train = y.iloc[train_idx]
    y_valid = y.iloc[valid_idx]

    del X, y, data, train_idx, valid_idx
    gc.collect()

    scale_pos_weight = (
        (y_train == 0).sum() / (y_train == 1).sum()
    )

    print(f"Training rows: {len(y_train)}")
    print(f"Validation rows: {len(y_valid)}")
    print(f"Positive-class weight: {scale_pos_weight:.2f}")

    print("\n===== TRAINING LIGHTGBM =====")

    model = lgb.LGBMClassifier(
        objective="binary",
        metric="None",
        n_estimators=2000,
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        scale_pos_weight=scale_pos_weight,
        random_state=RANDOM_STATE,
        n_jobs=4,
        verbosity=-1
    )

    model.fit(
        X_train,
        y_train,
        categorical_feature=categorical_features,
        eval_set=[(X_valid, y_valid)],
        eval_metric="average_precision",
        callbacks=[
            lgb.early_stopping(100),
            lgb.log_evaluation(100)
        ]
    )

    print("\n===== EVALUATION =====")

    probabilities = model.predict_proba(X_valid)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)

    print(f"Best iteration: {model.best_iteration_}")
    print(f"PR-AUC: {average_precision_score(y_valid, probabilities):.5f}")
    print(f"ROC-AUC: {roc_auc_score(y_valid, probabilities):.5f}")
    print(f"Precision: {precision_score(y_valid, predictions, zero_division=0):.5f}")
    print(f"Recall: {recall_score(y_valid, predictions, zero_division=0):.5f}")
    print(f"F1-score: {f1_score(y_valid, predictions, zero_division=0):.5f}")

    print("\nConfusion matrix:")
    print(confusion_matrix(y_valid, predictions))

    print("\nClassification report:")
    print(classification_report(y_valid, predictions, zero_division=0))

    print("\n===== TOP 20 FEATURES =====")

    importance = sorted(
        zip(feature_names, model.feature_importances_),
        key=lambda item: item[1],
        reverse=True
    )

    for name, score in importance[:20]:
        print(f"{name:20s} {score}")

    print("\n===== SAVING MODEL =====")

    joblib.dump(model, MODEL_DIR / "lightgbm_fraud_model.joblib")

    metadata = {
        "model": "LightGBM",
        "target": "isFraud",
        "features": feature_names,
        "categorical_features": categorical_features,
        "threshold": 0.5,
        "random_state": RANDOM_STATE,
        "best_iteration": model.best_iteration_,
        "validation_pr_auc": float(
            average_precision_score(y_valid, probabilities)
        )
    }

    with open(MODEL_DIR / "model_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("Model saved: models/lightgbm_fraud_model.joblib")
    print("Metadata saved: models/model_metadata.json")
    print("\n===== TRAINING COMPLETE =====")


if __name__ == "__main__":
    main()
