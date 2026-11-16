from pathlib import Path
import pandas as pd

DATA_DIR = Path(
    "/mnt/c/Users/Iftikhar alam/Downloads/sem5/DEPROJECT/ieee-fraud-detection"
)

TARGET = "isFraud"

# Identity fields with comparatively useful coverage.
IDENTITY_FEATURES = (
    [f"id_{i:02d}" for i in range(1, 7)]
    + [f"id_{i:02d}" for i in range(9, 21)]
    + [f"id_{i:02d}" for i in range(28, 39)]
    + ["DeviceType", "DeviceInfo"]
)

CATEGORICAL_FEATURES = [
    "ProductCD",
    *[f"card{i}" for i in range(1, 7)],
    "addr1", "addr2",
    "P_emaildomain", "R_emaildomain",
    *[f"M{i}" for i in range(1, 10)],
    "DeviceType", "DeviceInfo",
    "id_12", "id_15", "id_16",
    "id_28", "id_29", "id_30", "id_31",
    "id_33", "id_34", "id_35", "id_36", "id_37", "id_38",
]


def load_data():
    transaction_path = DATA_DIR / "train_transaction.csv"
    identity_path = DATA_DIR / "train_identity.csv"

    if not transaction_path.exists() or not identity_path.exists():
        raise FileNotFoundError("IEEE-CIS training CSV files not found.")

    print("Reading transaction columns...")

    transaction_columns = pd.read_csv(
        transaction_path, nrows=0
    ).columns.tolist()

    selected_transaction = [
        col for col in transaction_columns
        if col in {
            "TransactionID", TARGET, "TransactionDT", "TransactionAmt",
            "ProductCD", "dist1", "dist2",
            "addr1", "addr2", "P_emaildomain", "R_emaildomain",
        }
        or col.startswith(("card", "C", "D", "M", "V"))
    ]

    print(f"Loading {len(selected_transaction)} transaction columns...")

    transactions = pd.read_csv(
        transaction_path,
        usecols=selected_transaction,
        low_memory=False
    )

    identity_columns = pd.read_csv(
        identity_path, nrows=0
    ).columns.tolist()

    selected_identity = [
        col for col in IDENTITY_FEATURES
        if col in identity_columns
    ]

    print(f"Loading {len(selected_identity)} identity columns...")

    identity = pd.read_csv(
        identity_path,
        usecols=["TransactionID"] + selected_identity,
        low_memory=False
    )

    print("Joining transaction and identity data...")

    data = transactions.merge(
        identity,
        on="TransactionID",
        how="left",
        validate="one_to_one"
    )

    # Remove columns that contain no useful variation.
    feature_columns = [
        col for col in data.columns
        if col not in ["TransactionID", TARGET]
    ]

    constant_columns = [
        col for col in feature_columns
        if data[col].nunique(dropna=True) <= 1
    ]

    if constant_columns:
        print("Removing constant columns:", constant_columns)
        data.drop(columns=constant_columns, inplace=True)

    categorical = [
        col for col in CATEGORICAL_FEATURES
        if col in data.columns
    ]

    for col in categorical:
        data[col] = data[col].astype("category")

    print(f"Final dataset shape: {data.shape}")
    print(f"Fraud transactions: {int(data[TARGET].sum())}")
    print(f"Categorical features: {len(categorical)}")

    return data, categorical
