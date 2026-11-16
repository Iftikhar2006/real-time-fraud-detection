import json
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.utils.database import get_summary, get_transactions


st.set_page_config(
    page_title="Historical Analytics",
    page_icon="📊",
    layout="wide",
)

PROJECT_DIR = Path(__file__).resolve().parents[2]


def load_json(relative_path):
    path = PROJECT_DIR / relative_path
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


st.title("📊 Historical Fraud Analytics")
st.caption(
    "From historical transaction patterns to machine-learning "
    "predictions and behavioural risk signals."
)

st.info(
    "Historical environment: fraud_detection. "
    "This page does not read from the live demo database."
)

if st.button("Refresh Historical Analytics"):
    st.cache_data.clear()
    st.rerun()


# --------------------------------------------------
# LOAD SAVED ARTIFACTS AND HISTORICAL DATABASE
# --------------------------------------------------

try:
    summary = get_summary("historical")
    transactions = get_transactions("historical", limit=100)

except Exception as error:
    st.error(f"Could not read historical data: {error}")
    st.stop()

try:
    behaviour_summary = load_json("results/behaviour_summary.json")
    model_metadata = load_json("models/model_metadata.json")

except Exception as error:
    st.error(f"Could not load saved analysis metadata: {error}")
    st.stop()


# --------------------------------------------------
# 1. DATASET ORIGIN
# --------------------------------------------------

st.header("1. Understanding the Dataset")

st.markdown(
    """
    The project uses the **IEEE-CIS Fraud Detection** dataset, a
    transaction-fraud benchmark containing transaction information
    and corresponding fraud labels.

    The main transaction file is `train_transaction.csv`.
    The behavioural analysis reads its transaction timestamp,
    transaction amount, fraud label, and six card-related fields.

    The target variable is `isFraud`:
    - `0` represents a legitimate transaction.
    - `1` represents a fraudulent transaction.

    The behavioural analysis uses the six card fields to construct
    anonymized transaction-history signatures.
    """
)

st.caption(
    "The behavioural analysis reads train_transaction.csv directly. "
    "Its statistics below are from that analysis, not from the "
    "historical MongoDB snapshot."
)

st.divider()


# --------------------------------------------------
# 2. HISTORICAL DATABASE SNAPSHOT
# --------------------------------------------------

st.header("2. Historical Database Snapshot")

st.markdown(
    """
    These metrics describe the records currently stored in the
    project's historical MongoDB database. They are not the total
    size of the original IEEE-CIS dataset.
    """
)

c1, c2, c3 = st.columns(3)
c1.metric("Stored Transactions", f"{summary['total']:,}")
c2.metric("Model-Predicted Fraud", f"{summary['predicted_fraud']:,}")
c3.metric("Dataset-Labelled Fraud", f"{summary['actual_fraud']:,}")

c4, c5, c6 = st.columns(3)
c4.metric("High-Risk Transactions", f"{summary['high_risk']:,}")
c5.metric("Normal Transactions", f"{summary['normal']:,}")
c6.metric("Historical Alerts", f"{summary['alerts']:,}")

st.warning(
    "Model-predicted fraud and dataset-labelled fraud are two "
    "different measurements. Their aggregate counts alone do not "
    "tell us how many predictions were correct."
)

st.divider()


# --------------------------------------------------
# 3. MODEL METHODOLOGY
# --------------------------------------------------

st.header("3. Machine-Learning Model")

st.markdown(
    """
    The project uses **LightGBM**, a gradient-boosted decision-tree
    model for binary fraud classification.

    The model produces a fraud probability. The risk engine applies
    the configured threshold to convert that probability into a
    binary prediction.
    """
)

m1, m2, m3 = st.columns(3)
m1.metric("Model", model_metadata["model"])
m2.metric("Input Features", f"{len(model_metadata['features']):,}")
m3.metric(
    "Categorical Features",
    f"{len(model_metadata['categorical_features']):,}",
)

m4, m5, m6 = st.columns(3)
m4.metric("Decision Threshold", str(model_metadata["threshold"]))
m5.metric("Best Iteration", f"{model_metadata['best_iteration']:,}")
m6.metric(
    "Validation PR-AUC",
    f"{model_metadata['validation_pr_auc']:.4f}",
)

with st.expander("How was the model trained?"):
    st.markdown(
        """
        The training script uses:

        - Stratified 80:20 train-validation split.
        - Random state 42 for reproducibility.
        - LightGBM binary classification.
        - Class weighting to account for class imbalance.
        - Early stopping based on validation average precision.
        - A 0.5 threshold for the reported binary predictions.

        The saved validation PR-AUC measures ranking performance
        on the validation split. It is not a measurement of accuracy
        on the current historical MongoDB records.
        """
    )

st.divider()


# --------------------------------------------------
# --------------------------------------------------
# 4. FINAL FRAUD DECISION AND ALERT GENERATION
# --------------------------------------------------

st.header("4. How the Final Fraud Decision Is Made")

st.markdown(
    """
    Every incoming transaction passes through machine-learning
    inference and behavioural analysis. The RiskEngine then creates
    a final classification and determines whether the transaction
    should be sent for review.
    """
)

threshold = float(model_metadata["threshold"])

st.subheader("A. Machine-learning decision rule")

st.markdown(
    """
    LightGBM produces a fraud probability between 0 and 1.
    The RiskEngine compares this probability with the configured
    threshold. The current threshold is 0.5.
    """
)

c1, c2 = st.columns(2)
c1.metric("Configured Threshold", f"{threshold:.2f}")
c2.metric("Model", model_metadata["model"])

decision_col1, decision_col2 = st.columns(2)

with decision_col1:
    st.success(
        f"HIGH RISK: probability ≥ {threshold:.2f}\n\n"
        "predicted_fraud = 1\n\n"
        "review_recommended = True"
    )

with decision_col2:
    st.info(
        f"NORMAL: probability < {threshold:.2f}\n\n"
        "predicted_fraud = 0\n\n"
        "review_recommended = False"
    )

st.caption(
    "The threshold converts a probability into a binary decision. "
    "It is a configured operating point, not a guarantee that every "
    "HIGH transaction is fraudulent."
)

st.divider()

st.subheader("B. Behavioural rules and why they matter")

st.markdown(
    """
    The BehaviourEngine retrieves previous activity using a
    six-field card signature. The signature is hashed with SHA-256
    before it is used as a Redis key. This allows the system to
    associate transactions with prior card activity without using
    the raw signature as the Redis key.
    """
)

rules = [
    {
        "Rule": "Amount at least 3× previous average",
        "Condition": "amount_ratio >= 3",
        "Flag": "AMOUNT_3X_PREVIOUS_AVERAGE",
        "Purpose": "Highlights a sudden increase compared with the card's previous average transaction amount.",
    },
    {
        "Rule": "Previous transaction within 60 seconds",
        "Condition": "0 <= time_since_previous <= 60",
        "Flag": "TRANSACTION_WITHIN_60_SECONDS",
        "Purpose": "Highlights unusually rapid successive transactions, which may warrant investigation.",
    },
    {
        "Rule": "Incomplete card signature",
        "Condition": "One or more of card1–card6 is missing",
        "Flag": "BEHAVIOURAL_HISTORY_UNAVAILABLE",
        "Purpose": "Warns that reliable card-specific history cannot be retrieved.",
    },
]

st.dataframe(
    rules,
    use_container_width=True,
    hide_index=True,
)

st.warning(
    "These behavioural conditions produce explanatory flags. "
    "They do not currently add points to the model probability "
    "or independently change the HIGH/NORMAL classification."
)

with st.expander("Other behavioural features calculated by the engine"):
    st.markdown(
        """
        The BehaviourEngine also calculates:

        - Previous transaction count.
        - Previous average transaction amount.
        - Recent transaction count over 10 minutes and one hour.
        - Recent spending over 10 minutes and one hour.

        These features describe transaction history, but the current
        RiskEngine does not use them to generate additional flags.
        """
    )

st.divider()

st.subheader("C. How the model and behavioural signals work together")

st.markdown(
    """
    The two components have different responsibilities:

    - **LightGBM:** determines the fraud probability and the
      threshold-based HIGH/NORMAL prediction.
    - **BehaviourEngine:** provides historical context and
      behavioural flags.
    - **RiskEngine:** combines these outputs into one transaction
      result. Behavioural flags explain the circumstances but do
      not override the model classification.
    """
)

interaction = [
    {
        "Model probability": "0.82",
        "Behavioural flags": "None",
        "Risk level": "HIGH",
        "Review recommended": "Yes",
        "Explanation": "Model probability crosses the threshold.",
    },
    {
        "Model probability": "0.72",
        "Behavioural flags": "Transaction within 60 seconds",
        "Risk level": "HIGH",
        "Review recommended": "Yes",
        "Explanation": "Model triggers the alert; behaviour adds context.",
    },
    {
        "Model probability": "0.31",
        "Behavioural flags": "Amount 3× previous average",
        "Risk level": "NORMAL",
        "Review recommended": "No",
        "Explanation": "Behavioural flag does not override the model.",
    },
    {
        "Model probability": "0.18",
        "Behavioural flags": "History unavailable",
        "Risk level": "NORMAL",
        "Review recommended": "No",
        "Explanation": "Missing history is a warning, not a fraud prediction.",
    },
]

st.dataframe(
    interaction,
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Illustrative examples showing the implemented decision logic; "
    "these are not additional measured transactions."
)

st.divider()

st.subheader("D. From a decision to a stored fraud alert")

st.code(
    """
Incoming transaction
        |
        v
LightGBM inference
        |
        +--> Fraud probability
        |
        v
Redis-backed BehaviourEngine
        |
        +--> Previous card activity
        +--> Behavioural features and flags
        |
        v
RiskEngine
        |
        +--> Apply probability threshold
        +--> Set HIGH or NORMAL
        +--> Set review_recommended
        |
        v
Redis
        |
        +--> Store every transaction
        +--> Maintain alert key and alert ID set
        |
        v
MongoDB
        |
        +--> transactions: every processed transaction
        +--> alerts: transactions recommended for review
""",
    language="text",
)

st.markdown(
    """
    MongoDB maintains two collections:

    - **transactions:** stores every processed transaction.
    - **alerts:** stores only transactions where
      `review_recommended` is true.

    If a transaction is subsequently processed as NORMAL, its
    previous alert entry is removed. Transaction IDs are used for
    idempotent updates, avoiding duplicate MongoDB documents.
    """
)

st.info(
    "The current implementation does not calculate a separate "
    "weighted risk score. The fraud probability comes from LightGBM; "
    "HIGH/NORMAL is its threshold-based classification; behavioural "
    "flags provide additional explanations."
)

st.divider()


# 5. HISTORICAL BEHAVIOURAL ANALYSIS
# --------------------------------------------------

st.header("5. Historical Behavioural Analysis")

label_counts = behaviour_summary["transactions"]
eligible_counts = behaviour_summary["eligible_complete_signatures"]

total_analysed = sum(label_counts.values())
total_eligible = sum(eligible_counts.values())
incomplete = behaviour_summary["incomplete_signatures"]

b1, b2, b3 = st.columns(3)
b1.metric("Transactions Analysed", f"{total_analysed:,}")
b2.metric("Complete Signatures", f"{total_eligible:,}")
b3.metric("Incomplete Signatures", f"{incomplete:,}")

st.markdown(
    f"""
    The offline behavioural analysis processed **{total_analysed:,}**
    transactions chronologically. It maintained transaction history
    for complete six-field card signatures.

    **{total_eligible:,}** transactions had complete signatures and
    **{incomplete:,}** had at least one missing card field.
    The analysis tracked **{behaviour_summary['unique_signatures']:,}**
    unique complete signatures.
    """
)

class_df = pd.DataFrame(
    {
        "Transactions": [
            label_counts["0"],
            label_counts["1"],
        ]
    },
    index=["Legitimate", "Fraud"],
)

st.subheader("Transaction labels in the behavioural analysis")
st.bar_chart(class_df, y="Transactions")

st.caption(
    "These are the class counts from the earlier offline analysis, "
    "not the current MongoDB transaction counts."
)

st.divider()


# --------------------------------------------------
# 6. BEHAVIOURAL FEATURE COMPARISONS
# --------------------------------------------------

st.header("6. How Transaction Behaviour Differs")

st.markdown(
    """
    Behavioural features describe how a transaction compares with
    the earlier activity associated with its card signature.

    The following charts compare selected quantiles for legitimate
    and fraudulent transactions. They are descriptive comparisons,
    not standalone fraud classifiers.
    """
)

quantiles = behaviour_summary["quantiles"]

feature_charts = [
    (
        "time_since_previous",
        "Time Since Previous Transaction",
        "Seconds",
        ["0.5", "0.75", "0.9", "0.95", "0.99"],
    ),
    (
        "amount_ratio",
        "Transaction Amount / Previous Average",
        "Ratio",
        ["0.5", "0.75", "0.9", "0.95", "0.99"],
    ),
    (
        "recent_spend_10m",
        "Recent Spending in Previous 10 Minutes",
        "Dataset amount units",
        ["0.5", "0.75", "0.9", "0.95", "0.99"],
    ),
    (
        "previous_count",
        "Number of Previous Transactions",
        "Transaction count",
        ["0.5", "0.75", "0.9", "0.95", "0.99"],
    ),
]

for feature, title, unit, levels in feature_charts:
    st.subheader(title)

    legitimate = quantiles[feature]["legitimate"]["values"]
    fraud = quantiles[feature]["fraud"]["values"]

    chart_data = pd.DataFrame(
        {
            "Legitimate": [legitimate[level] for level in levels],
            "Fraud": [fraud[level] for level in levels],
        },
        index=[
            f"{int(float(level) * 100)}th percentile"
            for level in levels
        ],
    )

    st.bar_chart(chart_data)
    st.caption(f"Measurement: {unit}")

    sample_left, sample_right = st.columns(2)
    sample_left.caption(
        "Legitimate sample: "
        f"{quantiles[feature]['legitimate']['sample_size']:,} "
        f"of {quantiles[feature]['legitimate']['observations']:,} "
        "eligible observations"
    )
    sample_right.caption(
        "Fraud sample: "
        f"{quantiles[feature]['fraud']['sample_size']:,} "
        f"of {quantiles[feature]['fraud']['observations']:,} "
        "eligible observations"
    )

with st.expander("How should these charts be interpreted?"):
    st.markdown(
        """
        A percentile indicates the value below which a given
        percentage of observations falls.

        For example, the 95th percentile is the value at or below
        which 95% of the observations fall.

        The saved analysis uses bounded reservoir samples of up
        to 5,000 values per feature and class. The observation
        counts indicate the larger eligible populations.

        Overlapping distributions are expected. A single behavioural
        feature should not be treated as proof of fraud.
        """
    )

st.divider()


# --------------------------------------------------
# 7. BEHAVIOURAL RULE COUNTS
# --------------------------------------------------

st.header("7. Behavioural Rule Analysis")

st.markdown(
    """
    The offline analysis also counted transactions satisfying three
    illustrative behavioural conditions.
    """
)

rule_descriptions = {
    "amount_ratio_3": "Current amount at least 3× previous average",
    "rapid_60s": "Previous transaction within 60 seconds",
    "previous_count_5": "At least five previous transactions",
}

rule_results = behaviour_summary["rules"]

rule_rows = []

for rule, description in rule_descriptions.items():
    rule_rows.append(
        {
            "Behavioural Rule": description,
            "Legitimate Matches": rule_results["0"][rule],
            "Fraud Matches": rule_results["1"][rule],
        }
    )

rule_df = pd.DataFrame(rule_rows)

st.dataframe(
    rule_df,
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Rule counts can overlap: one transaction can satisfy more than "
    "one condition. They are not unique fraud counts or model predictions."
)

st.divider()


# --------------------------------------------------
# 8. HOW BEHAVIOURAL ANALYSIS ENTERS THE LIVE PIPELINE
# --------------------------------------------------

st.header("8. From Historical Analysis to Real-Time Detection")

st.markdown(
    """
    The offline analysis helps us understand historical transaction
    patterns. The real-time pipeline uses a Redis-backed
    `BehaviourEngine` to calculate corresponding features as new
    transactions arrive.
    """
)

st.code(
    """
Kafka
  |
  v
Flink transaction processing
  |
  +---- LightGBM inference
  |       |
  |       +---- Fraud probability
  |
  +---- BehaviourEngine
  |       |
  |       +---- Redis transaction history
  |       +---- Behavioural features
  |
  v
RiskEngine
  |
  +---- Model threshold decision
  +---- Behavioural flags
  |
  v
Combined transaction result
  |
  +---- Redis transaction / alert state
  |
  +---- MongoDB persistent storage
""",
    language="text",
)

st.success(
    "LightGBM remains the primary fraud decision-maker. "
    "Behavioural signals provide additional context and explanatory flags."
)

with st.expander("Examples of behavioural flags"):
    st.markdown(
        """
        - `AMOUNT_3X_PREVIOUS_AVERAGE`: current transaction amount
          is at least three times the previous average.
        - `TRANSACTION_WITHIN_60_SECONDS`: the previous transaction
          occurred within 60 seconds.
        - `BEHAVIOURAL_HISTORY_UNAVAILABLE`: a complete card
          signature was not available.
        """
    )

st.divider()


# --------------------------------------------------
# 9. HISTORICAL TRANSACTION EXPLORER
# --------------------------------------------------

st.header("9. Historical Transaction Explorer")

st.caption(
    "The explorer retrieves at most the latest 100 records. "
    "Summary metrics above represent the full historical database."
)

if not transactions:
    st.info("No historical transaction records were returned.")

else:
    df = pd.DataFrame(transactions)

    display_columns = [
        "TransactionID",
        "fraud_probability",
        "predicted_fraud",
        "ground_truth",
        "risk_level",
        "review_recommended",
        "processed_at",
    ]

    available_columns = [
        column for column in display_columns
        if column in df.columns
    ]

    st.dataframe(
        df[available_columns],
        use_container_width=True,
        hide_index=True,
    )

st.caption(
    "Historical Analytics • Database: fraud_detection • "
    "Maximum 100 transaction records loaded"
)
