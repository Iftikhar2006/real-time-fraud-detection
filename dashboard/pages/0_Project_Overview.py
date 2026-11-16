import streamlit as st

st.set_page_config(
    page_title="Fraud Detection Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🛡️ Real-Time Financial Fraud Detection Platform")

st.caption(
    "A distributed data engineering pipeline for transaction processing, "
    "behavioural analysis, and machine-learning-based fraud detection."
)

st.divider()

# --------------------------------------------------
# PROJECT OBJECTIVE
# --------------------------------------------------

st.header("Project Overview")

st.write(
    """
    Financial institutions process large volumes of transactions every
    second. Detecting suspicious activity requires more than checking
    individual transaction amounts: it also requires understanding
    behavioural patterns, transaction history, and contextual anomalies.

    This project implements a real-time transaction processing architecture
    that combines distributed stream processing, behavioural feature
    engineering, and machine learning to identify potentially fraudulent
    transactions.
    """
)

# --------------------------------------------------
# ARCHITECTURE
# --------------------------------------------------

st.header("System Architecture")

st.code(
    """
Transaction Source
       |
       v
     Apache Kafka
       |
       v
     Apache Flink
       |
       +---- Behavioural Feature Engineering
       |
       +---- Machine Learning Fraud Scoring
       |
       +---- Risk Classification
       |
       v
    MongoDB  <---->  Redis
       |
       v
   Monitoring Dashboard
    """,
    language="text",
)

st.caption(
    "Conceptual architecture of the transaction processing and "
    "fraud detection pipeline."
)

# --------------------------------------------------
# DATASET
# --------------------------------------------------

st.header("Dataset")

c1, c2 = st.columns(2)

with c1:
    st.subheader("IEEE-CIS Fraud Detection")
    st.write(
        """
        The historical model development and evaluation pipeline uses
        the IEEE-CIS Fraud Detection dataset.
        """
    )

    st.markdown(
        """
        - Transaction data
        - Identity information
        - TransactionID-based integration
        - Binary fraud target: `isFraud`
        """
    )

with c2:
    st.subheader("Two Separate Data Environments")

    st.info(
        """
        **Historical Environment**

        Original IEEE-CIS development and analytical results.
        """
    )

    st.success(
        """
        **Demo Environment**

        Dedicated environment for fresh transaction processing and
        live monitoring.
        """
    )

# --------------------------------------------------
# TECHNOLOGY STACK
# --------------------------------------------------

st.header("Technology Stack")

technology = {
    "Streaming": "Apache Kafka",
    "Stream Processing": "Apache Flink",
    "Machine Learning": "LightGBM",
    "Programming": "Python",
    "Historical Storage": "MongoDB",
    "Low-Latency State": "Redis",
    "Dashboard": "Streamlit",
}

columns = st.columns(4)

for index, (category, technology_name) in enumerate(technology.items()):
    with columns[index % 4]:
        st.metric(category, technology_name)

# --------------------------------------------------
# MODEL PERFORMANCE
# --------------------------------------------------

st.header("Model Evaluation")

st.caption(
    "Historical validation results from the IEEE-CIS model development "
    "pipeline. These are not live demo metrics."
)

m1, m2, m3 = st.columns(3)

m1.metric("Validation PR-AUC", "0.877466")
m2.metric("Best Iteration", "1,999")
m3.metric("Classification Threshold", "0.50")

st.write(
    """
    The fraud classifier uses LightGBM with 423 engineered features,
    including 35 categorical features. The validation split was
    stratified, with an 80:20 training-to-validation ratio.
    """
)

# --------------------------------------------------
# SYSTEM PERFORMANCE
# --------------------------------------------------

st.header("Pipeline Performance Benchmarks")

st.caption(
    "Previously measured persistence-window throughput. "
    "These values are not claims of maximum sustainable capacity."
)

benchmark_data = [
    {"Target Rate": "10 tx/s", "Observed Rate": "9.57 tx/s",
     "Average Latency": "1.126 s", "P95 Latency": "1.953 s"},
    {"Target Rate": "20 tx/s", "Observed Rate": "17.80 tx/s",
     "Average Latency": "1.233 s", "P95 Latency": "1.976 s"},
    {"Target Rate": "50 tx/s", "Observed Rate": "35.34 tx/s",
     "Average Latency": "2.108 s", "P95 Latency": "3.215 s"},
]

st.dataframe(
    benchmark_data,
    use_container_width=True,
    hide_index=True,
)

st.metric("Inference Speedup", "40.23×")

st.divider()

st.caption(
    "Project Overview • Historical results are presented separately "
    "from live demo activity."
)
