import socket

import pandas as pd
import streamlit as st

from dashboard.utils.database import (
    get_summary,
    get_transactions,
)

st.set_page_config(
    page_title="Live Monitor",
    page_icon="📡",
    layout="wide",
)

st.title("📡 Live Transaction Monitor")
st.caption(
    "Real-time monitoring of the isolated demo transaction environment."
)

st.info(
    "Data source: fraud_detection_demo. "
    "Historical IEEE-CIS records are never displayed on this page."
)


def check_service(host, port):
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


# --------------------------------------------------
# REFRESH CONTROLS
# --------------------------------------------------

if st.button("Refresh Live Monitor", use_container_width=False):
    st.cache_data.clear()
    st.rerun()


# --------------------------------------------------
# DEMO DATABASE
# --------------------------------------------------

try:
    summary = get_summary("demo")
    transactions = get_transactions("demo", limit=100)

except Exception as error:
    st.error(f"Could not read the demo database: {error}")
    st.stop()


# --------------------------------------------------
# TRANSACTION METRICS
# --------------------------------------------------

st.header("Transaction Overview")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Transactions", f"{summary['total']:,}")
c2.metric("High-Risk", f"{summary['high_risk']:,}")
c3.metric("Predicted Fraud", f"{summary['predicted_fraud']:,}")
c4.metric("Fraud Alerts", f"{summary['alerts']:,}")
c5.metric("Normal Transactions", f"{summary['normal']:,}")


# --------------------------------------------------
# PIPELINE STATUS
# --------------------------------------------------

st.divider()
st.header("Pipeline Connectivity")

kafka_online = check_service("localhost", 9092)
flink_online = check_service("localhost", 8081)

p1, p2, p3 = st.columns(3)

p1.metric(
    "Kafka Broker",
    "ONLINE" if kafka_online else "OFFLINE",
)

p2.metric(
    "Flink REST API",
    "ONLINE" if flink_online else "OFFLINE",
)

p3.metric("Demo Database", "CONNECTED")


if not kafka_online and not flink_online:
    st.info(
        "The demo pipeline is currently stopped. "
        "No new transactions are being processed. "
        "This is expected while the project is in development mode."
    )
elif kafka_online and flink_online:
    st.success("Kafka and Flink endpoints are reachable.")
else:
    st.warning(
        "Only part of the streaming pipeline is reachable. "
        "Check the Infrastructure page for more details."
    )


# --------------------------------------------------
# EMPTY STATE
# --------------------------------------------------

st.divider()
st.header("Transaction Activity")

if summary["total"] == 0:
    st.info(
        "Waiting for incoming transactions. "
        "The demo database is currently empty."
    )

    st.markdown(
        """
        **When the demo pipeline starts processing transactions, this
        section will automatically populate with:**

        - Transaction IDs and processing timestamps
        - Fraud probabilities and risk classifications
        - Behavioural flags
        - Recent transaction activity
        """
    )

else:
    # --------------------------------------------------
    # RISK DISTRIBUTION
    # --------------------------------------------------

    st.subheader("Risk Distribution")

    risk_data = pd.DataFrame({
        "Risk Level": ["HIGH", "NORMAL"],
        "Transactions": [
            summary["high_risk"],
            summary["normal"],
        ],
    })

    st.bar_chart(
        risk_data,
        x="Risk Level",
        y="Transactions",
    )

    # --------------------------------------------------
    # RECENT TRANSACTIONS
    # --------------------------------------------------

    st.subheader("Recent Transactions")

    transaction_rows = []

    for record in transactions:
        flags = record.get("behavioural_flags") or []

        if isinstance(flags, list):
            flags = ", ".join(str(flag) for flag in flags)
        else:
            flags = str(flags)

        probability = record.get("fraud_probability")

        transaction_rows.append({
            "Transaction ID": record.get("TransactionID"),
            "Fraud Probability": (
                round(float(probability), 4)
                if probability is not None else None
            ),
            "Risk": record.get("risk_level", "UNKNOWN"),
            "Predicted Fraud": record.get("predicted_fraud"),
            "Review Recommended": record.get("review_recommended"),
            "Behavioural Flags": flags,
            "Processed At": record.get("processed_at"),
        })

    st.dataframe(
        pd.DataFrame(transaction_rows),
        use_container_width=True,
        hide_index=True,
    )


st.divider()
st.caption(
    "Live Monitor • Database: fraud_detection_demo • "
    "Refresh interval for cached queries: 5 seconds"
)
