import streamlit as st

st.set_page_config(
    page_title="Fraud Detection Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

overview = st.Page(
    "pages/0_Project_Overview.py",
    title="Project Overview",
    icon="🛡️",
    default=True,
)

live_monitor = st.Page(
    "pages/1_Live_Monitor.py",
    title="Live Monitor",
    icon="📡",
)

fraud_alerts = st.Page(
    "pages/2_Fraud_Alerts.py",
    title="Fraud Alerts",
    icon="🚨",
)

historical_analytics = st.Page(
    "pages/3_Historical_Analytics.py",
    title="Historical Analytics",
    icon="📊",
)

infrastructure = st.Page(
    "pages/4_Infrastructure.py",
    title="Infrastructure",
    icon="🖥️",
)

navigation = st.navigation(
    {
        "Platform": [overview],
        "Monitoring": [
            live_monitor,
            fraud_alerts,
            historical_analytics,
            infrastructure,
        ],
    },
    position="sidebar",
)

navigation.run()
