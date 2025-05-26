import streamlit as st

st.set_page_config(page_title="Project Dashboard", layout="centered")

st.title("📊 Project Service Dashboard")

st.markdown("### 🔗 Quick Links")

services = {
    "MongoDB - Trusted Zone": "http://localhost:8081",
    "MongoDB - Exploitation Zone": "http://localhost:8083",
    "Airflow": "http://localhost:8082",
    "InfluxDB": "http://localhost:8087",
    "Spark": "http://localhost:8080",
}

for name, url in services.items():
    st.markdown(f"- [{name}]({url})")

st.info("Click the links above to open each service in a new tab.")
