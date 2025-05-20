import streamlit as st

st.set_page_config(page_title="Project Dashboard", layout="centered")

st.title("📊 Project Service Dashboard")

st.markdown("### 🔗 Quick Links")

services = {
    "Airflow": "http://localhost:8081",
    "InfluxDB": "http://localhost:8087",
    "MongoDB": "http://localhost:8080"
}

for name, url in services.items():
    st.markdown(f"- [{name}]({url})")

st.info("Click the links above to open each service in a new tab.")
