# app.py (No-DB)
import streamlit as st, pandas as pd, requests

st.set_page_config(page_title="Pernod Ricard — Agent (No-DB)", layout="wide")
st.title("Pernod Ricard — Agent (No-DB)")

# Passen Sie user/repo an:
DATA_URL = "https://github.com/maxkirchhoffrevoic/Pernod_Ricard_Agent/blob/main/pernod_ricard_agent_repo_full/data/latest.json"

@st.cache_data(ttl=300)
def load_data():
    r = requests.get(DATA_URL, timeout=15)
    r.raise_for_status()
    return r.json()

try:
    data = load_data()
except Exception as e:
    st.error(f"Keine Daten gefunden/ladbar: {e}")
    st.stop()

st.caption(f"Stand: {data.get('generated_at','–')}")

sig = pd.DataFrame(data.get("signals", []))
src = pd.DataFrame(data.get("sources", []))

st.subheader("Signale")
st.dataframe(sig if not sig.empty else pd.DataFrame([{"info":"Noch keine Signale"}]), use_container_width=True)

st.subheader("Quellen")
st.dataframe(src if not src.empty else pd.DataFrame([{"info":"Noch keine Quellen"}]), use_container_width=True)
