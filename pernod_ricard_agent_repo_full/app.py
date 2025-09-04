# app.py
import os, json, requests, streamlit as st, pandas as pd

st.set_page_config(page_title="Pernod Ricard — Agent (No-DB)", layout="wide")
st.title("Pernod Ricard — Agent (No-DB)")

DATA_URL = st.secrets.get("DATA_URL", "").strip()  # optionaler Fallback auf RAW-URL

@st.cache_data(ttl=300)
def load_data():
    local_path = "data/latest.json"
    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            return json.load(f)
    if DATA_URL:
        r = requests.get(DATA_URL, timeout=15)
        r.raise_for_status()
        return r.json()
    raise FileNotFoundError("data/latest.json nicht gefunden und keine DATA_URL gesetzt.")

try:
    data = load_data()
except Exception as e:
    st.error(f"Keine Daten gefunden/ladbar: {e}")
    st.stop()

st.caption(f"Stand: {data.get('generated_at','–')}")

sig = pd.DataFrame(data.get("signals", []))
src = pd.DataFrame(data.get("sources", []))

st.subheader("Signale")
if sig.empty:
    st.dataframe(pd.DataFrame([{"info": "Noch keine Signale"}]), use_container_width=True)
else:
    st.dataframe(sig, use_container_width=True, height=420)

st.subheader("Quellen")
if src.empty:
    st.dataframe(pd.DataFrame([{"info": "Noch keine Quellen"}]), use_container_width=True)
else:
    st.dataframe(src, use_container_width=True, height=420)
