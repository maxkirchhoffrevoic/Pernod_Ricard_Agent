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

# ... oberer Teil deiner app.py bleibt gleich (load_data etc.)

st.caption(f"Stand: {data.get('generated_at','–')}")

sig_raw = data.get("signals", [])
src_raw = data.get("sources", [])

import pandas as pd

def flatten_signals(signals: list[dict]) -> pd.DataFrame:
    # verschachtelte Felder (value.*) zu Spalten machen
    if not signals:
        return pd.DataFrame()
    df = pd.json_normalize(signals, sep="_")
    # Spalten schöner anordnen
    value_cols = [c for c in df.columns if c.startswith("value_")]
    front = [c for c in ["type", "confidence"] if c in df.columns]
    others = [c for c in df.columns if c not in front + value_cols]
    ordered = front + value_cols + others
    return df[ordered]

sig_df = flatten_signals(sig_raw)
src_df = pd.DataFrame(src_raw)

st.subheader("Signale")

if sig_df.empty:
    st.dataframe(pd.DataFrame([{"info": "Noch keine Signale"}]), use_container_width=True)
else:
    # Filter
    types = sorted(sig_df["type"].dropna().unique().tolist())
    col1, col2 = st.columns([2, 1])
    with col1:
        sel_types = st.multiselect("Filter: Typ", types, default=types)
    with col2:
        min_conf = st.slider("Min. Confidence", 0.0, 1.0, 0.5, 0.05)

    view = sig_df.copy()
    if sel_types:
        view = view[view["type"].isin(sel_types)]
    view = view[view["confidence"].fillna(0) >= min_conf]

    # Anzeige
    st.dataframe(
        view,
        use_container_width=True,
        height=420
    )

    # Details pro Zeile (schön formatiert)
    with st.expander("Details je Signal anzeigen"):
        for _, row in view.iterrows():
            title_bits = [str(row.get("type", ""))]
            for key in ["value_metric", "value_topic", "value_headline", "value_note"]:
                if key in row and pd.notna(row[key]):
                    title_bits.append(str(row[key])[:90])
                    break
            st.markdown("— " + " · ".join(title_bits) + f" · conf={row.get('confidence', 0):.2f}")
            # vollständiges JSON (ohne NaN) zeigen
            clean = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            st.json(clean)

    # Export
    exp_col1, exp_col2 = st.columns(2)
    with exp_col1:
        st.download_button(
            "CSV exportieren",
            view.to_csv(index=False).encode("utf-8"),
            file_name="signals.csv",
            mime="text/csv",
        )
    with exp_col2:
        import json
        st.download_button(
            "JSON exportieren",
            json.dumps(sig_raw, ensure_ascii=False, indent=2),
            file_name="signals.json",
            mime="application/json",
        )

st.subheader("Quellen")
if src_df.empty:
    st.dataframe(pd.DataFrame([{"info": "Noch keine Quellen"}]), use_container_width=True)
else:
    # klickbare Links
    st.dataframe(
        src_df.assign(url_link=src_df["url"].apply(lambda u: f"[Link]({u})"))[["title", "url_link"]],
        use_container_width=True,
        height=420
    )

