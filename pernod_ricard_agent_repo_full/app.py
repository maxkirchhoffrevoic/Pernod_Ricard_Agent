# app.py
# Streamlit UI – zeigt Bericht, Signale & Quellen; inkl. LinkedIn-Ansicht

import os
import io
import json
from datetime import datetime, timezone
import pandas as pd
import requests
import streamlit as st

LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "data/latest.json")
RAW_DATA_URL    = os.getenv("RAW_DATA_URL", "").strip()
PAGE_TITLE      = os.getenv("PAGE_TITLE", "Pernod Ricard — Agent (No-DB)")

st.set_page_config(page_title=PAGE_TITLE, layout="wide")

# ------------------------------ Utils ------------------------------
def load_json() -> dict:
    if os.path.exists(LOCAL_JSON_PATH):
        try:
            with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Konnte '{LOCAL_JSON_PATH}' nicht lesen: {e}")
    if RAW_DATA_URL:
        try:
            r = requests.get(RAW_DATA_URL, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            st.error(f"RAW_DATA_URL fehlgeschlagen: {e}")
    st.error(
        "Keine Daten gefunden. Prüfe data/latest.json oder setze RAW_DATA_URL "
        "(z. B. https://raw.githubusercontent.com/<user>/<repo>/main/data/latest.json)."
    )
    st.stop()

def parse_dt(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def flatten_signals(signals: list[dict]) -> pd.DataFrame:
    rows = []
    for idx, s in enumerate(signals or []):
        t = s.get("type")
        c = s.get("confidence")
        v = s.get("value") or {}
        rows.append({
            "idx": idx,
            "type": t,
            "confidence": c,
            "value_headline": v.get("headline"),
            "value_metric": v.get("metric"),
            "value_value": v.get("value"),
            "value_unit": v.get("unit"),
            "value_topic": v.get("topic"),
            "value_summary": v.get("summary"),
            "value_note": v.get("note"),
            "value_period": v.get("period"),
            "value_region": v.get("region"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    return df

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

def to_json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")

# ------------------------------ Daten ------------------------------
data = load_json()

company       = data.get("company", "Unbekannt")
generated_at  = data.get("generated_at") or ""
generated_dt  = parse_dt(generated_at)
signals       = data.get("signals") or []
sources       = data.get("sources") or []
report_md     = data.get("report_markdown") or ""
report_meta   = data.get("report_meta") or {}
report_used   = data.get("report_used_sources") or []

st.title(company + " — Agent (No-DB)")

col_l, col_r = st.columns([3, 3], gap="large")
with col_l:
    st.caption(f"Stand: {generated_dt.isoformat()} (UTC)" if generated_dt else "Stand: unbekannt")
with col_r:
    total_li = sum(1 for s in sources if "linkedin.com" in (s.get("url","")) or str(s.get("source","")).startswith("linkedin"))
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Signale", len(signals))
    k2.metric("Quellen", len(sources))
    k3.metric("LinkedIn-Quellen", total_li)
    k4.metric("Lookback (Tage)", report_meta.get("lookback_days", "—"))

# ---------------------------- Bericht -----------------------------
if report_md:
    st.subheader("Analysebericht (automatisch)")
    st.markdown(report_md)
    st.download_button(
        "Bericht als Markdown herunterladen",
        data=report_md.encode("utf-8"),
        file_name="pernod_ricard_bericht.md",
        mime="text/markdown",
        use_container_width=True,
    )
    with st.expander(f"Quellen, die in den Bericht eingeflossen sind ({len(report_used)})"):
        if report_used:
            df_ru = pd.DataFrame(report_used)
            try:
                st.dataframe(
                    df_ru,
                    use_container_width=True,
                    hide_index=True,
                    column_config={"url": st.column_config.LinkColumn("url")}
                )
            except Exception:
                st.dataframe(df_ru, use_container_width=True, hide_index=True)
    st.divider()

# ---------------------------- Signale -----------------------------
st.header("Signale")

df_signals = flatten_signals(signals)
if df_signals.empty:
    st.info("Noch keine Signale verfügbar.")
else:
    f1, f2, f3 = st.columns([2, 2, 3])
    types = sorted(list(df_signals["type"].dropna().unique()))
    sel_types = f1.multiselect("Filter: Typ", options=types, default=types)
    min_conf_default = float(0 if df_signals["confidence"].isna().all() else max(0.0, float(df_signals["confidence"].min(skipna=True) or 0.0)))
    min_conf = f2.slider("Min. Confidence", 0.0, 1.0, min(min_conf_default, 0.15), 0.05)
    q = f3.text_input("Suche (Headline/Topic/Summary)", "")

    fdf = df_signals.copy()
    if sel_types:
        fdf = fdf[fdf["type"].isin(sel_types)]
    fdf = fdf[(fdf["confidence"].fillna(0) >= min_conf)]
    if q:
        ql = q.lower().strip()
        mask = (
            fdf["value_headline"].fillna("").str.lower().str.contains(ql)
            | fdf["value_topic"].fillna("").str.lower().str.contains(ql)
            | fdf["value_summary"].fillna("").str.lower().str.contains(ql)
        )
        fdf = fdf[mask]

    show_cols = [
        "type","confidence",
        "value_headline","value_metric","value_value","value_unit",
        "value_topic","value_summary","value_note","value_period","value_region",
    ]
    show_cols = [c for c in show_cols if c in fdf.columns]

    st.dataframe(
        fdf[show_cols].sort_values(by=["confidence","type"], ascending=[False, True]),
        use_container_width=True,
        hide_index=True,
    )

    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Signale als CSV", to_csv_bytes(fdf[show_cols]), "signals.csv", "text/csv", use_container_width=True)
    with c2:
        st.download_button("Signale als JSON", to_json_bytes(fdf.to_dict(orient="records")), "signals.json", "application/json", use_container_width=True)

    with st.expander("Details je Signal anzeigen"):
        for _, row in fdf.iterrows():
            st.markdown(f"**{row.get('type','?')}** – {row.get('value_headline','(ohne Headline)')}")
            st.json(signals[int(row["idx"])])

st.divider()

# ---------------------------- LinkedIn ----------------------------
st.header("LinkedIn Beiträge")

li_sources = [s for s in sources if "linkedin.com" in (s.get("url","")) or str(s.get("source","")).startswith("linkedin")]
if not li_sources:
    st.caption("Keine LinkedIn-Quellen im aktuellen Datensatz.")
else:
    df_li = pd.DataFrame(li_sources)
    qli = st.text_input("Suche in LinkedIn-Quellen (Titel/URL)", "", key="q_li")
    if qli:
        ql = qli.lower().strip()
        mask = (
            df_li["title"].fillna("").str.lower().str.contains(ql)
            | df_li["url"].fillna("").str.lower().str.contains(ql)
            | df_li["source"].fillna("").str.lower().str.contains(ql)
        )
        df_li = df_li[mask]

    try:
        st.dataframe(
            df_li,
            use_container_width=True,
            hide_index=True,
            column_config={"url": st.column_config.LinkColumn("url")}
        )
    except Exception:
        st.dataframe(df_li, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        st.download_button("LinkedIn-Quellen (CSV)", to_csv_bytes(df_li), "linkedin_sources.csv", "text/csv", use_container_width=True)
    with c2:
        st.download_button("LinkedIn-Quellen (JSON)", to_json_bytes(df_li.to_dict(orient="records")), "linkedin_sources.json", "application/json", use_container_width=True)

st.divider()

# ----------------------------- Alle Quellen -----------------------
st.header("Alle Quellen")

df_src = pd.DataFrame(sources) if sources else pd.DataFrame(columns=["title","url","source"])
qsrc = st.text_input("Quellensuche (Titel/URL/Source)", "", key="qsrc_all")
if qsrc:
    ql = qsrc.lower().strip()
    mask = (
        df_src["title"].fillna("").str.lower().str.contains(ql)
        | df_src["url"].fillna("").str.lower().str.contains(ql)
        | df_src["source"].fillna("").str.lower().str.contains(ql)
    )
    df_src = df_src[mask]

try:
    st.dataframe(
        df_src,
        use_container_width=True,
        hide_index=True,
        column_config={"url": st.column_config.LinkColumn("url")}
    )
except Exception:
    st.dataframe(df_src, use_container_width=True, hide_index=True)

c1, c2 = st.columns(2)
with c1:
    st.download_button("Quellen als CSV", to_csv_bytes(df_src), "sources.csv", "text/csv", use_container_width=True)
with c2:
    st.download_button("Quellen als JSON", to_json_bytes(df_src.to_dict(orient="records")), "sources.json", "application/json", use_container_width=True)

# ----------------------------- Rohdaten --------------------------
with st.expander("Rohdaten anzeigen (latest.json)"):
    st.json(data)
