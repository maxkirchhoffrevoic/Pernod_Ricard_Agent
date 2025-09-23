# app.py
# EU/DE + E-Commerce fokussierte Streamlit-App (No-DB)

import os
import io
import json
from datetime import datetime, timezone
import pandas as pd
import requests
import streamlit as st

LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "data/latest.json")
RAW_DATA_URL    = os.getenv("RAW_DATA_URL", "").strip()
PAGE_TITLE      = os.getenv("PAGE_TITLE", "EU/DE E-Commerce Agent — Pernod Ricard")

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
    st.error("Keine Daten gefunden. Prüfe data/latest.json oder setze RAW_DATA_URL.")
    st.stop()

def parse_dt(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def flatten_signals(signals: list[dict]) -> pd.DataFrame:
    rows=[]
    for idx,s in enumerate(signals or []):
        t=s.get("type")
        c=s.get("confidence")
        v=s.get("value") or {}
        rows.append({
            "idx": idx,
            "type": t,
            "confidence": c,
            "headline": v.get("headline"),
            "metric": v.get("metric"),
            "value": v.get("value"),
            "unit": v.get("unit"),
            "topic": v.get("topic"),
            "summary": v.get("summary"),
            "note": v.get("note"),
            "period": v.get("period"),
            "region": v.get("region"),
        })
    df=pd.DataFrame(rows)
    if not df.empty:
        df["confidence"]=pd.to_numeric(df["confidence"], errors="coerce")
    return df

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf=io.StringIO(); df.to_csv(buf, index=False); return buf.getvalue().encode("utf-8")
def to_json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")

def is_eu_url(url: str) -> bool:
    EU_TLDS = (".de",".at",".ch",".fr",".it",".es",".nl",".se",".pl",".dk",".no",".fi",".ie",".uk",".eu")
    try:
        from urllib.parse import urlsplit
        netloc = urlsplit(url).netloc.lower()
        return netloc.endswith(EU_TLDS) or any(netloc.endswith(tld) for tld in EU_TLDS)
    except Exception:
        return False

def is_ecom_row(row) -> bool:
    txt = " ".join([str(row.get(k,"") or "") for k in ("headline","topic","summary")]).lower()
    keys = [
        "e-commerce","ecommerce","onlinehandel","marktplatz","marketplace",
        "retail media","amazon","zalando","d2c","online sales","digital commerce",
        "buy box","gmv","cart","checkout","conversion"
    ]
    return any(k in txt for k in keys) or (str(row.get("type","")).lower() in ("ecommerce","retail_media"))

# ------------------------------ Daten ------------------------------
data = load_json()
company       = data.get("company","Unbekannt")
generated_at  = data.get("generated_at") or ""
generated_dt  = parse_dt(generated_at)
signals       = data.get("signals") or []
sources       = data.get("sources") or []
report_md     = data.get("report_markdown") or ""
report_meta   = data.get("report_meta") or {}
report_used   = data.get("report_used_sources") or []

st.title(f"{company} — EU/DE E-Commerce Agent (No-DB)")

# Header KPIs
eu_sources = sum(1 for s in sources if is_eu_url(s.get("url","")))
li_sources = sum(1 for s in sources if "linkedin.com" in (s.get("url","")) or str(s.get("source","")).startswith("linkedin"))

k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("Signale", len(signals))
k2.metric("Quellen (gesamt)", len(sources))
k3.metric("EU-Quellen", eu_sources)
k4.metric("LinkedIn-Quellen", li_sources)
k5.metric("Lookback (Tage)", report_meta.get("lookback_days","—"))
st.caption(f"Stand: {generated_dt.isoformat()} (UTC)" if generated_dt else "Stand: unbekannt")

# Bericht
if report_md:
    st.subheader("Bericht (EU/DE, E-Commerce-fokussiert)")
    st.markdown(report_md)
    st.download_button("Bericht als Markdown", report_md.encode("utf-8"),
                       "bericht_eu_de_ecommerce.md", "text/markdown", use_container_width=True)
    with st.expander(f"Quellen im Bericht ({len(report_used)})"):
        if report_used:
            df_ru = pd.DataFrame(report_used)
            try:
                st.dataframe(df_ru, use_container_width=True, hide_index=True,
                             column_config={"url": st.column_config.LinkColumn("url")})
            except Exception:
                st.dataframe(df_ru, use_container_width=True, hide_index=True)
    st.divider()

# Signale
st.header("Signale")
df = flatten_signals(signals)

if df.empty:
    st.info("Keine Signale vorhanden.")
else:
    colf1, colf2, colf3, colf4 = st.columns([2,2,2,3])
    types = sorted(list(df["type"].dropna().unique()))
    sel_types = colf1.multiselect("Typ", types, default=types)
    min_conf = colf2.slider("Min. Confidence", 0.0, 1.0, 0.2, 0.05)
    only_eu = colf3.checkbox("Nur EU-Regionen", value=False, help="Filtert auf region ∈ {DE, EU, …}")
    ecom_focus = colf4.checkbox("E-Commerce-Fokus", value=True, help="Zeigt nur E-Commerce/Marktplatz/Retail Media relevante Signale")

    q = st.text_input("Volltextsuche (Headline/Topic/Summary)", "")

    fdf = df.copy()
    if sel_types: fdf = fdf[fdf["type"].isin(sel_types)]
    fdf = fdf[(fdf["confidence"].fillna(0) >= min_conf)]
    if only_eu:
        fdf = fdf[fdf["region"].fillna("").str.len() > 0]
        fdf = fdf[fdf["region"].str.upper().isin(["EU","DE","AT","CH","FR","IT","ES","NL","SE","PL","DK","NO","FI","IE","UK"])]
    if ecom_focus:
        fdf = fdf[fdf.apply(is_ecom_row, axis=1)]
    if q:
        ql=q.lower().strip()
        mask = (
            fdf["headline"].fillna("").str.lower().str.contains(ql)
            | fdf["topic"].fillna("").str.lower().str.contains(ql)
            | fdf["summary"].fillna("").str.lower().str.contains(ql)
        )
        fdf = fdf[mask]

    show_cols = ["type","confidence","headline","metric","value","unit","topic","summary","note","period","region"]
    show_cols = [c for c in show_cols if c in fdf.columns]
    st.dataframe(fdf[show_cols].sort_values(by=["confidence","type"], ascending=[False,True]),
                 use_container_width=True, hide_index=True)

    c1,c2 = st.columns(2)
    with c1:
        st.download_button("Signale als CSV", to_csv_bytes(fdf[show_cols]),
                           "signals_eu_de_ecom.csv", "text/csv", use_container_width=True)
    with c2:
        st.download_button("Signale als JSON", to_json_bytes(fdf.to_dict(orient="records")),
                           "signals_eu_de_ecom.json", "application/json", use_container_width=True)

    with st.expander("Signal-Details"):
        for _, row in fdf.iterrows():
            st.markdown(f"**{row.get('type','?')}** — {row.get('headline','(ohne Headline)')}")
            st.json(signals[int(row["idx"])])

st.divider()

# LinkedIn View
st.header("LinkedIn-Quellen")
li = [s for s in sources if "linkedin.com" in (s.get("url","")) or str(s.get("source","")).startswith("linkedin")]
if not li:
    st.caption("Keine LinkedIn-Quellen im Datensatz.")
else:
    df_li = pd.DataFrame(li)
    qli = st.text_input("Suche in LinkedIn-Quellen (Titel/URL/Source)", "", key="q_li")
    if qli:
        ql=qli.lower().strip()
        mask = (
            df_li["title"].fillna("").str.lower().str.contains(ql)
            | df_li["url"].fillna("").str.lower().str.contains(ql)
            | df_li["source"].fillna("").str.lower().str.contains(ql)
        )
        df_li = df_li[mask]
    try:
        st.dataframe(df_li, use_container_width=True, hide_index=True,
                     column_config={"url": st.column_config.LinkColumn("url")})
    except Exception:
        st.dataframe(df_li, use_container_width=True, hide_index=True)

    c1,c2 = st.columns(2)
    with c1:
        st.download_button("LinkedIn-Quellen (CSV)", to_csv_bytes(df_li),
                           "linkedin_sources.csv","text/csv", use_container_width=True)
    with c2:
        st.download_button("LinkedIn-Quellen (JSON)", to_json_bytes(df_li.to_dict(orient="records")),
                           "linkedin_sources.json","application/json", use_container_width=True)

st.divider()

# Alle Quellen
st.header("Alle Quellen")
df_src = pd.DataFrame(sources) if sources else pd.DataFrame(columns=["title","url","source"])
qsrc = st.text_input("Quellensuche gesamt (Titel/URL/Source)", "", key="qsrc_all")
if qsrc:
    ql=qsrc.lower().strip()
    mask = (
        df_src["title"].fillna("").str.lower().str.contains(ql)
        | df_src["url"].fillna("").str.lower().str.contains(ql)
        | df_src["source"].fillna("").str.lower().str.contains(ql)
    )
    df_src = df_src[mask]
try:
    st.dataframe(df_src, use_container_width=True, hide_index=True,
                 column_config={"url": st.column_config.LinkColumn("url")})
except Exception:
    st.dataframe(df_src, use_container_width=True, hide_index=True)

c1,c2 = st.columns(2)
with c1:
    st.download_button("Quellen (CSV)", to_csv_bytes(df_src),
                       "sources_all.csv","text/csv", use_container_width=True)
with c2:
    st.download_button("Quellen (JSON)", to_json_bytes(df_src.to_dict(orient="records")),
                       "sources_all.json","application/json", use_container_width=True)

with st.expander("Rohdaten (latest.json)"):
    st.json(data)
