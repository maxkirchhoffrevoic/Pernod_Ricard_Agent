# app.py
# Streamlit UI für den Pernod Ricard Agent (No-DB)
# Lädt data/latest.json, zeigt Bericht, Signale & Quellen mit Filtern und Export.

import os
import io
import json
from datetime import datetime, timezone
import pandas as pd
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# Konfiguration / Pfade
# -----------------------------------------------------------------------------
LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "data/latest.json")
RAW_DATA_URL    = os.getenv("RAW_DATA_URL", "").strip()  # optionaler Fallback (raw.githubusercontent.com/…)
PAGE_TITLE      = os.getenv("PAGE_TITLE", "Pernod Ricard — Agent (No-DB)")

st.set_page_config(page_title=PAGE_TITLE, layout="wide")

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def load_json() -> dict:
    """Lädt zuerst lokal, optional Fallback per RAW_DATA_URL.
       Wirft st.error mit Hinweisen, wenn nichts gefunden wird."""
    # Lokal
    if os.path.exists(LOCAL_JSON_PATH):
        try:
            with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Konnte '{LOCAL_JSON_PATH}' nicht lesen: {e}")

    # Remote (optional)
    if RAW_DATA_URL:
        try:
            r = requests.get(RAW_DATA_URL, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            st.error(f"RAW_DATA_URL fehlgeschlagen: {e}")

    # Nichts gefunden
    st.error(
        "Keine Daten gefunden. Prüfen Sie, ob 'data/latest.json' im Repo vorhanden ist "
        "oder setzen Sie die Umgebungsvariable RAW_DATA_URL auf eine gültige Raw-URL "
        "(z. B. https://raw.githubusercontent.com/<user>/<repo>/main/data/latest.json)."
    )
    st.stop()


def parse_dt(s: str):
    try:
        # Eingangsformat: "YYYY-MM-DDTHH:MM:SSZ"
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def flatten_signals(signals: list[dict]) -> pd.DataFrame:
    """Macht aus der Liste strukturierter Signale eine flache Tabelle."""
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
        # Numerisch erzwingen für Slider/Sortierung
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    return df


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def to_json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


# -----------------------------------------------------------------------------
# Daten laden
# -----------------------------------------------------------------------------
data = load_json()

company       = data.get("company", "Unbekannt")
generated_at  = data.get("generated_at") or ""
generated_dt  = parse_dt(generated_at)
signals       = data.get("signals") or []
sources       = data.get("sources") or []
report_md     = data.get("report_markdown") or ""
report_meta   = data.get("report_meta") or {}

# -----------------------------------------------------------------------------
# Kopf / Kennzahlen
# -----------------------------------------------------------------------------
st.title(company + " — Agent (No-DB)")

col_l, col_r = st.columns([3, 2], gap="large")

with col_l:
    if generated_dt:
        st.caption(f"Stand: {generated_dt.isoformat()} (UTC)")
    else:
        st.caption("Stand: unbekannt")

with col_r:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Signale", len(signals))
    k2.metric("Quellen", len(sources))
    k3.metric("Lookback (Tage)", report_meta.get("lookback_days", "—"))
    k4.metric("Texte selektiert", report_meta.get("texts_selected", "—"))

# -----------------------------------------------------------------------------
# Bericht (Markdown)
# -----------------------------------------------------------------------------
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
    st.divider()

# -----------------------------------------------------------------------------
# Signale
# -----------------------------------------------------------------------------
st.header("Signale")

df_signals = flatten_signals(signals)

if df_signals.empty:
    st.info("Noch keine Signale verfügbar.")
else:
    # Filterleiste
    with st.container():
        f1, f2, f3 = st.columns([2, 2, 3])
        types = sorted(list(df_signals["type"].dropna().unique()))
        sel_types = f1.multiselect("Filter: Typ", options=types, default=types)

        min_conf_default = float(0 if df_signals["confidence"].isna().all() else max(0.0, float(df_signals["confidence"].min(skipna=True) or 0.0)))
        min_conf = f2.slider("Min. Confidence", min_value=0.0, max_value=1.0, value=min(min_conf_default, 0.15), step=0.05)

        q = f3.text_input("Suche (Headline/Topic/Summary)", "")

    # Filtern
    fdf = df_signals.copy()
    if sel_types:
        fdf = fdf[fdf["type"].isin(sel_types)]
    if "confidence" in fdf:
        fdf = fdf[(fdf["confidence"].fillna(0) >= min_conf)]

    if q:
        ql = q.lower().strip()
        mask = (
            fdf["value_headline"].fillna("").str.lower().str.contains(ql)
            | fdf["value_topic"].fillna("").str.lower().str.contains(ql)
            | fdf["value_summary"].fillna("").str.lower().str.contains(ql)
        )
        fdf = fdf[mask]

    # Anzeige
    show_cols = [
        "type", "confidence",
        "value_headline", "value_metric", "value_value", "value_unit",
        "value_topic", "value_summary", "value_note", "value_period", "value_region",
    ]
    show_cols = [c for c in show_cols if c in fdf.columns]

    st.dataframe(
        fdf[show_cols].sort_values(by=["confidence", "type"], ascending=[False, True]),
        use_container_width=True,
        hide_index=True,
    )

    # Download-Buttons
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "Signale als CSV",
            data=to_csv_bytes(fdf[show_cols]),
            file_name="signals.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "Signale als JSON",
            data=to_json_bytes(fdf.to_dict(orient="records")),
            file_name="signals.json",
            mime="application/json",
            use_container_width=True,
        )

    # Detailansicht je Signal
    with st.expander("Details je Signal anzeigen"):
        for _, row in fdf.iterrows():
            i = int(row["idx"])
            s = signals[i]
            st.markdown(f"**{s.get('type','?')}** – { (s.get('value') or {}).get('headline','(ohne Headline)') }")
            st.json(s)

st.divider()

# -----------------------------------------------------------------------------
# Quellen
# -----------------------------------------------------------------------------
st.header("Quellen")

if not sources:
    st.info("Noch keine Quellen vorhanden.")
else:
    # DataFrame bauen
    src_rows = []
    for s in sources:
        src_rows.append({
            "title": s.get("title"),
            "url": s.get("url"),
        })
    df_src = pd.DataFrame(src_rows)

    # Suche
    qsrc = st.text_input("Quellensuche (Titel/URL)", "", key="qsrc")
    if qsrc:
        ql = qsrc.lower().strip()
        mask = (
            df_src["title"].fillna("").str.lower().str.contains(ql)
            | df_src["url"].fillna("").str.lower().str.contains(ql)
        )
        df_src = df_src[mask]

    # Anzeige (mit Link-Spalte, falls LinkColumn verfügbar)
    try:
        st.dataframe(
            df_src,
            use_container_width=True,
            hide_index=True,
            column_config={
                "url": st.column_config.LinkColumn("url", help="Quelle öffnen"),
                "title": st.column_config.TextColumn("title"),
            },
        )
    except Exception:
        # Fallback ohne LinkColumn
        st.dataframe(df_src, use_container_width=True, hide_index=True)

    # Downloads
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "Quellen als CSV",
            data=to_csv_bytes(df_src),
            file_name="sources.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "Quellen als JSON",
            data=to_json_bytes(df_src.to_dict(orient="records")),
            file_name="sources.json",
            mime="application/json",
            use_container_width=True,
        )

# -----------------------------------------------------------------------------
# Rohdaten-Expander
# -----------------------------------------------------------------------------
with st.expander("Rohdaten anzeigen (latest.json)"):
    st.json(data)
