# app.py
import streamlit as st
from sqlalchemy import text
from urllib.parse import urlparse
from db import engine, DATABASE_URL

u = urlparse(DATABASE_URL if DATABASE_URL.startswith("postgresql://") else DATABASE_URL.replace("postgres://","postgresql://"))
masked = f"{u.scheme}://{u.username}:***@{u.hostname}:{u.port}{u.path}"
st.caption(f"DB target: {masked} (sslmode={'sslmode=' in (u.query or '')})")

try:
    with engine.connect() as c:
        c.execute(text("select 1"))
    st.success("DB-Verbindung ok.")
except Exception as e:
    st.error(f"DB-Verbindung fehlgeschlagen: {type(e).__name__}: {e}")
    st.stop()


st.set_page_config(page_title="Pernod Ricard — Agent MVP", layout="wide")

# --- DB laden + Diagnose ---
try:
    from db import engine, DATABASE_URL  # DATABASE_URL wird in db.py gebaut (inkl. sslmode=require)
except Exception as e:
    st.error(f"Datenbank-Setup fehlgeschlagen: {e}")
    st.stop()

# Maskierte URL (zur schnellen Fehlerdiagnose)
try:
    u = urlparse(DATABASE_URL)
    masked = f"{u.scheme}://{u.username or ''}:***@{u.hostname or ''}:{u.port or ''}{u.path or ''}"
    st.caption(f"DB target: {masked} (sslmode={'sslmode=' in (u.query or '')})")
except Exception:
    st.caption("DB target: (konnte URL nicht parsen)")

# Verbindungs-Check
try:
    with engine.connect() as c:
        c.execute(text("select 1"))
    st.success("DB-Verbindung ok.")
except Exception as e:
    st.error(f"DB-Verbindung fehlgeschlagen: {type(e).__name__}. "
             "Prüfen Sie in Streamlit Secrets: DATABASE_URL (postgresql://…?sslmode=require) "
             "und ggf. URL-encoding des Passworts.")
    st.stop()

st.title("Pernod Ricard — Agent MVP")

# --- Daten laden ---
@st.cache_data(ttl=60)
def load_sources(limit: int = 50):
    # Lädt die letzten Quellen (falls Tabelle existiert)
    with engine.connect() as conn:
        try:
            q = text("""
                select id, url, title, published_at
                from public.source
                order by published_at desc nulls last, id desc
                limit :lim
            """)
            rows = conn.execute(q, {"lim": limit}).mappings().all()
            return [dict(r) for r in rows]
        except Exception as ex:
            # Häufigster Grund: Tabelle existiert noch nicht
            return {"error": str(ex)}

data = load_sources(50)

if isinstance(data, dict) and "error" in data:
    st.warning("Konnte Tabelle 'public.source' nicht lesen. "
               "Haben Sie das SQL aus models.sql in Supabase ausgeführt?")
else:
    if not data:
        st.info("Noch keine Quellen in der Datenbank. Führen Sie lokal "
                "`python scripts/run_agent.py` aus, um Einträge zu erzeugen.")
    else:
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True, height=420)

# --- Quick Actions ---
st.subheader("Quick actions")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("FY25 Sales & Results öffnen"):
        st.write("[FY25 Sales & Results (Pernod Ricard)](https://www.pernod-ricard.com/en/media/fy25-full-year-sales-and-results)")

with col2:
    st.caption("Agent läuft in Streamlit Cloud nicht als Background-Job.")
    if st.button("Agent-Hinweis"):
        st.info("Starten Sie den Agenten lokal: `python scripts/run_agent.py` "
                "oder richten Sie eine GitHub Action für den täglichen Lauf ein.")

with col3:
    if st.button("DB-Healthcheck"):
        try:
            with engine.connect() as c:
                c.execute(text("select 1"))
            st.success("DB ok.")
        except Exception as ex:
            st.error(f"DB-Check fehlgeschlagen: {ex}")

st.caption("Hinweis: Für Streamlit Cloud müssen Secrets gesetzt sein: DATABASE_URL und OPENAI_API_KEY.")
