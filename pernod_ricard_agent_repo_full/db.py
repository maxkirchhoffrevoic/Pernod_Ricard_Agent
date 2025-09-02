from sqlalchemy import create_engine, text
import os
try:
    import streamlit as st
except Exception:
    st = None

def _get_database_url():
    url = os.getenv("DATABASE_URL")
    if (not url) and st is not None and "DATABASE_URL" in st.secrets:
        url = st.secrets["DATABASE_URL"]
    if not url:
        raise RuntimeError("DATABASE_URL not set.")

    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

DATABASE_URL = _get_database_url()
engine = create_engine(DATABASE_URL, pool_pre_ping=True, echo=False)
