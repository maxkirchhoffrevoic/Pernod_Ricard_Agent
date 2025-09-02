# db.py
from sqlalchemy import create_engine, text
import os
try:
    import streamlit as st
except Exception:
    st = None

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL and st is not None and "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]

# falls kein sslmode in der URL steht, hänge ihn an
if DATABASE_URL and "sslmode=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set. Set in .env (lokal) oder in Streamlit Secrets.")

engine = create_engine(DATABASE_URL, echo=False)
