# db.py
from sqlalchemy import create_engine, text
import os
try:
    import streamlit as st
except Exception:
    st = None

# erst ENV, dann st.secrets als Fallback
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL and st is not None and "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set. Set in .env (lokal) oder in Streamlit Secrets.")

engine = create_engine(DATABASE_URL, echo=False)

def init_db():
    with engine.begin() as conn:
        sql = open('models.sql').read()
        conn.execute(text(sql))

if __name__ == '__main__':
    init_db()
    print('DB initialized')
