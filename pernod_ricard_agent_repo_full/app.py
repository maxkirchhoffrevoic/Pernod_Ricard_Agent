# app.py
import streamlit as st
import pandas as pd
from db import engine
from sqlalchemy import text

st.set_page_config(page_title='Kundenradar — Pernod Ricard', layout='wide')
st.title('Pernod Ricard — Agent MVP')

with engine.connect() as conn:
    res = conn.execute(text('select id, url, title, published_at from source order by published_at desc nulls last limit 50'))
    rows = [dict(r) for r in res]

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)
else:
    st.info('Noch keine Quellen in der DB. Führe scripts/run_agent.py aus.')

if st.button('Agent jetzt laufen lassen (lokal)'):
    st.write('Starte Agent (script run_agent.py) — siehe Terminal für Logs.')

st.markdown('**Quick actions:**')
if st.button('Öffne FY25 PR'):
    st.markdown('[FY25 Sales & Results (Pernod Ricard)](https://www.pernod-ricard.com/en/media/fy25-full-year-sales-and-results)')
