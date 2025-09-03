# db.py
from sqlalchemy import create_engine, text
import os, socket
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

try:
    import streamlit as st
except Exception:
    st = None

def _get_raw_url():
    url = os.getenv("DATABASE_URL")
    if (not url) and st is not None and "DATABASE_URL" in st.secrets:
        url = st.secrets["DATABASE_URL"]
    if not url:
        raise RuntimeError("DATABASE_URL not set.")
    # postgres:// -> postgresql://
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url

def _enforce_ssl_and_ipv4(url: str) -> str:
    u = urlparse(url)
    # Query-Params zusammenführen
    q = dict(parse_qsl(u.query or "", keep_blank_values=True))
    # sslmode erzwingen
    q.setdefault("sslmode", "require")

    # IPv4-Host auflösen und als hostaddr anhängen (nur wenn verfügbar)
    host = u.hostname
    if host:
        try:
            # erste IPv4-Adresse nehmen
            infos = socket.getaddrinfo(host, None, socket.AF_INET)
            if infos:
                ipv4 = infos[0][4][0]
                q["hostaddr"] = ipv4
        except Exception:
            # wenn IPv4 nicht auflösbar, lassen wir hostaddr weg
            pass

    new_query = urlencode(q)
    # schema sicherstellen
    scheme = "postgresql"
    new_url = urlunparse((
        scheme,
        u.netloc,   # enthält user:pass@host:port
        u.path,
        u.params,
        new_query,
        u.fragment
    ))
    return new_url

DATABASE_URL = _enforce_ssl_and_ipv4(_get_raw_url())

# kleiner, stabiler Pool; schneller Timeout
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=0,
    connect_args={"connect_timeout": 10},
    echo=False,
)

def init_db():
    with engine.begin() as conn:
        sql = open('models.sql', encoding="utf-8").read()
        conn.execute(text(sql))

if __name__ == '__main__':
    init_db()
