# scripts/build_json.py
# Sammeln von Quellen (Newsroom + Google News RSS 24h), Extraktion, LLM-Signale → data/latest.json

import os
import re
import json
import time
import math
import html
import urllib.parse as ul
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dateparser

# Optional .env laden (lokal). In GitHub Actions kommt der Key als Secret.
try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

# =========================
# Konfiguration
# =========================
COMPANY = "Pernod Ricard"

# Offizieller Newsroom (Seite wird gecrawlt)
NEWS_INDEX = "https://www.pernod-ricard.com/en/media"

# Google News RSS (keine API nötig; beachtet aber die jeweiligen Nutzungsbedingungen)
# last 24h via "when:1d". Du kannst weitere Sprachen/Regionen hinzufügen.
GOOGLE_NEWS_LANGS = [
    ("de", "DE", "DE:de"),  # Deutsch
    ("en", "US", "US:en"),  # Englisch
]

LOOKBACK_HOURS = 24           # wie viele Stunden zurück
MAX_PER_SOURCE = 6            # pro Quelle nicht mehr als X Artikel
TIMEOUT = 30
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PernodRicardAgent/1.0)"}

# OpenAI
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# =========================
# Utilities
# =========================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def is_recent(dt: datetime, hours: int) -> bool:
    if not isinstance(dt, datetime):
        return False
    return (now_utc() - dt) <= timedelta(hours=hours)

def clean_text_from_html(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def fetch(url: str, timeout: int = TIMEOUT) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def safe_get_text(url: str) -> str:
    try:
        html_ = fetch(url)
        return clean_text_from_html(html_)
    except Exception:
        return ""

def norm_url(u: str) -> str:
    try:
        p = ul.urlsplit(u)
        # Normalisierung (ohne Fragmente, standard query)
        q = ul.parse_qsl(p.query, keep_blank_values=True)
        q = ul.urlencode(sorted(q))
        return ul.urlunsplit((p.scheme, p.netloc, p.path.rstrip("/"), q, ""))
    except Exception:
        return u

def dedupe(items, key="url"):
    seen = set()
    out = []
    for it in items:
        val = norm_url(it.get(key, "")).lower()
        if not val or val in seen:
            continue
        seen.add(val)
        out.append(it)
    return out

def clip(text: str, n=9000) -> str:
    return text if len(text) <= n else text[:n] + " ..."

# =========================
# Quellen
# =========================
def discover_from_newsroom(index_url=NEWS_INDEX, max_items=MAX_PER_SOURCE):
    """Scrape den offiziellen Newsroom und hole die letzten Artikel-URLs + Titel + Datum (falls erkennbar)."""
    out = []
    try:
        html_ = fetch(index_url)
        soup = BeautifulSoup(html_, "html.parser")

        # recht generisch: alle Links innerhalb der „media“-Seite
        links = []
        for a in soup.select("a[href]"):
            href = a["href"].strip()
            # Nur interne Artikelpfade erlauben, die nach "media" aussehen
            if not href.startswith("http"):
                href = ul.urljoin(index_url, href)
            if "/media/" in href:
                title = a.get_text(strip=True) or "Pernod Ricard – Media"
                links.append((href, title))

        # dedupe & kürzen
        seen = set()
        for href, title in links:
            if href in seen:
                continue
            seen.add(href)
            out.append({"url": href, "title": title, "source": "newsroom"})
            if len(out) >= max_items:
                break
    except Exception:
        pass
    return out

def google_news_rss_url(query: str, lang="en", gl="US", ceid="US:en", hours=24):
    # when:1d → letzte 24h; wenn du 48h willst: when:2d
    when = max(1, math.ceil(hours / 24))
    q = f"{query} when:{when}d"
    base = "https://news.google.com/rss/search"
    params = {"q": q, "hl": lang, "gl": gl, "ceid": ceid}
    return base + "?" + ul.urlencode(params)

def discover_from_google_news(query=COMPANY, hours=LOOKBACK_HOURS, max_items=MAX_PER_SOURCE):
    out = []
    for lang, gl, ceid in GOOGLE_NEWS_LANGS:
        url = google_news_rss_url(query, lang=lang, gl=gl, ceid=ceid, hours=hours)
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[: max_items]:
                link = e.get("link") or ""
                title = html.unescape(e.get("title", "")).strip()
                # Publikationszeit
                dt = None
                for key in ("published", "updated"):
                    if key in e:
                        try:
                            dt = dateparser.parse(e[key])
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
                out.append({
                    "url": link,
                    "title": title or "News",
                    "source": f"google-news:{lang}",
                    "published_at": dt.isoformat() if isinstance(dt, datetime) else None,
                })
        except Exception:
            pass
    return out

# =========================
# LLM-Extraktion
# =========================
def llm_extract_signals(company: str, texts: list[dict]) -> list[dict]:
    """Erzeuge strukturierte Signale via OpenAI. Fällt bei Key/Fehler auf leichten Heuristik-Fallback zurück
