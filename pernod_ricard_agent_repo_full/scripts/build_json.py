# scripts/build_json.py
# Quellen (Newsroom + Google News 24h/30T), Extraktion mit Readability,
# LLM-Batch + per-Artikel-Nachschlag -> data/latest.json

import os, re, json, html, math, urllib.parse as ul
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dateparser

# optional .env laden (lokal)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------- Config ----------------
COMPANY = "Pernod Ricard"

NEWS_INDEX = "https://www.pernod-ricard.com/en/media"
GOOGLE_NEWS_LANGS = [("de","DE","DE:de"), ("en","US","US:en")]

LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))       # hier kannst du 30 setzen
LOOKBACK_HOURS = LOOKBACK_DAYS * 24
MAX_PER_SOURCE = int(os.getenv("MAX_PER_SOURCE", "8"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PernodRicardAgent/1.1)"}
TIMEOUT = 30

MIN_TEXT_CHARS = 600           # Mindestlänge je Artikel
TOP_TEXTS = 10                 # nur die besten N Texte ins LLM geben
SIGNAL_LIMIT = 8               # max. Signale gesamt

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# -------------- Utils --------------
def now_utc(): return datetime.now(timezone.utc)

def is_recent(dt: datetime, hours: int) -> bool:
    if not isinstance(dt, datetime): return False
    return (now_utc() - dt) <= timedelta(hours=hours)

def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def norm_url(u: str) -> str:
    try:
        p = ul.urlsplit(u)
        q = ul.parse_qsl(p.query, keep_blank_values=True)
        q = ul.urlencode(sorted(q))
        return ul.urlunsplit((p.scheme, p.netloc, p.path.rstrip("/"), q, ""))
    except Exception:
        return u

def dedupe(items, key="url"):
    seen, out = set(), []
    for it in items:
        val = norm_url(it.get(key,"")).lower()
        if not val or val in seen: continue
        seen.add(val); out.append(it)
    return out

def clean_article_text(html_text: str) -> str:
    """Readability -> cleaner Text; Fallback auf Plain-Text."""
    if not html_text: return ""
    text = ""
    try:
        from readability import Document
        doc = Document(html_text)
        article_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(article_html, "html.parser")
        for t in soup(["script","style","noscript"]): t.decompose()
        text = soup.get_text(" ").strip()
    except Exception:
        pass
    if len(text) < 200:  # Fallback
        soup = BeautifulSoup(html_text, "html.parser")
        for t in soup(["script","style","noscript"]): t.decompose()
        text = soup.get_text(" ").strip()
    text = re.sub(r"\s+"," ", text)
    return text

def extract_published_at(html_text: str):
    """Versuche Datum aus meta/time zu lesen."""
    soup = BeautifulSoup(html_text, "html.parser")
    cand = (
        soup.find("meta", {"name":"date"}) or
        soup.find("meta", property="article:published_time") or
        soup.find("time")
    )
    if not cand: return None
    val = cand.get("content") or cand.get("datetime") or cand.get_text(strip=True)
    try:
        dt = dateparser.parse(val)
        if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

# -------------- Quellen --------------
def discover_from_newsroom(index_url=NEWS_INDEX, max_items=MAX_PER_SOURCE):
    out = []
    try:
        html_ = fetch(index_url)
        soup = BeautifulSoup(html_, "html.parser")
        links = []
        for a in soup.select("a[href]"):
            href = a.get("href","").strip()
            if not href: continue
            if not href.startswith("http"): href = ul.urljoin(index_url, href)
            if "/media/" in href:
                title = a.get_text(strip=True) or "Pernod Ricard – Media"
                links.append((href, title))
        seen = set()
        for href, title in links:
            if href in seen: continue
            seen.add(href)
            out.append({"url": href, "title": title, "source":"newsroom"})
            if len(out) >= max_items: break
    except Exception:
        pass
    return out

def google_news_rss_url(query: str, lang="en", gl="US", ceid="US:en", hours=LOOKBACK_HOURS):
    when_days = max(1, math.ceil(hours/24))
    base = "https://news.google.com/rss/search"
    q = f"{query} when:{when_days}d"
    return base + "?" + ul.urlencode({"q": q, "hl": lang, "gl": gl, "ceid": ceid})

def discover_from_google_news(query=COMPANY):
    out = []
    for lang,gl,ceid in GOOGLE_NEWS_LANGS:
        url = google_news_rss_url(query, lang=lang, gl=gl, ceid=ceid)
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:MAX_PER_SOURCE]:
                link  = e.get("link") or ""
                title = html.unescape(e.get("title","")).strip() or "News"
                dt = None
                for k in ("published","updated"):
                    if k in e:
                        try:
                            dt = dateparser.parse(e[k])
                            if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                        except Exception: pass
                out.append({"url": link, "title": title, "source":f"google-news:{lang}",
                            "published_at": dt.isoformat() if dt else None})
        except Exception:
            pass
    return out

# -------------- LLM --------------
def llm_batch_signals(company: str, texts: list[dict], limit=SIGNAL_LIMIT) -> list[dict]:
    """Batch über mehrere Texte (zusammengefasst). Mindestens 3, bis zu 'limit' Signale."""
    if not OPENAI_API_KEY: return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        joined = "\n\n".join(
            f"### {t.get('title','(ohne Titel)')}\n{t.get('text','')[:5000]}"
            for t in texts
        )[:20000]  # ~ 6-7k tokens

        system = (
            "Du extrahierst faktenbasierte, strukturierte Signale zu der Firma. "
            "Gib mindestens 3, bis zu 8 Signale zurück. "
            "Nur JSON im Format:\n"
            "{ \"signals\": [ {"
            "\"type\":\"financial|strategy|markets|risks|product|leadership|sustainability\","
            "\"value\": {"
            "\"headline\":\"...\","
            "\"metric\":\"...\","
            "\"value\":\"...\","
            "\"unit\":\"...\","
            "\"topic\":\"...\","
            "\"summary\":\"...\","
            "\"note\":\"...\","
            "\"period\":\"...\","
            "\"region\":\"...\"},"
            "\"confidence\": 0.0 } ] }"
        )
        user = f"Firma: {company}\nQuellen:\n{joined}"

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[{"role":"system","content":system},
                      {"role":"user","content":user}],
        )
        data = json.loads(resp.choices[0].message.content)
        out = []
        for s in data.get("signals", []):
            if not isinstance(s, dict): continue
            s["type"] = str(s.get("type","summary"))
            try: c = float(s.get("confidence", 0.5))
            except: c = 0.5
            s["confidence"] = max(0.0, min(1.0, c))
            out.append(s)
        return out[:limit]
    except Exception:
        return []

def llm_per_article(company: str, article: dict) -> list[dict]:
    """Feinextraktion pro Artikel (0–2 Signale)."""
    if not OPENAI_API_KEY: return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        text = article.get("text","")[:6000]
        system = (
            "Extrahiere bis zu 2 faktenbasierte Signale aus dem Artikel. "
            "Nur JSON: {\"signals\":[{...}]} wie zuvor beschrieben."
        )
        user = f"Firma: {company}\nTitel: {article.get('title')}\nText:\n{text}"

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type":"json_object"},
            messages=[{"role":"system","content":system},
                      {"role":"user","content":user}],
        )
        data = json.loads(resp.choices[0].message.content)
        out=[]
        for s in data.get("signals", []):
            if not isinstance(s, dict): continue
            s["type"]=str(s.get("type","summary"))
            try: c=float(s.get("confidence",0.5))
            except: c=0.5
            s["confidence"]=max(0.0,min(1.0,c))
            out.append(s)
        return out[:2]
    except Exception:
        return []

def heuristic_summary(company: str, texts: list[dict]) -> list[dict]:
    if not texts:
        return [{
            "type":"summary",
            "value":{"headline": company,"summary":"Keine verwertbaren Artikeltexte gefunden.","note":"Fallback"},
            "confidence":0.2,
        }]
    head = texts[0]
    return [{
        "type":"summary",
        "value":{"headline": head.get("title") or company,
                 "summary": head.get("text","")[:280],
                 "note":"Fallback"},
        "confidence":0.35,
    }]
def llm_generate_report_markdown(company: str, texts: list[dict], signals: list[dict], sources: list[dict]) -> str:
    """
    Erzeugt einen ausführlichen, gegliederten Bericht als Markdown.
    Abschnitte: Executive Summary, Finanzen, Strategie, Produkte/Innovation, Führung, Märkte/Wettbewerb,
                Nachhaltigkeit/ESG, Risiken, Ausblick.
    Zitate als [1], [2], ... verweisen auf die Quellenliste (Reihenfolge = übergebene sources).
    """
    if not OPENAI_API_KEY:
        return ""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        # 1) kompaktes Material zusammenstellen (nicht zu groß, aber nützlich)
        #    - Top Texte (Titel + Auszug)
        text_snippets = []
        for t in texts[:8]:  # reichen i. d. R. für den Bericht
            snip = t.get("text", "")[:3500]
            title = t.get("title") or "(ohne Titel)"
            text_snippets.append(f"# {title}\n{snip}")

        joined_snippets = "\n\n---\n\n".join(text_snippets)[:24000]

        #    - Signale in knapper JSON-ähnlicher Darstellung
        sig_lines = []
        for s in signals[:12]:
            t = s.get("type", "")
            v = s.get("value", {}) or {}
            head = v.get("headline", "")
            summ = v.get("summary", "")
            metric = v.get("metric", "")
            topic = v.get("topic", "")
            sig_lines.append(f"- type={t}; headline={head}; metric={metric}; topic={topic}; summary={summ}")
        signals_digest = "\n".join(sig_lines)

        #    - Quellenverzeichnis (nummeriert), das der Autor verwenden soll
        #      Wichtig: Reihenfolge hier = Nummerierung im Bericht
        numbered_sources = []
        for i, s in enumerate(sources, start=1):
            ttl = (s.get("title") or "").strip()
            url = (s.get("url") or "").strip()
            if ttl:
                numbered_sources.append(f"[{i}] {ttl} — {url}")
            else:
                numbered_sources.append(f"[{i}] {url}")
        sources_list = "\n".join(numbered_sources)

        system = (
            "Du bist ein Analyst. Erstelle einen sachlichen, faktenbasierten Bericht über die Firma. "
            "Struktur und Format: Markdown mit klaren H2-Überschriften in dieser Reihenfolge:\n"
            "## Executive Summary\n"
            "## Finanzen\n"
            "## Strategie\n"
            "## Produkte & Innovation\n"
            "## Führung & Organisation\n"
            "## Märkte & Wettbewerb\n"
            "## Nachhaltigkeit & ESG\n"
            "## Risiken\n"
            "## Ausblick\n\n"
            "Regeln:\n"
            "- Schreibe auf Deutsch.\n"
            "- Belege konkrete Aussagen mit Zitatnummern in eckigen Klammern, z. B. [1], [3]. "
            "Verwende ausschließlich die unten gelisteten Quellen; erfinde keine.\n"
            "- Keine PR-Sprache, keine Spekulationen. Wenn Zahlen unsicher sind, formuliere vorsichtig.\n"
            "- Länge: grob 600–1200 Wörter.\n"
        )

        user = (
            f"Firma: {company}\n\n"
            "Verfügbare Signale (kompakt):\n"
            f"{signals_digest}\n\n"
            "Artikel-Auszüge:\n"
            f"{joined_snippets}\n\n"
            "Quellenliste (für Zitate):\n"
            f"{sources_list}\n\n"
            "Erzeuge jetzt den Bericht in Markdown. Verwende Zitatnummern [n] passend zur Quellenliste."
        )

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        md = resp.choices[0].message.content.strip()
        return md
    except Exception:
        return ""

# -------------- Pipeline --------------
def main():
    # 1) Links
    items = []
    items += discover_from_newsroom()
    items += discover_from_google_news(COMPANY)
    items = dedupe(items, key="url")

    # 2) Inhalte abrufen + Datum bestimmen
    enriched, sources = [], []
    for it in items:
        url, title = it["url"], it.get("title","")
        try:
            html_ = fetch(url)
        except Exception:
            sources.append({"url": url, "title": title}); continue

        dt = it.get("published_at")
        if dt: 
            try: dt = dateparser.parse(dt)
            except: dt = None
        if not isinstance(dt, datetime):
            try: dt = extract_published_at(html_)
            except: dt = None

        text = clean_article_text(html_)
        if len(text) >= MIN_TEXT_CHARS:
            enriched.append({
                "url": url, "title": title, "source": it.get("source",""),
                "published_at": dt, "text": text
            })

        sources.append({"url": url, "title": title})

    # 3) strenger Lookback
    enriched_recent = []
    for a in enriched:
        dt = a.get("published_at")
        if dt and not is_recent(dt, LOOKBACK_HOURS):
            continue
        enriched_recent.append(a)

    # Scoring: Länge + Frische
    def score(a):
        L = len(a.get("text",""))
        dt = a.get("published_at")
        bonus = 0.0
        if dt:
            hours = max(1.0, (now_utc()-dt).total_seconds()/3600.0)
            bonus = 1.0 / hours
        return L/1500.0 + bonus

    enriched_recent.sort(key=score, reverse=True)
    selected = enriched_recent[:TOP_TEXTS]

    # 4) LLM-Batch
    signals = []
    if OPENAI_API_KEY and selected:
        signals = llm_batch_signals(COMPANY, selected, limit=SIGNAL_LIMIT)

        # Nachschlag pro Artikel, falls zu wenige Signale
        if len(signals) < 3:
            for art in selected[:min(6,len(selected))]:
                signals += llm_per_article(COMPANY, art)
                if len(signals) >= SIGNAL_LIMIT:
                    break

        # Dedupe by (headline, topic)
        dedup = {}
        for s in signals:
            val = s.get("value", {})
            key = (val.get("headline","").strip().lower(), val.get("topic","").strip().lower())
            if key not in dedup: dedup[key] = s
        signals = list(dedup.values())[:SIGNAL_LIMIT]

    if not signals:
        signals = heuristic_summary(COMPANY, selected)

    # 5) Schreiben
    os.makedirs("data", exist_ok=True)
    out = {
        "company": COMPANY,
        "generated_at": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "signals": signals,
        "sources": sources
    }
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Wrote data/latest.json with {len(signals)} signals and {len(sources)} sources; texts_selected={len(selected)}.")

if __name__ == "__main__":
    main()
