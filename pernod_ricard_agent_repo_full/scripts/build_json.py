# scripts/build_json.py
# -----------------------------------------------------------------------------
# Täglicher Builder: sammelt Quellen (Newsroom + Google News + LinkedIn),
# extrahiert Artikeltexte, erzeugt strukturierte Signale per LLM und schreibt
# data/latest.json. Zusätzlich: gegliederter Markdown-Bericht.
#
# Abhängigkeiten (requirements.txt):
#   requests, beautifulsoup4, feedparser, python-dateutil,
#   readability-lxml, lxml, openai>=1.30.0, python-dotenv (optional)
# -----------------------------------------------------------------------------

import os
import re
import json
import html
import math
import urllib.parse as ul
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dateparser

# optional .env (nur lokal relevant)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----------------------------- Konfiguration ---------------------------------
COMPANY = os.getenv("COMPANY", "Pernod Ricard")

# Newsroom-Übersicht
NEWS_INDEX = os.getenv("NEWS_INDEX", "https://www.pernod-ricard.com/en/media")

# Google-News Sprachen (Lang, GL, CEID)
GOOGLE_NEWS_LANGS = [
    ("de", "DE", "DE:de"),
    ("en", "US", "US:en"),
]

# LinkedIn:
#   - Setze LINKEDIN_RSS_URLS auf kommagetrennte RSS-Feeds (z. B. aus RSSHub / Social-Tool),
#     z. B. "https://rsshub.app/linkedin/company/pernod-ricard,https://…/user/xyz"
#   - Zusätzlich versuchen wir (optional) Google-News-RSS mit site:linkedin.com
LINKEDIN_RSS_URLS = [u.strip() for u in os.getenv("LINKEDIN_RSS_URLS", "").split(",") if u.strip()]
INCLUDE_GNEWS_LINKEDIN = os.getenv("INCLUDE_GNEWS_LINKEDIN", "1") in ("1", "true", "True")

# Zeitraum (Standard: 7 Tage); alternativ in GitHub Actions als ENV setzen
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))
LOOKBACK_HOURS = LOOKBACK_DAYS * 24

# wie viele Treffer pro Quelle
MAX_PER_SOURCE = int(os.getenv("MAX_PER_SOURCE", "8"))

# HTTP
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PernodRicardAgent/1.2)"}
TIMEOUT = 30

# Auswahl/Qualität
MIN_TEXT_CHARS_ARTICLE  = int(os.getenv("MIN_TEXT_CHARS_ARTICLE", "600"))  # Artikel
MIN_TEXT_CHARS_LINKEDIN = int(os.getenv("MIN_TEXT_CHARS_LINKEDIN", "140")) # LinkedIn-Posts sind kürzer
TOP_TEXTS      = int(os.getenv("TOP_TEXTS", "10"))         # beste N Texte ins LLM
SIGNAL_LIMIT   = int(os.getenv("SIGNAL_LIMIT", "8"))       # max. Anzahl Signale

# OpenAI
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Report-Prompt Feintuning
REPORT_MAX_TEXTS     = int(os.getenv("REPORT_MAX_TEXTS", "12"))
REPORT_MIN_CITATIONS = int(os.getenv("REPORT_MIN_CITATIONS", "6"))

# ------------------------------- Utilities -----------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def is_recent(dt: datetime, hours: int) -> bool:
    if not isinstance(dt, datetime):
        return False
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
        val = norm_url(it.get(key, "")).lower()
        if not val or val in seen:
            continue
        seen.add(val)
        out.append(it)
    return out

def clean_article_text(html_text: str) -> str:
    """Versucht zuerst Readability, fällt dann auf Plain Text zurück."""
    if not html_text:
        return ""
    text = ""
    try:
        from readability import Document
        doc = Document(html_text)
        article_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(article_html, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        text = soup.get_text(" ").strip()
    except Exception:
        pass

    if len(text) < 200:   # Fallback
        soup = BeautifulSoup(html_text, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        text = soup.get_text(" ").strip()

    text = re.sub(r"\s+", " ", text)
    return text

def extract_published_at(html_text: str):
    """Versucht Veröffentlichungsdatum aus meta/time zu parsen."""
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        cand = (
            soup.find("meta", {"name": "date"}) or
            soup.find("meta", property="article:published_time") or
            soup.find("time")
        )
        if not cand:
            return None
        val = cand.get("content") or cand.get("datetime") or cand.get_text(strip=True)
        dt = dateparser.parse(val)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def clean_from_html_fragment(fragment: str) -> str:
    """HTML-Fragment (z. B. aus RSS summary/content) zu Text."""
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text(" ").strip()
    return re.sub(r"\s+", " ", text)

# ----------------------------- Quellen-Finder --------------------------------
def discover_from_newsroom(index_url=NEWS_INDEX, max_items=MAX_PER_SOURCE):
    out = []
    try:
        html_ = fetch(index_url)
        soup = BeautifulSoup(html_, "html.parser")
        links = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if not href.startswith("http"):
                href = ul.urljoin(index_url, href)
            if "/media/" in href:
                title = a.get_text(strip=True) or "Pernod Ricard – Media"
                links.append((href, title))

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

def google_news_rss_url(query: str, lang="en", gl="US", ceid="US:en", hours=LOOKBACK_HOURS):
    when_days = max(1, math.ceil(hours / 24))
    base = "https://news.google.com/rss/search"
    q = f"{query} when:{when_days}d"
    return base + "?" + ul.urlencode({"q": q, "hl": lang, "gl": gl, "ceid": ceid})

def discover_from_google_news(query=COMPANY):
    out = []
    for lang, gl, ceid in GOOGLE_NEWS_LANGS:
        url = google_news_rss_url(query, lang=lang, gl=gl, ceid=ceid)
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:MAX_PER_SOURCE]:
                link  = e.get("link") or ""
                title = html.unescape(e.get("title", "")).strip() or "News"
                dt = None
                for k in ("published", "updated"):
                    if k in e:
                        try:
                            dt = dateparser.parse(e[k])
                            if dt and dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
                out.append({
                    "url": link,
                    "title": title,
                    "source": f"google-news:{lang}",
                    "published_at": dt.isoformat() if dt else None
                })
        except Exception:
            pass
    return out

# ----------------------------- LinkedIn Finder -------------------------------
def discover_from_linkedin_rss(max_items=MAX_PER_SOURCE):
    """Liest kommagetrennte RSS-Feeds aus LINKEDIN_RSS_URLS (z. B. RSSHub, Social-Tool)."""
    out = []
    for feed_url in LINKEDIN_RSS_URLS:
        try:
            feed = feedparser.parse(feed_url)
            for e in feed.entries[:max_items]:
                link = e.get("link") or ""
                title = html.unescape(e.get("title", "")).strip() or "LinkedIn"
                dt = None
                for k in ("published", "updated"):
                    if k in e:
                        try:
                            dt = dateparser.parse(e[k])
                            if dt and dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
                # Prefetched summary/content
                content = ""
                if "summary" in e and e.summary:
                    content = clean_from_html_fragment(e.summary)
                elif "content" in e and e.content:
                    try:
                        content = clean_from_html_fragment(e.content[0].value)
                    except Exception:
                        pass

                out.append({
                    "url": link,
                    "title": title,
                    "source": "linkedin:rss",
                    "published_at": dt.isoformat() if dt else None,
                    "prefetched_text": content
                })
        except Exception:
            pass
    return out

def discover_from_google_news_linkedin(company=COMPANY, max_items=MAX_PER_SOURCE):
    """Best-Effort: Google News mit site:linkedin.com Query (funktioniert nicht immer, aber schadet nicht)."""
    out = []
    for lang, gl, ceid in GOOGLE_NEWS_LANGS:
        when_days = max(1, math.ceil(LOOKBACK_HOURS / 24))
        base = "https://news.google.com/rss/search"
        q = f'{company} site:linkedin.com when:{when_days}d'
        url = base + "?" + ul.urlencode({"q": q, "hl": lang, "gl": gl, "ceid": ceid})
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:max_items]:
                link  = e.get("link") or ""
                title = html.unescape(e.get("title", "")).strip() or "LinkedIn (via GNews)"
                dt = None
                for k in ("published","updated"):
                    if k in e:
                        try:
                            dt = dateparser.parse(e[k])
                            if dt and dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
                out.append({
                    "url": link,
                    "title": title,
                    "source": f"linkedin:gnews:{lang}",
                    "published_at": dt.isoformat() if dt else None
                })
        except Exception:
            pass
    return out

# ----------------------------- LLM-Funktionen --------------------------------
def llm_batch_signals(company: str, texts: list[dict], limit=SIGNAL_LIMIT) -> list[dict]:
    """Batch über mehrere Texte. Liefert 3..limit strukturierte Signale."""
    if not OPENAI_API_KEY:
        return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        joined = "\n\n".join(
            f"### {t.get('title','(ohne Titel)')}\n{t.get('text','')[:5000]}"
            for t in texts
        )[:20000]  # ~ 6-7k Tokens

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
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        data = json.loads(resp.choices[0].message.content)
        out = []
        for s in data.get("signals", []):
            if not isinstance(s, dict):
                continue
            s["type"] = str(s.get("type", "summary"))
            try:
                c = float(s.get("confidence", 0.5))
            except Exception:
                c = 0.5
            s["confidence"] = max(0.0, min(1.0, c))
            out.append(s)
        return out[:limit]
    except Exception:
        return []

def llm_per_article(company: str, article: dict) -> list[dict]:
    """Feinextraktion pro Artikel (0–2 Signale)."""
    if not OPENAI_API_KEY:
        return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        text = article.get("text", "")[:6000]

        system = (
            "Extrahiere bis zu 2 faktenbasierte Signale aus dem Artikel. "
            "Nur JSON: {\"signals\":[{...}]} wie zuvor beschrieben."
        )
        user = f"Firma: {company}\nTitel: {article.get('title')}\nText:\n{text}"

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type":"json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        data = json.loads(resp.choices[0].message.content)
        out = []
        for s in data.get("signals", []):
            if not isinstance(s, dict):
                continue
            s["type"] = str(s.get("type", "summary"))
            try:
                c = float(s.get("confidence", 0.5))
            except Exception:
                c = 0.5
            s["confidence"] = max(0.0, min(1.0, c))
            out.append(s)
        return out[:2]
    except Exception:
        return []

def heuristic_summary(company: str, texts: list[dict]) -> list[dict]:
    """Fallback ohne LLM – liefert ein kurzes Summary-Signal."""
    if not texts:
        return [{
            "type": "summary",
            "value": {
                "headline": company,
                "summary": "Keine verwertbaren Texte gefunden.",
                "note": "Fallback"
            },
            "confidence": 0.2,
        }]
    head = texts[0]
    return [{
        "type": "summary",
        "value": {
            "headline": head.get("title") or company,
            "summary": head.get("text", "")[:280],
            "note": "Fallback"
        },
        "confidence": 0.35,
    }]

def llm_generate_report_markdown(company: str, texts: list[dict], signals: list[dict], sources: list[dict],
                                 max_texts: int = REPORT_MAX_TEXTS,
                                 min_citations: int = REPORT_MIN_CITATIONS,
                                 use_only_selected_sources: bool = True):
    """
    Erzeugt einen ausführlichen Bericht (Markdown).
    Gibt zusätzlich die tatsächlich referenzierbare Quellenliste zurück (für Transparenz).
    """
    if not OPENAI_API_KEY:
        return "", []

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        use_texts = texts[:max_texts]
        # Artikel-Auszüge
        text_snippets = []
        for t in use_texts:
            snip = (t.get("text") or "")[:3500]
            title = t.get("title") or "(ohne Titel)"
            text_snippets.append(f"# {title}\n{snip}")
        joined_snippets = "\n\n---\n\n".join(text_snippets)[:24000]

        # kompakte Signalsicht
        sig_lines = []
        for s in signals[:12]:
            v = s.get("value") or {}
            sig_lines.append(
                f"- type={s.get('type','')}; headline={v.get('headline','')}; "
                f"metric={v.get('metric','')}; topic={v.get('topic','')}; summary={v.get('summary','')}"
            )
        signals_digest = "\n".join(sig_lines)

        # Quellenliste (nur Texte, die wirklich ins Prompt gehen)
        if use_only_selected_sources:
            selected_urls = {t.get("url") for t in use_texts}
            sources_for_report = [s for s in sources if s.get("url") in selected_urls]
        else:
            sources_for_report = list(sources)

        numbered_sources = []
        for i, s in enumerate(sources_for_report, start=1):
            ttl = (s.get("title") or "").strip()
            url = (s.get("url") or "").strip()
            numbered_sources.append(f"[{i}] {ttl+' — ' if ttl else ''}{url}")
        sources_list = "\n".join(numbered_sources)

        system = (
            "Du bist ein Analyst. Erstelle einen faktenbasierten Bericht über die Firma. "
            "Struktur (H2):\n"
            "## Executive Summary\n## Finanzen\n## Strategie\n## Produkte & Innovation\n"
            "## Führung & Organisation\n## Märkte & Wettbewerb\n## Nachhaltigkeit & ESG\n## Risiken\n## Ausblick\n\n"
            "Regeln:\n"
            f"- Deutsch, 600–1200 Wörter.\n- Belege Aussagen mit Zitatnummern [n] aus der Quellenliste.\n"
            f"- Versuche, mindestens {min_citations} unterschiedliche Quellen zu zitieren (sofern sinnvoll).\n"
            "- Keine PR-Sprache, keine erfundenen Quellen/Zahlen."
        )

        user = (
            f"Firma: {company}\n\n"
            f"Verfügbare Signale (kompakt):\n{signals_digest}\n\n"
            f"Artikel-/Post-Auszüge:\n{joined_snippets}\n\n"
            f"Quellenliste (nur diese dürfen zitiert werden):\n{sources_list}\n\n"
            "Erzeuge den Bericht."
        )

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
        )
        md = resp.choices[0].message.content.strip()
        return md, sources_for_report
    except Exception:
        return "", []

# -------------------------------- Pipeline -----------------------------------
def main():
    # 1) Links sammeln
    items = []
    items += discover_from_newsroom()
    items += discover_from_google_news(COMPANY)
    # LinkedIn (RSS)
    if LINKEDIN_RSS_URLS:
        items += discover_from_linkedin_rss()
    # LinkedIn (via Google News site:linkedin.com)
    if INCLUDE_GNEWS_LINKEDIN:
        items += discover_from_google_news_linkedin(COMPANY)
    items = dedupe(items, key="url")

    # 2) Inhalte abrufen + Datum bestimmen (Posts/Artikel)
    enriched, sources = [], []
    for it in items:
        url, title, src = it["url"], it.get("title", ""), it.get("source", "")
        prefetched = it.get("prefetched_text", "")

        html_ = None
        # Wenn wir bereits Text aus RSS haben (LinkedIn), nutze ihn
        if prefetched:
            text = prefetched
            html_ = None
        else:
            # versuche Seite zu laden (bei LinkedIn kann das scheitern → dann leeren Text)
            try:
                html_ = fetch(url)
                text = clean_article_text(html_)
            except Exception:
                text = ""

        # Veröffentlichungszeit
        dt = it.get("published_at")
        if dt:
            try:
                dt = dateparser.parse(dt)
            except Exception:
                dt = None
        if not isinstance(dt, datetime) and html_:
            dt = extract_published_at(html_)

        # Mindestlängen je Quelle
        min_chars = MIN_TEXT_CHARS_LINKEDIN if src.startswith("linkedin") or "linkedin.com" in url else MIN_TEXT_CHARS_ARTICLE
        if len(text) >= min_chars:
            enriched.append({
                "url": url,
                "title": title,
                "source": src,
                "published_at": dt,
                "text": text
            })

        sources.append({"url": url, "title": title, "source": src})

    # 3) strenger Lookback (nur Artikel/Posts mit Datum, die ≤ LOOKBACK_HOURS alt sind)
    enriched_recent = []
    for a in enriched:
        dt = a.get("published_at")
        if dt and not is_recent(dt, LOOKBACK_HOURS):
            continue
        enriched_recent.append(a)

    # Scoring: Länge + Frische
    def score(a):
        L = len(a.get("text", ""))
        dt = a.get("published_at")
        bonus = 0.0
        if dt:
            hours = max(1.0, (now_utc() - dt).total_seconds() / 3600.0)
            bonus = 1.0 / hours
        # LinkedIn-Posts sind kürzer; gebe ihnen kleinen Bonus, damit sie reinkommen
        src = a.get("source", "")
        li_bonus = 0.2 if (src.startswith("linkedin") or "linkedin.com" in a.get("url","")) else 0.0
        return L / 1500.0 + bonus + li_bonus

    enriched_recent.sort(key=score, reverse=True)
    selected = enriched_recent[:TOP_TEXTS]

    # 4) LLM-Signale
    signals = []
    if OPENAI_API_KEY and selected:
        # Batch
        signals = llm_batch_signals(COMPANY, selected, limit=SIGNAL_LIMIT)

        # Nachschlag pro Artikel/Post, falls zu wenige
        if len(signals) < 3:
            for art in selected[:min(6, len(selected))]:
                signals += llm_per_article(COMPANY, art)
                if len(signals) >= SIGNAL_LIMIT:
                    break

        # Dedupe by (headline, topic)
        dedup = {}
        for s in signals:
            val = s.get("value", {})
            key = (val.get("headline", "").strip().lower(),
                   val.get("topic", "").strip().lower())
            if key not in dedup:
                dedup[key] = s
        signals = list(dedup.values())[:SIGNAL_LIMIT]

    if not signals:
        signals = heuristic_summary(COMPANY, selected)

    # 4b) Ausführlichen Bericht erzeugen + genutzte Quellen
    report_md, report_used_sources = "", []
    try:
        report_md, report_used_sources = llm_generate_report_markdown(
            COMPANY, selected, signals, sources
        )
    except Exception:
        report_md, report_used_sources = "", []

    # 5) Schreiben
    os.makedirs("data", exist_ok=True)
    out = {
        "company": COMPANY,
        "generated_at": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "signals": signals,
        "sources": sources,
        "report_markdown": report_md,
        "report_used_sources": report_used_sources,
        "report_meta": {
            "lookback_days": LOOKBACK_DAYS,
            "texts_selected": len(selected),
            "report_max_texts": REPORT_MAX_TEXTS
        }
    }
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # kleines Log
    li_sources = sum(1 for s in sources if "linkedin.com" in (s.get("url","")) or str(s.get("source","")).startswith("linkedin"))
    print(
        f"Wrote data/latest.json with {len(signals)} signals; sources={len(sources)} (linkedin={li_sources}); "
        f"texts_selected={len(selected)}."
    )

if __name__ == "__main__":
    main()
