# scripts/build_json.py
# -----------------------------------------------------------------------------
# EU/DE-fokussierter Builder: Newsroom + Google News (EU-Editionen) + LinkedIn,
# E-Commerce-Queries, Readability-Extraktion, LLM-Signale & Bericht.
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

# optional .env (lokal)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ============================ Konfiguration ==================================
COMPANY = os.getenv("COMPANY", "Pernod Ricard")

# Newsroom
NEWS_INDEX = os.getenv("NEWS_INDEX", "https://www.pernod-ricard.com/en/media")

# EU-Google-News Editionen: (lang, gl, ceid)
EU_GNEWS = [
    ("de", "DE", "DE:de"),
    ("de", "AT", "AT:de"),
    ("de", "CH", "CH:de"),
    ("fr", "FR", "FR:fr"),
    ("it", "IT", "IT:it"),
    ("es", "ES", "ES:es"),
    ("nl", "NL", "NL:nl"),
    ("sv", "SE", "SE:sv"),
    ("pl", "PL", "PL:pl"),
    ("da", "DK", "DK:da"),
    ("no", "NO", "NO:no"),
    ("fi", "FI", "FI:fi"),
    ("en-IE", "IE", "IE:en-IE"),
    ("en-GB", "GB", "GB:en-GB"),
]

# E-Commerce Query-Varianten (werden zusätzlich zu einer generischen Firmen-Suche abgefragt)
ECOM_QUERIES = [
    # deutsch
    '{company} (E-Commerce OR "E-Commerce" OR Onlinehandel OR Marktplatz OR "Retail Media" OR Amazon OR Zalando OR D2C OR "digitale Verkäufe")',
    # englisch
    '{company} (ecommerce OR "e-commerce" OR marketplace OR "retail media" OR Amazon OR Zalando OR D2C OR "online sales")',
    # französisch
    '{company} (e-commerce OR "commerce en ligne" OR marketplace OR "retail media" OR Amazon OR D2C OR "ventes en ligne")',
    # italienisch
    '{company} (e-commerce OR "commercio online" OR marketplace OR "retail media" OR Amazon OR D2C OR "vendite online")',
    # spanisch
    '{company} (ecommerce OR "comercio electrónico" OR marketplace OR "retail media" OR Amazon OR D2C OR "ventas en línea")',
]

# LinkedIn RSS (optional, z. B. aus RSSHub)
LINKEDIN_RSS_URLS = [u.strip() for u in os.getenv("LINKEDIN_RSS_URLS", "").split(",") if u.strip()]
INCLUDE_GNEWS_LINKEDIN = os.getenv("INCLUDE_GNEWS_LINKEDIN", "1") in ("1", "true", "True")

# Zeitraum/Umfang
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "14"))  # EU/DE-Analysen oft länger sinnvoll
LOOKBACK_HOURS = LOOKBACK_DAYS * 24
MAX_PER_SOURCE = int(os.getenv("MAX_PER_SOURCE", "10"))
TOP_TEXTS      = int(os.getenv("TOP_TEXTS", "14"))
SIGNAL_LIMIT   = int(os.getenv("SIGNAL_LIMIT", "10"))

# Mindestlängen
MIN_TEXT_CHARS_ARTICLE  = int(os.getenv("MIN_TEXT_CHARS_ARTICLE", "600"))
MIN_TEXT_CHARS_LINKEDIN = int(os.getenv("MIN_TEXT_CHARS_LINKEDIN", "140"))

# HTTP
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EU-DE-Ecom-Agent/1.0)"}
TIMEOUT = 30

# OpenAI
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5")

# Bericht
REPORT_MAX_TEXTS     = int(os.getenv("REPORT_MAX_TEXTS", "14"))
REPORT_MIN_CITATIONS = int(os.getenv("REPORT_MIN_CITATIONS", "8"))

# EU-Domain-Heuristik
EU_TLDS = (
    ".de",".at",".ch",".fr",".it",".es",".nl",".se",".pl",".dk",".no",".fi",".ie",".uk",".eu"
)

ECOM_KEYWORDS = [
    "e-commerce","ecommerce","onlinehandel","marktplatz","marketplace",
    "retail media","amazon","zalando","d2c","online sales","digital commerce",
    "prime","seller","vendor","shop","webshop","checkout","basket","conversion","acquisition",
    "gmv","cart","buy box","fulfillment","fba","retouren","click & collect"
]

# ================================ Utils ======================================
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
        if not val or val in seen: continue
        seen.add(val); out.append(it)
    return out

def clean_article_text(html_text: str) -> str:
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
    if len(text) < 200:
        soup = BeautifulSoup(html_text, "html.parser")
        for t in soup(["script","style","noscript"]): t.decompose()
        text = soup.get_text(" ").strip()
    text = re.sub(r"\s+"," ", text)
    return text

def extract_published_at(html_text: str):
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        cand = (
            soup.find("meta", {"name":"date"}) or
            soup.find("meta", property="article:published_time") or
            soup.find("time")
        )
        if not cand: return None
        val = cand.get("content") or cand.get("datetime") or cand.get_text(strip=True)
        dt = dateparser.parse(val)
        if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def clean_from_html_fragment(fragment: str) -> str:
    if not fragment: return ""
    soup = BeautifulSoup(fragment, "html.parser")
    for t in soup(["script","style","noscript"]): t.decompose()
    text = soup.get_text(" ").strip()
    return re.sub(r"\s+"," ", text)

def is_eu_url(url: str) -> bool:
    try:
        netloc = ul.urlsplit(url).netloc.lower()
        return netloc.endswith(EU_TLDS) or any(netloc.endswith(tld) for tld in EU_TLDS)
    except Exception:
        return False

def has_ecom_keywords(text: str) -> bool:
    if not text: return False
    low = text.lower()
    return any(k in low for k in ECOM_KEYWORDS)

# ============================== Quellen-Finder ================================
def discover_from_newsroom(index_url=NEWS_INDEX, max_items=MAX_PER_SOURCE):
    out = []
    try:
        html_ = fetch(index_url)
        soup = BeautifulSoup(html_, "html.parser")
        links = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href: continue
            if not href.startswith("http"): href = ul.urljoin(index_url, href)
            if "/media/" in href:
                title = a.get_text(strip=True) or "Pernod Ricard – Media"
                links.append((href, title))
        seen = set()
        for href, title in links:
            if href in seen: continue
            seen.add(href)
            out.append({"url": href, "title": title, "source": "newsroom"})
            if len(out) >= max_items: break
    except Exception:
        pass
    return out

def gnews_url(query: str, lang="de", gl="DE", ceid="DE:de", hours=LOOKBACK_HOURS):
    when_days = max(1, math.ceil(hours/24))
    base = "https://news.google.com/rss/search"
    q = f"{query} when:{when_days}d"
    return base + "?" + ul.urlencode({"q": q, "hl": lang, "gl": gl, "ceid": ceid})

def discover_from_gnews_queries(company=COMPANY):
    """EU-fokussierte GNews: Firmen-Query + E-Commerce-Queries über EU-Editionen."""
    out = []
    # 1) generische Firmenabfrage je EU Edition
    base_query = f'"{company}"'
    for lang, gl, ceid in EU_GNEWS:
        url = gnews_url(base_query, lang=lang, gl=gl, ceid=ceid)
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:MAX_PER_SOURCE]:
                out.append(_entry_to_item(e, f"gnews:{gl.lower()}:{lang}"))
        except Exception:
            pass
    # 2) E-Commerce-Queries je Edition
    for template in ECOM_QUERIES:
        q = template.format(company=company)
        for lang, gl, ceid in EU_GNEWS:
            url = gnews_url(q, lang=lang, gl=gl, ceid=ceid)
            try:
                feed = feedparser.parse(url)
                for e in feed.entries[:MAX_PER_SOURCE]:
                    out.append(_entry_to_item(e, f"gnews-ecom:{gl.lower()}:{lang}"))
            except Exception:
                pass
    return out

def _entry_to_item(e, source_tag):
    link  = e.get("link") or ""
    title = html.unescape(e.get("title", "")).strip() or "News"
    dt = None
    for k in ("published","updated"):
        if k in e:
            try:
                dt = dateparser.parse(e[k])
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return {
        "url": link,
        "title": title,
        "source": source_tag,
        "published_at": dt.isoformat() if dt else None
    }

def discover_from_linkedin_rss(max_items=MAX_PER_SOURCE):
    out = []
    for feed_url in LINKEDIN_RSS_URLS:
        try:
            feed = feedparser.parse(feed_url)
            for e in feed.entries[:max_items]:
                link = e.get("link") or ""
                title = html.unescape(e.get("title","")).strip() or "LinkedIn"
                dt = None
                for k in ("published","updated"):
                    if k in e:
                        try:
                            dt = dateparser.parse(e[k])
                            if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
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

def discover_from_gnews_linkedin(company=COMPANY):
    """Best-Effort: site:linkedin.com – EU Editionen."""
    out = []
    when_days = max(1, math.ceil(LOOKBACK_HOURS/24))
    for lang, gl, ceid in EU_GNEWS:
        base = "https://news.google.com/rss/search"
        q = f'"{company}" site:linkedin.com when:{when_days}d'
        url = base + "?" + ul.urlencode({"q": q, "hl": lang, "gl": gl, "ceid": ceid})
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:MAX_PER_SOURCE]:
                out.append(_entry_to_item(e, f"linkedin:gnews:{gl.lower()}:{lang}"))
        except Exception:
            pass
    return out

# =============================== LLM =========================================
def llm_batch_signals(company: str, texts: list[dict], limit=SIGNAL_LIMIT) -> list[dict]:
    if not OPENAI_API_KEY: return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        joined = "\n\n".join(
            f"### {t.get('title','(ohne Titel)')}\n{t.get('text','')[:5000]}"
            for t in texts
        )[:22000]

        system = (
            "Du extrahierst faktenbasierte, strukturierte Signale zur Firma – mit Fokus auf Europa/Deutschland "
            "und den E-Commerce-Kanal. Antworte NUR als JSON:\n"
            "{ \"signals\": [ {"
            "\"type\":\"financial|strategy|markets|risks|product|leadership|sustainability|ecommerce|retail_media\","
            "\"value\": {"
            "\"headline\":\"...\","
            "\"metric\":\"...\","
            "\"value\":\"...\","
            "\"unit\":\"...\","
            "\"topic\":\"...\","
            "\"summary\":\"...\","
            "\"note\":\"...\","
            "\"period\":\"...\","
            "\"region\":\"DE|EU|...\"},"
            "\"confidence\": 0.0 } ] }\n"
            "Regeln: Mindestens 4, bis zu 10 Signale. Bevorzuge EU/DE-bezogene Aussagen und E-Commerce-bezogene Fakten "
            "(Online-Umsatz, Marktplätze, Retail Media, D2C, Amazon/Zalando etc.)."
        )
        user = f"Firma: {company}\nQuellenauszüge:\n{joined}"

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role":"system","content":system},
                {"role":"user","content":user},
            ],
        )
        data = json.loads(resp.choices[0].message.content)
        out=[]
        for s in data.get("signals", []):
            if not isinstance(s, dict): continue
            s["type"] = str(s.get("type","summary"))
            try: c=float(s.get("confidence",0.5))
            except: c=0.5
            s["confidence"] = max(0.0, min(1.0, c))
            out.append(s)
        return out[:limit]
    except Exception:
        return []

def llm_per_article(company: str, article: dict) -> list[dict]:
    if not OPENAI_API_KEY: return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        text = article.get("text","")[:6000]

        system = (
            "Extrahiere bis zu 2 Signale mit Europa/Deutschland- und E-Commerce-Fokus. "
            "Nur JSON wie zuvor (type kann auch 'ecommerce' oder 'retail_media' sein)."
        )
        user = f"Firma: {company}\nTitel: {article.get('title')}\nText:\n{text}"

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type":"json_object"},
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
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
            "value":{"headline": company,
                     "summary":"Keine verwertbaren EU/DE/E-Commerce-Texte gefunden.",
                     "note":"Fallback","region":"EU"},
            "confidence":0.2,
        }]
    head = texts[0]
    return [{
        "type":"summary",
        "value":{"headline": head.get("title") or company,
                 "summary": head.get("text","")[:280],
                 "note":"Fallback","region":"EU"},
        "confidence":0.35,
    }]

def llm_generate_report_markdown(company: str, texts: list[dict], signals: list[dict], sources: list[dict],
                                 max_texts: int = REPORT_MAX_TEXTS,
                                 min_citations: int = REPORT_MIN_CITATIONS,
                                 use_only_selected_sources: bool = True):
    if not OPENAI_API_KEY:
        return "", []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        use_texts = texts[:max_texts]
        # Auszüge
        text_snippets = []
        for t in use_texts:
            snip = (t.get("text",""))[:3500]
            title = t.get("title") or "(ohne Titel)"
            text_snippets.append(f"# {title}\n{snip}")
        joined_snippets = "\n\n---\n\n".join(text_snippets)[:24000]

        # kompakte Signalsicht
        sig_lines=[]
        for s in signals[:14]:
            v=s.get("value") or {}
            sig_lines.append(
                f"- type={s.get('type','')}; headline={v.get('headline','')}; "
                f"topic={v.get('topic','')}; region={v.get('region','')}; summary={v.get('summary','')}"
            )
        signals_digest = "\n".join(sig_lines)

        # Quellenliste
        if use_only_selected_sources:
            selected_urls = {t.get("url") for t in use_texts}
            sources_for_report = [s for s in sources if s.get("url") in selected_urls]
        else:
            sources_for_report = list(sources)

        numbered_sources=[]
        for i,s in enumerate(sources_for_report, start=1):
            ttl=(s.get("title") or "").strip()
            url=(s.get("url") or "").strip()
            numbered_sources.append(f"[{i}] {ttl+' — ' if ttl else ''}{url}")
        sources_list="\n".join(numbered_sources)

        system = (
            "Erstelle einen faktenbasierten Bericht (Markdown) zur Firma mit Fokus auf Europa/Deutschland und E-Commerce. "
            "Struktur (H2):\n"
            "## Executive Summary\n"
            "## Finanzen (Europa/Deutschland)\n"
            "## E-Commerce (Europa/Deutschland)\n"
            "## Retail Media & Marktplätze (Amazon/Zalando …)\n"
            "## Strategie\n"
            "## Produkte & Innovation\n"
            "## Führung & Organisation\n"
            "## Märkte & Wettbewerb (EU/DE)\n"
            "## Nachhaltigkeit & ESG\n"
            "## Risiken\n"
            "## Ausblick\n\n"
            f"Regeln:\n- Deutsch, 700–1400 Wörter.\n- Nutze Zitatnummern [n] aus der Quellenliste; verwende mindestens {min_citations} verschiedene Quellen, sofern sinnvoll.\n"
            "- Keine PR-Sprache; fokussiere Kennzahlen/Trends für EU/DE und E-Commerce.\n"
        )

        user = (
            f"Firma: {company}\n\n"
            f"Signale (kompakt):\n{signals_digest}\n\n"
            f"Material-Auszüge:\n{joined_snippets}\n\n"
            f"Quellenliste (nur diese dürfen zitiert werden):\n{sources_list}\n\n"
            "Erzeuge den Bericht jetzt."
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

# ================================ Pipeline ===================================
def main():
    # 1) Links
    items = []
    items += discover_from_newsroom()
    items += discover_from_gnews_queries(COMPANY)
    if LINKEDIN_RSS_URLS: items += discover_from_linkedin_rss()
    if INCLUDE_GNEWS_LINKEDIN: items += discover_from_gnews_linkedin(COMPANY)
    items = dedupe(items, key="url")

    # 2) Inhalte + Datum
    enriched, sources = [], []
    for it in items:
        url, title, src = it["url"], it.get("title",""), it.get("source","")
        prefetched = it.get("prefetched_text","")

        html_ = None
        if prefetched:
            text = prefetched
        else:
            try:
                html_ = fetch(url)
                text = clean_article_text(html_)
            except Exception:
                text = ""

        dt = it.get("published_at")
        if dt:
            try: dt = dateparser.parse(dt)
            except: dt = None
        if not isinstance(dt, datetime) and html_:
            dt = extract_published_at(html_)

        min_chars = MIN_TEXT_CHARS_LINKEDIN if src.startswith("linkedin") or "linkedin.com" in url else MIN_TEXT_CHARS_ARTICLE
        if len(text) >= min_chars:
            enriched.append({
                "url": url, "title": title, "source": src,
                "published_at": dt, "text": text
            })

        sources.append({"url": url, "title": title, "source": src})

    # 3) Lookback
    enriched_recent=[]
    for a in enriched:
        dt=a.get("published_at")
        if dt and not is_recent(dt, LOOKBACK_HOURS): continue
        enriched_recent.append(a)

    # 4) Scoring: EU/DE & E-Commerce bevorzugen
    def score(a):
        L=len(a.get("text",""))
        dt=a.get("published_at")
        fresh_bonus=0.0
        if dt:
            hours=max(1.0,(now_utc()-dt).total_seconds()/3600.0)
            fresh_bonus=1.0/hours
        url=a.get("url","")
        eu_bonus=0.5 if is_eu_url(url) else 0.0
        ecom_bonus=0.4 if (has_ecom_keywords(a.get("title","")) or has_ecom_keywords(a.get("text",""))) else 0.0
        li_bonus=0.2 if ("linkedin" in a.get("source","")) else 0.0
        return L/1500.0 + fresh_bonus + eu_bonus + ecom_bonus + li_bonus

    enriched_recent.sort(key=score, reverse=True)
    selected=enriched_recent[:TOP_TEXTS]

    # 5) LLM-Signale
    signals=[]
    if OPENAI_API_KEY and selected:
        signals = llm_batch_signals(COMPANY, selected, limit=SIGNAL_LIMIT)
        if len(signals) < 4:
            for art in selected[:min(6,len(selected))]:
                signals += llm_per_article(COMPANY, art)
                if len(signals) >= SIGNAL_LIMIT: break
        # Dedupe
        ded={}
        for s in signals:
            v=s.get("value",{})
            key=(v.get("headline","").strip().lower(), v.get("topic","").strip().lower())
            if key not in ded: ded[key]=s
        signals=list(ded.values())[:SIGNAL_LIMIT]
    if not signals:
        signals = heuristic_summary(COMPANY, selected)

    # 6) Bericht
    report_md, report_used_sources = "", []
    try:
        report_md, report_used_sources = llm_generate_report_markdown(COMPANY, selected, signals, sources)
    except Exception:
        pass

    # 7) Schreiben
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
            "top_texts": TOP_TEXTS,
            "signal_limit": SIGNAL_LIMIT,
            "eu_bias": True,
            "ecommerce_bias": True
        }
    }
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    eu_count = sum(1 for s in sources if is_eu_url(s.get("url","")))
    print(f"Wrote data/latest.json with {len(signals)} signals; sources={len(sources)} (EU={eu_count}); selected={len(selected)}.")

if __name__ == "__main__":
    main()
