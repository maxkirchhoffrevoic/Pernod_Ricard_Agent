# scripts/build_json.py
import os, json, time, hashlib, re
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup

# -------------------------
# Konfiguration
# -------------------------
COMPANY = "Pernod Ricard"
NEWS_INDEX = "https://www.pernod-ricard.com/en/media"   # Newsroom-Übersicht
MAX_ARTICLES = 6                                        # wie viele neue Artikel pro Lauf auswerten
TIMEOUT = 30

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# -------------------------
# HTTP helpers
# -------------------------
def http_get(url: str) -> requests.Response:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # häufig sitzen Inhalte in <article>, <main>, <section>
    main = soup.find(["article", "main"]) or soup
    text = main.get_text(" ", strip=True)
    # Mehrfach-Spaces reduzieren
    text = re.sub(r"\s+", " ", text)
    return text

def parse_title_and_date(html: str, fallback_url: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = (
        (soup.find("meta", property="og:title") or {}).get("content")
        or (soup.title.string.strip() if soup.title else "")
        or fallback_url
    )
    # Veröffentlichungsdatum (falls vorhanden)
    pub = (soup.find("meta", {"name": "date"}) or {}).get("content") \
          or (soup.find("meta", property="article:published_time") or {}).get("content") \
          or ""
    return title.strip(), pub.strip()

def discover_article_links(index_url: str, max_n: int) -> List[str]:
    """Findet Artikel-Links auf der News-Übersichtsseite."""
    r = http_get(index_url)
    soup = BeautifulSoup(r.text, "html.parser")
    links = []

    # Alle internen Links, die nach News-Artikeln aussehen
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = "https://www.pernod-ricard.com" + href
        # Heuristik: englische Medienseiten
        if "/en/media/" in href and len(href) < 200:
            links.append(href)

    # Deduplizieren, Reihenfolge beibehalten
    seen = set()
    uniq = []
    for u in links:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
        if len(uniq) >= max_n:
            break
    return uniq

# -------------------------
# LLM-Extraktion (optional)
# -------------------------
def llm_extract_signals(texts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ruft OpenAI auf, wenn OPENAI_API_KEY gesetzt ist.
    Gibt eine Liste einheitlicher 'signals'-Objekte zurück.
    """
    api_key = os.environ.get("OPENAI_API_KEY") or ""
    if not api_key:
        # Fallback ohne LLM: generische Zusammenfassung
        approx_words = sum(len(t["text"].split()) for t in texts)
        return [{
            "type": "summary",
            "value": {
                "note": f"Automatische Kurz-Zusammenfassung ohne LLM. "
                        f"{len(texts)} Quelle(n), ~{approx_words} Wörter extrahiert."
            },
            "confidence": 0.4,
        }]

    try:
        # OpenAI SDK v1.x
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        # Wir chunk-en sanft, damit Tokens klein bleiben
        joined = "\n\n---\n\n".join(
            f"TITLE: {t['title']}\nURL: {t['url']}\nTEXT: {t['text'][:8000]}"
            for t in texts
        )

        system = (
            "Extrahiere strukturierte, faktenbasierte Signale zu einer Firma. "
            "Erlaube Typen wie: financial, strategy, sustainability, markets, risks, leadership, product. "
            "Antworte ausschließlich als JSON mit einem Array 'signals', "
            "jedes Element: {type: string, value: object, confidence: number}."
        )
        user = (
            f"Firma: {COMPANY}\n\n"
            f"Quellentexte (gekürzt):\n{joined}\n\n"
            "Liefere 3–6 prägnante Signale. confidence zwischen 0 und 1."
        )

        # gpt-4o-mini ist günstig/robust
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
        signals = data.get("signals", [])
        # Minimal validieren
        out = []
        for s in signals:
            out.append({
                "type": str(s.get("type", "other"))[:40],
                "value": s.get("value", {}),
                "confidence": float(s.get("confidence", 0.5)),
            })
        return out or [{
            "type": "summary",
            "value": {"note": "LLM lieferte kein verwertbares JSON."},
            "confidence": 0.3,
        }]
    except Exception as e:
        return [{
            "type": "summary",
            "value": {"note": f"LLM-Extraktion fehlgeschlagen: {e}"},
            "confidence": 0.2,
        }]

# -------------------------
# Orchestrierung
# -------------------------
def main():
    # 1) Artikel finden
    links = discover_article_links(NEWS_INDEX, MAX_ARTICLES)

    # 2) Inhalte holen
    items = []
    for url in links:
        try:
            r = http_get(url)
            title, pub = parse_title_and_date(r.text, url)
            txt = text_from_html(r.text)
            items.append({
                "url": url,
                "title": title,
                "published_at": pub,
                "text": txt,
                "hash": hashlib.sha256((url + ":" + txt[:2000]).encode("utf-8")).hexdigest()
            })
        except Exception as e:
            # Quelle überspringen, aber notieren
            items.append({
                "url": url, "title": url, "published_at": "",
                "text": "", "error": str(e), "hash": hashlib.sha256(url.encode()).hexdigest()
            })

    # 3) Signale extrahieren (LLM oder Fallback)
    texts_for_llm = [{"url": it["url"], "title": it["title"], "text": it.get("text","")} for it in items if it.get("text")]
    signals = llm_extract_signals(texts_for_llm)

    # 4) JSON schreiben
    out = {
        "company": COMPANY,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "signals": signals,
        "sources": [{"url": it["url"], "title": it["title"], "published_at": it["published_at"]} for it in items],
    }

    os.makedirs("data", exist_ok=True)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Wrote data/latest.json with {len(signals)} signals and {len(items)} sources.")

if __name__ == "__main__":
    main()
