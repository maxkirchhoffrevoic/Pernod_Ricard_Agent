# scripts/build_json.py
import os, json, time, requests
from bs4 import BeautifulSoup
from extractor import call_llm_extract  # Ihre vorhandene Datei

COMPANY = "Pernod Ricard"
URLS = [
    "https://www.pernod-ricard.com/en/media/fy25-full-year-sales-and-results",
    # fügen Sie weitere Quellen hinzu
]

def fetch_html_text(url: str) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for s in soup(["script", "style", "noscript"]):
        s.extract()
    text = soup.get_text(" ", strip=True)
    title = soup.title.text.strip() if soup.title else url
    return {"url": url, "title": title, "text": text[:15000]}

def main():
    items = [fetch_html_text(u) for u in URLS]
    big_text = "\n\n".join(i["text"] for i in items)

    # LLM-Extraktion (nutzt OPENAI_API_KEY aus Env/Secrets)
    res = call_llm_extract(big_text, company=COMPANY)
    signals = [s.dict() for s in res.signals]

    out = {
        "company": COMPANY,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "signals": signals,
        "sources": [{"url": i["url"], "title": i["title"]} for i in items],
    }

    os.makedirs("data", exist_ok=True)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
