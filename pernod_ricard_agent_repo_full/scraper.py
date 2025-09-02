# scraper.py
import httpx
from bs4 import BeautifulSoup
from readability import Document
from urllib.parse import urljoin
import hashlib
from dateutil import parser as dateparser

USER_AGENT = "PernodAgent/1.0 (+https://yourorg.example)"

async def fetch_url(url: str, timeout: int = 15) -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}, timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        text = r.text
        doc = Document(text)
        title = doc.short_title()
        content = doc.summary()
        # strip html
        soup = BeautifulSoup(content, "html.parser")
        plain = soup.get_text(separator=" ").strip()
        # try to detect a date from meta tags
        published = None
        try:
            # naive: look for time tag
            t = soup.find('time')
            if t and t.get('datetime'):
                published = dateparser.parse(t.get('datetime'))
        except Exception:
            published = None

        return {"url": url, "title": title, "text": plain, "published": published}

def hash_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# simple sync helper
def fetch_sync(url: str) -> dict:
    import asyncio
    return asyncio.get_event_loop().run_until_complete(fetch_url(url))
