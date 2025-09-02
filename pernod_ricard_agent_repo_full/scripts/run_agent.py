# run_agent.py
import asyncio
from scraper import fetch_url, hash_text
from extractor import call_llm_extract
from db import engine
from sqlalchemy import text

# minimal orchestrator: seed a few Pernod URLs and run
SEED_URLS = [
    'https://www.pernod-ricard.com/en/media/fy25-full-year-sales-and-results',
    'https://www.pernod-ricard.com/en/newsroom',
]

async def main():
    for u in SEED_URLS:
        try:
            doc = await fetch_url(u)
            txt = doc['text']
            h = hash_text(txt)
            # naive: check DB for hash
            with engine.connect() as conn:
                res = conn.execute(text("select id from source where hash = :h"), {"h": h}).fetchone()
                if res:
                    print('already saw', u)
                    continue
                # call extractor
                try:
                    ex = call_llm_extract(txt)
                except Exception as e:
                    print('extract failed', e)
                    continue
                # store source + signals (naive)
                ins = text("insert into source(url,title,published_at,raw_text,hash) values(:url,:title,null,:txt,:h) returning id")
                sid = conn.execute(ins, {"url":u, "title":doc['title'], "txt":txt, "h":h}).scalar()
                for s in ex.signals:
                    conn.execute(text("insert into signal(company_id, type, value, confidence, source_ids) values (null,:type,:value::jsonb,:conf,:sids)"),
                                 {"type": s.type, "value": json.dumps(s.value), "conf": s.confidence, "sids": [sid]})
                print('processed', u)
        except Exception as e:
            print('error', u, e)

if __name__ == '__main__':
    asyncio.run(main())
