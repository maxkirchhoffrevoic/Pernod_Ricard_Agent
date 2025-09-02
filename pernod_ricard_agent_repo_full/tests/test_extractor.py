# test_extractor.py
from extractor import call_llm_extract

SAMPLE = "Pernod Ricard reports FY25 net sales €10.959bn, organic -3.0%; China sales down c.21%."

def test_extract_basic():
    res = call_llm_extract(SAMPLE)
    assert res.company.lower().startswith('pernod')
    assert any(s.type == 'Financials' for s in res.signals)
