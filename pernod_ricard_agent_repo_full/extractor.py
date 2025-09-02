# extractor.py
"""
Extrahiert strukturierte Signale (Financials, RegionalPerformance, etc.)
aus Rohtext mithilfe eines LLM. Liest OPENAI_API_KEY aus .env oder
Streamlit Secrets. Erwartet die Prompt-Datei unter prompts/extract_prompt.txt.
"""

from __future__ import annotations
import os
import re
import json
from typing import List, Optional

# .env laden (lokal)
from dotenv import load_dotenv
load_dotenv()

# Streamlit-Secrets als Fallback
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None  # Streamlit ist lokal evtl. nicht installiert

# OpenAI-Client initialisieren
import openai

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY and st is not None and "OPENAI_API_KEY" in st.secrets:
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]

if not OPENAI_API_KEY:
    raise RuntimeError(
        "OPENAI_API_KEY not set. Hinterlege ihn lokal in .env oder in Streamlit Cloud unter Secrets."
    )

openai.api_key = OPENAI_API_KEY

# -----------------------------
# Modelle (Pydantic)
# -----------------------------
from pydantic import BaseModel, ValidationError, Field


class Signal(BaseModel):
    type: str = Field(..., description="Signaltyp, z. B. Financials, Restructuring, ...")
    value: dict = Field(..., description="Strukturierte Payload (Zahl, Einheit, Zeitraum, Notizen).")
    verbatim: Optional[str] = Field(None, description="Kurzes Belegzitat (<=20 Wörter).")
    confidence: float = Field(..., ge=0.0, le=1.0, description="0..1 Vertrauensgrad")


class ExtractionResult(BaseModel):
    company: str
    signals: List[Signal]
    detected_at: Optional[str] = None


# -----------------------------
# Prompt laden
# -----------------------------
def _load_prompt() -> str:
    # Robuster Pfad: funktioniert auch, wenn Skript woanders aufgerufen wird
    here = os.path.dirname(os.path.abspath(__file__))
    prompt_path = os.path.join(here, "prompts", "extract_prompt.txt")
    if not os.path.exists(prompt_path):
        raise FileNotFoundError(
            f"Prompt-Datei nicht gefunden: {prompt_path}. "
            "Lege sie unter prompts/extract_prompt.txt ab."
        )
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


# -----------------------------
# JSON-Parsing Hilfen
# -----------------------------
_JSON_BLOCK_RE = re.compile(
    r"(?:```json\s*)?(\{[\s\S]*\})(?:\s*```)?", re.IGNORECASE
)


def _coerce_json(payload: str) -> dict:
    """
    Versucht mehrere Strategien, um valides JSON aus einem LLM-String zu gewinnen:
    1) Direkt json.loads
    2) JSON-Block zwischen ```json ... ``` extrahieren
    3) Größtes {...}-Objekt per Regex extrahieren
    """
    # 1) Direkt
    try:
        return json.loads(payload)
    except Exception:
        pass

    # 2/3) Codefence oder größtes Objekt
    m = _JSON_BLOCK_RE.search(payload)
    if m:
        candidate = m.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            # Letzter Rettungsanker: geschweifte Klammern austarieren
            pass

    # 4) Einfache Klammer-Balance-Heuristik (stabil bei einzelnen Top-Level-Objekten)
    start = payload.find("{")
    end = payload.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(payload[start : end + 1])
        except Exception:
            pass

    raise ValueError("Konnte keine gültige JSON-Antwort aus dem LLM-Output extrahieren.")


# -----------------------------
# LLM-Aufruf
# -----------------------------
def call_llm_extract(
    text: str,
    company: str = "Pernod Ricard",
    model: str = "gpt-4o-mini",
    max_tokens: int = 900,
    temperature: float = 0.1,
) -> ExtractionResult:
    """
    Ruft das LLM auf, erzwingt JSON-Output und validiert gegen Pydantic.
    """
    base_prompt = _load_prompt()
    prompt = (
        base_prompt.replace("<<COMPANY>>", company)
        .replace("<<SOURCE_TEXT>>", text[:40_000])  # Sicherheitslimit
    )

    # ChatCompletion (kompatibel mit vielen OpenAI-Versionen)
    resp = openai.ChatCompletion.create(
        model=model,
        temperature=temperature,
        messages=[
            {"role": "system", "content": "Du bist ein faktenorientierter Extraktor. Antworte ausschließlich als JSON."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=max_tokens,
    )

    content = resp["choices"][0]["message"]["content"]
    data = _coerce_json(content)

    try:
        result = ExtractionResult(**data)
    except ValidationError as e:
        # Für Debugging in Logs hilfreich
        raise ValueError(f"Pydantic-Validierung fehlgeschlagen: {e}") from e

    # Sanity-Check: Liste vorhanden, sonst leeres Array setzen
    if result.signals is None:
        result.signals = []

    return result


# -----------------------------
# Convenience-Wrapper
# -----------------------------
def extract_signals(text: str, company: str = "Pernod Ricard") -> List[Signal]:
    """
    Liefert direkt die Liste der Signale (Kurzform).
    """
    res = call_llm_extract(text=text, company=company)
    return res.signals


# -----------------------------
# Optionaler CLI-Test
# -----------------------------
if __name__ == "__main__":
    demo = (
        "Pernod Ricard reports FY25 net sales €10.959bn "
        "with organic growth -3.0%. China sales down about 21%."
    )
    try:
        out = call_llm_extract(demo, company="Pernod Ricard")
        print(json.dumps(out.dict(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print("Extraction error:", exc)
