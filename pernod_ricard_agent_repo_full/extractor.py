# extractor.py
from pydantic import BaseModel, ValidationError
from typing import List, Optional
import os
import openai
import json
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

PROMPT = open("prompts/extract_prompt.txt").read()

class Signal(BaseModel):
    type: str
    value: dict
    verbatim: Optional[str]
    confidence: float

class ExtractionResult(BaseModel):
    company: str
    signals: List[Signal]
    detected_at: Optional[str]

def call_llm_extract(text: str, company: str = "Pernod Ricard") -> ExtractionResult:
    full_prompt = PROMPT.replace("<<COMPANY>>", company).replace("<<SOURCE_TEXT>>", text)
    # Use function-calling or plain completions depending on model availability
    resp = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role":"system","content":"Du bist ein faktenorientierter Extraktor."},
            {"role":"user","content": full_prompt}
        ],
        max_tokens=800
    )
    content = resp['choices'][0]['message']['content']
    try:
        j = json.loads(content)
    except Exception:
        # fallback: try to extract json block
        import re
        m = re.search(r"\{[\s\S]*\}", content)
        j = json.loads(m.group(0)) if m else None

    if j is None:
        raise ValueError("LLM returned no JSON")

    try:
        result = ExtractionResult(**j)
        return result
    except ValidationError as e:
        print('ValidationError from pydantic:', e)
        raise
