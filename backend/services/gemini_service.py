"""
Gemini Service — Google AI Studio
All AI calls: intent classify · question resolve · SQL gen · NL answer · embeddings · RAG answer

Prompts are assembled by prompt_assembler.py — no inline prompt strings here.
"""
import re, logging, httpx
from backend.config import settings
from backend.services.prompt_assembler import (
    build_question_resolver_prompt,
    build_intent_prompt,
    build_sql_prompt,
    build_nl_answer_prompt,
    build_rag_answer_prompt,
    is_followup,
)
from backend.services.context_store import ConversationTurn

logger    = logging.getLogger(__name__)
BASE      = "https://generativelanguage.googleapis.com/v1beta"
CHAT      = "gemini-2.5-flash-lite"
EMBED     = "gemini-embedding-001"
EMBED_DIM = 768

FORBIDDEN = [r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b', r'\bDROP\b',
             r'\bCREATE\b', r'\bALTER\b', r'\bTRUNCATE\b', r'\bMERGE\b']


# ── Question Resolver ─────────────────────────────────────────────────────────

async def resolve_question(question: str, context: list[ConversationTurn]) -> str:
    """
    If the question is a follow-up, rewrite it as a complete standalone question
    using conversation context (prior questions, answers, and raw data).

    Returns the original question unchanged if:
      - No context exists
      - Heuristic says it's not a follow-up (saves an API call)
      - Gemini returns an empty response
    """
    if not is_followup(question, context):
        return question
    prompt = build_question_resolver_prompt(question, context)
    try:
        resolved = await _call(prompt, 0.0, max_tokens=150)
        resolved = resolved.strip().strip('"').strip("'")
        return resolved if resolved else question
    except Exception as e:
        logger.warning(f"Question resolver failed, using original: {e}")
        return question


# ── Intent Classification ─────────────────────────────────────────────────────

async def classify_intent(question: str, context: list[ConversationTurn] = None) -> str:
    """Returns 'SQL' or 'RAG'. Edge cases handled before this is called."""
    prompt = build_intent_prompt(question, context)
    try:
        r = await _call(prompt, 0.0)
        i = r.strip().upper().split()[0] if r.strip() else "SQL"
        return i if i in ("SQL", "RAG") else "SQL"
    except Exception as e:
        logger.warning(f"Intent classification failed, defaulting to SQL: {e}")
        return "SQL"


# ── SQL Generation ────────────────────────────────────────────────────────────

async def generate_sql(question: str, context: list[ConversationTurn] = None) -> tuple[str, float]:
    prompt = build_sql_prompt(question, context)
    raw    = await _call(prompt, 0.05)
    sql    = _clean_sql(raw)
    conf   = 0.0 if "CANNOT_ANSWER" in sql else 0.9
    logger.info(f"Generated SQL: {sql[:150]}")
    return sql, conf


# ── Natural Language Answer ───────────────────────────────────────────────────

async def generate_nl_answer(
    question: str, sql: str, results: list, row_count: int,
    language: str = "en", context: list[ConversationTurn] = None,
) -> str:
    if row_count == 0:
        no_data = {
            "en": "No records found for this query. Please try a different filter or question.",
            "hi": "इस प्रश्न के लिए कोई रिकॉर्ड नहीं मिला। कृपया कोई अन्य प्रश्न आज़माएं।",
            "te": "ఈ ప్రశ్నకు రికార్డులు కనుగొనబడలేదు। దయచేసి వేరే ప్రశ్న ప్రయత్నించండి।",
        }
        return no_data.get(language, no_data["en"])
    prompt = build_nl_answer_prompt(question, sql, results, row_count, language, context)
    return await _call(prompt, 0.2)


# ── RAG Answer ────────────────────────────────────────────────────────────────

async def rag_answer(
    question: str, chunks: list[str], language: str = "en",
    context: list[ConversationTurn] = None,
) -> str:
    if not chunks:
        return ("This information is not available in the DSSY knowledge base. "
                "Please contact the Directorate of Social Welfare, Government of Goa.")
    prompt = build_rag_answer_prompt(question, chunks, language, context)
    return await _call(prompt, 0.15)


# ── Embeddings ────────────────────────────────────────────────────────────────

async def embed_text(text: str) -> list[float]:
    url = f"{BASE}/models/{EMBED}:embedContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "model": f"models/{EMBED}",
        "content": {"parts": [{"text": text[:2000]}]},
        "outputDimensionality": EMBED_DIM,
    }
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(url, json=payload)
    r.raise_for_status()
    return r.json()["embedding"]["values"]


# ── Health Check ──────────────────────────────────────────────────────────────

async def check_health() -> bool:
    try:
        return len(await _call("Say OK", 0.0)) > 0
    except Exception:
        return False


# ── SQL Validation ────────────────────────────────────────────────────────────

def validate_sql(sql: str) -> tuple[bool, str]:
    u = sql.upper().strip()
    if not (u.startswith("SELECT") or u.startswith("WITH")):
        return False, "Only SELECT/WITH statements allowed"
    for p in FORBIDDEN:
        if re.search(p, sql, re.IGNORECASE):
            return False, "Forbidden keyword detected"
    if u.count("(") != u.count(")"):
        return False, "Unbalanced parentheses"
    return True, "OK"


# ── Chart Suggestion ──────────────────────────────────────────────────────────

def suggest_chart(results: list) -> str | None:
    if not results or len(results) < 2:
        return None
    cols = list(results[0].keys())
    if len(cols) < 2:
        return None
    num = [c for c in cols if all(_is_num(r.get(c)) for r in results)]
    lbl = [c for c in cols if c not in num]
    if not num or not lbl:
        return None
    lc = lbl[0].lower()
    if any(k in lc for k in ["month", "year", "date", "period", "week", "quarter", "trend"]):
        return "line"
    if len(results) <= 6:
        return "doughnut"
    return "bar"


# ── Internal Helpers ──────────────────────────────────────────────────────────

def _is_num(v):
    try:
        float(str(v))
        return True
    except (ValueError, TypeError):
        return False

def _clean_sql(raw: str) -> str:
    sql = re.sub(r"```sql\s*", "", raw, flags=re.IGNORECASE)
    sql = re.sub(r"```\s*", "", sql).strip()
    if sql and not sql.endswith(";"):
        sql += ";"
    return sql

async def _call(prompt: str, temp: float, max_tokens: int = 1024) -> str:
    url = f"{BASE}/models/{CHAT}:generateContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temp, "maxOutputTokens": max_tokens},
    }
    async with httpx.AsyncClient(timeout=60.0) as c:
        r = await c.post(url, json=payload)
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
