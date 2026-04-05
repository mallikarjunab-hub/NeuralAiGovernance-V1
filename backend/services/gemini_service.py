"""
Gemini Service — domain logic layer for DSSY AI analytics
============================================================
Handles: intent classification · question resolution · SQL generation ·
         NL answer generation · RAG answer synthesis · chart suggestion ·
         SQL validation

All actual HTTP calls go through ai_service.py (retry, circuit breaker,
deduplication, token guard). Nothing here touches httpx directly.

Architecture:
  query.py (router)
      │
  gemini_service.py ◄── YOU ARE HERE (domain logic)
      │
  ai_service.py     (transport: retry, circuit breaker, dedup)
      │
  httpx → Gemini API
"""
import re
import logging

from backend.services.ai_service import ai_call, embed_text as _embed_text, ai_health
from backend.services.prompt_assembler import (
    build_question_resolver_prompt,
    build_intent_prompt,
    build_sql_prompt,
    build_nl_answer_prompt,
    build_rag_answer_prompt,
    is_followup,
)
from backend.services.context_store import ConversationTurn

# Re-export for query.py (transcribe endpoint needs these)
from backend.services.ai_service import _BASE as BASE, _CHAT_MODEL as CHAT

logger = logging.getLogger(__name__)

# SQL keywords that must NEVER appear in generated queries
FORBIDDEN_SQL = [
    r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b', r'\bDROP\b',
    r'\bCREATE\b', r'\bALTER\b', r'\bTRUNCATE\b', r'\bMERGE\b',
    r'\bGRANT\b', r'\bREVOKE\b', r'\bEXEC\b', r'\bEXECUTE\b',
]


# ── Question Resolver ─────────────────────────────────────────────────────────

async def resolve_question(question: str, context: list[ConversationTurn]) -> str:
    """Resolve follow-up questions into standalone questions using conversation context."""
    if not is_followup(question, context):
        return question
    prompt = build_question_resolver_prompt(question, context)
    try:
        resolved = await ai_call(prompt, temperature=0.0, max_tokens=150)
        resolved = resolved.strip().strip('"').strip("'")
        return resolved if resolved else question
    except Exception as e:
        logger.warning("Question resolver failed, using original: %s", e)
        return question


# ── Intent Classification ─────────────────────────────────────────────────────

async def classify_intent(question: str, context: list[ConversationTurn] = None) -> str:
    """Classify question as SQL or RAG using Gemini with zero temperature."""
    prompt = build_intent_prompt(question, context)
    try:
        r = await ai_call(prompt, temperature=0.0, max_tokens=8)
        i = r.strip().upper().split()[0] if r.strip() else "SQL"
        return i if i in ("SQL", "RAG") else "SQL"
    except Exception as e:
        logger.warning("Intent classification failed, defaulting to SQL: %s", e)
        return "SQL"


# ── SQL Generation (NL-to-SQL Engine) ────────────────────────────────────────
#
# This is the core NL-to-SQL engine. It:
#   1. Takes a natural language question + conversation context
#   2. Sends it to Gemini with full schema + COUNTS_GUARD + few-shot examples
#   3. Returns clean, validated PostgreSQL SQL
#   4. Temperature 0.05 for near-deterministic output
#   5. COUNTS_GUARD prevents hallucinated numbers
#   6. Few-shot examples cover 60+ query patterns including:
#      - Last 3 years payout comparison
#      - YoY payment trends
#      - Life certificate compliance
#      - Payment batch analysis
#      - Category/district/taluka cross-breakdowns

async def generate_sql(question: str, context: list[ConversationTurn] = None) -> tuple[str, float]:
    """
    Generate PostgreSQL SQL from natural language question.

    Returns:
        (sql, confidence) — confidence is 0.0 for CANNOT_ANSWER, 0.9 otherwise
    """
    prompt = build_sql_prompt(question, context)
    raw = await ai_call(prompt, temperature=0.05, max_tokens=512)
    sql = _clean_sql(raw)
    conf = 0.0 if "CANNOT_ANSWER" in sql else 0.9
    logger.info("Generated SQL: %s", sql[:150])
    return sql, conf


# ── Natural Language Answer ───────────────────────────────────────────────────

async def generate_nl_answer(
    question: str, sql: str, results: list, row_count: int,
    language: str = "en", context: list[ConversationTurn] = None,
) -> str:
    """Generate human-readable answer from SQL results with anti-hallucination grounding."""
    if row_count == 0:
        msgs = {
            "en": "No records found for this query. Please try a different filter or question.",
            "hi": "इस प्रश्न के लिए कोई रिकॉर्ड नहीं मिला। कृपया कोई अन्य प्रश्न आज़माएं।",
            "te": "ఈ ప్రశ్నకు రికార్డులు కనుగొనబడలేదు. దయచేసి వేరే ప్రశ్న ప్రయత్నించండి.",
            "kn": "ಈ ಪ್ರಶ್ನೆಗೆ ದಾಖಲೆಗಳು ಕಂಡುಬಂದಿಲ್ಲ. ದಯವಿಟ್ಟು ಬೇರೆ ಪ್ರಶ್ನೆ ಪ್ರಯತ್ನಿಸಿ.",
            "mr": "या प्रश्नासाठी कोणत्याही नोंदी आढळल्या नाहीत. कृपया वेगळा प्रश्न विचारा.",
            "kok": "ह्या प्रश्नाक कोणत्योच नोंदी मेळ्ळ्यो नात. कृपया वेगळो प्रश्न विचारा.",
        }
        return msgs.get(language, msgs["en"])
    prompt = build_nl_answer_prompt(question, sql, results, row_count, language, context)
    return await ai_call(prompt, temperature=0.2, max_tokens=512)


# ── RAG Answer ────────────────────────────────────────────────────────────────

async def rag_answer(
    question: str, chunks: list[str], language: str = "en",
    context: list[ConversationTurn] = None,
) -> str:
    """Generate document-grounded answer from RAG chunks. No hallucination — only answers from context."""
    if not chunks:
        return (
            "This information is not available in the DSSY knowledge base. "
            "Please contact the Directorate of Social Welfare, Government of Goa."
        )
    prompt = build_rag_answer_prompt(question, chunks, language, context)
    return await ai_call(prompt, temperature=0.15, max_tokens=512)


# ── Embeddings ────────────────────────────────────────────────────────────────

async def embed_text(text: str) -> list[float]:
    """Generate 768-dim embedding vector using Gemini embedding-001."""
    return await _embed_text(text)


# ── Health Check ──────────────────────────────────────────────────────────────

async def check_health() -> bool:
    result = await ai_health()
    return result["status"] in ("ok", "degraded")


# ── SQL Validation (strict whitelist) ────────────────────────────────────────

def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate generated SQL against strict rules:
    - Only SELECT/WITH statements allowed
    - No DDL or DML keywords
    - Balanced parentheses
    - No multiple statements (no semicolons mid-query)
    """
    u = sql.upper().strip().rstrip(";").strip()
    if not (u.startswith("SELECT") or u.startswith("WITH")):
        return False, "Only SELECT/WITH statements allowed"
    for p in FORBIDDEN_SQL:
        if re.search(p, sql, re.IGNORECASE):
            return False, f"Forbidden keyword detected: {p}"
    if u.count("(") != u.count(")"):
        return False, "Unbalanced parentheses"
    # Check for multiple statements (SQL injection guard)
    clean = re.sub(r"'[^']*'", "", sql)  # remove string literals
    if ";" in clean.rstrip(";"):
        return False, "Multiple statements not allowed"
    return True, "OK"


# ── Chart Suggestion ──────────────────────────────────────────────────────────

def suggest_chart(results: list) -> str | None:
    """Suggest chart type based on result data shape."""
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


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_num(v) -> bool:
    try:
        float(str(v))
        return True
    except (ValueError, TypeError):
        return False


def _clean_sql(raw: str) -> str:
    """Strip markdown code fences and normalize SQL output."""
    sql = re.sub(r"```sql\s*", "", raw, flags=re.IGNORECASE)
    sql = re.sub(r"```\s*", "", sql).strip()
    if sql and not sql.endswith(";"):
        sql += ";"
    return sql
