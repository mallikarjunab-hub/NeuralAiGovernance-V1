"""
Gemini Service — domain logic layer for DSSS AI analytics
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

from backend.services.ai_service import ai_call, embed_text as _embed_text, ai_health, web_grounded_search as _web_search
from backend.services.prompt_assembler import (
    build_question_resolver_prompt,
    build_intent_prompt,
    build_sql_prompt,
    build_nl_answer_prompt,
    build_rag_answer_prompt,
    build_reason_prompt,
    is_followup,
    is_reason_question,
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
    """
    Classify question as SQL, RAG, or REASON using Gemini with zero temperature.

    Fast-path: if the local heuristic `is_reason_question` is confident the
    question is reasoning over prior data, skip the API call entirely.
    """
    # Cheap local short-circuit — saves an API call when it's obvious.
    if is_reason_question(question, context or []):
        return "REASON"

    prompt = build_intent_prompt(question, context)
    try:
        r = await ai_call(prompt, temperature=0.0, max_tokens=8)
        i = r.strip().upper().split()[0] if r.strip() else "SQL"
        if i in ("SQL", "RAG", "REASON"):
            # Guard: model may say REASON when there's no prior data — downgrade.
            if i == "REASON":
                analytical = [t for t in (context or []) if t.intent != "EDGE" and t.sql_data]
                if not analytical:
                    return "SQL"
            return i
        return "SQL"
    except Exception as e:
        logger.warning("Intent classification failed, defaulting to SQL: %s", e)
        return "SQL"


async def answer_from_context(
    question: str,
    context: list[ConversationTurn],
    language: str = "en",
) -> str:
    """
    Answer the user's question by REASONING over data already in conversation
    history. No SQL, no RAG, no web search. Used for "why is X the lowest?",
    "explain that", "summarize the trend", "which one is the highest among
    these?" — questions that should NOT trigger a fresh data fetch.
    """
    prompt = build_reason_prompt(question, context, language)
    try:
        return await ai_call(prompt, temperature=0.2, max_tokens=512)
    except Exception as e:
        logger.warning("answer_from_context failed: %s", e)
        # Graceful degradation — let the caller fall back to SQL if it wants.
        raise


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
            "This information is not available in the DSSS knowledge base. "
            "Please contact the Directorate of Social Welfare, Government of Goa."
        )
    prompt = build_rag_answer_prompt(question, chunks, language, context)
    return await ai_call(prompt, temperature=0.15, max_tokens=512)


# ── Web Search Fallback (Agentic RAG) ────────────────────────────────────────

async def web_search_fallback(question: str, language: str = "en") -> dict | None:
    """
    Agentic RAG fallback: when local RAG has no answer, use Gemini with
    Google Search grounding to find the answer from the web.

    Returns:
        {"answer": str, "sources": list[dict], "grounded": bool} or None on failure.
    """
    try:
        result = await _web_search(question, max_tokens=768)
        if not result or not result.get("answer"):
            return None

        # If language is not English, translate the answer
        if language != "en" and result["answer"]:
            lang_names = {
                "hi": "Hindi", "kn": "Kannada",
                "mr": "Marathi", "kok": "Konkani",
            }
            lang_name = lang_names.get(language)
            if lang_name:
                translate_prompt = (
                    f"Translate the following text to {lang_name}. "
                    f"Keep numbers, proper nouns, and scheme names (like DSSS) in English. "
                    f"Return only the translation:\n\n{result['answer']}"
                )
                try:
                    translated = await ai_call(translate_prompt, temperature=0.1, max_tokens=768)
                    result["answer"] = translated.strip()
                except Exception:
                    pass  # keep English answer if translation fails

        return result
    except Exception as e:
        logger.warning("Web search fallback failed: %s", e)
        return None


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
    """
    Suggest chart type based on result data shape.

    Rules (in order):
      1. Single number result (1 row, all numeric cols or 1 col) → None (KPI, no chart)
      2. Year/payment_year col + 2–8 rows + ≥2 numeric metric cols → grouped_bar
         (covers 3-year comparison = 3 rows, 6-year trend with multiple cols = 6 rows)
      3. Time-series label col (month/date/period/week/quarter/trend) → line
      4. Year col with >8 rows → line (many-year trend)
      5. ≤6 categorical rows → doughnut
      6. >6 categorical rows → bar
    """
    if not results:
        return None
    cols = list(results[0].keys())

    # Rule 1a: Truly single KPI — one row, one column
    if len(results) == 1 and len(cols) == 1:
        return None

    # Rule 1b: One row with only one meaningful numeric value → KPI, no chart
    if len(results) == 1:
        num_vals = [c for c in cols if _is_num(results[0].get(c))]
        if len(num_vals) <= 1:
            return None

    if len(cols) < 2:
        return None

    # Classify columns: year/payment_year are always treated as label columns
    # even though their values are integers (e.g. 2023, 2024, 2025).
    _YEAR_COLS = {"year", "payment_year", "registration_year", "fiscal_year", "due_year"}

    def _is_year_col(c: str) -> bool:
        return c.lower() in _YEAR_COLS or c.lower().endswith("_year")

    lbl = [c for c in cols if _is_year_col(c) or not all(_is_num(r.get(c)) for r in results)]
    num = [c for c in cols if c not in lbl and all(_is_num(r.get(c)) for r in results)]

    if not num:
        return None
    if not lbl:
        # All numeric — single row is a KPI
        if len(results) == 1:
            return None
        return "bar"

    lc = lbl[0].lower()

    # Rule 2: Year/payment_year + 2–8 rows + ≥2 metric numeric cols → grouped_bar
    # Expanded from 5 to 8 rows to accommodate 6-year payment_summary data.
    if (
        (_is_year_col(lbl[0]) or "year" in lc)
        and 2 <= len(results) <= 8
        and len(num) >= 2
    ):
        return "grouped_bar"

    # Rule 3: Time-series label → line
    if any(k in lc for k in ["month", "date", "period", "week", "quarter", "trend"]):
        return "line"

    # Rule 4: Year col with many rows → line (year-over-year trend)
    if _is_year_col(lbl[0]) and len(results) > 8:
        return "line"

    # Rule 5: Few categorical rows → doughnut
    if len(results) <= 6:
        return "doughnut"

    # Rule 6: Many categorical rows → bar
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
