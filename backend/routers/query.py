"""
/api/query — single endpoint, 3-way auto-routing:
  1. EDGE  : greetings, identity, silly, off-topic → instant canned response (no API cost)
  2. SQL   : data question → Gemini generates BigQuery SQL → execute → NL answer + chart
  3. RAG   : scheme knowledge → Neon pgvector hybrid search → Gemini answer from DSSY docs

Multi-turn conversation:
  - session_id ties requests together across a browser session
  - Each question is first resolved into a standalone question using prior context
    ("what about inactive?" → "How many inactive beneficiaries are there?")
  - Resolved question + raw SQL data is stored per turn so arithmetic follow-ups
    ("sum of active and inactive?") produce correct, coherent answers

Smart fallback: SQL fail → try RAG.  RAG low confidence → try SQL.
"""
import time, logging, base64
from fastapi import APIRouter, HTTPException, UploadFile, File, Form

from backend.database import execute_bq_query, neon_session_context
from backend.schemas import QueryRequest, QueryResponse
from backend.services.edge_handler import detect_edge_case
from backend.services.gemini_service import (
    resolve_question,
    classify_intent, generate_sql, generate_nl_answer,
    rag_answer, validate_sql, suggest_chart,
    BASE, CHAT,
)
from backend.config import settings
import httpx
from backend.services.rag_service import search as rag_search
from backend.services.cache import get_cached, set_cached
from backend.services.context_store import context_store

router = APIRouter(prefix="/api/query", tags=["Query"])
logger = logging.getLogger(__name__)


# ── RAG helper ────────────────────────────────────────────────────────────────

async def _try_rag(question: str, language: str, start: float,
                   context=None) -> QueryResponse | None:
    """Attempt RAG search on Neon. Returns QueryResponse if good match, else None."""
    try:
        async with neon_session_context() as neon_db:
            chunks = await rag_search(neon_db, question, top_k=7)
            relevant = [c for c in chunks if c["similarity"] >= 0.18] or chunks[:3]
            if not relevant:
                return None
            answer = await rag_answer(question, [c["text"] for c in relevant], language, context)
            if "not available" in answer.lower() and relevant[0]["similarity"] < 0.22:
                return None
            return QueryResponse(
                question=question, answer=answer, intent="RAG",
                row_count=0,
                execution_time_ms=int((time.time() - start) * 1000),
                confidence="high" if relevant[0]["similarity"] > 0.60 else "medium",
            )
    except Exception as e:
        logger.warning(f"RAG fallback failed: {e}")
        return None


# ── Main query endpoint ───────────────────────────────────────────────────────

@router.post("", response_model=QueryResponse)
async def query(req: QueryRequest):
    start = time.time()

    # ── Load conversation context for this session ────────────
    ctx = await context_store.get_context(req.session_id) if req.session_id else []

    # ── Step 1: Edge Case Check (FREE — no API call) ──────────
    edge = detect_edge_case(req.question)
    if edge:
        logger.info(f"Edge case: {edge['type']} | Q: {req.question[:60]}")
        await context_store.add_turn(
            req.session_id, req.question, req.question,
            edge["response"], "EDGE",
        )
        return QueryResponse(
            question=req.question,
            answer=edge["response"],
            intent="EDGE",
            edge_type=edge["type"],
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="high",
        )

    # ── Step 2: Resolve follow-up questions (multi-turn core) ─
    # "what about inactive?" → "How many inactive beneficiaries are there?"
    # "sum of active and inactive?" → "What is the combined total of active and inactive beneficiaries?"
    resolved = await resolve_question(req.question, ctx)
    if resolved != req.question:
        logger.info(f"Resolved: '{req.question[:60]}' → '{resolved[:80]}'")

    # ── Step 3: Classify Intent via Gemini ────────────────────
    intent = await classify_intent(resolved, ctx)
    logger.info(f"Intent: {intent} | Q: {resolved[:80]}")

    # ── Step 4a: RAG Path ─────────────────────────────────────
    if intent == "RAG":
        rag_result = await _try_rag(resolved, req.language, start, ctx)
        if rag_result:
            await context_store.add_turn(
                req.session_id, req.question, resolved,
                rag_result.answer, "RAG",
            )
            return rag_result
        fallback = (
            "Thank you for your question. I was unable to find specific information "
            "about this in the DSSY knowledge base. You may contact the Directorate "
            "of Social Welfare, Government of Goa at https://socialwelfare.goa.gov.in "
            "for detailed assistance."
        )
        await context_store.add_turn(req.session_id, req.question, resolved, fallback, "RAG")
        return QueryResponse(
            question=req.question, intent="RAG", answer=fallback,
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="low",
        )

    # ── Step 4b: SQL Path (BigQuery) ──────────────────────────
    # Cache key uses the RESOLVED question so "what about inactive?" hits
    # the same cache as "How many inactive beneficiaries are there?"
    cached = await get_cached(resolved)
    if cached:
        cached["intent"]    = "SQL"
        cached["question"]  = req.question  # always reflect what the user actually typed
        if not req.include_sql:
            cached.pop("sql_query", None)
        # Save to context so follow-ups work even on cache hits
        await context_store.add_turn(
            req.session_id, req.question, resolved,
            cached.get("answer", ""), "SQL",
            sql_data=cached.get("data"),
        )
        return QueryResponse(**cached)

    try:
        sql, conf = await generate_sql(resolved, ctx)

        # SQL cannot answer → fallback to RAG
        if "CANNOT_ANSWER" in sql:
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result
            fallback = (
                "This information is not available in the DSSY beneficiary database "
                "or scheme knowledge base. You can ask about beneficiary statistics, "
                "district/taluka distribution, payment status, scheme eligibility, "
                "or application procedures."
            )
            await context_store.add_turn(req.session_id, req.question, resolved, fallback, "RAG")
            return QueryResponse(
                question=req.question, intent="RAG", answer=fallback,
                row_count=0,
                execution_time_ms=int((time.time() - start) * 1000),
                confidence="low",
            )

        ok, reason = validate_sql(sql)
        if not ok:
            raise HTTPException(422, f"Query validation failed: {reason}")

        # Execute on BigQuery
        try:
            results = await execute_bq_query(sql)
        except Exception as e:
            logger.warning(f"BigQuery exec failed: {e}")
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result
            fallback = (
                "I encountered an issue processing that query. Could you please "
                "try rephrasing? For example: 'How many active beneficiaries are there?' "
                "or 'Show district-wise beneficiary count'."
            )
            await context_store.add_turn(req.session_id, req.question, resolved, fallback, "SQL")
            return QueryResponse(
                question=req.question, intent="SQL", answer=fallback,
                row_count=0,
                execution_time_ms=int((time.time() - start) * 1000),
                confidence="low",
            )

        row_count = len(results)

        # SQL returned 0 rows → try RAG before saying "no data"
        if row_count == 0:
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result

        # Generate NL answer — context is passed so the model can reference
        # prior numbers (e.g., "active was 45,231, inactive is 12,453, combined = 57,684")
        answer = await generate_nl_answer(
            resolved, sql, results, row_count, req.language, ctx
        )
        chart_type = suggest_chart(results)
        ms = int((time.time() - start) * 1000)

        payload = {
            "question":          req.question,
            "answer":            answer,
            "intent":            "SQL",
            "data":              results[:100],
            "sql_query":         sql,
            "row_count":         row_count,
            "execution_time_ms": ms,
            "confidence":        "high" if conf > 0.7 else "medium",
            "chart_type":        chart_type,
        }
        await set_cached(resolved, payload)

        # Store turn WITH raw data so arithmetic follow-ups work
        await context_store.add_turn(
            req.session_id, req.question, resolved,
            answer, "SQL",
            sql_data=results[:50],
        )

        if not req.include_sql:
            payload.pop("sql_query", None)
        return QueryResponse(**payload)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        raise HTTPException(500, "We encountered an issue. Please try again shortly.")


# ── Transcribe endpoint ───────────────────────────────────────────────────────

_LANG_HINT = {"en-IN": "English", "hi-IN": "Hindi", "te-IN": "Telugu"}

_ALLOWED_AUDIO_TYPES = {"audio/webm", "audio/wav", "audio/mp3", "audio/ogg", "audio/mpeg", "audio/mp4"}
_MAX_AUDIO_BYTES = 5 * 1024 * 1024  # 5 MB

@router.post("/transcribe")
async def transcribe(
    audio: UploadFile = File(...),
    language: str = Form("en-IN"),
):
    audio_bytes = await audio.read()
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise HTTPException(413, "Audio file too large. Maximum size is 5 MB.")
    mime_type = (audio.content_type or "audio/webm").split(";")[0].strip()
    if mime_type not in _ALLOWED_AUDIO_TYPES:
        mime_type = "audio/webm"
    audio_b64 = base64.b64encode(audio_bytes).decode()
    lang      = _LANG_HINT.get(language, "English")

    payload = {"contents": [{"parts": [
        {"text": f"Transcribe this audio exactly as spoken in {lang}. Return only the transcribed text, nothing else."},
        {"inline_data": {"mime_type": mime_type, "data": audio_b64}},
    ]}]}

    try:
        url = f"{BASE}/models/{CHAT}:generateContent?key={settings.GEMINI_API_KEY}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        transcript = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        return {"transcript": transcript}
    except httpx.TimeoutException:
        raise HTTPException(504, "Transcription timed out. Please try again.")
    except Exception as e:
        logger.error(f"Transcription error: {e}")
        raise HTTPException(502, "Transcription failed. Please try again.")


# ── Suggestions endpoint ──────────────────────────────────────────────────────

@router.get("/suggestions")
async def suggestions():
    return {"categories": {
        "📊 Data Queries": [
            "How many total beneficiaries are there in DSSY?",
            "Show taluka-wise active beneficiary count",
            "Compare North Goa vs South Goa beneficiaries",
            "What is the gender-wise breakdown of beneficiaries?",
            "Which taluka has the most Senior Citizen beneficiaries?",
            "What is the total monthly payout for active beneficiaries?",
            "Show category-wise beneficiary distribution",
            "How many beneficiaries are above 80 years old?",
            "List inactive beneficiaries by district",
            "Show age group distribution of beneficiaries",
            "Female beneficiaries count by district",
            "Payment compliance status summary",
            "How many widow beneficiaries are there?",
            "How many deceased beneficiaries are recorded?",
        ],
        "📋 Scheme Information": [
            "Who is eligible for DSSY benefits?",
            "What documents are required to apply for DSSY?",
            "How much pension do widows receive under DSSY?",
            "What is the financial assistance for disabled persons?",
            "What is the Life Certificate requirement?",
            "When was DSSY launched?",
            "What is the difference between DSSY and DDSSY?",
            "Can both husband and wife receive DSSY?",
            "What are the cancellation rules for DSSY?",
            "What is the registration fee for DSSY?",
            "How is DSSY payment made to beneficiaries?",
            "What happens if Life Certificate is not submitted?",
            "What are the DSSY amendment changes in 2021?",
            "What is the residency requirement for DSSY?",
            "Can a divorced woman apply for DSSY?",
            "What is the income limit to qualify for DSSY?",
            "Who approves DSSY applications?",
            "Can a disabled person continue DSSY after marriage?",
            "What is the medical assistance for senior citizens?",
            "How much can a disabled person claim for aids and appliances?",
            "What happened to DSSY payments before ECS was introduced?",
            "What did the CAG audit find about DSSY in 2008?",
            "Which schemes were amalgamated into DSSY?",
            "What is the Griha Aadhar scheme and how is it related to DSSY?",
            "Can children receive DSSY if parents are already beneficiaries?",
            "What is the notification number of DSSY scheme?",
            "How many widows benefited from the 2021 amendment?",
            "Where is the Department of Social Welfare located?",
            "What was the original pension amount when DSSY started?",
        ],
        "📈 Analytics": [
            "Show year-wise registration trend",
            "What is the payment success rate?",
            "Category-wise monthly payout breakdown",
            "Top 5 talukas by active beneficiaries",
            "District-wise payment comparison",
        ],
    }}
