"""
/api/query — single endpoint, 4-way auto-routing (Agentic RAG):
  1. EDGE  : greetings, identity, silly, off-topic → instant canned response (no API cost)
  2. SQL   : data question → Gemini generates PostgreSQL SQL → execute → NL answer + chart
  3. RAG   : scheme knowledge → Neon pgvector hybrid search → Gemini answer from DSSS docs
  4. WEB   : Gemini web-grounded search fallback when local RAG has no answer

Multi-turn conversation:
  - session_id ties requests together across a browser session
  - Each question is first resolved into a standalone question using prior context
    ("what about inactive?" → "How many inactive beneficiaries are there?")
  - Resolved question + raw SQL data is stored per turn so arithmetic follow-ups
    ("sum of active and inactive?") produce correct, coherent answers

Smart fallback chain: SQL fail → RAG → Web Search.  RAG low confidence → Web Search.
"""
import time, logging, base64, json as _json
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import StreamingResponse

from backend.database import execute_sql_query, neon_session_context
from backend.schemas import QueryRequest, QueryResponse
from backend.services.edge_handler import detect_edge_case
from backend.services.gemini_service import (
    resolve_question,
    classify_intent, generate_sql, generate_nl_answer,
    rag_answer, validate_sql, suggest_chart,
    web_search_fallback,
    answer_from_context,
    BASE, CHAT,
)
from backend.config import settings
import httpx
from backend.services.rag_service import (
    search as rag_search,
    extract_direct_answer,
    HIGH_CONFIDENCE, MEDIUM_CONFIDENCE,
)
from backend.services.cache import get_cached, set_cached
from backend.services.context_store import context_store
from backend.services.forecast_service import is_forecast_question, compute_forecast

router = APIRouter(prefix="/api/query", tags=["Query"])
logger = logging.getLogger(__name__)


# ── RAG helper ────────────────────────────────────────────────────────────────

async def _try_rag(question: str, language: str, start: float,
                   context=None) -> QueryResponse | None:
    """
    Tiered RAG with Gemini cost optimization:
      HIGH   (>= 0.55): direct-answer chunk → return immediately (ZERO Gemini cost)
      MEDIUM (>= 0.25): decent match → Gemini synthesizes answer from chunks (1 API call)
      LOW    (< 0.25):  weak match → return None (caller falls back to web search)
    """
    try:
        async with neon_session_context() as neon_db:
            chunks = await rag_search(neon_db, question, top_k=8)
            relevant = [c for c in chunks if c["similarity"] >= 0.15] or chunks[:4]
            if not relevant:
                return None

            best_score = relevant[0]["similarity"]

            # ── TIER 1: HIGH confidence — direct answer, skip Gemini ──
            if best_score >= HIGH_CONFIDENCE:
                direct = extract_direct_answer(relevant, question)
                if direct:
                    logger.info(
                        "RAG TIER-1 (direct, no Gemini): score=%.3f | Q: %s",
                        best_score, question[:60],
                    )
                    return QueryResponse(
                        question=question, answer=direct, intent="RAG",
                        row_count=0,
                        execution_time_ms=int((time.time() - start) * 1000),
                        confidence="high",
                    )
                # High score but not a direct-answer chunk → fall through to Gemini synthesis

            # ── TIER 2: MEDIUM confidence — Gemini synthesis from chunks ──
            if best_score >= MEDIUM_CONFIDENCE:
                answer = await rag_answer(
                    question, [c["text"] for c in relevant], language, context
                )
                if "not available" in answer.lower() and best_score < 0.30:
                    return None  # Gemini couldn't answer either → let web search try
                logger.info(
                    "RAG TIER-2 (Gemini synthesis): score=%.3f | Q: %s",
                    best_score, question[:60],
                )
                return QueryResponse(
                    question=question, answer=answer, intent="RAG",
                    row_count=0,
                    execution_time_ms=int((time.time() - start) * 1000),
                    confidence="high" if best_score > 0.55 else "medium",
                )

            # ── TIER 3: LOW confidence — not worth Gemini call ──
            logger.info(
                "RAG TIER-3 (low confidence, skipping Gemini): score=%.3f | Q: %s",
                best_score, question[:60],
            )
            return None

    except Exception as e:
        logger.warning(f"RAG fallback failed: {e}")
        return None


async def _try_web_search(question: str, language: str, start: float) -> QueryResponse | None:
    """Agentic RAG fallback: use Gemini web-grounded search when local RAG has no answer."""
    try:
        result = await web_search_fallback(question, language)
        if not result or not result.get("answer"):
            return None

        answer = result["answer"]

        return QueryResponse(
            question=question,
            answer=answer,
            intent="RAG",
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="medium" if result.get("grounded") else "low",
        )
    except Exception as e:
        logger.warning(f"Web search fallback failed: {e}")
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

    # ── Step 3.5: REASON Path — answer over prior data (no fetch) ─
    # Used for "why is X the lowest?", "explain that", "summarize the trend",
    # "which one is the highest among these?". The model reasons from the
    # conversation history; no SQL or RAG. If reasoning fails, we fall
    # through to the SQL path so the user always gets an answer.
    if intent == "REASON":
        try:
            reason_text = await answer_from_context(resolved, ctx, req.language)
            if reason_text and reason_text.strip():
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    reason_text, "REASON",
                )
                # Carry the most recent prior turn's data forward so the UI
                # can keep showing the same chart/table the user was
                # discussing — REASON is about the prior data, not new data.
                prior_with_data = next(
                    (t for t in reversed(ctx) if t.intent != "EDGE" and t.sql_data),
                    None,
                )
                prior_data = prior_with_data.sql_data if prior_with_data else None
                return QueryResponse(
                    question=req.question,
                    answer=reason_text,
                    intent="REASON",
                    data=prior_data or [],
                    row_count=len(prior_data) if prior_data else 0,
                    execution_time_ms=int((time.time() - start) * 1000),
                    confidence="high",
                )
            logger.info("REASON returned empty — falling through to SQL")
        except Exception as e:
            logger.warning(f"REASON path failed, falling through to SQL: {e}")
        # Fall through to SQL path below — intent reset so we don't re-enter REASON.
        intent = "SQL"

    # ── Step 4a: RAG Path (Agentic: local RAG → web search fallback) ─
    if intent == "RAG":
        rag_result = await _try_rag(resolved, req.language, start, ctx)
        if rag_result:
            await context_store.add_turn(
                req.session_id, req.question, resolved,
                rag_result.answer, "RAG",
            )
            return rag_result

        # Local RAG failed → try Gemini web-grounded search
        web_result = await _try_web_search(resolved, req.language, start)
        if web_result:
            logger.info("Agentic RAG: web search answered '%s'", resolved[:60])
            web_result.intent = "WEB"
            await context_store.add_turn(
                req.session_id, req.question, resolved,
                web_result.answer, "WEB",
            )
            return web_result

        fallback = (
            "Thank you for your question. I was unable to find specific information "
            "about this in the DSSS knowledge base or through web search. You may contact "
            "the Directorate of Social Welfare, Government of Goa at "
            "https://socialwelfare.goa.gov.in for detailed assistance."
        )
        await context_store.add_turn(req.session_id, req.question, resolved, fallback, "RAG")
        return QueryResponse(
            question=req.question, intent="RAG", answer=fallback,
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="low",
        )

    # ── Step 4b-pre: FORECAST Path ────────────────────────────
    # Detected before normal SQL path. Runs the SQL to get historical data,
    # then applies linear regression to project future periods.
    if is_forecast_question(resolved):
        try:
            sql, _ = await generate_sql(resolved, ctx)
            if "CANNOT_ANSWER" not in sql:
                ok, _ = validate_sql(sql)
                if ok:
                    results = await execute_sql_query(sql)
                    if results and len(results) >= 2:
                        forecast = await compute_forecast(results, n_periods=3)
                        answer = await generate_nl_answer(
                            resolved, sql, results, len(results), req.language, ctx
                        )
                        # Append projection info to answer
                        if forecast.get("projections"):
                            proj_lines = [f"{p['label']}: {list(p.values())[-1]:,.0f}"
                                          for p in forecast["projections"]]
                            answer += f"\n\n**Projected next {len(forecast['projections'])} periods:** " + ", ".join(proj_lines)
                        ms = int((time.time() - start) * 1000)
                        payload = {
                            "question": req.question, "answer": answer,
                            "intent": "SQL", "data": results[:100],
                            "sql_query": sql, "row_count": len(results),
                            "execution_time_ms": ms, "confidence": "medium",
                            "chart_type": "forecast",
                            "forecast": forecast,
                        }
                        await set_cached(resolved, payload)
                        await context_store.add_turn(
                            req.session_id, req.question, resolved,
                            answer, "SQL", sql_data=results[:50]
                        )
                        if not req.include_sql:
                            payload.pop("sql_query", None)
                        return QueryResponse(**{k: v for k, v in payload.items()
                                                if k in QueryResponse.__fields__})
        except Exception as fe:
            logger.warning(f"Forecast path failed, falling through to SQL: {fe}")

    # ── Step 4b: SQL Path (Neon PostgreSQL) ───────────────────
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

        # SQL cannot answer → fallback to RAG → web search
        if "CANNOT_ANSWER" in sql:
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result

            # Agentic fallback: web search
            web_result = await _try_web_search(resolved, req.language, start)
            if web_result:
                logger.info("Agentic RAG (SQL→RAG→Web): '%s'", resolved[:60])
                web_result.intent = "WEB"
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    web_result.answer, "WEB",
                )
                return web_result

            fallback = (
                "This information is not available in the DSSS beneficiary database, "
                "scheme knowledge base, or web search. You can ask about beneficiary "
                "statistics, district/taluka distribution, payment status, scheme "
                "eligibility, or application procedures."
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

        # Execute on Neon PostgreSQL
        try:
            results = await execute_sql_query(sql)
        except RuntimeError as e:
            # Enhancement 9: friendly Postgres error message already formatted
            logger.warning(f"Neon PostgreSQL exec failed: {e}")
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result
            # Agentic fallback: web search
            web_result = await _try_web_search(resolved, req.language, start)
            if web_result:
                web_result.intent = "WEB"
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    web_result.answer, "WEB",
                )
                return web_result
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

        # SQL returned 0 rows → try RAG → web search before saying "no data"
        if row_count == 0:
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            if rag_result:
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    rag_result.answer, "RAG",
                )
                return rag_result
            web_result = await _try_web_search(resolved, req.language, start)
            if web_result:
                web_result.intent = "WEB"
                await context_store.add_turn(
                    req.session_id, req.question, resolved,
                    web_result.answer, "WEB",
                )
                return web_result

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


# ── SSE Streaming endpoint ───────────────────────────────────────────────────
# Streams the answer token-by-token via Server-Sent Events.
# The frontend connects with EventSource and receives:
#   data: {"token": "word"}\n\n   ← during generation
#   data: {"done": true, "intent": "SQL", "chart_type": "bar", "data": [...]}\n\n  ← at end

@router.post("/stream")
async def query_stream(req: QueryRequest, request: Request):
    """SSE streaming version of /api/query — streams answer tokens as they arrive."""

    async def event_generator():
        start = time.time()
        ctx = await context_store.get_context(req.session_id) if req.session_id else []

        # ── Edge check ────────────────────────────────────────────
        from backend.services.edge_handler import detect_edge_case
        edge = detect_edge_case(req.question)
        if edge:
            await context_store.add_turn(
                req.session_id, req.question, req.question,
                edge["response"], "EDGE",
            )
            yield f"data: {_json.dumps({'token': edge['response']})}\n\n"
            yield f"data: {_json.dumps({'done': True, 'intent': 'EDGE'})}\n\n"
            return

        # ── Resolve follow-up ─────────────────────────────────────
        resolved = await resolve_question(req.question, ctx)

        # ── Intent classification ─────────────────────────────────
        intent = await classify_intent(resolved, ctx)

        # ── REASON path (no fetch, stream the reasoning answer) ───
        if intent == "REASON":
            try:
                reason_text = await answer_from_context(resolved, ctx, req.language)
                if reason_text:
                    # Stream word-by-word simulation (full text arrives at once from Gemini)
                    words = reason_text.split()
                    for i, word in enumerate(words):
                        chunk = word + (" " if i < len(words) - 1 else "")
                        yield f"data: {_json.dumps({'token': chunk})}\n\n"
                    prior = next((t for t in reversed(ctx) if t.intent != "EDGE" and t.sql_data), None)
                    await context_store.add_turn(req.session_id, req.question, resolved, reason_text, "REASON")
                    yield f"data: {_json.dumps({'done': True, 'intent': 'REASON', 'data': prior.sql_data if prior else []})}\n\n"
                    return
            except Exception:
                pass
            intent = "SQL"

        # ── RAG path ──────────────────────────────────────────────
        if intent == "RAG":
            rag_result = await _try_rag(resolved, req.language, start, ctx)
            answer = rag_result.answer if rag_result else (
                "This information is not available in the DSSS knowledge base. "
                "Please contact the Directorate of Social Welfare, Government of Goa."
            )
            words = answer.split()
            for i, word in enumerate(words):
                yield f"data: {_json.dumps({'token': word + (' ' if i < len(words)-1 else '')})}\n\n"
            await context_store.add_turn(req.session_id, req.question, resolved, answer, "RAG")
            yield f"data: {_json.dumps({'done': True, 'intent': 'RAG'})}\n\n"
            return

        # ── SQL path ──────────────────────────────────────────────
        try:
            cached = await get_cached(resolved)
            if cached:
                answer = cached.get("answer", "")
                words = answer.split()
                for i, word in enumerate(words):
                    yield f"data: {_json.dumps({'token': word + (' ' if i < len(words)-1 else '')})}\n\n"
                await context_store.add_turn(req.session_id, req.question, resolved, answer, "SQL", sql_data=cached.get("data"))
                yield f"data: {_json.dumps({'done': True, 'intent': 'SQL', 'chart_type': cached.get('chart_type'), 'data': cached.get('data', [])[:100]})}\n\n"
                return

            sql, conf = await generate_sql(resolved, ctx)
            if "CANNOT_ANSWER" in sql:
                msg = "I cannot answer this from the available database schema."
                yield f"data: {_json.dumps({'token': msg})}\n\n"
                yield f"data: {_json.dumps({'done': True, 'intent': 'SQL'})}\n\n"
                return

            ok, reason = validate_sql(sql)
            if not ok:
                yield f"data: {_json.dumps({'token': f'Query validation failed: {reason}'})}\n\n"
                yield f"data: {_json.dumps({'done': True, 'intent': 'SQL'})}\n\n"
                return

            results = await execute_sql_query(sql)
            answer = await generate_nl_answer(resolved, sql, results, len(results), req.language, ctx)
            chart_type = suggest_chart(results)

            words = answer.split()
            for i, word in enumerate(words):
                yield f"data: {_json.dumps({'token': word + (' ' if i < len(words)-1 else '')})}\n\n"

            payload = {"question": req.question, "answer": answer, "intent": "SQL",
                       "data": results[:100], "sql_query": sql, "row_count": len(results),
                       "execution_time_ms": int((time.time()-start)*1000),
                       "confidence": "high" if conf > 0.7 else "medium", "chart_type": chart_type}
            await set_cached(resolved, payload)
            await context_store.add_turn(req.session_id, req.question, resolved, answer, "SQL", sql_data=results[:50])
            yield f"data: {_json.dumps({'done': True, 'intent': 'SQL', 'chart_type': chart_type, 'data': results[:100]})}\n\n"

        except Exception as e:
            logger.error(f"Stream query error: {e}", exc_info=True)
            yield f"data: {_json.dumps({'token': 'An error occurred. Please try again.'})}\n\n"
            yield f"data: {_json.dumps({'done': True, 'intent': 'SQL'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Transcribe endpoint ───────────────────────────────────────────────────────

_LANG_HINT = {"en-IN": "English", "hi-IN": "Hindi", "kok-IN": "Konkani"}

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
        "📊 Beneficiary Data": [
            "How many total beneficiaries are there?",
            "Break down beneficiaries by category",
            "Which category has the least beneficiaries?",
            "Show taluka-wise beneficiary count in North Goa?",
            "How many widows are there?",
            "What is the male vs female beneficiary breakdown?",
            "Show the age distribution of beneficiaries",
            "How many beneficiaries are above 80 years old?",
            "Show inactive beneficiary count by district",
            "Show beneficiary growth year-wise from 2020 to 2026",
            "Show gender breakdown by beneficiary category",
            "Which are the top 10 villages by beneficiary count?",
            "Show deceased beneficiary count by year",
        ],
        "💰 Payments & Trends": [
            "Compare total payments made in the last 3 years",
            "Show the monthly payment trend for 2024",
            "Which district has the most failed payments?",
            "Show the year-wise beneficiary registration trend",
            "What is the total monthly payout for all active beneficiaries?",
            "How many new beneficiaries were registered in the last 6 months?",
            "Show life certificate compliance rates by taluka",
            "Show a summary of recent payment batches",
            "What is the year-over-year payment success rate?",
            "Compare monthly payouts across beneficiary categories",
            "Which taluka has the highest pending payments?",
            "What is the payment forecast for the next 3 months?",
        ],
        "📋 Scheme Information": [
            "Who is eligible for DSSS?",
            "What documents are needed to apply?",
            "How much pension do widows get?",
            "What is the Life Certificate requirement?",
            "What is the difference between DSSS and DDSSY?",
            "Can both husband and wife get DSSS?",
            "What are the cancellation rules?",
            "What happens if Life Certificate is not submitted?",
            "What is the income limit for DSSS?",
            "Which schemes were merged into DSSS?",
            # New chips
            "What is the appeals process for rejected applications?",
            "How long does approval take?",
            "What are the bank requirements for DSSS?",
        ],
    },
    # Follow-up suggestions per chip (used by frontend to show contextual follow-ups)
    "followups": {
        "How many total beneficiaries are there?": [
            "How does that compare to last year?",
            "Which district has more?",
            "Break it down by category",
        ],
        "Break down beneficiaries by category": [
            "Which is the largest category?",
            "Which is the smallest category?",
            "Show only disabled categories",
            "Compare North and South Goa for each category",
        ],
        "Which category has the least beneficiaries?": [
            "Why is it the lowest?",
            "Show the trend for that category over years",
            "Which taluka has the least for that category?",
        ],
        "Show taluka-wise beneficiary count in North Goa?": [
            "Which taluka has the most beneficiaries?",
            "Show the same breakdown for South Goa",
            "Which taluka has the most inactive beneficiaries?",
        ],
        "How many widows are there?": [
            "Compare with last year",
            "Which district has more widows?",
            "How many widows are above 70 years old?",
        ],
        "What is the male vs female beneficiary breakdown?": [
            "Which year had the most female beneficiaries?",
            "Which district has more women?",
            "Show year-wise trend from 2020 to 2026",
        ],
        "Show the age distribution of beneficiaries": [
            "Which age group is the largest?",
            "Show only active beneficiaries by age",
            "Compare age distribution between districts",
        ],
        "How many beneficiaries are above 80 years old?": [
            "Which year did that number peak?",
            "Which district has more beneficiaries above 80?",
            "Which category has the most beneficiaries above 80?",
        ],
        "Show inactive beneficiary count by district": [
            "Which category has the most inactive beneficiaries?",
            "Show the trend of inactive beneficiaries over years",
            "Why does that district have more inactive beneficiaries?",
        ],
        "Compare total payments made in the last 3 years": [
            "Which year had the most failed payments?",
            "What is the payment success rate trend?",
            "Break down payments by district for each year",
        ],
        "Show the monthly payment trend for 2024": [
            "Which month had the highest payment?",
            "Compare 2024 payments with 2023",
            "Show failed payments for the same period",
        ],
        "Which district has the most failed payments?": [
            "Show failed payments by category",
            "What is the failure rate percentage?",
            "Show the trend of failed payments over months",
        ],
        "Show the year-wise beneficiary registration trend": [
            "Which year had the most registrations?",
            "Which category grew the fastest?",
            "Show the registration trend by district",
        ],
        "What is the total monthly payout for all active beneficiaries?": [
            "How has the monthly payout changed over years?",
            "Show the payout broken down by category",
            "Which district has the highest payout?",
        ],
        "How many new beneficiaries were registered in the last 6 months?": [
            "Which month had the most registrations?",
            "Which category had the most new registrations?",
            "Compare with the same period last year",
        ],
        "Show life certificate compliance rates by taluka": [
            "Which taluka has the worst compliance rate?",
            "Show only talukas with compliance below 80 percent",
            "How has compliance changed year over year?",
        ],
        "Show a summary of recent payment batches": [
            "Which batch had the most failures?",
            "Show only payment batches for 2024",
            "What is the average payment success rate?",
        ],
        "Who is eligible for DSSS?": [
            "What about disabled people?",
            "Is there an age limit?",
            "What is the income limit?",
        ],
        "What documents are needed to apply?": [
            "What if I don't have Aadhaar?",
            "Are there different documents for widows?",
            "What about life certificate?",
        ],
        "How much pension do widows get?": [
            "What about senior citizens?",
            "Has the amount changed recently?",
            "Compare all category amounts",
        ],
        "What is the Life Certificate requirement?": [
            "What happens if I miss it?",
            "How often must it be submitted?",
            "Can it be submitted online?",
        ],
        "What is the difference between DSSS and DDSSY?": [
            "Which one should I apply for?",
            "Can I get both?",
            "What are the eligibility differences?",
        ],
        "Can both husband and wife get DSSS?": [
            "What if only one is eligible?",
            "Are there income limits for couples?",
        ],
        "What are the cancellation rules?": [
            "Can a cancelled application be restored?",
            "What are the most common reasons for cancellation?",
        ],
        "What happens if Life Certificate is not submitted?": [
            "Is there a grace period?",
            "How do I reactivate after suspension?",
        ],
        "What is the income limit for DSSS?": [
            "What counts as income?",
            "Is agricultural income included?",
            "What if income changes after enrollment?",
        ],
        "Which schemes were merged into DSSS?": [
            "When did the merger happen?",
            "Were benefits changed after merger?",
        ],
        # ── New Beneficiary Data chips ────────────────────────────────
        "Show beneficiary growth year-wise from 2020 to 2026": [
            "Which year had the highest growth?",
            "Which year had the lowest registrations?",
            "Break down by category for each year",
            "Show the growth trend by district",
        ],
        "Show gender breakdown by beneficiary category": [
            "Which category has the most women?",
            "Which category has the highest male percentage?",
            "Show the gender breakdown for North Goa only",
            "Compare gender ratio between districts",
        ],
        "Which are the top 10 villages by beneficiary count?": [
            "Which village has the most beneficiaries?",
            "Show the top villages by district",
            "Which taluka do these top villages belong to?",
        ],
        "Show deceased beneficiary count by year": [
            "Which year had the most deceased beneficiaries?",
            "Break down deceased beneficiaries by category",
            "Compare deceased trend with inactive trend",
        ],
        # ── New Payments & Trends chips ───────────────────────────────
        "What is the year-over-year payment success rate?": [
            "Which year had the lowest success rate?",
            "What caused the dip in that year?",
            "Show failed payments for that year by category",
        ],
        "Compare monthly payouts across beneficiary categories": [
            "Which category has the highest monthly payout?",
            "How has the payout changed over years?",
            "Show payouts for disability categories only",
        ],
        "Which taluka has the highest pending payments?": [
            "Show the same for failed payments",
            "Which category has the most pending payments in that taluka?",
            "Show the pending payment trend over months",
        ],
        "What is the payment forecast for the next 3 months?": [
            "What is the expected total payout?",
            "Which category is projected to grow the most?",
            "How accurate was the last forecast?",
        ],
        # ── New Scheme Information chips ──────────────────────────────
        "What is the appeals process for rejected applications?": [
            "How long does an appeal take?",
            "What documents are needed for an appeal?",
            "What are the most common rejection reasons?",
        ],
        "How long does approval take?": [
            "What causes delays in approval?",
            "Is there a fast-track process?",
            "What happens after approval?",
        ],
        "What are the bank requirements for DSSS?": [
            "Which banks are accepted?",
            "Can I use a post office account?",
            "What if I change my bank account?",
        ],
    }}
