"""
/api/query — single endpoint, 3-way auto-routing:
  1. EDGE  : greetings, identity, silly, off-topic → instant canned response (no API cost)
  2. SQL   : data question → Gemini generates BigQuery SQL → execute → NL answer + chart
  3. RAG   : scheme knowledge → Neon pgvector hybrid search → Gemini answer from DSSY docs

Smart fallback: SQL fail → try RAG. RAG low confidence → try SQL.
"""
import time, logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from backend.database import get_bq_db, get_neon_db, execute_bq_query, neon_session_context
from backend.schemas import QueryRequest, QueryResponse
from backend.services.edge_handler import detect_edge_case
from backend.services.gemini_service import (
    classify_intent, generate_sql, generate_nl_answer,
    rag_answer, validate_sql, suggest_chart,
)
from backend.services.rag_service import search as rag_search
from backend.services.cache import get_cached, set_cached

router = APIRouter(prefix="/api/query", tags=["Query"])
logger = logging.getLogger(__name__)


async def _try_rag(question: str, language: str, start: float) -> QueryResponse | None:
    """Attempt RAG search on Neon. Returns QueryResponse if good match, else None."""
    try:
        async with neon_session_context() as neon_db:
            chunks = await rag_search(neon_db, question, top_k=5)
            relevant = [c for c in chunks if c["similarity"] >= 0.20] or chunks[:3]
            if not relevant:
                return None
            answer = await rag_answer(question, [c["text"] for c in relevant], language)
            if "not available" in answer.lower() and relevant[0]["similarity"] < 0.25:
                return None
            return QueryResponse(
                question=question, answer=answer, intent="RAG",
                row_count=0, execution_time_ms=int((time.time() - start) * 1000),
                confidence="high" if relevant[0]["similarity"] > 0.65 else "medium",
            )
    except Exception as e:
        logger.warning(f"RAG fallback failed: {e}")
        return None


@router.post("", response_model=QueryResponse)
async def query(req: QueryRequest):
    start = time.time()

    # ── Step 1: Edge Case Check (FREE — no API call) ──────────
    edge = detect_edge_case(req.question)
    if edge:
        logger.info(f"Edge case: {edge['type']} | Q: {req.question[:60]}")
        return QueryResponse(
            question=req.question,
            answer=edge["response"],
            intent="EDGE",
            edge_type=edge["type"],
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="high",
        )

    # ── Step 2: Classify Intent via Gemini ────────────────────
    intent = await classify_intent(req.question)
    logger.info(f"Intent: {intent} | Q: {req.question[:80]}")

    # ── Step 3a: RAG Path ─────────────────────────────────────
    if intent == "RAG":
        rag_result = await _try_rag(req.question, req.language, start)
        if rag_result:
            return rag_result
        return QueryResponse(
            question=req.question, intent="RAG",
            answer=(
                "Thank you for your question. I was unable to find specific information "
                "about this in the DSSY knowledge base. You may contact the Directorate "
                "of Social Welfare, Government of Goa at https://socialwelfare.goa.gov.in "
                "for detailed assistance."
            ),
            row_count=0,
            execution_time_ms=int((time.time() - start) * 1000),
            confidence="low",
        )

    # ── Step 3b: SQL Path (BigQuery) ──────────────────────────
    cached = await get_cached(req.question)
    if cached:
        cached["intent"] = "SQL"
        if not req.include_sql:
            cached.pop("sql_query", None)
        return QueryResponse(**cached)

    try:
        sql, conf = await generate_sql(req.question)

        # SQL cannot answer → fallback to RAG
        if "CANNOT_ANSWER" in sql:
            rag_result = await _try_rag(req.question, req.language, start)
            if rag_result:
                return rag_result
            return QueryResponse(
                question=req.question, intent="RAG",
                answer=(
                    "This information is not available in the DSSY beneficiary database "
                    "or scheme knowledge base. You can ask about beneficiary statistics, "
                    "district/taluka distribution, payment status, scheme eligibility, "
                    "or application procedures."
                ),
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
            # SQL execution failed → try RAG as fallback
            rag_result = await _try_rag(req.question, req.language, start)
            if rag_result:
                return rag_result
            return QueryResponse(
                question=req.question, intent="SQL",
                answer=(
                    "I encountered an issue processing that query. Could you please "
                    "try rephrasing? For example: 'How many active beneficiaries are there?' "
                    "or 'Show district-wise beneficiary count'."
                ),
                row_count=0,
                execution_time_ms=int((time.time() - start) * 1000),
                confidence="low",
            )

        row_count = len(results)

        # SQL returned 0 rows → try RAG before saying "no data"
        if row_count == 0:
            rag_result = await _try_rag(req.question, req.language, start)
            if rag_result:
                return rag_result

        answer = await generate_nl_answer(
            req.question, sql, results, row_count, req.language
        )
        chart_type = suggest_chart(results)
        ms = int((time.time() - start) * 1000)

        payload = {
            "question": req.question, "answer": answer, "intent": "SQL",
            "data": results[:100], "sql_query": sql, "row_count": row_count,
            "execution_time_ms": ms,
            "confidence": "high" if conf > 0.7 else "medium",
            "chart_type": chart_type,
        }
        await set_cached(req.question, payload)

        if not req.include_sql:
            payload.pop("sql_query", None)
        return QueryResponse(**payload)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        raise HTTPException(500, "We encountered an issue. Please try again shortly.")


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