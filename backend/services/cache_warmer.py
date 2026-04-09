"""
Cache Warmer (Enhancement 7)
==============================
On startup, pre-executes the top 20 most common analytical queries so the
first real user gets an instant cached response instead of waiting for
Gemini + Neon round-trips.

Called once from main.py lifespan after Neon + Gemini are confirmed healthy.
All failures are non-fatal — the app starts normally either way.
"""
import asyncio, logging
from backend.services.cache import get_cached, set_cached

logger = logging.getLogger(__name__)

# Top 20 queries to pre-warm. These match the most-clicked suggestion chips.
_WARM_QUERIES = [
    "How many total beneficiaries are there?",
    "Break down beneficiaries by category",
    "Compare North Goa and South Goa beneficiaries",
    "Which category has the least beneficiaries?",
    "Show taluka-wise count for North Goa",
    "How many widows are there?",
    "Male vs female breakdown of beneficiaries",
    "Age distribution of beneficiaries",
    "How many beneficiaries are above 80 years old?",
    "Inactive beneficiaries by district",
    "Compare payments across last 3 years",
    "Monthly payment trend for 2024",
    "Which district has the most failed payments?",
    "Year-wise registration trend",
    "Total monthly payout for all active beneficiaries",
    "Life certificate compliance by taluka",
    "Year-over-year payment success rate",
    "Category-wise monthly payout comparison",
    "Year-wise beneficiary growth 2020 to 2026",
    "Deceased beneficiaries by year",
]


async def warm_cache() -> int:
    """
    Pre-fill Redis cache with the top queries.
    Returns the count of queries successfully warmed.
    Runs each query with a 20-second timeout; skips on failure.
    """
    from backend.services.gemini_service import generate_sql, generate_nl_answer, suggest_chart
    from backend.services.gemini_service import validate_sql
    from backend.database import execute_sql_query

    warmed = 0
    for q in _WARM_QUERIES:
        try:
            # Skip if already cached from a previous startup
            cached = await get_cached(q)
            if cached:
                warmed += 1
                continue

            async with asyncio.timeout(20):
                sql, conf = await generate_sql(q, context=[])
                if "CANNOT_ANSWER" in sql:
                    continue
                ok, _ = validate_sql(sql)
                if not ok:
                    continue
                results = await execute_sql_query(sql)
                if not results:
                    continue
                answer     = await generate_nl_answer(q, sql, results, len(results), "en", [])
                chart_type = suggest_chart(results)
                payload = {
                    "question":          q,
                    "answer":            answer,
                    "intent":            "SQL",
                    "data":              results[:100],
                    "sql_query":         sql,
                    "row_count":         len(results),
                    "execution_time_ms": 0,
                    "confidence":        "high" if conf > 0.7 else "medium",
                    "chart_type":        chart_type,
                }
                await set_cached(q, payload)
                warmed += 1
                logger.info(f"Cache warmed: {q[:60]}")
                # Small pause to avoid hammering Gemini API at startup
                await asyncio.sleep(0.5)

        except asyncio.TimeoutError:
            logger.warning(f"Cache warm timeout for: {q[:60]}")
        except Exception as e:
            logger.warning(f"Cache warm failed for '{q[:50]}': {e}")

    return warmed
