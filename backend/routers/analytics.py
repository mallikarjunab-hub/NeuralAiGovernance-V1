"""Dashboard KPIs and chart data from BigQuery."""
import asyncio, logging
from fastapi import APIRouter, Query
from backend.database import execute_bq_query

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)

_Q = execute_bq_query


@router.get("/dashboard")
async def dashboard(
    range: str = Query("all", description="Date range: 7d|30d|90d|6m|1y|all")
):
    try:
        # ── Run all queries in parallel ───────────────────────
        (
            r_total, r_active, r_inactive, r_deceased,
            r_payout, r_payments, r_category, r_gender,
            r_district, r_trend, r_age, r_top_talukas,
            r_all_talukas, r_cat_payout,
        ) = await asyncio.gather(
            _Q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries`"),
            _Q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='active'"),
            _Q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='inactive'"),
            _Q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='deceased'"),
            _Q("""
                SELECT COALESCE(SUM(c.monthly_amount),0) AS n
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                WHERE b.status='active'
            """),
            _Q("SELECT payment_status AS status, COUNT(*) AS cnt FROM `edw-pilot.neural.payments` GROUP BY payment_status"),
            _Q("""
                SELECT c.category_name AS category, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                WHERE b.status='active' GROUP BY c.category_name ORDER BY count DESC
            """),
            _Q("SELECT gender, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE status='active' GROUP BY gender ORDER BY count DESC"),
            _Q("""
                SELECT d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                WHERE b.status='active' GROUP BY d.district_name ORDER BY count DESC
            """),
            _Q("SELECT registration_date_id AS period, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` GROUP BY registration_date_id ORDER BY registration_date_id"),
            _Q("""
                SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age < 60 THEN '40-59'
                            WHEN age < 70 THEN '60-69' WHEN age < 80 THEN '70-79' ELSE '80+' END AS age_group,
                       COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` WHERE status='active' GROUP BY age_group
            """),
            _Q("""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                WHERE b.status='active' GROUP BY t.taluka_name, d.district_name ORDER BY count DESC LIMIT 5
            """),
            _Q("""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                WHERE b.status='active' GROUP BY t.taluka_name, d.district_name ORDER BY count DESC
            """),
            _Q("""
                SELECT c.category_name AS category, COUNT(*) AS beneficiaries,
                       SUM(c.monthly_amount) AS monthly_payout
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                WHERE b.status='active' GROUP BY c.category_name ORDER BY monthly_payout DESC
            """),
        )

        # ── Assemble response ─────────────────────────────────
        pc = {row["status"]: int(row["cnt"]) for row in r_payments}
        paid, pending, failed = pc.get("paid", 0), pc.get("pending", 0), pc.get("failed", 0)
        tp = paid + pending + failed

        return {
            "status": "ok",
            "range": range,
            "data": {
                "total_beneficiaries":  r_total[0]["n"],
                "active_beneficiaries": r_active[0]["n"],
                "inactive_beneficiaries": r_inactive[0]["n"],
                "deceased_beneficiaries": r_deceased[0]["n"],
                "total_monthly_payout": float(r_payout[0]["n"]),
                "payment_compliance": {
                    "paid": paid, "pending": pending, "failed": failed,
                    "compliance_pct": round(paid / tp * 100, 1) if tp else 0,
                },
                "by_category":    [{"category": r["category"], "count": int(r["count"])} for r in r_category],
                "by_gender":      [{"gender": r["gender"], "count": int(r["count"])} for r in r_gender],
                "by_district":    [{"district": r["district"], "count": int(r["count"])} for r in r_district],
                "registration_trend": [{"period": str(r["period"]), "count": int(r["count"])} for r in r_trend],
                "age_distribution":   [{"age_group": r["age_group"], "count": int(r["count"])} for r in r_age],
                "top_talukas":    [{"taluka": r["taluka"], "district": r["district"], "count": int(r["count"])} for r in r_top_talukas],
                "all_talukas":    [{"taluka": r["taluka"], "district": r["district"], "count": int(r["count"])} for r in r_all_talukas],
                "category_payout":[{"category": r["category"], "beneficiaries": int(r["beneficiaries"]), "monthly_payout": float(r["monthly_payout"])} for r in r_cat_payout],
            },
        }

    except Exception as e:
        logger.error(f"Dashboard error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e), "data": {}}