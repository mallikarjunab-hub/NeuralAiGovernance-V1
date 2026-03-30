"""Dashboard KPIs and chart data from BigQuery."""
import logging
from fastapi import APIRouter, Query
from backend.database import execute_bq_query

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)


@router.get("/dashboard")
async def dashboard(
    range: str = Query("all", description="Date range: 7d|30d|90d|6m|1y|all")
):
    try:
        d = {}
        q = execute_bq_query

        # ── Build date filter for registration_date_id if needed ──
        # For now, range filters are applied to registration trends
        # KPIs always show full data

        # ── KPIs ──────────────────────────────────────────────
        r = await q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries`")
        d["total_beneficiaries"] = r[0]["n"]

        r = await q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='active'")
        d["active_beneficiaries"] = r[0]["n"]

        r = await q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='inactive'")
        d["inactive_beneficiaries"] = r[0]["n"]

        r = await q("SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` WHERE status='deceased'")
        d["deceased_beneficiaries"] = r[0]["n"]

        r = await q("""
            SELECT COALESCE(SUM(c.monthly_amount),0) AS n
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
            WHERE b.status='active'
        """)
        d["total_monthly_payout"] = float(r[0]["n"])

        # ── Payment Compliance ────────────────────────────────
        r = await q("""
            SELECT payment_status AS status, COUNT(*) AS cnt
            FROM `edw-pilot.neural.payments`
            GROUP BY payment_status
        """)
        pc = {row["status"]: int(row["cnt"]) for row in r}
        paid = pc.get("paid", 0)
        pending = pc.get("pending", 0)
        failed = pc.get("failed", 0)
        tp = paid + pending + failed
        d["payment_compliance"] = {
            "paid": paid, "pending": pending, "failed": failed,
            "compliance_pct": round(paid / tp * 100, 1) if tp else 0
        }

        # ── Category Distribution ─────────────────────────────
        r = await q("""
            SELECT c.category_name AS category, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
            WHERE b.status='active'
            GROUP BY c.category_name
            ORDER BY count DESC
        """)
        d["by_category"] = [{"category": row["category"], "count": int(row["count"])} for row in r]

        # ── Gender Distribution ───────────────────────────────
        r = await q("""
            SELECT gender, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries` WHERE status='active'
            GROUP BY gender ORDER BY count DESC
        """)
        d["by_gender"] = [{"gender": row["gender"], "count": int(row["count"])} for row in r]

        # ── District Distribution ─────────────────────────────
        r = await q("""
            SELECT d.district_name AS district, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
            WHERE b.status='active'
            GROUP BY d.district_name ORDER BY count DESC
        """)
        d["by_district"] = [{"district": row["district"], "count": int(row["count"])} for row in r]

        # ── Registration Trend ────────────────────────────────
        r = await q("""
            SELECT registration_date_id AS period, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries`
            GROUP BY registration_date_id
            ORDER BY registration_date_id
        """)
        d["registration_trend"] = [
            {"period": str(row["period"]), "count": int(row["count"])} for row in r
        ]

        # ── Age Distribution ──────────────────────────────────
        r = await q("""
            SELECT
                CASE
                    WHEN age < 40 THEN 'Under 40'
                    WHEN age < 60 THEN '40-59'
                    WHEN age < 70 THEN '60-69'
                    WHEN age < 80 THEN '70-79'
                    ELSE '80+'
                END AS age_group,
                COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries`
            WHERE status='active'
            GROUP BY age_group
        """)
        d["age_distribution"] = [
            {"age_group": row["age_group"], "count": int(row["count"])} for row in r
        ]

        # ── Top Talukas ───────────────────────────────────────
        r = await q("""
            SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
            JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
            WHERE b.status='active'
            GROUP BY t.taluka_name, d.district_name
            ORDER BY count DESC
            LIMIT 5
        """)
        d["top_talukas"] = [
            {"taluka": row["taluka"], "district": row["district"], "count": int(row["count"])}
            for row in r
        ]

        # ── All Talukas (for full taluka chart) ───────────────
        r = await q("""
            SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
            JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
            WHERE b.status='active'
            GROUP BY t.taluka_name, d.district_name
            ORDER BY count DESC
        """)
        d["all_talukas"] = [
            {"taluka": row["taluka"], "district": row["district"], "count": int(row["count"])}
            for row in r
        ]

        # ── Category-wise Payout ──────────────────────────────
        r = await q("""
            SELECT c.category_name AS category, COUNT(*) AS beneficiaries,
                   SUM(c.monthly_amount) AS monthly_payout
            FROM `edw-pilot.neural.beneficiaries` b
            JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
            WHERE b.status='active'
            GROUP BY c.category_name
            ORDER BY monthly_payout DESC
        """)
        d["category_payout"] = [
            {
                "category": row["category"],
                "beneficiaries": int(row["beneficiaries"]),
                "monthly_payout": float(row["monthly_payout"])
            }
            for row in r
        ]

        return {"status": "ok", "data": d, "range": range}

    except Exception as e:
        logger.error(f"Dashboard error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e), "data": {}}