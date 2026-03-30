"""Dashboard KPIs and chart data from BigQuery, with date-range filtering."""
import asyncio, logging, re
from fastapi import APIRouter, Query
from backend.database import execute_bq_query

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)
_Q = execute_bq_query

_INTERVALS = {
    "7d": "7 DAY", "30d": "30 DAY", "90d": "90 DAY",
    "6m": "6 MONTH", "1y": "1 YEAR",
}
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _bene_join(range_str: str, date_from: str, date_to: str) -> tuple[str, str]:
    """Returns (join_sql, where_condition) for beneficiary registration-date filter."""
    if range_str == "custom" and date_from and date_to:
        if _DATE_RE.match(date_from) and _DATE_RE.match(date_to):
            return (
                "JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id",
                f"rd.date BETWEEN DATE('{date_from}') AND DATE('{date_to}')",
            )
    if range_str in _INTERVALS:
        return (
            "JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id",
            f"rd.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {_INTERVALS[range_str]})",
        )
    return ("", "")


def _pay_join(range_str: str, date_from: str, date_to: str) -> tuple[str, str]:
    """Returns (join_sql, where_condition) for payment-date filter."""
    if range_str == "custom" and date_from and date_to:
        if _DATE_RE.match(date_from) and _DATE_RE.match(date_to):
            return (
                "JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id",
                f"pd.date BETWEEN DATE('{date_from}') AND DATE('{date_to}')",
            )
    if range_str in _INTERVALS:
        return (
            "JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id",
            f"pd.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {_INTERVALS[range_str]})",
        )
    return ("", "")


def _and(existing_where: str, extra_cond: str) -> str:
    """Merge an existing WHERE clause with an extra AND condition.
    existing_where must start with 'WHERE ' or be empty.
    """
    if not extra_cond:
        return existing_where
    if existing_where:
        return f"{existing_where} AND {extra_cond}"
    return f"WHERE {extra_cond}"


@router.get("/dashboard")
async def dashboard(
    range: str = Query("all", description="all | 7d | 30d | 90d | 6m | 1y | custom"),
    date_from: str = Query(None, description="YYYY-MM-DD start (custom range only)"),
    date_to:   str = Query(None, description="YYYY-MM-DD end   (custom range only)"),
):
    try:
        bj, bw = _bene_join(range, date_from or "", date_to or "")
        pj, pw = _pay_join(range,  date_from or "", date_to or "")

        # Pre-compute WHERE clauses to avoid backslash-in-f-string issues
        wa  = _and("WHERE b.status='active'",   bw)
        wi  = _and("WHERE b.status='inactive'",  bw)
        wd  = _and("WHERE b.status='deceased'",  bw)
        wall = _and("", bw)
        wpay = _and("", pw)

        (
            r_total, r_active, r_inactive, r_deceased,
            r_payout, r_payments, r_category, r_gender,
            r_district, r_trend, r_age, r_top_talukas,
            r_all_talukas, r_cat_payout,
        ) = await asyncio.gather(
            # ── Beneficiary counts (filter by registration date) ──────────────
            _Q(f"SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` b {bj} {wall}"),
            _Q(f"SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` b {bj} {wa}"),
            _Q(f"SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` b {bj} {wi}"),
            _Q(f"SELECT COUNT(*) AS n FROM `edw-pilot.neural.beneficiaries` b {bj} {wd}"),
            # ── Monthly payout (active beneficiaries in period) ───────────────
            _Q(f"""
                SELECT COALESCE(SUM(c.monthly_amount),0) AS n
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                {bj} {wa}
            """),
            # ── Payment compliance (filter by payment date) ───────────────────
            _Q(f"""
                SELECT p.payment_status AS status, COUNT(*) AS cnt
                FROM `edw-pilot.neural.payments` p {pj} {wpay}
                GROUP BY p.payment_status
            """),
            # ── Breakdown charts (filter by registration date) ────────────────
            _Q(f"""
                SELECT c.category_name AS category, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                {bj} {wa}
                GROUP BY c.category_name ORDER BY count DESC
            """),
            _Q(f"""
                SELECT b.gender, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                {bj} {wa}
                GROUP BY b.gender ORDER BY count DESC
            """),
            _Q(f"""
                SELECT d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY d.district_name ORDER BY count DESC
            """),
            # ── Registration trend — monthly buckets (full history, readable scale) ─
            _Q("""
                SELECT
                    FORMAT_DATE('%Y-%m', rd.date) AS period,
                    COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id
                GROUP BY period
                ORDER BY period
            """),
            # ── Age distribution (filter by registration date) ────────────────
            _Q(f"""
                SELECT CASE WHEN b.age < 40 THEN 'Under 40' WHEN b.age < 60 THEN '40-59'
                            WHEN b.age < 70 THEN '60-69' WHEN b.age < 80 THEN '70-79' ELSE '80+' END AS age_group,
                       COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                {bj} {wa}
                GROUP BY age_group
            """),
            # ── Taluka tables (filter by registration date) ───────────────────
            _Q(f"""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY t.taluka_name, d.district_name ORDER BY count DESC LIMIT 5
            """),
            _Q(f"""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id
                JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY t.taluka_name, d.district_name ORDER BY count DESC
            """),
            _Q(f"""
                SELECT c.category_name AS category, COUNT(*) AS beneficiaries,
                       SUM(c.monthly_amount) AS monthly_payout
                FROM `edw-pilot.neural.beneficiaries` b
                JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id
                {bj} {wa}
                GROUP BY c.category_name ORDER BY monthly_payout DESC
            """),
        )

        pc = {row["status"]: int(row["cnt"]) for row in r_payments}
        paid, pending, failed = pc.get("paid", 0), pc.get("pending", 0), pc.get("failed", 0)
        tp = paid + pending + failed

        return {
            "status":    "ok",
            "range":     range,
            "date_from": date_from,
            "date_to":   date_to,
            "data": {
                "total_beneficiaries":    r_total[0]["n"],
                "active_beneficiaries":   r_active[0]["n"],
                "inactive_beneficiaries": r_inactive[0]["n"],
                "deceased_beneficiaries": r_deceased[0]["n"],
                "total_monthly_payout":   float(r_payout[0]["n"]),
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
