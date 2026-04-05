"""Dashboard KPIs and chart data from Neon PostgreSQL, with date-range filtering."""
import asyncio, logging, re
from fastapi import APIRouter, Query
from backend.database import execute_sql_query

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)
_Q = execute_sql_query

_INTERVALS = {
    "7d": "7 days", "30d": "30 days", "90d": "90 days",
    "6m": "6 months", "1y": "1 year",
}
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _bene_join(range_str: str, date_from: str, date_to: str) -> tuple[str, str]:
    """Returns (join_sql, where_condition) for beneficiary registration-date filter."""
    if range_str == "custom" and date_from and date_to:
        if _DATE_RE.match(date_from) and _DATE_RE.match(date_to):
            return (
                "",
                f"b.registration_date BETWEEN '{date_from}'::date AND '{date_to}'::date",
            )
    if range_str in _INTERVALS:
        return (
            "",
            f"b.registration_date >= CURRENT_DATE - INTERVAL '{_INTERVALS[range_str]}'",
        )
    return ("", "")


def _pay_join(range_str: str, date_from: str, date_to: str) -> tuple[str, str]:
    """Returns (join_sql, where_condition) for payment-date filter."""
    if range_str == "custom" and date_from and date_to:
        if _DATE_RE.match(date_from) and _DATE_RE.match(date_to):
            return (
                "",
                f"p.payment_date BETWEEN '{date_from}'::date AND '{date_to}'::date",
            )
    if range_str in _INTERVALS:
        return (
            "",
            f"p.payment_date >= CURRENT_DATE - INTERVAL '{_INTERVALS[range_str]}'",
        )
    return ("", "")


def _and(existing_where: str, extra_cond: str) -> str:
    """Merge an existing WHERE clause with an extra AND condition."""
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

        # Pre-compute WHERE clauses — Title Case to match seed data
        wa   = _and("WHERE b.status='Active'",   bw)
        wi   = _and("WHERE b.status='Inactive'",  bw)
        wd   = _and("WHERE b.status='Deceased'",  bw)
        wall = _and("", bw)
        wpay = _and("", pw)

        (
            r_total, r_active, r_inactive, r_deceased,
            r_payout, r_payments, r_category, r_gender,
            r_district, r_trend, r_age, r_top_talukas,
            r_all_talukas, r_cat_payout,
            r_status_trends, r_yoy_payments,
            r_batch_stats, r_life_cert_compliance,
        ) = await asyncio.gather(
            # ── Beneficiary counts (filter by registration date) ──────────────
            _Q(f"SELECT COUNT(*) AS n FROM beneficiaries b {bj} {wall}"),
            _Q(f"SELECT COUNT(*) AS n FROM beneficiaries b {bj} {wa}"),
            _Q(f"SELECT COUNT(*) AS n FROM beneficiaries b {bj} {wi}"),
            _Q(f"SELECT COUNT(*) AS n FROM beneficiaries b {bj} {wd}"),
            # ── Monthly payout (active beneficiaries in period) ───────────────
            _Q(f"""
                SELECT COALESCE(SUM(c.current_monthly_amount),0) AS n
                FROM beneficiaries b
                JOIN categories c ON b.category_id = c.category_id
                {bj} {wa}
            """),
            # ── Payment compliance (filter by payment date) ───────────────────
            _Q(f"""
                SELECT p.status AS status, COUNT(*) AS cnt
                FROM payments p {pj} {wpay}
                GROUP BY p.status
            """),
            # ── Breakdown charts (filter by registration date) ────────────────
            _Q(f"""
                SELECT c.category_name AS category, COUNT(*) AS count
                FROM beneficiaries b
                JOIN categories c ON b.category_id = c.category_id
                {bj} {wa}
                GROUP BY c.category_name ORDER BY count DESC
            """),
            _Q(f"""
                SELECT b.gender, COUNT(*) AS count
                FROM beneficiaries b
                {bj} {wa}
                GROUP BY b.gender ORDER BY count DESC
            """),
            _Q(f"""
                SELECT d.district_name AS district, COUNT(*) AS count
                FROM beneficiaries b
                JOIN districts d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY d.district_name ORDER BY count DESC
            """),
            # ── Registration trend — monthly buckets ──────────────────────────
            _Q(f"""
                SELECT
                    TO_CHAR(DATE_TRUNC('month', b.registration_date), 'YYYY-MM') AS period,
                    COUNT(*) AS count
                FROM beneficiaries b
                {bj} {wall}
                GROUP BY DATE_TRUNC('month', b.registration_date)
                ORDER BY period
            """),
            # ── Age distribution ──────────────────────────────────────────────
            _Q(f"""
                SELECT CASE WHEN b.age < 40 THEN 'Under 40' WHEN b.age < 60 THEN '40-59'
                            WHEN b.age < 70 THEN '60-69' WHEN b.age < 80 THEN '70-79' ELSE '80+' END AS age_group,
                       COUNT(*) AS count
                FROM beneficiaries b
                {bj} {wa}
                GROUP BY age_group
            """),
            # ── Taluka tables ─────────────────────────────────────────────────
            _Q(f"""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM beneficiaries b
                JOIN talukas t ON b.taluka_id = t.taluka_id
                JOIN districts d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY t.taluka_name, d.district_name ORDER BY count DESC LIMIT 5
            """),
            _Q(f"""
                SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count
                FROM beneficiaries b
                JOIN talukas t ON b.taluka_id = t.taluka_id
                JOIN districts d ON b.district_id = d.district_id
                {bj} {wa}
                GROUP BY t.taluka_name, d.district_name ORDER BY count DESC
            """),
            _Q(f"""
                SELECT c.category_name AS category, COUNT(*) AS beneficiaries,
                       SUM(c.current_monthly_amount) AS monthly_payout
                FROM beneficiaries b
                JOIN categories c ON b.category_id = c.category_id
                {bj} {wa}
                GROUP BY c.category_name ORDER BY monthly_payout DESC
            """),
            # ── Beneficiary status trends by year ────────────────────────────
            _Q(f"""
                SELECT
                    EXTRACT(YEAR FROM b.registration_date)::INT AS year,
                    b.status,
                    COUNT(*) AS count
                FROM beneficiaries b
                {bj} {_and("WHERE b.registration_date IS NOT NULL", bw)}
                GROUP BY year, b.status
                ORDER BY year, b.status
            """),
            # ── YoY payments — last 3 fiscal years from payment_summary ───────
            _Q("""
                SELECT
                    ps.payment_year AS year,
                    SUM(ps.total_net_amount)  AS total_paid,
                    SUM(ps.paid_count)        AS paid_count,
                    SUM(ps.failed_count)      AS failed_count,
                    SUM(ps.total_beneficiaries) AS total_bens
                FROM payment_summary ps
                WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 3
                GROUP BY ps.payment_year
                ORDER BY ps.payment_year
            """),
            # ── Payment batch stats (last 12 completed batches) ───────────────
            _Q("""
                SELECT
                    pb.batch_reference,
                    pb.payment_year,
                    pb.payment_month,
                    pb.fiscal_year_label,
                    pb.batch_status,
                    pb.total_beneficiaries,
                    pb.total_amount,
                    pb.paid_count,
                    pb.failed_count,
                    pb.pending_count
                FROM payment_batches pb
                ORDER BY pb.payment_year DESC, pb.payment_month DESC
                LIMIT 12
            """),
            # ── Life certificate compliance ────────────────────────────────────
            _Q("""
                SELECT
                    lc.due_year,
                    COUNT(*)                                                      AS total_certs,
                    COUNT(*) FILTER (WHERE lc.payment_suspended = FALSE)          AS compliant,
                    COUNT(*) FILTER (WHERE lc.payment_suspended = TRUE)           AS suspended,
                    COUNT(*) FILTER (WHERE lc.is_late_submission = TRUE)          AS late_submissions,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE lc.payment_suspended = FALSE)
                        / NULLIF(COUNT(*), 0), 1
                    ) AS compliance_pct
                FROM life_certificates lc
                GROUP BY lc.due_year
                ORDER BY lc.due_year DESC
                LIMIT 4
            """),
        )

        # Payment compliance — Title Case status values
        pc = {row["status"]: int(row["cnt"]) for row in r_payments}
        paid    = pc.get("Paid", 0)
        pending = pc.get("Pending", 0)
        failed  = pc.get("Failed", 0)
        tp = paid + pending + failed

        # Batch stats summary
        batches = r_batch_stats or []
        completed_batches = [b for b in batches if b.get("batch_status") == "Completed"]
        last_batch = batches[0] if batches else {}
        total_batch_amount = sum(float(b.get("total_amount") or 0) for b in completed_batches)

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
                "category_payout": [{"category": r["category"], "beneficiaries": int(r["beneficiaries"]), "monthly_payout": float(r["monthly_payout"])} for r in r_cat_payout],
                "beneficiary_status_trends": [{"year": r["year"], "status": r["status"], "count": int(r["count"])} for r in r_status_trends],
                # YoY payments from payment_summary (no hallucination)
                "yoy_payments": [
                    {
                        "year":        r["year"],
                        "total_paid":  float(r["total_paid"] or 0),
                        "paid_count":  int(r["paid_count"] or 0),
                        "failed_count": int(r["failed_count"] or 0),
                        "total_bens":  int(r["total_bens"] or 0),
                    }
                    for r in r_yoy_payments
                ],
                # Payment batch stats
                "payment_batches": [
                    {
                        "batch_reference":    r["batch_reference"],
                        "payment_year":       r["payment_year"],
                        "payment_month":      r["payment_month"],
                        "fiscal_year_label":  r["fiscal_year_label"],
                        "batch_status":       r["batch_status"],
                        "total_beneficiaries": int(r["total_beneficiaries"] or 0),
                        "total_amount":       float(r["total_amount"] or 0),
                        "paid_count":         int(r["paid_count"] or 0),
                        "failed_count":       int(r["failed_count"] or 0),
                        "pending_count":      int(r["pending_count"] or 0),
                    }
                    for r in batches
                ],
                "batch_summary": {
                    "last_batch_reference": last_batch.get("batch_reference", ""),
                    "last_batch_amount":    float(last_batch.get("total_amount") or 0),
                    "last_batch_paid":      int(last_batch.get("paid_count") or 0),
                    "last_batch_failed":    int(last_batch.get("failed_count") or 0),
                    "completed_batches_total_amount": total_batch_amount,
                },
                # Life certificate compliance
                "life_cert_compliance": [
                    {
                        "due_year":        r["due_year"],
                        "total_certs":     int(r["total_certs"] or 0),
                        "compliant":       int(r["compliant"] or 0),
                        "suspended":       int(r["suspended"] or 0),
                        "late_submissions": int(r["late_submissions"] or 0),
                        "compliance_pct":  float(r["compliance_pct"] or 0),
                    }
                    for r in r_life_cert_compliance
                ],
            },
        }

    except Exception as e:
        logger.error(f"Dashboard error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e), "data": {}}
