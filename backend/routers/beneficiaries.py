"""Beneficiary listing with filters — queries BigQuery."""
import logging
from typing import Optional
from fastapi import APIRouter, Query
from backend.database import execute_bq_query

router = APIRouter(prefix="/api/beneficiaries", tags=["Beneficiaries"])
logger = logging.getLogger(__name__)


@router.get("")
async def list_beneficiaries(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    district_id: Optional[int] = None,
    category_id: Optional[int] = None,
    search: Optional[str] = None,
):
    where = []
    if status:
        where.append(f"LOWER(b.status) = '{status.lower()}'")
    if district_id:
        where.append(f"b.district_id = {int(district_id)}")
    if category_id:
        where.append(f"b.category_id = {int(category_id)}")
    if search:
        safe = search.replace("'", "''")
        where.append(
            f"(CAST(b.beneficiary_id AS STRING) LIKE '%{safe}%' "
            f"OR LOWER(b.first_name) LIKE '%{safe.lower()}%' "
            f"OR LOWER(b.last_name) LIKE '%{safe.lower()}%')"
        )

    wc = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    try:
        ct = await execute_bq_query(f"""
            SELECT COUNT(*) AS n
            FROM `edw-pilot.neural.beneficiaries` b {wc}
        """)
        total = int(ct[0]["n"]) if ct else 0

        rows = await execute_bq_query(f"""
            SELECT
                b.beneficiary_id,
                b.first_name,
                b.last_name,
                b.gender,
                b.age,
                b.dob,
                b.status,
                d.district_name,
                t.taluka_name,
                v.village_name,
                c.category_name,
                c.monthly_amount
            FROM `edw-pilot.neural.beneficiaries` b
            LEFT JOIN `edw-pilot.neural.districts`  d ON b.district_id  = d.district_id
            LEFT JOIN `edw-pilot.neural.talukas`    t ON b.taluka_id    = t.taluka_id
            LEFT JOIN `edw-pilot.neural.villages`   v ON b.village_id   = v.village_id
            LEFT JOIN `edw-pilot.neural.categories` c ON b.category_id  = c.category_id
            {wc}
            ORDER BY b.beneficiary_id DESC
            LIMIT {page_size} OFFSET {offset}
        """)

        return {
            "status": "ok",
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": rows,
        }
    except Exception as e:
        logger.error(f"Beneficiaries error: {e}", exc_info=True)
        return {"status": "error", "total": 0, "items": [], "detail": str(e)}