"""Beneficiary listing with filters — queries Neon PostgreSQL."""
import logging
from typing import Optional
from fastapi import APIRouter, Query
from backend.database import execute_sql_query

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
    params = []
    idx = 1

    if status:
        where.append(f"LOWER(b.status) = LOWER(${idx})")
        params.append(status)
        idx += 1
    if district_id:
        where.append(f"b.district_id = ${idx}")
        params.append(district_id)
        idx += 1
    if category_id:
        where.append(f"b.category_id = ${idx}")
        params.append(category_id)
        idx += 1
    if search:
        pattern = f"%{search}%"
        where.append(
            f"(CAST(b.beneficiary_id AS TEXT) LIKE ${idx} "
            f"OR LOWER(b.first_name) LIKE LOWER(${idx}) "
            f"OR LOWER(b.last_name) LIKE LOWER(${idx}))"
        )
        params.append(pattern)
        idx += 1

    wc = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    limit_idx = idx
    offset_idx = idx + 1
    params_with_paging = params + [page_size, offset]

    try:
        ct = await execute_sql_query(f"""
            SELECT COUNT(*) AS n
            FROM beneficiaries b {wc}
        """, params)
        total = int(ct[0]["n"]) if ct else 0

        rows = await execute_sql_query(f"""
            SELECT
                b.beneficiary_id,
                b.first_name,
                b.last_name,
                b.gender,
                b.age,
                b.date_of_birth,
                b.status,
                d.district_name,
                t.taluka_name,
                v.village_name,
                c.category_name,
                c.current_monthly_amount AS monthly_amount
            FROM beneficiaries b
            LEFT JOIN districts  d ON b.district_id  = d.district_id
            LEFT JOIN talukas    t ON b.taluka_id    = t.taluka_id
            LEFT JOIN villages   v ON b.village_id   = v.village_id
            LEFT JOIN categories c ON b.category_id  = c.category_id
            {wc}
            ORDER BY b.beneficiary_id DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """, params_with_paging)

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
