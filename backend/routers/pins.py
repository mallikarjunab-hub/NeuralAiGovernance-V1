"""
/api/pins — Dashboard chart pinning (Enhancement 4)

Endpoints:
  GET    /api/pins                         list all pinned charts
  POST   /api/pins                         pin a chart
  DELETE /api/pins/{pin_id}               unpin a chart

Pins are stored in the dashboard_pins Neon table and loaded on frontend startup
so pinned charts appear as widgets on the dashboard.
"""
import logging, json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from backend.database import neon_session_context

router = APIRouter(prefix="/api/pins", tags=["Pins"])
logger = logging.getLogger(__name__)

_TABLE_READY = False


async def _ensure_table():
    global _TABLE_READY
    if _TABLE_READY:
        return
    async with neon_session_context() as db:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS dashboard_pins (
                id          SERIAL PRIMARY KEY,
                title       TEXT NOT NULL,
                question    TEXT NOT NULL,
                answer      TEXT,
                intent      TEXT,
                chart_type  TEXT,
                data_json   TEXT,
                position    INT  DEFAULT 0,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await db.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_pins_pos ON dashboard_pins (position ASC, created_at DESC)"
        ))
    _TABLE_READY = True


class PinRequest(BaseModel):
    title:      str
    question:   str
    answer:     Optional[str] = None
    intent:     Optional[str] = None
    chart_type: Optional[str] = None
    data:       Optional[list] = None
    position:   int = 0


@router.get("")
async def list_pins():
    """Return all pinned charts ordered by position."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            result = await db.execute(text("""
                SELECT id, title, question, answer, intent, chart_type, data_json, position, created_at
                FROM dashboard_pins
                ORDER BY position ASC, created_at DESC
                LIMIT 20
            """))
            rows = result.fetchall()
        return {"pins": [
            {"id": r[0], "title": r[1], "question": r[2],
             "answer": r[3], "intent": r[4], "chart_type": r[5],
             "data": json.loads(r[6]) if r[6] else None,
             "position": r[7], "created_at": str(r[8])}
            for r in rows
        ]}
    except Exception as e:
        logger.warning(f"Pins list failed: {e}")
        return {"pins": []}


@router.post("")
async def add_pin(req: PinRequest):
    """Pin a chart widget to the dashboard."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            # Cap at 12 pins
            count_result = await db.execute(text("SELECT COUNT(*) FROM dashboard_pins"))
            count = count_result.scalar()
            if count >= 12:
                raise HTTPException(400, "Maximum 12 pinned charts allowed. Remove some first.")
            result = await db.execute(text("""
                INSERT INTO dashboard_pins
                    (title, question, answer, intent, chart_type, data_json, position)
                VALUES (:ti, :q, :a, :i, :ct, :dj, :pos)
                RETURNING id
            """), {
                "ti":  req.title[:100],
                "q":   req.question,
                "a":   req.answer,
                "i":   req.intent,
                "ct":  req.chart_type,
                "dj":  json.dumps(req.data, default=str) if req.data else None,
                "pos": req.position,
            })
            pin_id = result.fetchone()[0]
        return {"id": pin_id, "message": "Chart pinned to dashboard"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pin save failed: {e}")
        raise HTTPException(500, "Failed to pin chart")


@router.delete("/{pin_id}")
async def delete_pin(pin_id: int):
    """Remove a pinned chart widget."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            await db.execute(
                text("DELETE FROM dashboard_pins WHERE id = :id"),
                {"id": pin_id}
            )
        return {"message": "Chart unpinned"}
    except Exception as e:
        logger.error(f"Pin delete failed: {e}")
        raise HTTPException(500, "Failed to unpin chart")
