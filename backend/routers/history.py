"""
/api/history  — query history and favorites (Enhancement 2)

Endpoints:
  GET  /api/history?session_id=...&limit=20   list recent queries for a session
  POST /api/history/favorite                  star/favorite a query
  GET  /api/history/favorites                 list all starred queries
  DELETE /api/history/favorite/{id}           un-star a query

Table (created on first call):
  query_favorites (id, session_id, question, answer, intent, chart_type, data_json,
                   created_at, label)
"""
import logging, json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from backend.database import neon_session_context

router = APIRouter(prefix="/api/history", tags=["History"])
logger = logging.getLogger(__name__)

_TABLE_READY = False


async def _ensure_table():
    global _TABLE_READY
    if _TABLE_READY:
        return
    async with neon_session_context() as db:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS query_favorites (
                id          SERIAL PRIMARY KEY,
                session_id  TEXT,
                question    TEXT NOT NULL,
                answer      TEXT,
                intent      TEXT,
                chart_type  TEXT,
                data_json   TEXT,
                label       TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await db.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_qfav_session ON query_favorites (session_id, created_at DESC)"
        ))
    _TABLE_READY = True


class FavoriteRequest(BaseModel):
    session_id: Optional[str] = None
    question:   str
    answer:     Optional[str] = None
    intent:     Optional[str] = None
    chart_type: Optional[str] = None
    data:       Optional[list] = None
    label:      Optional[str] = None


@router.get("")
async def get_history(session_id: str, limit: int = 20):
    """Return most recent queries for a session from conversation_context table."""
    if not session_id:
        raise HTTPException(400, "session_id required")
    try:
        async with neon_session_context() as db:
            result = await db.execute(text("""
                SELECT question, resolved_question, answer, intent, created_at
                FROM conversation_context
                WHERE session_id = :sid
                  AND created_at > NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
                LIMIT :lim
            """), {"sid": session_id, "lim": min(limit, 100)})
            rows = result.fetchall()
        return {"history": [
            {"question": r[0], "resolved_question": r[1], "answer": r[2][:200],
             "intent": r[3], "created_at": str(r[4])}
            for r in rows
        ]}
    except Exception as e:
        logger.warning(f"History fetch failed: {e}")
        return {"history": []}


@router.post("/favorite")
async def add_favorite(req: FavoriteRequest):
    """Star/favorite a query result for quick re-access."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            result = await db.execute(text("""
                INSERT INTO query_favorites
                    (session_id, question, answer, intent, chart_type, data_json, label)
                VALUES
                    (:sid, :q, :a, :i, :ct, :dj, :lbl)
                RETURNING id
            """), {
                "sid": req.session_id,
                "q":   req.question,
                "a":   req.answer,
                "i":   req.intent,
                "ct":  req.chart_type,
                "dj":  json.dumps(req.data, default=str) if req.data else None,
                "lbl": req.label or req.question[:60],
            })
            fav_id = result.fetchone()[0]
        return {"id": fav_id, "message": "Saved to favorites"}
    except Exception as e:
        logger.error(f"Favorite save failed: {e}")
        raise HTTPException(500, "Failed to save favorite")


@router.get("/favorites")
async def list_favorites(session_id: Optional[str] = None, limit: int = 50):
    """List all starred queries, optionally filtered by session."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            if session_id:
                result = await db.execute(text("""
                    SELECT id, question, answer, intent, chart_type, data_json, label, created_at
                    FROM query_favorites
                    WHERE session_id = :sid
                    ORDER BY created_at DESC LIMIT :lim
                """), {"sid": session_id, "lim": min(limit, 200)})
            else:
                result = await db.execute(text("""
                    SELECT id, question, answer, intent, chart_type, data_json, label, created_at
                    FROM query_favorites
                    ORDER BY created_at DESC LIMIT :lim
                """), {"lim": min(limit, 200)})
            rows = result.fetchall()
        return {"favorites": [
            {"id": r[0], "question": r[1], "answer": (r[2] or "")[:200],
             "intent": r[3], "chart_type": r[4],
             "data": json.loads(r[5]) if r[5] else None,
             "label": r[6], "created_at": str(r[7])}
            for r in rows
        ]}
    except Exception as e:
        logger.warning(f"Favorites list failed: {e}")
        return {"favorites": []}


@router.delete("/favorite/{fav_id}")
async def delete_favorite(fav_id: int):
    """Remove a query from favorites."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            await db.execute(
                text("DELETE FROM query_favorites WHERE id = :id"),
                {"id": fav_id}
            )
        return {"message": "Removed from favorites"}
    except Exception as e:
        logger.error(f"Favorite delete failed: {e}")
        raise HTTPException(500, "Failed to remove favorite")
