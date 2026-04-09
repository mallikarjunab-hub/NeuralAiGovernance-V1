"""
/api/audit — Audit trail endpoint (Enhancement 8)

Surfaces the audit_log table (if it exists) and auto-creates a simple one
backed by the conversation_context table so there is always something to show.

Endpoints:
  GET  /api/audit?limit=50&intent=SQL    list recent audit events
  POST /api/audit/log                    log a manual audit event (internal use)

The frontend shows a collapsible audit panel that polls this endpoint.
"""
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from backend.database import neon_session_context

router = APIRouter(prefix="/api/audit", tags=["Audit"])
logger = logging.getLogger(__name__)

_TABLE_READY = False


async def _ensure_table():
    global _TABLE_READY
    if _TABLE_READY:
        return
    async with neon_session_context() as db:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id          SERIAL PRIMARY KEY,
                event_type  TEXT NOT NULL,          -- 'QUERY','EXPORT','FAVORITE','PIN','ERROR'
                session_id  TEXT,
                ip_address  TEXT,
                question    TEXT,
                intent      TEXT,
                details     TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await db.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_log (created_at DESC)"
        ))
    _TABLE_READY = True


class AuditEvent(BaseModel):
    event_type: str
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    question:   Optional[str] = None
    intent:     Optional[str] = None
    details:    Optional[str] = None


@router.get("")
async def list_audit(
    limit:  int           = 50,
    intent: Optional[str] = None,
    event_type: Optional[str] = None,
):
    """
    Return recent audit events. If audit_log is empty, falls back to
    conversation_context as a proxy audit trail.
    """
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            # Try audit_log first
            if intent:
                result = await db.execute(text("""
                    SELECT id, event_type, session_id, ip_address, question, intent, details, created_at
                    FROM audit_log WHERE intent = :i
                    ORDER BY created_at DESC LIMIT :lim
                """), {"i": intent, "lim": min(limit, 500)})
            elif event_type:
                result = await db.execute(text("""
                    SELECT id, event_type, session_id, ip_address, question, intent, details, created_at
                    FROM audit_log WHERE event_type = :et
                    ORDER BY created_at DESC LIMIT :lim
                """), {"et": event_type, "lim": min(limit, 500)})
            else:
                result = await db.execute(text("""
                    SELECT id, event_type, session_id, ip_address, question, intent, details, created_at
                    FROM audit_log
                    ORDER BY created_at DESC LIMIT :lim
                """), {"lim": min(limit, 500)})
            rows = result.fetchall()

            # If audit_log is empty, use conversation_context as fallback
            if not rows:
                result2 = await db.execute(text("""
                    SELECT id, intent AS event_type, session_id,
                           NULL AS ip_address, question, intent, answer, created_at
                    FROM conversation_context
                    ORDER BY created_at DESC LIMIT :lim
                """), {"lim": min(limit, 500)})
                rows = result2.fetchall()

        return {"events": [
            {"id": r[0], "event_type": r[1], "session_id": r[2],
             "ip_address": r[3], "question": r[4], "intent": r[5],
             "details": (r[6] or "")[:200], "created_at": str(r[7])}
            for r in rows
        ], "total": len(rows)}
    except Exception as e:
        logger.warning(f"Audit list failed: {e}")
        return {"events": [], "total": 0}


@router.post("/log")
async def log_event(ev: AuditEvent):
    """Write a manual audit event — called internally by other endpoints."""
    await _ensure_table()
    try:
        async with neon_session_context() as db:
            await db.execute(text("""
                INSERT INTO audit_log (event_type, session_id, ip_address, question, intent, details)
                VALUES (:et, :sid, :ip, :q, :i, :d)
            """), {
                "et": ev.event_type, "sid": ev.session_id, "ip": ev.ip_address,
                "q": ev.question, "i": ev.intent, "d": ev.details,
            })
        return {"message": "Event logged"}
    except Exception as e:
        logger.warning(f"Audit log write failed: {e}")
        raise HTTPException(500, "Failed to log event")


# ── Auto-log helper (called from query.py) ────────────────────────────────────

async def auto_log(session_id: str, ip: str, question: str, intent: str, details: str = ""):
    """Non-blocking fire-and-forget audit log write."""
    import asyncio
    try:
        await _ensure_table()
        async with neon_session_context() as db:
            await db.execute(text("""
                INSERT INTO audit_log (event_type, session_id, ip_address, question, intent, details)
                VALUES ('QUERY', :sid, :ip, :q, :i, :d)
            """), {"sid": session_id, "ip": ip, "q": question[:500], "i": intent, "d": details[:200]})
    except Exception:
        pass  # audit is non-fatal
