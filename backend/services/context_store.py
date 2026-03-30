"""
Context Store — Neon PostgreSQL-backed conversation context with in-memory L1 cache.

Storage strategy:
  L1  in-memory dict    — instant reads within the same server process
  L2  Neon PostgreSQL   — persistent across restarts, shared across instances

Write path: update L1 immediately → schedule Neon write as background task (non-blocking).
Read path:  L1 hit → return immediately.  L1 miss → read from Neon (≤2s timeout) → populate L1.

If Neon is unavailable the store degrades gracefully to in-memory only — no query failures.

Table DDL (created on startup via setup()):
  conversation_context (
    id                SERIAL PRIMARY KEY,
    session_id        TEXT NOT NULL,
    question          TEXT NOT NULL,
    resolved_question TEXT NOT NULL,
    answer            TEXT NOT NULL,
    intent            TEXT NOT NULL,     -- SQL | RAG | EDGE
    sql_data          TEXT,              -- JSON-encoded list[dict]
    created_at        TIMESTAMPTZ DEFAULT NOW()
  )
"""
import asyncio, json, logging, random
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import text

logger = logging.getLogger(__name__)

MAX_TURNS    = 5     # keep last 5 turns per session
MAX_SESSIONS = 500   # L1 cache cap — evict oldest when exceeded
PURGE_ODDS   = 20    # run global Neon purge on 1-in-N writes (probabilistic)

# Background-task references — prevents GC before coroutine completes
_bg_tasks: set = set()


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ConversationTurn:
    question:          str
    resolved_question: str
    answer:            str
    intent:            str              # "SQL" | "RAG" | "EDGE"
    sql_data:          Optional[list]   # raw BigQuery rows for arithmetic follow-ups


# ── Store ─────────────────────────────────────────────────────────────────────

class ContextStore:
    """
    Hybrid context store.  All public methods are async to support the Neon path.
    """

    def __init__(self):
        # L1 cache: session_id → list[ConversationTurn] (most recent MAX_TURNS)
        self._cache: dict[str, list[ConversationTurn]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def setup(self, db) -> None:
        """
        Create the conversation_context table and index if they don't exist.
        Call once from the app lifespan (alongside RAG setup) — idempotent.
        """
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS conversation_context (
                id                SERIAL PRIMARY KEY,
                session_id        TEXT        NOT NULL,
                question          TEXT        NOT NULL,
                resolved_question TEXT        NOT NULL,
                answer            TEXT        NOT NULL,
                intent            TEXT        NOT NULL,
                sql_data          TEXT,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await db.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ctx_session_time
            ON conversation_context (session_id, created_at DESC)
        """))

    async def get_context(self, session_id: str) -> list[ConversationTurn]:
        """
        Return last MAX_TURNS conversation turns for this session.
        L1 hit → instant.  L1 miss → Neon read with 2-second timeout → fallback to [].
        """
        if not session_id:
            return []

        # ── L1 cache hit ──────────────────────────────────────
        if session_id in self._cache:
            return self._cache[session_id]

        # ── L2 Neon read (first request after server start) ───
        try:
            turns = await asyncio.wait_for(
                self._read_from_neon(session_id), timeout=2.0
            )
            self._cache[session_id] = turns
            return turns
        except asyncio.TimeoutError:
            logger.warning(f"Context read timed out for session {session_id[:12]}… — using empty context")
            return []
        except Exception as e:
            logger.warning(f"Context read from Neon failed (non-fatal): {e}")
            return []

    async def add_turn(
        self,
        session_id:        str,
        question:          str,
        resolved_question: str,
        answer:            str,
        intent:            str,
        sql_data:          Optional[list] = None,
    ) -> None:
        """
        Append a conversation turn.
        L1 is updated synchronously; Neon write is fire-and-forget (does not block response).
        """
        if not session_id:
            return

        turn = ConversationTurn(
            question=question,
            resolved_question=resolved_question,
            answer=answer[:800] + ("…" if len(answer) > 800 else ""),
            intent=intent,
            sql_data=sql_data[:50] if sql_data else None,
        )

        # ── Update L1 immediately ─────────────────────────────
        bucket = self._cache.setdefault(session_id, [])
        bucket.append(turn)
        if len(bucket) > MAX_TURNS:
            self._cache[session_id] = bucket[-MAX_TURNS:]

        # Evict oldest sessions if L1 grows too large
        if len(self._cache) > MAX_SESSIONS:
            oldest = next(iter(self._cache))
            del self._cache[oldest]

        # ── Schedule Neon write as background task ────────────
        task = asyncio.create_task(self._write_to_neon(session_id, turn))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _read_from_neon(self, session_id: str) -> list[ConversationTurn]:
        from backend.database import neon_session_context  # local import avoids circular
        async with neon_session_context() as db:
            result = await db.execute(text("""
                SELECT question, resolved_question, answer, intent, sql_data
                FROM conversation_context
                WHERE session_id = :sid
                  AND created_at > NOW() - INTERVAL '30 minutes'
                ORDER BY created_at DESC
                LIMIT :n
            """), {"sid": session_id, "n": MAX_TURNS})
            rows = result.fetchall()

        # Rows come back newest-first; reverse so oldest-first for the prompt
        turns = []
        for row in reversed(rows):
            turns.append(ConversationTurn(
                question=row[0],
                resolved_question=row[1],
                answer=row[2],
                intent=row[3],
                sql_data=json.loads(row[4]) if row[4] else None,
            ))
        return turns

    async def _write_to_neon(self, session_id: str, turn: ConversationTurn) -> None:
        from backend.database import neon_session_context
        try:
            async with neon_session_context() as db:
                # Insert the new turn
                await db.execute(text("""
                    INSERT INTO conversation_context
                        (session_id, question, resolved_question, answer, intent, sql_data)
                    VALUES
                        (:sid, :q, :rq, :a, :i, :sd)
                """), {
                    "sid": session_id,
                    "q":   turn.question,
                    "rq":  turn.resolved_question,
                    "a":   turn.answer,
                    "i":   turn.intent,
                    "sd":  json.dumps(turn.sql_data, default=str) if turn.sql_data else None,
                })
                # Keep only the last MAX_TURNS rows for this session
                await db.execute(text("""
                    DELETE FROM conversation_context
                    WHERE session_id = :sid
                      AND id NOT IN (
                          SELECT id FROM conversation_context
                          WHERE session_id = :sid
                          ORDER BY created_at DESC
                          LIMIT :n
                      )
                """), {"sid": session_id, "n": MAX_TURNS})
                # Opportunistic global purge — runs 1-in-PURGE_ODDS writes only
                if random.randint(1, PURGE_ODDS) == 1:
                    await db.execute(text("""
                        DELETE FROM conversation_context
                        WHERE created_at < NOW() - INTERVAL '30 minutes'
                    """))
        except Exception as e:
            logger.warning(f"Context write to Neon failed (non-fatal, L1 still valid): {e}")


# ── Module-level singleton ─────────────────────────────────────────────────────
context_store = ContextStore()
