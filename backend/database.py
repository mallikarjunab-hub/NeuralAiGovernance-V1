"""
Database Layer — Neon PostgreSQL (single unified database)
  Beneficiary queries, analytics, RAG pgvector, conversation context — all on Neon.
"""
import logging, asyncio
from typing import AsyncGenerator
from sqlalchemy import text, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from backend.config import settings

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════

def _fix_neon(url: str) -> str:
    if not url:
        raise ValueError("NEON_DATABASE_URL not set")
    url = url.strip().strip('"').strip("'")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    if "?" in url:
        base, params = url.split("?", 1)
        keep = [p for p in params.split("&") if not any(k in p for k in ["channel_binding", "connect_timeout"])]
        url = base + ("?" + "&".join(keep) if keep else "")
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    url += "&connect_timeout=10"
    return url


class AsyncResultWrapper:
    def __init__(self, result):
        self._result = result
    def keys(self):
        return self._result.keys()
    def fetchmany(self, size: int):
        return self._result.fetchmany(size)
    def fetchall(self):
        return self._result.fetchall()
    def scalar(self):
        return self._result.scalar()


class AsyncSessionWrapper:
    def __init__(self, sync_session):
        self._sync_session = sync_session
    async def execute(self, statement, params=None):
        if params:
            result = await asyncio.to_thread(self._sync_session.execute, statement, params)
        else:
            result = await asyncio.to_thread(self._sync_session.execute, statement)
        return AsyncResultWrapper(result)
    async def commit(self):
        await asyncio.to_thread(self._sync_session.commit)
    async def rollback(self):
        await asyncio.to_thread(self._sync_session.rollback)
    async def close(self):
        await asyncio.to_thread(self._sync_session.close)


# ═══════════════════════════════════════════════════════════════
# Neon PostgreSQL Engine
# ═══════════════════════════════════════════════════════════════

_neon_engine = None
_NeonSessionFactory = None
_last_neon_error = None


def _init_neon():
    global _neon_engine, _NeonSessionFactory
    if _neon_engine is not None:
        return
    try:
        _neon_engine = create_engine(
            _fix_neon(settings.NEON_DATABASE_URL),
            pool_size=settings.NEON_POOL_SIZE,
            max_overflow=settings.NEON_MAX_OVERFLOW,
            pool_pre_ping=True,
            pool_timeout=15,
            pool_recycle=300,
            echo=settings.DEBUG,
        )
        _NeonSessionFactory = sessionmaker(bind=_neon_engine, autocommit=False, autoflush=False)
        logger.info("Neon PostgreSQL engine initialized")
    except Exception as e:
        logger.error(f"Neon init failed: {e}")
        _neon_engine = None


class neon_session_context:
    async def __aenter__(self):
        _init_neon()
        if _NeonSessionFactory is None:
            raise RuntimeError("Neon DB not initialized")
        self._session = AsyncSessionWrapper(_NeonSessionFactory())
        return self._session
    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                await self._session.rollback()
            else:
                try:
                    await self._session.commit()
                except Exception:
                    # Session may be in a bad state (e.g. PendingRollbackError
                    # from a prior failed operation) — rollback to clean up
                    try:
                        await self._session.rollback()
                    except Exception:
                        pass
        finally:
            await self._session.close()


async def get_neon_db() -> AsyncGenerator[AsyncSessionWrapper, None]:
    async with neon_session_context() as s:
        yield s


# ═══════════════════════════════════════════════════════════════
# SQL Execution — direct asyncpg for generated SQL queries
# ═══════════════════════════════════════════════════════════════

def _friendly_pg_error(e: Exception) -> str:
    """
    Map PostgreSQL error codes to friendly, actionable messages (Enhancement 9).
    Covers the most common runtime errors encountered in NL-to-SQL pipelines.
    """
    msg = str(e).lower()
    code = getattr(getattr(e, "pgcode", None), "__str__", lambda: "")() or ""

    # Syntax / structure errors
    if "42601" in code or "syntax error" in msg:
        return ("The generated SQL contained a syntax error. "
                "Please rephrase your question or try a simpler query.")
    if "42703" in code or "column" in msg and "does not exist" in msg:
        col = ""
        import re; m = re.search(r'column "([^"]+)"', str(e))
        if m: col = f' "{m.group(1)}"'
        return (f"Column{col} not found in the table. "
                "This can happen with ambiguous column names — try being more specific.")
    if "42p01" in code or "relation" in msg and "does not exist" in msg:
        return ("A table referenced in the query does not exist. "
                "Please report this issue — the schema may have changed.")
    if "42883" in code or "function" in msg and "does not exist" in msg:
        return ("An unsupported SQL function was used. "
                "Please rephrase your question differently.")

    # Data / value errors
    if "22003" in code or "numeric field overflow" in msg:
        return ("A numeric value in the query exceeded the column's precision limit. "
                "Try aggregating with ROUND() or reduce decimal places.")
    if "22007" in code or "invalid input syntax for type date" in msg:
        return ("An invalid date format was used. "
                "Dates should be in YYYY-MM-DD format.")
    if "22012" in code or "division by zero" in msg:
        return ("Division by zero occurred in the query — "
                "this usually means a category had zero beneficiaries for the filter applied.")
    if "23505" in code or "unique constraint" in msg and "violates" in msg:
        return ("A duplicate record was detected. No changes were made.")

    # Connectivity / timeout
    if "connection" in msg and ("refused" in msg or "timeout" in msg or "reset" in msg):
        return ("The database connection timed out. "
                "Neon PostgreSQL may be in cold-start — please wait 10 seconds and try again.")
    if "too many connections" in msg or "53300" in code:
        return ("The database connection pool is at capacity. "
                "Please retry in a moment — connections will free up automatically.")

    # Auth
    if "28" in code or "password authentication" in msg:
        return ("Database authentication failed. "
                "Please check NEON_DATABASE_URL in your .env file.")

    # Generic fallback
    return (f"Database error: {str(e)[:120]}. "
            "Please try rephrasing your question or contact support.")


async def execute_sql_query(sql: str, params: list | None = None) -> list[dict]:
    """
    Execute a PostgreSQL SELECT on Neon and return rows as dicts.
    Raises RuntimeError with a friendly message on failure (Enhancement 9).
    """
    import asyncpg
    try:
        conn = await asyncpg.connect(_fix_neon(settings.NEON_DATABASE_URL))
        try:
            rows = await conn.fetch(sql, *(params or []))
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    except asyncpg.PostgresError as e:
        friendly = _friendly_pg_error(e)
        raise RuntimeError(friendly) from e
    except Exception as e:
        friendly = _friendly_pg_error(e)
        raise RuntimeError(friendly) from e



# ═══════════════════════════════════════════════════════════════
# Health Checks
# ═══════════════════════════════════════════════════════════════

async def check_neon_health() -> bool:
    global _last_neon_error
    try:
        async with neon_session_context() as s:
            ok = (await s.execute(text("SELECT 1"))).scalar() == 1
            _last_neon_error = None
            return ok
    except Exception as e:
        _last_neon_error = str(e)
        return False


def get_last_neon_error() -> str | None:
    return _last_neon_error



async def wake_neon(retries: int = 3, delay: float = 2.0) -> bool:
    for i in range(1, retries + 1):
        if await check_neon_health():
            return True
        if _last_neon_error:
            logger.warning(f"Neon PostgreSQL attempt {i}/{retries} failed: {_last_neon_error}")
        if i < retries:
            await asyncio.sleep(delay)
    return False



# ═══════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════

async def dispose_all():
    if _neon_engine:
        try:
            await asyncio.to_thread(_neon_engine.dispose)
        except Exception:
            pass


class Base(DeclarativeBase):
    pass
