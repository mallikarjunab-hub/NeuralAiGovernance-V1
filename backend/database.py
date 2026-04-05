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
                await self._session.commit()
        finally:
            await self._session.close()


async def get_neon_db() -> AsyncGenerator[AsyncSessionWrapper, None]:
    async with neon_session_context() as s:
        yield s


# ═══════════════════════════════════════════════════════════════
# SQL Execution — direct asyncpg for generated SQL queries
# ═══════════════════════════════════════════════════════════════

async def execute_sql_query(sql: str, params: list | None = None) -> list[dict]:
    """Execute a PostgreSQL SELECT on Neon and return rows as dicts."""
    import asyncpg
    conn = await asyncpg.connect(settings.NEON_DATABASE_URL)
    try:
        rows = await conn.fetch(sql, *(params or []))
        return [dict(r) for r in rows]
    finally:
        await conn.close()



# ═══════════════════════════════════════════════════════════════
# Health Checks
# ═══════════════════════════════════════════════════════════════

async def check_neon_health() -> bool:
    try:
        async with neon_session_context() as s:
            return (await s.execute(text("SELECT 1"))).scalar() == 1
    except Exception:
        return False



async def wake_neon(retries: int = 3, delay: float = 2.0) -> bool:
    for i in range(1, retries + 1):
        try:
            if await check_neon_health():
                return True
        except Exception as e:
            logger.warning(f"Neon PostgreSQL attempt {i}/{retries}: {e}")
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
