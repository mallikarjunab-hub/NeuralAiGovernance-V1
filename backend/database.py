"""
Dual Database Layer
  BigQuery  → beneficiary data, analytics, SQL queries
  Neon PG   → pgvector RAG chunks only
"""
import logging, asyncio
from typing import AsyncGenerator
from sqlalchemy import text, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from backend.config import settings

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# BigQuery Connection (data queries)
# ═══════════════════════════════════════════════════════════════

def _fix_bq(url: str) -> str:
    if not url:
        raise ValueError("DATABASE_URL not set — need BigQuery connection string")
    return url.strip().strip('"').strip("'")

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

bq_engine = create_engine(_fix_bq(settings.DATABASE_URL), echo=settings.DEBUG)
BQSessionFactory = sessionmaker(bind=bq_engine, autocommit=False, autoflush=False)

class bq_session_context:
    async def __aenter__(self):
        self._session = AsyncSessionWrapper(BQSessionFactory())
        return self._session
    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc:
                await self._session.rollback()
            else:
                await self._session.commit()
        finally:
            await self._session.close()

async def get_bq_db() -> AsyncGenerator[AsyncSessionWrapper, None]:
    async with bq_session_context() as s:
        yield s

async def execute_bq_query(sql: str) -> list[dict]:
    async with bq_session_context() as s:
        result = await s.execute(text(sql))
        cols = list(result.keys())
        return [dict(zip(cols, r)) for r in result.fetchmany(settings.MAX_SQL_ROWS)]

async def check_bq_health() -> bool:
    try:
        async with bq_session_context() as s:
            return (await s.execute(text("SELECT 1"))).scalar() == 1
    except:
        return False

async def wake_bigquery(retries=3, delay=2.0) -> bool:
    for i in range(1, retries + 1):
        try:
            if await check_bq_health():
                return True
        except Exception as e:
            logger.warning(f"BigQuery attempt {i}/{retries}: {e}")
            if i < retries:
                await asyncio.sleep(delay)
    return False


# ═══════════════════════════════════════════════════════════════
# Neon PostgreSQL Connection (RAG only)
# ═══════════════════════════════════════════════════════════════

_neon_engine = None
_NeonSessionFactory = None

def _fix_neon(url: str) -> str:
    if not url:
        raise ValueError("NEON_DATABASE_URL not set")
    url = url.strip().strip('"').strip("'")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if "?" in url:
        base, params = url.split("?", 1)
        keep = [p for p in params.split("&") if not any(k in p for k in ["channel_binding"])]
        url = base + ("?" + "&".join(keep) if keep else "")
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

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
            if exc:
                await self._session.rollback()
            else:
                await self._session.commit()
        finally:
            await self._session.close()

async def get_neon_db() -> AsyncGenerator[AsyncSessionWrapper, None]:
    async with neon_session_context() as s:
        yield s

async def check_neon_health() -> bool:
    try:
        async with neon_session_context() as s:
            return (await s.execute(text("SELECT 1"))).scalar() == 1
    except:
        return False

async def wake_neon(retries=3, delay=2.0) -> bool:
    for i in range(1, retries + 1):
        try:
            if await check_neon_health():
                return True
        except Exception as e:
            logger.warning(f"Neon attempt {i}/{retries}: {e}")
            if i < retries:
                await asyncio.sleep(delay)
    return False


# ═══════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════

async def dispose_all():
    try:
        await asyncio.to_thread(bq_engine.dispose)
    except:
        pass
    if _neon_engine:
        try:
            await asyncio.to_thread(_neon_engine.dispose)
        except:
            pass

class Base(DeclarativeBase):
    pass
