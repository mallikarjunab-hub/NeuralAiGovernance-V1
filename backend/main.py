"""
Neural AI Governance v3.0
Database: Neon PostgreSQL (data + pgvector RAG)
3-way routing: Edge → SQL → RAG
"""
import logging, os, time
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from backend.config import settings
from backend.database import (
    check_neon_health, wake_neon,
    neon_session_context, dispose_all,
)
from backend.services.cache import check_health as cache_ok
from backend.services.gemini_service import check_health as gemini_ok
from backend.services.rag_service import setup, is_ingested, ingest
from scripts.ingest_web_sources import ingest_web_sources
from backend.services.context_store import context_store
from backend.routers.query         import router as query_router
from backend.routers.analytics     import router as analytics_router
from backend.routers.beneficiaries import router as beneficiaries_router
from backend.routers.rag           import router as rag_router
from backend.routers.history       import router as history_router
from backend.routers.export        import router as export_router
from backend.routers.pins          import router as pins_router
from backend.routers.audit         import router as audit_router
from backend.middleware.rate_limit import RateLimitMiddleware

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("neural_ai_governance")

_KB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "documents", "dssy_knowledge_base.md")
_FE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info("  Neural AI Governance v3.0")
    logger.info(f"  Environment : {settings.ENVIRONMENT}")
    logger.info(f"  Data DB     : Neon PostgreSQL")
    logger.info(f"  RAG DB      : Neon PostgreSQL + pgvector")
    logger.info(f"  AI Engine   : Gemini (gemini-2.5-flash-lite + gemini-embedding-001)")
    logger.info("=" * 60)

    # ── Neon PostgreSQL health check + RAG setup ────────────────
    neon_ok = False
    if settings.NEON_DATABASE_URL:
        neon_ok = await wake_neon(retries=5, delay=3.0)   # 15s window for cold start
        logger.info(f"{'✅' if neon_ok else '⚠️ '} Neon PostgreSQL {'connected' if neon_ok else 'FAILED — check NEON_DATABASE_URL'}")

        if neon_ok:
            async with neon_session_context() as db:
                try:
                    await setup(db)
                    logger.info("✅ pgvector ready")

                    # Context table for multi-turn conversation persistence
                    try:
                        await context_store.setup(db)
                        logger.info("✅ Conversation context table ready")
                    except Exception as ce:
                        logger.warning(f"⚠️  Context table setup failed (non-fatal): {ce}")

                    if not await is_ingested(db, "DSSY_Knowledge_Base"):
                        if os.path.exists(_KB):
                            try:
                                with open(_KB, encoding="utf-8") as f:
                                    kb = f.read()
                            except OSError as e:
                                logger.error(f"Failed to read KB file: {e}")
                                kb = ""
                            if kb:
                                logger.info(f"Ingesting DSSS Knowledge Base ({len(kb):,} chars)...")
                                await ingest(
                                    db, "DSSY_Knowledge_Base", kb,
                                    {"source": "DSSS Official Documents", "version": "2026"}
                                )
                                logger.info("DSSS Knowledge Base ingested")
                        else:
                            logger.warning(f"⚠️  KB file not found: {_KB}")
                    else:
                        logger.info("✅ DSSS Knowledge Base already loaded")

                    # ── Ingest web sources (static gov pages, runs once) ──
                    try:
                        wc = await ingest_web_sources(db)
                        if wc:
                            logger.info(f"✅ Ingested {wc} web sources into RAG")
                        else:
                            logger.info("✅ Web sources already loaded")
                    except Exception as we:
                        logger.warning(f"⚠️  Web source ingestion failed (non-fatal): {we}")

                except Exception as e:
                    logger.error(f"RAG setup error (non-fatal): {e}")
    else:
        logger.warning("⚠️  NEON_DATABASE_URL not set — RAG will be unavailable")

    # ── Gemini health check ───────────────────────────────────
    gok = await gemini_ok()
    logger.info(f"{'✅' if gok else '⚠️ '} Gemini AI {'ready' if gok else 'NOT responding — check GEMINI_API_KEY'}")

    # ── Cache warming (pre-fill top 20 queries, background task) ─
    if neon_ok and gok:
        try:
            from backend.services.cache_warmer import warm_cache

            def _on_warm_done(t):
                if t.cancelled():
                    return
                exc = t.exception()
                if exc:
                    logger.warning(f"⚠️  Cache warm failed: {exc}")
                else:
                    logger.info(f"✅ Cache warmed: {t.result()} queries pre-loaded")

            import asyncio
            task = asyncio.create_task(warm_cache())
            task.add_done_callback(_on_warm_done)
        except Exception as we:
            logger.warning(f"⚠️  Cache warming task failed (non-fatal): {we}")

    logger.info("-" * 60)
    logger.info(f"  Status: Neon={'OK' if neon_ok else 'DOWN'} | Gemini={'OK' if gok else 'DOWN'}")
    logger.info("=" * 60)

    yield
    await dispose_all()
    logger.info("Neural AI Governance shut down")


app = FastAPI(
    title="Neural AI Governance – DSSS v3",
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.ENVIRONMENT != "production" else None,
    redoc_url=None,
)

app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"], allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def timing(req: Request, call_next):
    s = time.time()
    resp = await call_next(req)
    resp.headers["X-Process-Time-Ms"] = str(int((time.time() - s) * 1000))
    return resp


app.include_router(query_router)
app.include_router(analytics_router)
app.include_router(beneficiaries_router)
app.include_router(rag_router)
app.include_router(history_router)
app.include_router(export_router)
app.include_router(pins_router)
app.include_router(audit_router)

if os.path.isdir(_FE):
    app.mount("/static", StaticFiles(directory=_FE), name="static")


@app.get("/health")
async def health():
    neon = await check_neon_health()
    gm = await gemini_ok()
    return {
        "status": "healthy" if (neon and gm) else "degraded",
        "neon": "connected" if neon else "error",
        "gemini": "ok" if gm else "error",
        "cache": "ok" if await cache_ok() else "unavailable",
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/")
async def root():
    idx = os.path.join(_FE, "index.html")
    return FileResponse(idx) if os.path.exists(idx) else {"app": settings.APP_NAME}


@app.exception_handler(Exception)
async def exc(req: Request, e: Exception):
    logger.error(f"Unhandled: {e}", exc_info=True)
    return JSONResponse(500, {"detail": "Internal server error."})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0", port=8000,
        reload=settings.ENVIRONMENT == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )
