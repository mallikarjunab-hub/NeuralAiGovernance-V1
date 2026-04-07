"""RAG status + reingest endpoints — Neon pgvector document management."""
import os, logging
from fastapi import APIRouter
from backend.database import neon_session_context
from backend.services.rag_service import get_status, ingest
from sqlalchemy import text

router = APIRouter(prefix="/api/rag", tags=["RAG"])
logger = logging.getLogger(__name__)

_KB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "documents", "dssy_knowledge_base.md")


@router.get("/status")
async def rag_status():
    try:
        async with neon_session_context() as db:
            docs = await get_status(db)
        return {"status": "ok", "documents": docs}
    except Exception as e:
        logger.error(f"RAG status error: {e}")
        return {"status": "error", "documents": [], "detail": str(e)}


@router.post("/reingest")
async def rag_reingest():
    """Force re-ingestion of the DSSY knowledge base. Clears old chunks first."""
    try:
        if not os.path.exists(_KB):
            return {"status": "error", "detail": f"KB file not found: {_KB}"}

        with open(_KB, encoding="utf-8") as f:
            kb = f.read()
        if not kb:
            return {"status": "error", "detail": "KB file is empty"}

        async with neon_session_context() as db:
            # Clear existing chunks for this document
            await db.execute(text("DELETE FROM document_chunks WHERE doc_name='DSSY_Knowledge_Base'"))
            await db.commit()
            logger.info("Cleared old DSSY_Knowledge_Base chunks — re-ingesting...")

            await ingest(db, "DSSY_Knowledge_Base", kb,
                         {"source": "DSSY Official Documents", "version": "2026"})

            docs = await get_status(db)
        return {"status": "ok", "message": "Re-ingestion complete", "documents": docs}

    except Exception as e:
        logger.error(f"Re-ingestion failed: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}
