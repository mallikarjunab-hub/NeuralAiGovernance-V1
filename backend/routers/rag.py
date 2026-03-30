"""RAG status endpoint — shows ingested documents from Neon pgvector."""
import logging
from fastapi import APIRouter
from backend.database import neon_session_context
from backend.services.rag_service import get_status

router = APIRouter(prefix="/api/rag", tags=["RAG"])
logger = logging.getLogger(__name__)


@router.get("/status")
async def rag_status():
    try:
        async with neon_session_context() as db:
            docs = await get_status(db)
        return {"status": "ok", "documents": docs}
    except Exception as e:
        logger.error(f"RAG status error: {e}")
        return {"status": "error", "documents": [], "detail": str(e)}
