import json, logging
from backend.config import settings
logger = logging.getLogger(__name__)
_r = None

async def _redis():
    global _r
    if _r is None:
        try:
            import redis.asyncio as a; _r = a.from_url(settings.REDIS_URL, decode_responses=True); await _r.ping()
        except: _r = None
    return _r

async def get_cached(q: str) -> dict | None:
    r = await _redis()
    if not r: return None
    try: d = await r.get(f"q:{hash(q.lower().strip())}"); return json.loads(d) if d else None
    except: return None

async def set_cached(q: str, payload: dict, ttl=300):
    r = await _redis()
    if not r: return
    try: await r.setex(f"q:{hash(q.lower().strip())}", ttl, json.dumps(payload, default=str))
    except: pass

async def check_health() -> bool:
    r = await _redis()
    if not r: return False
    try: await r.ping(); return True
    except: return False
