"""
Rate limiting middleware (Enhancement 6)
=========================================
Per-IP rate limit: 30 requests per minute on /api/query.
Returns HTTP 429 with Retry-After header.

Uses a simple in-memory sliding-window counter so no Redis dependency.
For production multi-instance deployments, replace with slowapi + Redis.
"""
import time, logging
from collections import defaultdict, deque
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# Path prefix to rate-limit
_LIMITED_PREFIX = "/api/query"

# Limit config
MAX_REQUESTS = 30   # per window
WINDOW_SEC   = 60   # rolling window in seconds


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Sliding-window per-IP rate limiter.
    Hits beyond MAX_REQUESTS in WINDOW_SEC return 429 + Retry-After.
    """

    def __init__(self, app):
        super().__init__(app)
        # {ip: deque of request timestamps}
        self._windows: dict[str, deque] = defaultdict(deque)

    def _get_ip(self, request: Request) -> str:
        # Honour X-Forwarded-For if behind a proxy
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith(_LIMITED_PREFIX):
            return await call_next(request)

        ip  = self._get_ip(request)
        now = time.monotonic()
        dq  = self._windows[ip]

        # Evict timestamps older than WINDOW_SEC
        while dq and now - dq[0] > WINDOW_SEC:
            dq.popleft()

        if len(dq) >= MAX_REQUESTS:
            retry_after = int(WINDOW_SEC - (now - dq[0])) + 1
            logger.warning(f"Rate limit hit: ip={ip} requests={len(dq)}")
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Max {MAX_REQUESTS} requests per minute. "
                              f"Please wait {retry_after}s.",
                    "retry_after": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        dq.append(now)
        # Prevent unbounded memory growth — cap per-IP history
        if len(dq) > MAX_REQUESTS * 2:
            while len(dq) > MAX_REQUESTS:
                dq.popleft()

        response = await call_next(request)
        remaining = MAX_REQUESTS - len(dq)
        response.headers["X-RateLimit-Limit"]     = str(MAX_REQUESTS)
        response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
        response.headers["X-RateLimit-Window"]    = str(WINDOW_SEC)
        return response
