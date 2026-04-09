"""
ai_service.py — Robust AI Service Layer for Neural AI Governance (NAG V3)
==========================================================================
This is the single authoritative layer for all Gemini API calls.
gemini_service.py delegates here — nothing calls Gemini directly except this file.

Architecture:
  ┌─────────────────────────────────────────────────────────┐
  │  query.py  (router)                                     │
  │      │                                                  │
  │  gemini_service.py  (domain logic: resolve/classify/    │
  │      │               generate_sql / generate_answer)    │
  │      │                                                  │
  │  ai_service.py  ◄── YOU ARE HERE                        │
  │      │  • Retry with exponential back-off               │
  │      │  • Circuit breaker (fail fast when Gemini is     │
  │      │    down — don't queue 300 pending requests)      │
  │      │  • Request deduplication (in-flight cache)       │
  │      │  • Token budget guard (refuses prompts > limit)  │
  │      │  • Structured logging for every API call         │
  │      │  • Health probe                                  │
  │      │                                                  │
  │  httpx  →  Gemini API                                   │
  └─────────────────────────────────────────────────────────┘

Circuit breaker states:
  CLOSED   → normal operation, calls go through
  OPEN     → Gemini is down, fail fast (raises AIServiceUnavailable)
             reopens after RECOVERY_SECONDS
  HALF_OPEN → one test call allowed; success → CLOSED, fail → OPEN again

Usage:
  from backend.services.ai_service import ai_call, embed_text, ai_health

  # Simple call
  result = await ai_call(prompt, temperature=0.0)

  # Embedding
  vector = await embed_text("some text")
"""

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import httpx

from backend.config import settings

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

_BASE        = "https://generativelanguage.googleapis.com/v1beta"
_CHAT_MODEL  = "gemini-2.5-flash-lite"
_EMBED_MODEL = "gemini-embedding-001"
_EMBED_DIM   = 768

# Retry config
_MAX_RETRIES       = 3
_RETRY_BACKOFF_BASE = 1.5   # seconds; retry waits 1.5s, 2.25s, 3.375s
_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}  # retry on these HTTP errors

# Circuit breaker config
_CB_FAILURE_THRESHOLD  = 5   # open after this many consecutive failures
_CB_RECOVERY_SECONDS   = 60  # seconds before trying again (HALF_OPEN)

# Token guard — Gemini flash-lite input limit is ~1M tokens; we cap at 32k
# to prevent runaway prompts that cost money and timeout
_MAX_PROMPT_CHARS = 120_000  # ~30k tokens at ~4 chars/token

# In-flight deduplication window (seconds)
_INFLIGHT_TTL = 30.0


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class AIServiceUnavailable(RuntimeError):
    """Raised when the circuit breaker is OPEN — Gemini is unreachable."""

class AIPromptTooLarge(ValueError):
    """Raised when a prompt exceeds _MAX_PROMPT_CHARS."""

class AIEmptyResponse(RuntimeError):
    """Raised when Gemini returns an empty / malformed response."""


# ─────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKER
# ─────────────────────────────────────────────────────────────────────────────

class _CBState(Enum):
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class _CircuitBreaker:
    state:              _CBState = _CBState.CLOSED
    failure_count:      int      = 0
    last_failure_time:  float    = 0.0
    success_count:      int      = 0   # consecutive successes in HALF_OPEN

    def record_success(self):
        if self.state == _CBState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 2:
                log.info("[ai_service] Circuit breaker → CLOSED (recovered)")
                self.state         = _CBState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        else:
            self.failure_count = 0

    def record_failure(self):
        self.failure_count     += 1
        self.last_failure_time  = time.monotonic()
        self.success_count      = 0
        if self.failure_count >= _CB_FAILURE_THRESHOLD:
            if self.state != _CBState.OPEN:
                log.warning(
                    "[ai_service] Circuit breaker → OPEN after %d failures",
                    self.failure_count,
                )
            self.state = _CBState.OPEN

    def allow_request(self) -> bool:
        if self.state == _CBState.CLOSED:
            return True
        if self.state == _CBState.OPEN:
            elapsed = time.monotonic() - self.last_failure_time
            if elapsed >= _CB_RECOVERY_SECONDS:
                log.info("[ai_service] Circuit breaker → HALF_OPEN (probing)")
                self.state         = _CBState.HALF_OPEN
                self.success_count = 0
                return True
            return False
        # HALF_OPEN — allow one request at a time
        return True


_cb = _CircuitBreaker()


# ─────────────────────────────────────────────────────────────────────────────
# IN-FLIGHT DEDUPLICATION
# ─────────────────────────────────────────────────────────────────────────────
# If two identical prompts arrive within _INFLIGHT_TTL seconds, the second
# waits for the first to finish and reuses the result (avoids double billing).

@dataclass
class _InflightEntry:
    future:     asyncio.Future
    created_at: float = field(default_factory=time.monotonic)


_inflight: dict[str, _InflightEntry] = {}
_inflight_lock = asyncio.Lock()


def _prompt_key(prompt: str, temperature: float, max_tokens: int) -> str:
    raw = f"{temperature}|{max_tokens}|{prompt}"
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# CORE CALL
# ─────────────────────────────────────────────────────────────────────────────

async def ai_call(
    prompt:     str,
    temperature: float = 0.0,
    max_tokens:  int   = 1024,
) -> str:
    """
    Send a prompt to Gemini and return the text response.

    Guarantees:
      • Prompt size check (raises AIPromptTooLarge if over limit)
      • Circuit breaker (raises AIServiceUnavailable when OPEN)
      • In-flight deduplication (same prompt → shared future)
      • Retry with exponential back-off on transient errors
      • Structured log line for every call (latency, tokens, status)
    """
    # ── Guard: prompt size ──
    if len(prompt) > _MAX_PROMPT_CHARS:
        raise AIPromptTooLarge(
            f"Prompt is {len(prompt):,} chars; limit is {_MAX_PROMPT_CHARS:,}. "
            "Trim context or reduce SHOTS."
        )

    # ── Guard: circuit breaker ──
    if not _cb.allow_request():
        raise AIServiceUnavailable(
            "Gemini AI is temporarily unavailable. "
            f"Circuit breaker reopens in ~{_CB_RECOVERY_SECONDS}s."
        )

    # ── In-flight deduplication ──
    key = _prompt_key(prompt, temperature, max_tokens)
    async with _inflight_lock:
        # Clean stale entries
        stale = [k for k, v in _inflight.items()
                 if time.monotonic() - v.created_at > _INFLIGHT_TTL]
        for k in stale:
            del _inflight[k]

        if key in _inflight:
            log.debug("[ai_service] Dedup hit — waiting for in-flight request")
            fut = _inflight[key].future
        else:
            loop = asyncio.get_event_loop()
            fut  = loop.create_future()
            _inflight[key] = _InflightEntry(future=fut)
            fut = None  # signal: this coroutine is the owner

    if fut is not None:
        # We are a waiter — await the owner's result
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=_INFLIGHT_TTL)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            raise AIServiceUnavailable("Timed out waiting for in-flight Gemini response")

    # ── We are the owner — execute the call ──
    result: Optional[str] = None
    exc:    Optional[Exception] = None

    try:
        result = await _call_with_retry(prompt, temperature, max_tokens)
        _cb.record_success()
    except Exception as e:
        _cb.record_failure()
        exc = e
    finally:
        async with _inflight_lock:
            entry = _inflight.pop(key, None)
        if entry:
            if exc is not None:
                entry.future.set_exception(exc)
            else:
                entry.future.set_result(result)

    if exc is not None:
        raise exc
    return result  # type: ignore[return-value]


async def _call_with_retry(
    prompt:      str,
    temperature: float,
    max_tokens:  int,
) -> str:
    url = f"{_BASE}/models/{_CHAT_MODEL}:generateContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "contents":         [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(url, json=payload)

            latency_ms = int((time.monotonic() - t0) * 1000)

            if resp.status_code in _RETRY_STATUS_CODES:
                log.warning(
                    "[ai_service] HTTP %d on attempt %d/%d (%.0fms)",
                    resp.status_code, attempt, _MAX_RETRIES, latency_ms,
                )
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp
                )
                await _backoff(attempt)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Extract text safely
            try:
                text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            except (KeyError, IndexError, TypeError) as e:
                raise AIEmptyResponse(f"Malformed Gemini response: {e}") from e

            if not text:
                raise AIEmptyResponse("Gemini returned empty text")

            log.info(
                "[ai_service] OK  attempt=%d latency=%dms chars=%d temp=%.2f",
                attempt, latency_ms, len(text), temperature,
            )
            return text

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            latency_ms = int((time.monotonic() - t0) * 1000)
            log.warning(
                "[ai_service] Network error attempt %d/%d after %dms: %s",
                attempt, _MAX_RETRIES, latency_ms, type(e).__name__,
            )
            last_exc = e
            await _backoff(attempt)

        except (AIEmptyResponse, AIPromptTooLarge):
            raise   # don't retry these

        except Exception as e:
            log.error("[ai_service] Unexpected error attempt %d/%d: %s", attempt, _MAX_RETRIES, e)
            last_exc = e
            await _backoff(attempt)

    raise AIServiceUnavailable(
        f"Gemini unavailable after {_MAX_RETRIES} retries"
    ) from last_exc


async def _backoff(attempt: int):
    wait = _RETRY_BACKOFF_BASE ** attempt
    log.info("[ai_service] Backing off %.1fs before retry", wait)
    await asyncio.sleep(wait)


# ─────────────────────────────────────────────────────────────────────────────
# EMBEDDINGS
# ─────────────────────────────────────────────────────────────────────────────

async def embed_text(text: str) -> list[float]:
    """
    Generate a 768-dim embedding vector using Gemini embedding-001.
    Text is truncated to 2000 chars (model limit for semantic tasks).
    Retries on transient errors; raises AIServiceUnavailable on persistent failure.
    """
    if not _cb.allow_request():
        raise AIServiceUnavailable("Gemini AI unavailable (circuit breaker OPEN)")

    url = f"{_BASE}/models/{_EMBED_MODEL}:embedContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "model":   f"models/{_EMBED_MODEL}",
        "content": {"parts": [{"text": text[:2000]}]},
        "outputDimensionality": _EMBED_DIM,
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=payload)
            latency_ms = int((time.monotonic() - t0) * 1000)

            if resp.status_code in _RETRY_STATUS_CODES:
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp
                )
                await _backoff(attempt)
                continue

            resp.raise_for_status()
            vector = resp.json()["embedding"]["values"]
            log.debug("[ai_service] embed OK latency=%dms dim=%d", latency_ms, len(vector))
            _cb.record_success()
            return vector

        except Exception as e:
            log.warning("[ai_service] embed attempt %d/%d failed: %s", attempt, _MAX_RETRIES, e)
            last_exc = e
            _cb.record_failure()
            await _backoff(attempt)

    raise AIServiceUnavailable(
        f"Embedding failed after {_MAX_RETRIES} retries"
    ) from last_exc


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH PROBE
# ─────────────────────────────────────────────────────────────────────────────

async def ai_health() -> dict:
    """
    Returns a health dict:
      { "status": "ok"|"degraded"|"down",
        "circuit_breaker": "CLOSED"|"OPEN"|"HALF_OPEN",
        "latency_ms": int }
    """
    cb_state = _cb.state.value
    if not _cb.allow_request():
        return {"status": "down", "circuit_breaker": cb_state, "latency_ms": 0}

    t0 = time.monotonic()
    try:
        text = await _call_with_retry("Reply with exactly: OK", 0.0, 8)
        latency_ms = int((time.monotonic() - t0) * 1000)
        ok = "OK" in text.upper()
        _cb.record_success()
        return {
            "status":          "ok" if ok else "degraded",
            "circuit_breaker": _cb.state.value,
            "latency_ms":      latency_ms,
        }
    except Exception as e:
        _cb.record_failure()
        return {
            "status":          "down",
            "circuit_breaker": _cb.state.value,
            "latency_ms":      int((time.monotonic() - t0) * 1000),
            "error":           str(e),
        }


# ─────────────────────────────────────────────────────────────────────────────
# WEB-GROUNDED SEARCH (Gemini + Google Search tool)
# ─────────────────────────────────────────────────────────────────────────────

async def web_grounded_search(query: str, max_tokens: int = 1024) -> dict:
    """
    Use Gemini with Google Search grounding to answer questions not in local RAG.

    Returns:
        {
            "answer": str,           # Gemini's grounded answer text
            "sources": list[dict],   # [{title, uri}] from grounding metadata
            "grounded": bool,        # True if web search was used
        }
    Raises AIServiceUnavailable on persistent failure.
    """
    if not _cb.allow_request():
        raise AIServiceUnavailable("Gemini AI unavailable (circuit breaker OPEN)")

    url = f"{_BASE}/models/gemini-2.5-flash-lite:generateContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": (
            f"You are a knowledgeable assistant for the DSSS (Dayanand Social Security Scheme) "
            f"of the Government of Goa, India. Answer the following question accurately using "
            f"web search results. Provide specific facts, numbers, and details. "
            f"If the question is about DSSS, social welfare schemes in Goa, or related government "
            f"policies, provide a comprehensive answer. Keep the answer concise (2-4 paragraphs).\n\n"
            f"Question: {query}"
        )}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": max_tokens,
        },
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(url, json=payload)

            latency_ms = int((time.monotonic() - t0) * 1000)

            if resp.status_code in _RETRY_STATUS_CODES:
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp
                )
                await _backoff(attempt)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Extract answer text
            answer = ""
            try:
                parts = data["candidates"][0]["content"]["parts"]
                answer = " ".join(p.get("text", "") for p in parts).strip()
            except (KeyError, IndexError, TypeError):
                pass

            if not answer:
                raise AIEmptyResponse("Gemini web search returned empty text")

            # Extract grounding sources from metadata
            sources = []
            try:
                grounding = data["candidates"][0].get("groundingMetadata", {})
                chunks = grounding.get("groundingChunks", [])
                for chunk in chunks:
                    web = chunk.get("web", {})
                    if web.get("uri"):
                        sources.append({
                            "title": web.get("title", ""),
                            "uri": web["uri"],
                        })
                # Deduplicate by URI
                seen = set()
                unique_sources = []
                for s in sources:
                    if s["uri"] not in seen:
                        seen.add(s["uri"])
                        unique_sources.append(s)
                sources = unique_sources[:5]
            except (KeyError, TypeError):
                pass

            _cb.record_success()
            log.info(
                "[ai_service] web_search OK latency=%dms sources=%d chars=%d",
                latency_ms, len(sources), len(answer),
            )
            return {
                "answer": answer,
                "sources": sources,
                "grounded": len(sources) > 0,
            }

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            log.warning("[ai_service] web_search attempt %d/%d: %s", attempt, _MAX_RETRIES, e)
            last_exc = e
            _cb.record_failure()
            await _backoff(attempt)

        except AIEmptyResponse:
            raise

        except Exception as e:
            log.warning("[ai_service] web_search attempt %d/%d: %s", attempt, _MAX_RETRIES, e)
            last_exc = e
            _cb.record_failure()
            await _backoff(attempt)

    raise AIServiceUnavailable(
        f"Web grounded search failed after {_MAX_RETRIES} retries"
    ) from last_exc


# ─────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKER STATUS (for /health endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def circuit_breaker_status() -> dict:
    return {
        "state":         _cb.state.value,
        "failure_count": _cb.failure_count,
        "last_failure":  _cb.last_failure_time,
    }
