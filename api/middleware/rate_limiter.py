"""
In-memory token bucket rate limiter.

Each IP gets a bucket with `capacity` tokens. Tokens refill at a constant
rate. If the bucket is empty when a request arrives, we return 429.

This is intentionally simple — no Redis, no distributed state. Fine for
a single-instance deploy. If we scale to multiple workers behind a load
balancer, we'd switch to something backed by Redis or use a gateway-level
rate limiter (e.g., nginx limit_req).

Known limitation: the cleanup task won't run if the event loop gets
starved. In practice this hasn't been an issue because our endpoints
are all fast DB reads.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class TokenBucket:
    __slots__ = ("capacity", "tokens", "refill_rate", "last_refill")

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-IP rate limiter using token buckets.

    Args:
        max_requests: bucket capacity (burst size)
        window_seconds: time window — we spread max_requests evenly across this
        cleanup_interval: how often we prune stale buckets (seconds)
    """

    def __init__(
        self,
        app,
        max_requests: int = 100,
        window_seconds: int = 60,
        cleanup_interval: int = 300,
        exempt_paths: Optional[set[str]] = None,
    ):
        super().__init__(app)
        self.max_requests = max_requests
        self.refill_rate = max_requests / window_seconds
        self.buckets: dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(max_requests, self.refill_rate)
        )
        self.cleanup_interval = cleanup_interval
        self.exempt_paths = exempt_paths or {"/api/health", "/docs", "/openapi.json"}
        self._cleanup_task: Optional[asyncio.Task] = None

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # don't rate-limit health checks or docs
        if request.url.path in self.exempt_paths:
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        bucket = self.buckets[client_ip]

        if not bucket.consume():
            retry_after = int(1.0 / self.refill_rate) + 1
            logger.warning("Rate limit exceeded for %s on %s", client_ip, request.url.path)
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Slow down."},
                headers={"Retry-After": str(retry_after)},
            )

        response = await call_next(request)

        # tell clients how many requests they have left (nice for debugging)
        response.headers["X-RateLimit-Remaining"] = str(int(bucket.tokens))
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)

        return response

    def _get_client_ip(self, request: Request) -> str:
        # respect X-Forwarded-For if behind a reverse proxy
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def _cleanup_stale_buckets(self) -> None:
        """Periodically remove buckets for IPs we haven't seen in a while."""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            now = time.monotonic()
            stale_cutoff = now - (self.cleanup_interval * 2)

            stale_ips = [
                ip for ip, bucket in self.buckets.items()
                if bucket.last_refill < stale_cutoff
            ]
            for ip in stale_ips:
                del self.buckets[ip]

            if stale_ips:
                logger.debug("Cleaned up %d stale rate limit buckets", len(stale_ips))
