"""Shared fail-closed rate limiting for the authenticated control boundary."""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections import defaultdict, deque
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from funding_arbitrage.config import Settings
from funding_arbitrage.internal_tls import redis_connection_kwargs
from funding_arbitrage.security.control_plane import (
    ControlPlaneRateLimiter,
    ControlPlaneSecurityError,
)

_REDIS_SCRIPT = """
local key = KEYS[1]
local cutoff = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local member = ARGV[3]
local limit = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)
if count >= limit then
  redis.call('PEXPIRE', key, ttl)
  return 0
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, ttl)
return 1
"""


class MemoryControlPlaneRateLimiter:
    """Single-process limiter for local tests and non-live development."""

    def __init__(self, requests_per_minute: int, *, maximum_identities: int = 10_000) -> None:
        self.limit = requests_per_minute
        self.maximum_identities = maximum_identities
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def require(self, identity: str) -> None:
        timestamp = time.monotonic()
        cutoff = timestamp - 60.0
        async with self._lock:
            for key in tuple(self._requests):
                window = self._requests[key]
                while window and window[0] <= cutoff:
                    window.popleft()
                if not window:
                    self._requests.pop(key, None)
            if identity not in self._requests and len(self._requests) >= self.maximum_identities:
                raise ControlPlaneSecurityError(503, "rate-limit capacity exceeded")
            window = self._requests[identity]
            if len(window) >= self.limit:
                raise ControlPlaneSecurityError(429, "control-plane rate limit exceeded")
            window.append(timestamp)

    async def probe(self) -> None:
        await asyncio.sleep(0)

    async def close(self) -> None:
        await asyncio.sleep(0)


class RedisControlPlaneRateLimiter:
    """Atomic cross-worker sliding-window limiter backed by bounded Redis state."""

    def __init__(
        self,
        redis_url: str,
        requests_per_minute: int,
        *,
        connection_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.limit = requests_per_minute
        self._redis = Redis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=30,
            **(connection_kwargs or {}),
        )

    async def require(self, identity: str) -> None:
        now_ms = int(time.time() * 1000)
        key = "control-plane:rate:" + hashlib.sha256(identity.encode()).hexdigest()
        member = f"{now_ms}:{uuid.uuid4().hex}"
        try:
            allowed = await self._redis.eval(
                _REDIS_SCRIPT,
                1,
                key,
                now_ms - 60_000,
                now_ms,
                member,
                self.limit,
                61_000,
            )
        except RedisError as error:
            raise ControlPlaneSecurityError(
                503, "shared rate-limit storage unavailable"
            ) from error
        if int(allowed) != 1:
            raise ControlPlaneSecurityError(429, "control-plane rate limit exceeded")

    async def probe(self) -> None:
        try:
            if not await self._redis.ping():
                raise RuntimeError("Redis ping returned false")
        except (RedisError, RuntimeError) as error:
            raise ControlPlaneSecurityError(
                503, "shared rate-limit storage unavailable"
            ) from error

    async def close(self) -> None:
        await self._redis.aclose()


def create_control_plane_rate_limiter(settings: Settings) -> ControlPlaneRateLimiter:
    if settings.control_plane_rate_limit_backend == "redis":
        return RedisControlPlaneRateLimiter(
            settings.redis_url,
            settings.control_plane_rate_limit_per_minute,
            connection_kwargs=redis_connection_kwargs(settings),
        )
    return MemoryControlPlaneRateLimiter(settings.control_plane_rate_limit_per_minute)