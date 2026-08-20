"""Shared JWT revocation state for HTTP and long-lived WebSocket sessions."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from funding_arbitrage.config import Settings
from funding_arbitrage.internal_tls import redis_connection_kwargs
from funding_arbitrage.security.control_plane import ControlPlaneSecurityError


class MemoryTokenRevocationStore:
    def __init__(self, *, maximum_entries: int = 10_000) -> None:
        self.maximum_entries = maximum_entries
        self._expires_at: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def is_revoked(self, token_id: str) -> bool:
        now = time.time()
        async with self._lock:
            self._expire(now)
            return token_id in self._expires_at

    async def revoke(self, token_id: str, expires_at: datetime) -> None:
        expiry = _utc(expires_at).timestamp()
        now = time.time()
        if expiry <= now:
            return
        async with self._lock:
            self._expire(now)
            if token_id not in self._expires_at and len(self._expires_at) >= self.maximum_entries:
                raise ControlPlaneSecurityError(503, "token revocation capacity exceeded")
            self._expires_at[token_id] = expiry

    async def probe(self) -> None:
        await asyncio.sleep(0)

    async def close(self) -> None:
        await asyncio.sleep(0)

    def _expire(self, now: float) -> None:
        for token_id in [
            token_id for token_id, expiry in self._expires_at.items() if expiry <= now
        ]:
            self._expires_at.pop(token_id, None)


class RedisTokenRevocationStore:
    def __init__(
        self,
        redis_url: str,
        *,
        connection_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._redis = Redis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=30,
            **(connection_kwargs or {}),
        )

    async def is_revoked(self, token_id: str) -> bool:
        try:
            return await self._redis.exists(_key(token_id)) == 1
        except RedisError as error:
            raise ControlPlaneSecurityError(
                503, "token revocation storage unavailable"
            ) from error

    async def revoke(self, token_id: str, expires_at: datetime) -> None:
        ttl = math.ceil((_utc(expires_at) - datetime.now(UTC)).total_seconds())
        if ttl <= 0:
            return
        try:
            await self._redis.set(_key(token_id), "1", ex=ttl)
        except RedisError as error:
            raise ControlPlaneSecurityError(
                503, "token revocation storage unavailable"
            ) from error

    async def probe(self) -> None:
        try:
            if not await self._redis.ping():
                raise RuntimeError("Redis ping returned false")
        except (RedisError, RuntimeError) as error:
            raise ControlPlaneSecurityError(
                503, "token revocation storage unavailable"
            ) from error

    async def close(self) -> None:
        await self._redis.aclose()


def create_token_revocation_store(
    settings: Settings,
) -> MemoryTokenRevocationStore | RedisTokenRevocationStore:
    if settings.control_plane_rate_limit_backend == "redis":
        return RedisTokenRevocationStore(
            settings.redis_url,
            connection_kwargs=redis_connection_kwargs(settings),
        )
    return MemoryTokenRevocationStore()


def _key(token_id: str) -> str:
    digest = hashlib.sha256(token_id.encode()).hexdigest()
    return f"control-plane:revoked-token:{digest}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)