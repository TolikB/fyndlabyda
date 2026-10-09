"""Bounded Redis state that is explicitly forbidden from accounting authority."""

from __future__ import annotations

import json
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EphemeralStatePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    namespace: str = "funding:v1:ephemeral"
    maximum_ttl_seconds: int = Field(default=3600, gt=0)
    maximum_payload_bytes: int = Field(default=1_000_000, gt=0)
    maximum_key_length: int = Field(default=256, gt=0)

    @field_validator("namespace")
    @classmethod
    def normalize_namespace(cls, value: str) -> str:
        normalized = value.strip().strip(":")
        if not normalized:
            raise ValueError("Redis ephemeral namespace cannot be blank")
        return normalized


class AsyncRedisLike(Protocol):
    async def set(self, name: str, value: bytes, *, ex: int) -> Any: ...

    async def get(self, name: str) -> bytes | str | None: ...

    async def delete(self, *names: str) -> int: ...

    async def ttl(self, name: str) -> int: ...


class RedisEphemeralStore:
    """TTL-only cache; durable orders, accounting, and audit are rejected by key."""

    FORBIDDEN_AUTHORITY_TOKENS = frozenset(
        {"ledger", "journal", "audit", "accounting_authority", "withdrawal_authority"}
    )

    def __init__(self, client: AsyncRedisLike, policy: EphemeralStatePolicy) -> None:
        self.client = client
        self.policy = policy

    async def put(
        self,
        key: str,
        value: dict[str, Any] | list[Any] | str | int | float | bool,
        *,
        ttl_seconds: int,
    ) -> None:
        full_key = self._key(key)
        if ttl_seconds <= 0 or ttl_seconds > self.policy.maximum_ttl_seconds:
            raise ValueError("Redis ephemeral TTL is outside policy")
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        if len(payload) > self.policy.maximum_payload_bytes:
            raise ValueError("Redis ephemeral payload exceeds policy")
        await self.client.set(full_key, payload, ex=ttl_seconds)

    async def get(self, key: str) -> Any | None:
        payload = await self.client.get(self._key(key))
        if payload is None:
            return None
        encoded = payload.encode() if isinstance(payload, str) else payload
        return json.loads(encoded)

    async def delete(self, key: str) -> bool:
        return bool(await self.client.delete(self._key(key)))

    async def assert_bounded(self, key: str) -> int:
        ttl = await self.client.ttl(self._key(key))
        if ttl <= 0 or ttl > self.policy.maximum_ttl_seconds:
            raise ValueError("Redis key is persistent or outside TTL policy")
        return ttl

    def _key(self, key: str) -> str:
        normalized = key.strip().lower()
        if not normalized or len(normalized) > self.policy.maximum_key_length:
            raise ValueError("Redis ephemeral key is invalid")
        if any(token in normalized for token in self.FORBIDDEN_AUTHORITY_TOKENS):
            raise ValueError("Redis cannot be used as accounting or audit authority")
        return f"{self.policy.namespace}:{normalized}"
