"""Durable control-plane replay protection."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import ApiIdempotencyRecord
from funding_arbitrage.security.control_plane import (
    CachedHttpResponse,
    ControlPlaneSecurityError,
    IdempotencySnapshot,
    IdempotencyState,
)

_PENDING = "PENDING"
_COMPLETED = "COMPLETED"


class DatabaseControlPlaneIdempotencyStore:
    """Retain completed responses and serialize each principal/key pair."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        ttl_seconds: int,
    ) -> None:
        self.session_factory = session_factory
        self.ttl_seconds = ttl_seconds
        self._process_lock = asyncio.Lock()

    async def reserve(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
    ) -> CachedHttpResponse | None:
        now = datetime.now(UTC)
        try:
            async with self._process_lock, self.session_factory() as session:
                await self._lock_identity(session, principal_id, key)
                row = await self._load(session, principal_id, key)
                if row is not None and row.state == _COMPLETED and _utc(row.expires_at) <= now:
                    row.request_hash = request_hash
                    row.state = _PENDING
                    row.status_code = None
                    row.response_body = None
                    row.response_headers = None
                    row.created_at = now
                    row.updated_at = now
                    row.expires_at = now + timedelta(seconds=self.ttl_seconds)
                    await session.commit()
                    return None
                if row is not None:
                    if row.request_hash != request_hash:
                        raise ControlPlaneSecurityError(409, "idempotency key payload conflict")
                    if row.state != _COMPLETED:
                        raise ControlPlaneSecurityError(
                            409, "idempotent request outcome is pending reconciliation"
                        )
                    return self._cached_response(row)
                session.add(
                    ApiIdempotencyRecord(
                        principal_id=principal_id,
                        idempotency_key=key,
                        request_hash=request_hash,
                        state=_PENDING,
                        status_code=None,
                        response_body=None,
                        response_headers=None,
                        created_at=now,
                        updated_at=now,
                        expires_at=now + timedelta(seconds=self.ttl_seconds),
                    )
                )
                await session.commit()
                return None
        except ControlPlaneSecurityError:
            raise
        except Exception as error:
            raise ControlPlaneSecurityError(503, "idempotency persistence unavailable") from error

    async def complete(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> None:
        now = datetime.now(UTC)
        try:
            async with self._process_lock, self.session_factory() as session:
                await self._lock_identity(session, principal_id, key)
                row = await self._load(session, principal_id, key)
                if row is None or row.request_hash != request_hash or row.state != _PENDING:
                    raise ControlPlaneSecurityError(503, "idempotency reservation is inconsistent")
                row.state = _COMPLETED
                row.status_code = response.status_code
                row.response_body = response.body
                row.response_headers = dict(response.headers)
                row.updated_at = now
                row.expires_at = now + timedelta(seconds=self.ttl_seconds)
                await session.commit()
        except ControlPlaneSecurityError:
            raise
        except Exception as error:
            raise ControlPlaneSecurityError(503, "idempotency persistence unavailable") from error

    async def inspect(
        self,
        principal_id: str,
        key: str,
    ) -> IdempotencySnapshot | None:
        try:
            async with self.session_factory() as session:
                row = await self._load(session, principal_id, key)
                if row is None:
                    return None
                try:
                    state = IdempotencyState(row.state)
                except ValueError as error:
                    raise ControlPlaneSecurityError(
                        503, "stored idempotency state is invalid"
                    ) from error
                return IdempotencySnapshot(
                    principal_id=row.principal_id,
                    idempotency_key=row.idempotency_key,
                    request_hash=row.request_hash,
                    state=state,
                    status_code=row.status_code,
                )
        except ControlPlaneSecurityError:
            raise
        except Exception as error:
            raise ControlPlaneSecurityError(503, "idempotency persistence unavailable") from error

    async def reconcile_pending(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> CachedHttpResponse:
        now = datetime.now(UTC)
        try:
            async with self._process_lock, self.session_factory() as session:
                await self._lock_identity(session, principal_id, key)
                row = await self._load(session, principal_id, key)
                if row is None:
                    raise ControlPlaneSecurityError(404, "idempotency reservation not found")
                if row.request_hash != request_hash:
                    raise ControlPlaneSecurityError(409, "idempotency request hash conflict")
                if row.state == _COMPLETED:
                    existing = self._cached_response(row)
                    if existing != response:
                        raise ControlPlaneSecurityError(409, "idempotency outcome conflict")
                    return existing
                if row.state != _PENDING:
                    raise ControlPlaneSecurityError(503, "stored idempotency state is invalid")
                row.state = _COMPLETED
                row.status_code = response.status_code
                row.response_body = response.body
                row.response_headers = dict(response.headers)
                row.updated_at = now
                row.expires_at = now + timedelta(seconds=self.ttl_seconds)
                await session.commit()
                return response
        except ControlPlaneSecurityError:
            raise
        except Exception as error:
            raise ControlPlaneSecurityError(503, "idempotency persistence unavailable") from error

    async def probe(self) -> None:
        try:
            async with self.session_factory() as session:
                await session.execute(select(ApiIdempotencyRecord.id).limit(1))
        except Exception as error:
            raise ControlPlaneSecurityError(503, "idempotency persistence unavailable") from error

    @staticmethod
    def _cached_response(row: ApiIdempotencyRecord) -> CachedHttpResponse:
        if row.status_code is None or row.response_body is None or row.response_headers is None:
            raise ControlPlaneSecurityError(503, "stored idempotency response is invalid")
        return CachedHttpResponse(
            status_code=row.status_code,
            body=row.response_body,
            headers=tuple(sorted(row.response_headers.items())),
        )

    @staticmethod
    async def _lock_identity(
        session: AsyncSession,
        principal_id: str,
        key: str,
    ) -> None:
        dialect = session.get_bind().dialect.name
        if dialect == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
            return
        if dialect != "postgresql":
            raise ControlPlaneSecurityError(503, "unsupported idempotency database dialect")
        lock_id = int.from_bytes(
            hashlib.sha256(f"{principal_id}\0{key}".encode()).digest()[:8],
            byteorder="big",
            signed=True,
        )
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": lock_id},
        )

    @staticmethod
    async def _load(
        session: AsyncSession,
        principal_id: str,
        key: str,
    ) -> ApiIdempotencyRecord | None:
        return await session.scalar(
            select(ApiIdempotencyRecord)
            .where(
                ApiIdempotencyRecord.principal_id == principal_id,
                ApiIdempotencyRecord.idempotency_key == key,
            )
            .with_for_update()
        )


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
