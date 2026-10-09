"""JWT, mTLS, RBAC, idempotency, rate limits, and immutable HTTP audit."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import re
import ssl
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import unquote

from fastapi import Request, Response
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

from funding_arbitrage.config import Settings

_JWT_PART = re.compile(r"^[A-Za-z0-9_-]+$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@/-]{1,160}$")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_PUBLIC_PATHS = frozenset({"/health", "/health/ready"})
_PUBLIC_PATH_PREFIXES = ("/dashboard",)
GENESIS_HASH = "0" * 64


class Role(StrEnum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    RISK_MANAGER = "risk_manager"
    ADMIN = "admin"


class Principal(BaseModel):
    model_config = ConfigDict(frozen=True)

    subject: str = Field(min_length=1, max_length=128)
    roles: frozenset[Role]
    token_id: str = Field(min_length=1, max_length=128)
    expires_at: datetime

    @field_validator("roles")
    @classmethod
    def require_roles(cls, value: frozenset[Role]) -> frozenset[Role]:
        if not value:
            raise ValueError("JWT must contain at least one role")
        return value


class ControlPlanePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    jwt_secret: SecretStr = SecretStr("")
    jwt_issuer: str = "funding-arbitrage-operator"
    jwt_audience: str = "funding-arbitrage-control"
    jwt_clock_skew_seconds: int = Field(default=30, ge=0, le=300)
    mtls_required: bool = False
    mtls_certificate_header_required: bool = False
    trusted_proxy_hosts: frozenset[str] = frozenset()
    client_certificate_fingerprints: frozenset[str] = frozenset()
    rate_limit_per_minute: int = Field(default=120, gt=0)
    idempotency_ttl_seconds: int = Field(default=86400, ge=60)
    maximum_request_bytes: int = Field(default=1_048_576, gt=0)
    maximum_cached_response_bytes: int = Field(default=2_097_152, gt=0)

    @classmethod
    def from_settings(cls, settings: Settings) -> ControlPlanePolicy:
        return cls(
            enabled=settings.control_plane_security_enabled,
            jwt_secret=settings.control_plane_jwt_secret,
            jwt_issuer=settings.control_plane_jwt_issuer,
            jwt_audience=settings.control_plane_jwt_audience,
            mtls_required=settings.control_plane_mtls_required,
            mtls_certificate_header_required=(
                settings.control_plane_mtls_certificate_header_required
            ),
            trusted_proxy_hosts=settings.control_plane_mtls_trusted_proxy_values,
            client_certificate_fingerprints=(settings.control_plane_mtls_client_fingerprint_values),
            rate_limit_per_minute=settings.control_plane_rate_limit_per_minute,
            idempotency_ttl_seconds=settings.control_plane_idempotency_ttl_seconds,
            maximum_request_bytes=settings.control_plane_max_request_bytes,
        )


class ControlPlaneSecurityError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class Hs256JwtAuthenticator:
    """Strict HS256 JWT verifier with fixed issuer, audience, and role schema."""

    def __init__(self, policy: ControlPlanePolicy) -> None:
        self.policy = policy
        if policy.enabled and len(policy.jwt_secret.get_secret_value()) < 32:
            raise ValueError("control-plane JWT secret must contain at least 32 bytes")

    def authenticate(
        self,
        authorization: str | None,
        *,
        now: datetime | None = None,
    ) -> Principal:
        if not authorization or not authorization.startswith("Bearer "):
            raise ControlPlaneSecurityError(401, "Bearer JWT is required")
        token = authorization.removeprefix("Bearer ").strip()
        if len(token) > 4096:
            raise ControlPlaneSecurityError(401, "JWT is invalid")
        parts = token.split(".")
        if len(parts) != 3 or any(not part or not _JWT_PART.fullmatch(part) for part in parts):
            raise ControlPlaneSecurityError(401, "JWT is invalid")
        header = _decode_json_part(parts[0])
        payload = _decode_json_part(parts[1])
        if header != {"alg": "HS256", "typ": "JWT"}:
            raise ControlPlaneSecurityError(401, "JWT algorithm is not allowed")
        signed = f"{parts[0]}.{parts[1]}".encode("ascii")
        expected = hmac.new(
            self.policy.jwt_secret.get_secret_value().encode("utf-8"),
            signed,
            hashlib.sha256,
        ).digest()
        try:
            supplied = _b64decode(parts[2])
        except ValueError as error:
            raise ControlPlaneSecurityError(401, "JWT signature is invalid") from error
        if not hmac.compare_digest(expected, supplied):
            raise ControlPlaneSecurityError(401, "JWT signature is invalid")

        current = int((now or datetime.now(UTC)).timestamp())
        skew = self.policy.jwt_clock_skew_seconds
        if payload.get("iss") != self.policy.jwt_issuer:
            raise ControlPlaneSecurityError(401, "JWT issuer is invalid")
        audience = payload.get("aud")
        if isinstance(audience, str):
            audiences = {audience}
        elif isinstance(audience, list) and all(isinstance(value, str) for value in audience):
            audiences = set(audience)
        else:
            raise ControlPlaneSecurityError(401, "JWT audience is invalid")
        if self.policy.jwt_audience not in audiences:
            raise ControlPlaneSecurityError(401, "JWT audience is invalid")
        subject = payload.get("sub")
        token_id = payload.get("jti")
        if not isinstance(subject, str) or not _IDENTIFIER.fullmatch(subject):
            raise ControlPlaneSecurityError(401, "JWT subject is invalid")
        if not isinstance(token_id, str) or not _IDENTIFIER.fullmatch(token_id):
            raise ControlPlaneSecurityError(401, "JWT token ID is invalid")
        expires = _integer_claim(payload, "exp")
        not_before = _integer_claim(payload, "nbf")
        issued_at = _integer_claim(payload, "iat")
        if expires <= issued_at or not_before > expires:
            raise ControlPlaneSecurityError(401, "JWT lifetime is invalid")
        if expires <= current - skew:
            raise ControlPlaneSecurityError(401, "JWT has expired")
        if not_before > current + skew or issued_at > current + skew:
            raise ControlPlaneSecurityError(401, "JWT is not active")
        raw_roles = payload.get("roles")
        if not isinstance(raw_roles, list) or not all(isinstance(role, str) for role in raw_roles):
            raise ControlPlaneSecurityError(401, "JWT roles are invalid")
        try:
            roles = frozenset(Role(role) for role in raw_roles)
            try:
                expires_at = datetime.fromtimestamp(expires, tz=UTC)
            except (OverflowError, OSError, ValueError) as error:
                raise ControlPlaneSecurityError(401, "JWT expiration is invalid") from error
            return Principal(
                subject=subject,
                roles=roles,
                token_id=token_id,
                expires_at=expires_at,
            )
        except ValueError as error:
            raise ControlPlaneSecurityError(401, "JWT roles are invalid") from error


class ControlPlaneRateLimiter(Protocol):
    async def require(self, identity: str) -> None: ...

    async def probe(self) -> None: ...

    async def close(self) -> None: ...


class ControlPlaneTokenRevocationStore(Protocol):
    async def is_revoked(self, token_id: str) -> bool: ...

    async def revoke(self, token_id: str, expires_at: datetime) -> None: ...

    async def probe(self) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class CachedHttpResponse:
    status_code: int
    body: bytes
    headers: tuple[tuple[str, str], ...]


class IdempotencyState(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"


class IdempotencySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    principal_id: str
    idempotency_key: str
    request_hash: str
    state: IdempotencyState
    status_code: int | None = None


class ControlPlaneIdempotencyStore(Protocol):
    async def reserve(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
    ) -> CachedHttpResponse | None: ...

    async def complete(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> None: ...

    async def inspect(
        self,
        principal_id: str,
        key: str,
    ) -> IdempotencySnapshot | None: ...

    async def reconcile_pending(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> CachedHttpResponse: ...

    async def probe(self) -> None: ...


@dataclass
class _IdempotencyEntry:
    request_hash: str
    expires_at: float
    response: CachedHttpResponse | None = None


class MemoryIdempotencyStore:
    """Bounded in-process replay guard; durable command IDs remain authoritative."""

    def __init__(self, ttl_seconds: int, *, maximum_entries: int = 10_000) -> None:
        self.ttl_seconds = ttl_seconds
        self.maximum_entries = maximum_entries
        self._entries: dict[tuple[str, str], _IdempotencyEntry] = {}
        self._lock = asyncio.Lock()

    async def reserve(
        self,
        principal_id: str,
        key: str | None,
        request_hash: str,
        *,
        now: float | None = None,
    ) -> CachedHttpResponse | None:
        if key is None or not _IDEMPOTENCY_KEY.fullmatch(key):
            raise ControlPlaneSecurityError(428, "valid Idempotency-Key is required")
        timestamp = time.monotonic() if now is None else now
        identity = (principal_id, key)
        async with self._lock:
            self._expire(timestamp)
            existing = self._entries.get(identity)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise ControlPlaneSecurityError(409, "idempotency key payload conflict")
                if existing.response is None:
                    raise ControlPlaneSecurityError(
                        409, "idempotent request outcome is pending reconciliation"
                    )
                return existing.response
            if len(self._entries) >= self.maximum_entries:
                raise ControlPlaneSecurityError(503, "idempotency registry capacity exceeded")
            self._entries[identity] = _IdempotencyEntry(
                request_hash=request_hash,
                expires_at=timestamp + self.ttl_seconds,
            )
        return None

    async def complete(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> None:
        identity = (principal_id, key)
        async with self._lock:
            entry = self._entries.get(identity)
            if entry is None or entry.request_hash != request_hash:
                raise RuntimeError("idempotency reservation disappeared")
            entry.response = response

    async def inspect(
        self,
        principal_id: str,
        key: str,
    ) -> IdempotencySnapshot | None:
        async with self._lock:
            entry = self._entries.get((principal_id, key))
            if entry is None:
                return None
            return IdempotencySnapshot(
                principal_id=principal_id,
                idempotency_key=key,
                request_hash=entry.request_hash,
                state=(
                    IdempotencyState.COMPLETED
                    if entry.response is not None
                    else IdempotencyState.PENDING
                ),
                status_code=entry.response.status_code if entry.response is not None else None,
            )

    async def reconcile_pending(
        self,
        principal_id: str,
        key: str,
        request_hash: str,
        response: CachedHttpResponse,
    ) -> CachedHttpResponse:
        async with self._lock:
            entry = self._entries.get((principal_id, key))
            if entry is None:
                raise ControlPlaneSecurityError(404, "idempotency reservation not found")
            if entry.request_hash != request_hash:
                raise ControlPlaneSecurityError(409, "idempotency request hash conflict")
            if entry.response is not None:
                if entry.response != response:
                    raise ControlPlaneSecurityError(409, "idempotency outcome conflict")
                return entry.response
            entry.response = response
            entry.expires_at = time.monotonic() + self.ttl_seconds
            return response

    async def probe(self) -> None:
        await asyncio.sleep(0)

    def _expire(self, now: float) -> None:
        for identity in [
            identity
            for identity, entry in self._entries.items()
            if entry.response is not None and entry.expires_at <= now
        ]:
            self._entries.pop(identity, None)


class ControlPlaneAuditDraft(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    request_id: str
    actor_id: str
    actor_roles: tuple[str, ...]
    action: str
    resource: str
    outcome: str
    status_code: int
    idempotency_key: str | None = None
    request_hash: str
    client_certificate_sha256: str | None = None


class ControlPlaneAuditRecord(ControlPlaneAuditDraft):
    sequence: int = Field(gt=0)
    previous_hash: str = Field(min_length=64, max_length=64)
    audit_hash: str = Field(min_length=64, max_length=64)


class ControlPlaneAuditSink(Protocol):
    async def append(self, event: ControlPlaneAuditDraft) -> ControlPlaneAuditRecord: ...


class MemoryImmutableAuditSink:
    """Hash-chained sink used by tests and isolated operator tools."""

    def __init__(self) -> None:
        self.records: list[ControlPlaneAuditRecord] = []
        self._lock = asyncio.Lock()

    async def append(self, event: ControlPlaneAuditDraft) -> ControlPlaneAuditRecord:
        async with self._lock:
            previous = self.records[-1].audit_hash if self.records else GENESIS_HASH
            candidate = {
                **event.model_dump(mode="json"),
                "sequence": len(self.records) + 1,
                "previous_hash": previous,
            }
            record = ControlPlaneAuditRecord.model_validate(
                {**candidate, "audit_hash": _hash(candidate)}
            )
            self.records.append(record)
            return record

    def verify(self) -> None:
        previous = GENESIS_HASH
        for sequence, record in enumerate(self.records, start=1):
            if record.sequence != sequence or record.previous_hash != previous:
                raise ValueError("control-plane audit chain mismatch")
            payload = record.model_dump(mode="json", exclude={"audit_hash"})
            if record.audit_hash != _hash(payload):
                raise ValueError("control-plane audit hash mismatch")
            previous = record.audit_hash


class ControlPlaneSecurity:
    def __init__(self, policy: ControlPlanePolicy) -> None:
        self.policy = policy
        self.jwt = Hs256JwtAuthenticator(policy)

    def authenticate_and_authorize(
        self,
        *,
        authorization: str | None,
        client_host: str,
        certificate_fingerprint: str | None,
        certificate_pem: str | None = None,
        method: str,
        path: str,
        now: datetime | None = None,
    ) -> tuple[Principal, str | None]:
        principal = self.jwt.authenticate(authorization, now=now)
        fingerprint = self._verify_mtls(client_host, certificate_fingerprint, certificate_pem)
        self._require_role(principal, method, path)
        return principal, fingerprint

    def _verify_mtls(
        self,
        client_host: str,
        supplied_fingerprint: str | None,
        certificate_pem: str | None,
    ) -> str | None:
        if not self.policy.mtls_required:
            return None
        if client_host.lower() not in self.policy.trusted_proxy_hosts:
            raise ControlPlaneSecurityError(403, "mTLS proxy source is not trusted")
        if self.policy.mtls_certificate_header_required:
            normalized = _certificate_fingerprint(certificate_pem)
        else:
            normalized = (supplied_fingerprint or "").lower().replace(":", "")
        if not _SHA256.fullmatch(normalized):
            raise ControlPlaneSecurityError(403, "verified client certificate is required")
        if normalized not in self.policy.client_certificate_fingerprints:
            raise ControlPlaneSecurityError(403, "client certificate is not allowlisted")
        return normalized

    def rate_limit_identity(
        self,
        client_host: str,
        certificate_fingerprint: str | None,
        certificate_pem: str | None = None,
    ) -> str:
        host = client_host.strip().lower() or "unknown"
        if host not in self.policy.trusted_proxy_hosts:
            fingerprint = "unverified"
        elif self.policy.mtls_certificate_header_required:
            fingerprint = _certificate_fingerprint(certificate_pem) or "unverified"
        else:
            fingerprint = (certificate_fingerprint or "").lower().replace(":", "")
            if not _SHA256.fullmatch(fingerprint):
                fingerprint = "unverified"
        return hashlib.sha256(f"{host}|{fingerprint}".encode()).hexdigest()

    @staticmethod
    def _require_role(principal: Principal, method: str, path: str) -> None:
        method = method.upper()
        if path.startswith("/control"):
            allowed = frozenset({Role.ADMIN})
        elif method in _READ_METHODS:
            allowed = frozenset(Role)
        elif path.startswith(("/scan", "/backtests", "/analytics")):
            allowed = frozenset({Role.OPERATOR, Role.ADMIN})
        elif path.startswith("/risk"):
            allowed = frozenset({Role.RISK_MANAGER, Role.ADMIN})
        else:
            allowed = frozenset({Role.ADMIN})
        if not principal.roles & allowed:
            raise ControlPlaneSecurityError(403, "RBAC permission denied")


class ControlPlaneMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: Any,
        *,
        security: ControlPlaneSecurity,
        audit_sink: ControlPlaneAuditSink,
        idempotency_store: ControlPlaneIdempotencyStore,
        rate_limiter: ControlPlaneRateLimiter,
        token_revocation_store: ControlPlaneTokenRevocationStore,
    ) -> None:
        super().__init__(app)
        self.security = security
        self.audit_sink = audit_sink
        self.idempotency_store = idempotency_store
        self.rate_limiter = rate_limiter
        self.token_revocation_store = token_revocation_store

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        policy = self.security.policy
        if not policy.enabled or _is_public_path(request.url.path):
            return await call_next(request)

        request_id = _request_id(request.headers.get("x-request-id"))
        method = request.method.upper()
        path = request.url.path
        principal: Principal | None = None
        fingerprint: str | None = None
        idempotency_key: str | None = None
        idempotency_reserved = False
        request_hash = _request_hash(request, b"")
        try:
            client_host = request.client.host if request.client else ""
            certificate_fingerprint = request.headers.get("x-client-cert-sha256")
            certificate_pem = request.headers.get("x-verified-client-cert")
            await self.rate_limiter.require(
                self.security.rate_limit_identity(
                    client_host, certificate_fingerprint, certificate_pem
                )
            )
            principal, fingerprint = self.security.authenticate_and_authorize(
                authorization=request.headers.get("authorization"),
                client_host=client_host,
                certificate_fingerprint=certificate_fingerprint,
                certificate_pem=certificate_pem,
                method=method,
                path=path,
            )
            if await self.token_revocation_store.is_revoked(principal.token_id):
                raise ControlPlaneSecurityError(401, "JWT has been revoked")
            request.state.principal = principal
            request.state.request_id = request_id
            if method in _WRITE_METHODS:
                _require_bounded_content_length(
                    request.headers.get("content-length"), policy.maximum_request_bytes
                )
                body = await _read_bounded_body(request, policy.maximum_request_bytes)
                request_hash = _request_hash(request, body)
                idempotency_key = _require_idempotency_key(request.headers.get("idempotency-key"))
                cached = await self.idempotency_store.reserve(
                    principal.subject, idempotency_key, request_hash
                )
                idempotency_reserved = cached is None
                if cached is not None:
                    await self._audit(
                        request_id=request_id,
                        principal=principal,
                        method=method,
                        path=path,
                        outcome="idempotent_replay",
                        status_code=cached.status_code,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                        fingerprint=fingerprint,
                    )
                    return Response(
                        content=cached.body,
                        status_code=cached.status_code,
                        headers=dict(cached.headers),
                    )

            response = await call_next(request)
            if method in _WRITE_METHODS:
                response = await self._finalize_write_response(
                    request=request,
                    response=response,
                    principal=principal,
                    request_id=request_id,
                    idempotency_key=idempotency_key or "",
                    request_hash=request_hash,
                    fingerprint=fingerprint,
                )
                return response
            await self._audit(
                request_id=request_id,
                principal=principal,
                method=method,
                path=path,
                outcome="allowed" if response.status_code < 400 else "rejected",
                status_code=response.status_code,
                idempotency_key=None,
                request_hash=request_hash,
                fingerprint=fingerprint,
            )
            return response
        except ControlPlaneSecurityError as error:
            response = JSONResponse(
                status_code=error.status_code,
                content={"detail": error.detail, "request_id": request_id},
            )
            if error.detail != "immutable security audit unavailable":
                try:
                    await self._audit(
                        request_id=request_id,
                        principal=principal,
                        method=method,
                        path=path,
                        outcome="rejected",
                        status_code=error.status_code,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                        fingerprint=fingerprint,
                    )
                except ControlPlaneSecurityError:
                    return JSONResponse(
                        status_code=503,
                        content={
                            "detail": "immutable security audit unavailable",
                            "request_id": request_id,
                        },
                    )
            if (
                idempotency_reserved
                and principal is not None
                and idempotency_key is not None
                and error.status_code < 500
            ):
                try:
                    await self._complete_deterministic_error(
                        principal=principal,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                        response=response,
                    )
                except ControlPlaneSecurityError as persistence_error:
                    return JSONResponse(
                        status_code=persistence_error.status_code,
                        content={
                            "detail": persistence_error.detail,
                            "request_id": request_id,
                        },
                    )
            return response
        except Exception:
            await self._audit(
                request_id=request_id,
                principal=principal,
                method=method,
                path=path,
                outcome="error",
                status_code=500,
                idempotency_key=None,
                request_hash=request_hash,
                fingerprint=fingerprint,
            )
            raise

    async def _complete_deterministic_error(
        self,
        *,
        principal: Principal,
        idempotency_key: str,
        request_hash: str,
        response: JSONResponse,
    ) -> None:
        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in {"content-length", "transfer-encoding"}
        }
        await self.idempotency_store.complete(
            principal.subject,
            idempotency_key,
            request_hash,
            CachedHttpResponse(
                status_code=response.status_code,
                body=response.body,
                headers=tuple(sorted(headers.items())),
            ),
        )

    async def _finalize_write_response(
        self,
        *,
        request: Request,
        response: Response,
        principal: Principal,
        request_id: str,
        idempotency_key: str,
        request_hash: str,
        fingerprint: str | None,
    ) -> Response:
        body_iterator = getattr(response, "body_iterator", None)
        if body_iterator is None:
            chunks = [getattr(response, "body", b"")]
        else:
            chunks = [chunk async for chunk in body_iterator]
        body = b"".join(chunk if isinstance(chunk, bytes) else chunk.encode() for chunk in chunks)
        if len(body) > self.security.policy.maximum_cached_response_bytes:
            raise ControlPlaneSecurityError(507, "response is too large for safe idempotency")
        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in {"content-length", "transfer-encoding"}
        }
        await self._audit(
            request_id=request_id,
            principal=principal,
            method=request.method,
            path=request.url.path,
            outcome="allowed" if response.status_code < 400 else "rejected",
            status_code=response.status_code,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            fingerprint=fingerprint,
        )
        cached = CachedHttpResponse(
            status_code=response.status_code,
            body=body,
            headers=tuple(sorted(headers.items())),
        )
        await self.idempotency_store.complete(
            principal.subject, idempotency_key, request_hash, cached
        )
        return Response(
            content=body,
            status_code=response.status_code,
            headers=headers,
            background=response.background,
        )

    async def _audit(
        self,
        *,
        request_id: str,
        principal: Principal | None,
        method: str,
        path: str,
        outcome: str,
        status_code: int,
        idempotency_key: str | None,
        request_hash: str,
        fingerprint: str | None,
    ) -> None:
        try:
            await self.audit_sink.append(
                ControlPlaneAuditDraft(
                    timestamp=datetime.now(UTC),
                    request_id=request_id,
                    actor_id=principal.subject if principal is not None else "anonymous",
                    actor_roles=tuple(
                        sorted(role.value for role in principal.roles)
                        if principal is not None
                        else ()
                    ),
                    action=method.upper(),
                    resource=path,
                    outcome=outcome,
                    status_code=status_code,
                    idempotency_key=_audit_idempotency_key(principal, idempotency_key),
                    request_hash=request_hash,
                    client_certificate_sha256=fingerprint,
                )
            )
        except Exception as error:
            raise ControlPlaneSecurityError(503, "immutable security audit unavailable") from error


def issue_hs256_token(
    policy: ControlPlanePolicy,
    *,
    subject: str,
    roles: Sequence[Role],
    token_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    """Sign an operator JWT; production issuance belongs in an external IAM tool."""

    header = {"alg": "HS256", "typ": "JWT"}
    issued = int(_utc(issued_at).timestamp())
    payload = {
        "iss": policy.jwt_issuer,
        "aud": policy.jwt_audience,
        "sub": subject,
        "jti": token_id,
        "roles": [role.value for role in roles],
        "iat": issued,
        "nbf": issued,
        "exp": int(_utc(expires_at).timestamp()),
    }
    encoded_header = _b64encode(_json(header))
    encoded_payload = _b64encode(_json(payload))
    signed = f"{encoded_header}.{encoded_payload}"
    signature = hmac.new(
        policy.jwt_secret.get_secret_value().encode("utf-8"),
        signed.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{signed}.{_b64encode(signature)}"


def _decode_json_part(value: str) -> dict[str, Any]:
    try:
        decoded = json.loads(_b64decode(value))
    except (ValueError, json.JSONDecodeError) as error:
        raise ControlPlaneSecurityError(401, "JWT is invalid") from error
    if not isinstance(decoded, dict):
        raise ControlPlaneSecurityError(401, "JWT is invalid")
    return decoded


def _integer_claim(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ControlPlaneSecurityError(401, f"JWT {name} claim is invalid")
    return value


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error) as error:
        raise ValueError("invalid base64url") from error
    if not hmac.compare_digest(_b64encode(decoded), value):
        raise ValueError("non-canonical base64url")
    return decoded


def _is_public_path(path: str) -> bool:
    return path in _PUBLIC_PATHS or path.startswith(_PUBLIC_PATH_PREFIXES)


def _require_bounded_content_length(value: str | None, maximum: int) -> None:
    if value is None:
        return
    try:
        content_length = int(value)
    except ValueError as error:
        raise ControlPlaneSecurityError(400, "Content-Length is invalid") from error
    if content_length < 0:
        raise ControlPlaneSecurityError(400, "Content-Length is invalid")
    if content_length > maximum:
        raise ControlPlaneSecurityError(413, "request body is too large")


async def _read_bounded_body(request: Request, maximum: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > maximum:
            raise ControlPlaneSecurityError(413, "request body is too large")
        chunks.append(chunk)
    body = b"".join(chunks)
    request._body = body  # Starlette CachedRequest replays this body to the endpoint.
    return body


def _certificate_fingerprint(value: str | None) -> str:
    if not value:
        return ""
    try:
        pem = unquote(value).strip()
        der = ssl.PEM_cert_to_DER_cert(pem)
        return hashlib.sha256(der).hexdigest()
    except (ValueError, UnicodeError):
        return ""


def _require_idempotency_key(value: str | None) -> str:
    if value is None or not _IDEMPOTENCY_KEY.fullmatch(value):
        raise ControlPlaneSecurityError(428, "valid Idempotency-Key is required")
    return value


def _audit_idempotency_key(principal: Principal | None, value: str | None) -> str | None:
    if principal is None or value is None:
        return None
    return hashlib.sha256(f"{principal.subject}|{value}".encode()).hexdigest()


def _request_id(value: str | None) -> str:
    if value is not None and _IDENTIFIER.fullmatch(value):
        return value
    return "req_" + uuid.uuid4().hex


def _request_hash(request: Request, body: bytes) -> str:
    return _hash(
        {
            "method": request.method.upper(),
            "path": request.url.path,
            "query": sorted(request.query_params.multi_items()),
            "body_sha256": hashlib.sha256(body).hexdigest(),
        }
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value)).hexdigest()


def _json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: (
            _utc(item).isoformat().replace("+00:00", "Z")
            if isinstance(item, datetime)
            else str(item)
        ),
    ).encode("utf-8")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
