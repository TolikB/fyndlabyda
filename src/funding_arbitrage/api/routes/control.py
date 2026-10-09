"""Administrative recovery endpoints for unknown control-command outcomes."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, cast

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from funding_arbitrage.security.control_plane import (
    CachedHttpResponse,
    ControlPlaneIdempotencyStore,
    ControlPlaneSecurityError,
    ControlPlaneTokenRevocationStore,
)

router = APIRouter(prefix="/control", tags=["control-plane"])

_PRINCIPAL_PATTERN = r"^[A-Za-z0-9._:@/-]{1,128}$"
_IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$"
_HASH_PATTERN = r"^[a-f0-9]{64}$"


class ReconcileIdempotencyRequest(BaseModel):
    """Authoritative outcome recorded only after external ledger reconciliation."""

    model_config = ConfigDict(extra="forbid")

    principal_id: str = Field(pattern=_PRINCIPAL_PATTERN)
    idempotency_key: str = Field(pattern=_IDEMPOTENCY_KEY_PATTERN)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    status_code: int = Field(ge=200, le=599)
    response_body: dict[str, JsonValue]

    @field_validator("status_code")
    @classmethod
    def reject_bodyless_statuses(cls, value: int) -> int:
        if value in {204, 205, 304}:
            raise ValueError("reconciled JSON responses require a response body")
        return value


class RevokeTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token_id: str = Field(pattern=_PRINCIPAL_PATTERN)
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def require_bounded_future_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware")
        expiry = value.astimezone(UTC)
        now = datetime.now(UTC)
        if expiry <= now:
            raise ValueError("expires_at must be in the future")
        if expiry > now + timedelta(days=30):
            raise ValueError("token revocation cannot exceed 30 days")
        return expiry


@router.post("/tokens/revoke")
async def revoke_token(
    payload: RevokeTokenRequest,
    request: Request,
) -> dict[str, object]:
    await _revocation_store(request).revoke(payload.token_id, payload.expires_at)
    return {
        "token_id_sha256": hashlib.sha256(payload.token_id.encode()).hexdigest(),
        "revoked_until": payload.expires_at.isoformat().replace("+00:00", "Z"),
    }


@router.get("/idempotency")
async def inspect_idempotency(
    request: Request,
    principal_id: Annotated[str, Query(pattern=_PRINCIPAL_PATTERN)],
    idempotency_key: Annotated[str, Query(pattern=_IDEMPOTENCY_KEY_PATTERN)],
) -> dict[str, object]:
    store = _store(request)
    snapshot = await store.inspect(principal_id, idempotency_key)
    if snapshot is None:
        raise ControlPlaneSecurityError(404, "idempotency reservation not found")
    return snapshot.model_dump(mode="json")


@router.post("/idempotency/reconcile")
async def reconcile_idempotency(
    payload: ReconcileIdempotencyRequest,
    request: Request,
) -> dict[str, object]:
    """Resolve PENDING without re-running the original side effect."""

    encoded_body = json.dumps(
        payload.response_body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    response = CachedHttpResponse(
        status_code=payload.status_code,
        body=encoded_body,
        headers=(("content-type", "application/json"),),
    )
    await _store(request).reconcile_pending(
        payload.principal_id,
        payload.idempotency_key,
        payload.request_hash,
        response,
    )
    return {
        "principal_id": payload.principal_id,
        "idempotency_key_sha256": hashlib.sha256(payload.idempotency_key.encode()).hexdigest(),
        "request_hash": payload.request_hash,
        "state": "COMPLETED",
        "result_status_code": payload.status_code,
        "result_body_sha256": hashlib.sha256(encoded_body).hexdigest(),
    }


def _store(request: Request) -> ControlPlaneIdempotencyStore:
    store = getattr(request.app.state, "control_plane_idempotency_store", None)
    if store is None:
        raise ControlPlaneSecurityError(503, "idempotency persistence unavailable")
    return cast(ControlPlaneIdempotencyStore, store)

def _revocation_store(request: Request) -> ControlPlaneTokenRevocationStore:
    store = getattr(request.app.state, "control_plane_token_revocation_store", None)
    if store is None:
        raise ControlPlaneSecurityError(503, "token revocation storage unavailable")
    return cast(ControlPlaneTokenRevocationStore, store)
