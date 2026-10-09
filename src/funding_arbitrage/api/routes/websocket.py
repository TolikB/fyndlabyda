from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from datetime import UTC, datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from funding_arbitrage.monitoring.metrics import websocket_connections
from funding_arbitrage.security.control_plane import (
    ControlPlaneAuditDraft,
    ControlPlaneSecurityError,
)

router = APIRouter()


async def _stream(websocket: WebSocket, payload_factory: Callable[[], object]) -> None:
    if not await _authorize(websocket):
        return
    await websocket.accept()
    websocket_connections.inc()
    try:
        while True:
            principal = getattr(websocket.state, "principal", None)
            if principal is not None and datetime.now(UTC) >= principal.expires_at:
                await _close_invalid_session(
                    websocket,
                    principal,
                    outcome="token_expired",
                    status_code=401,
                    close_code=4401,
                    reason="JWT has expired",
                )
                return
            if principal is not None:
                try:
                    revoked = (
                        await websocket.app.state.control_plane_token_revocation_store.is_revoked(
                            principal.token_id
                        )
                    )
                except ControlPlaneSecurityError:
                    await _close_invalid_session(
                        websocket,
                        principal,
                        outcome="revocation_check_failed",
                        status_code=503,
                        close_code=1011,
                        reason="token revocation storage unavailable",
                    )
                    return
                if revoked:
                    await _close_invalid_session(
                        websocket,
                        principal,
                        outcome="token_revoked",
                        status_code=401,
                        close_code=4401,
                        reason="JWT has been revoked",
                    )
                    return
            await websocket.send_json(payload_factory())
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=1)
            except TimeoutError:
                continue
    except WebSocketDisconnect:
        return
    finally:
        websocket_connections.dec()


async def _authorize(websocket: WebSocket) -> bool:
    security = websocket.app.state.control_plane_security
    if not security.policy.enabled:
        return True
    principal = None
    fingerprint = None
    request_hash = hashlib.sha256(websocket.url.path.encode()).hexdigest()
    try:
        client_host = websocket.client.host if websocket.client else ""
        certificate_fingerprint = websocket.headers.get("x-client-cert-sha256")
        certificate_pem = websocket.headers.get("x-verified-client-cert")
        await websocket.app.state.control_plane_rate_limiter.require(
            security.rate_limit_identity(client_host, certificate_fingerprint, certificate_pem)
        )
        principal, fingerprint = security.authenticate_and_authorize(
            authorization=websocket.headers.get("authorization"),
            client_host=client_host,
            certificate_fingerprint=certificate_fingerprint,
            certificate_pem=certificate_pem,
            method="GET",
            path=websocket.url.path,
        )
        if await websocket.app.state.control_plane_token_revocation_store.is_revoked(
            principal.token_id
        ):
            raise ControlPlaneSecurityError(401, "JWT has been revoked")
        await websocket.app.state.control_plane_audit_sink.append(
            ControlPlaneAuditDraft(
                timestamp=datetime.now(UTC),
                request_id=websocket.headers.get("x-request-id") or "websocket-connect",
                actor_id=principal.subject,
                actor_roles=tuple(sorted(role.value for role in principal.roles)),
                action="WS_CONNECT",
                resource=websocket.url.path,
                outcome="allowed",
                status_code=101,
                request_hash=request_hash,
                client_certificate_sha256=fingerprint,
            )
        )
        websocket.state.principal = principal
        return True
    except ControlPlaneSecurityError as error:
        try:
            await websocket.app.state.control_plane_audit_sink.append(
                ControlPlaneAuditDraft(
                    timestamp=datetime.now(UTC),
                    request_id=websocket.headers.get("x-request-id") or "websocket-rejected",
                    actor_id=principal.subject if principal is not None else "anonymous",
                    actor_roles=(
                        tuple(sorted(role.value for role in principal.roles))
                        if principal is not None
                        else ()
                    ),
                    action="WS_CONNECT",
                    resource=websocket.url.path,
                    outcome="rejected",
                    status_code=error.status_code,
                    request_hash=request_hash,
                    client_certificate_sha256=fingerprint,
                )
            )
        except Exception:
            await websocket.close(code=1011, reason="immutable security audit unavailable")
            return False
        close_code = (
            4401
            if error.status_code == 401
            else 4429
            if error.status_code == 429
            else 4503
            if error.status_code == 503
            else 4403
        )
        await websocket.close(code=close_code, reason=error.detail)
        return False


async def _close_invalid_session(
    websocket: WebSocket,
    principal: object,
    *,
    outcome: str,
    status_code: int,
    close_code: int,
    reason: str,
) -> None:
    actor_id = str(getattr(principal, "subject", "unknown"))
    roles = getattr(principal, "roles", ())
    try:
        await websocket.app.state.control_plane_audit_sink.append(
            ControlPlaneAuditDraft(
                timestamp=datetime.now(UTC),
                request_id=websocket.headers.get("x-request-id") or "websocket-session-invalid",
                actor_id=actor_id,
                actor_roles=tuple(sorted(role.value for role in roles)),
                action="WS_DISCONNECT",
                resource=websocket.url.path,
                outcome=outcome,
                status_code=status_code,
                request_hash=hashlib.sha256(websocket.url.path.encode()).hexdigest(),
                client_certificate_sha256=None,
            )
        )
    except Exception:
        await websocket.close(code=1011, reason="immutable security audit unavailable")
        return
    await websocket.close(code=close_code, reason=reason)


@router.websocket("/ws/opportunities")
async def opportunities_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket,
        lambda: [
            item.model_dump(mode="json") for item in websocket.app.state.runtime.opportunities
        ],
    )


@router.websocket("/ws/portfolio")
async def portfolio_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket, lambda: websocket.app.state.runtime.portfolio.snapshot().model_dump(mode="json")
    )


@router.websocket("/ws/market")
async def market_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket,
        lambda: {
            "captured_at": websocket.app.state.runtime.latest_snapshot.captured_at.isoformat()
            if websocket.app.state.runtime.latest_snapshot
            else None
        },
    )
