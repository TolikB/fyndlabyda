from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from redis.exceptions import RedisError
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.websockets import WebSocketDisconnect

from funding_arbitrage.api.routes.control import router as control_router
from funding_arbitrage.api.routes.websocket import router as websocket_router
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import ApiIdempotencyRecord, Base, ImmutableAuditRecord
from funding_arbitrage.database.repositories.audit import DatabaseControlPlaneAuditSink
from funding_arbitrage.database.repositories.control_plane import (
    DatabaseControlPlaneIdempotencyStore,
)
from funding_arbitrage.database.session import init_database
from funding_arbitrage.main import create_app
from funding_arbitrage.security.control_plane import (
    CachedHttpResponse,
    ControlPlaneAuditDraft,
    ControlPlaneAuditRecord,
    ControlPlaneMiddleware,
    ControlPlanePolicy,
    ControlPlaneSecurity,
    ControlPlaneSecurityError,
    MemoryIdempotencyStore,
    MemoryImmutableAuditSink,
    Role,
    issue_hs256_token,
)
from funding_arbitrage.security.rate_limit import (
    MemoryControlPlaneRateLimiter,
    RedisControlPlaneRateLimiter,
)
from funding_arbitrage.security.revocation import (
    MemoryTokenRevocationStore,
    RedisTokenRevocationStore,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
SECRET = "0123456789abcdef0123456789abcdef"
FINGERPRINT = "a" * 64
IDEMPOTENCY_KEY = "control-command-000001"


def _policy(*, rate_limit: int = 100) -> ControlPlanePolicy:
    return ControlPlanePolicy(
        enabled=True,
        jwt_secret=SecretStr(SECRET),
        jwt_issuer="test-issuer",
        jwt_audience="test-audience",
        mtls_required=True,
        trusted_proxy_hosts=frozenset({"testclient"}),
        client_certificate_fingerprints=frozenset({FINGERPRINT}),
        rate_limit_per_minute=rate_limit,
    )


def _token(policy: ControlPlanePolicy, *roles: Role) -> str:
    issued_at = datetime.now(UTC)
    return issue_hs256_token(
        policy,
        subject="operator-a",
        roles=roles,
        token_id="token-1",
        issued_at=issued_at,
        expires_at=issued_at + timedelta(hours=1),
    )


def _replace_claim(token: str, name: str, value: object) -> str:
    encoded_header, encoded_payload, _ = token.split(".")
    payload = json.loads(
        base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4))
    )
    payload[name] = value
    replacement = (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )
        .rstrip(b"=")
        .decode()
    )
    signed = f"{encoded_header}.{replacement}"
    signature = hmac.new(SECRET.encode(), signed.encode(), hashlib.sha256).digest()
    encoded_signature = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    return f"{signed}.{encoded_signature}"


def _headers(policy: ControlPlanePolicy, *roles: Role) -> dict[str, str]:
    return {
        "authorization": f"Bearer {_token(policy, *roles)}",
        "x-client-cert-sha256": FINGERPRINT,
    }


def _app(
    policy: ControlPlanePolicy,
    *,
    audit_sink: MemoryImmutableAuditSink | None = None,
) -> tuple[FastAPI, MemoryImmutableAuditSink, dict[str, int]]:
    app = FastAPI()
    sink = audit_sink or MemoryImmutableAuditSink()
    store = MemoryIdempotencyStore(policy.idempotency_ttl_seconds)
    revocations = MemoryTokenRevocationStore()
    counter = {"writes": 0}
    app.state.control_plane_idempotency_store = store
    app.state.control_plane_token_revocation_store = revocations
    app.add_middleware(
        ControlPlaneMiddleware,
        security=ControlPlaneSecurity(policy),
        audit_sink=sink,
        idempotency_store=store,
        rate_limiter=MemoryControlPlaneRateLimiter(policy.rate_limit_per_minute),
        token_revocation_store=revocations,
    )
    app.include_router(control_router)

    @app.get("/private")
    async def private_read() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/scan/command")
    async def controlled_write(payload: dict[str, object]) -> dict[str, object]:
        counter["writes"] += 1
        return {"writes": counter["writes"], "payload": payload}

    @app.post("/scan/fail")
    async def unknown_write() -> None:
        counter["writes"] += 1
        raise RuntimeError("simulated unknown command outcome")

    return app, sink, counter


def test_http_auth_rbac_idempotency_and_audit_chain() -> None:
    policy = _policy()
    app, audit, counter = _app(policy)
    with TestClient(app) as client:
        assert client.get("/private").status_code == 401
        viewer = _headers(policy, Role.VIEWER)
        assert client.get("/private", headers=viewer).status_code == 200
        assert client.post("/scan/command", headers=viewer, json={}).status_code == 403

        operator = _headers(policy, Role.OPERATOR)
        assert client.post("/scan/command", headers=operator, json={}).status_code == 428
        command_headers = {**operator, "idempotency-key": IDEMPOTENCY_KEY}
        first = client.post("/scan/command", headers=command_headers, json={"size": 1})
        replay = client.post("/scan/command", headers=command_headers, json={"size": 1})
        conflict = client.post("/scan/command", headers=command_headers, json={"size": 2})

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert conflict.status_code == 409
    assert counter["writes"] == 1
    audit.verify()
    assert any(record.outcome == "idempotent_replay" for record in audit.records)


def test_mtls_jwt_and_rate_limit_fail_closed() -> None:
    policy = _policy(rate_limit=1)
    app, audit, _ = _app(policy)
    headers = _headers(policy, Role.VIEWER)
    with TestClient(app) as client:
        missing_certificate = dict(headers)
        missing_certificate.pop("x-client-cert-sha256")
        assert client.get("/private", headers=missing_certificate).status_code == 403
        assert client.get("/private", headers=headers).status_code == 200
        assert client.get("/private", headers=headers).status_code == 429
    audit.verify()


def test_jwt_rejects_expired_wrong_audience_algorithm_and_tamper() -> None:
    policy = _policy()
    security = ControlPlaneSecurity(policy)
    expired = issue_hs256_token(
        policy,
        subject="operator-a",
        roles=(Role.VIEWER,),
        token_id="expired-token",
        issued_at=NOW - timedelta(hours=2),
        expires_at=NOW - timedelta(hours=1),
    )
    with pytest.raises(ControlPlaneSecurityError, match="expired"):
        security.jwt.authenticate(f"Bearer {expired}", now=NOW)

    other_policy = policy.model_copy(update={"jwt_audience": "other-audience"})
    wrong_audience = _token(other_policy, Role.VIEWER)
    with pytest.raises(ControlPlaneSecurityError, match="audience"):
        security.jwt.authenticate(f"Bearer {wrong_audience}", now=NOW)

    valid = _token(policy, Role.VIEWER)
    malformed_audience = _replace_claim(valid, "aud", {"invalid": "mapping"})
    with pytest.raises(ControlPlaneSecurityError, match="audience"):
        security.jwt.authenticate(f"Bearer {malformed_audience}")
    header = (
        base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode())
        .rstrip(b"=")
        .decode()
    )
    bad_algorithm = f"{header}.{valid.split('.')[1]}.{valid.split('.')[2]}"
    with pytest.raises(ControlPlaneSecurityError, match="algorithm"):
        security.jwt.authenticate(f"Bearer {bad_algorithm}", now=NOW)

    tampered = valid[:-1] + ("A" if valid[-1] != "A" else "B")
    with pytest.raises(ControlPlaneSecurityError, match="signature"):
        security.jwt.authenticate(f"Bearer {tampered}", now=NOW)


def test_unknown_write_outcome_remains_reserved() -> None:
    policy = _policy()
    app, audit, counter = _app(policy)
    headers = {
        **_headers(policy, Role.OPERATOR),
        "idempotency-key": "unknown-command-000001",
    }
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.post("/scan/fail", headers=headers).status_code == 500
        retry = client.post("/scan/fail", headers=headers)
    assert retry.status_code == 409
    assert "pending reconciliation" in retry.json()["detail"]
    assert counter["writes"] == 1
    audit.verify()


async def test_database_idempotency_survives_store_recreation() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first_store = DatabaseControlPlaneIdempotencyStore(factory, 3600)
    assert await first_store.reserve("operator-a", IDEMPOTENCY_KEY, "1" * 64) is None
    expected = CachedHttpResponse(
        status_code=202,
        body=b'{"accepted":true}',
        headers=(("content-type", "application/json"),),
    )
    await first_store.complete("operator-a", IDEMPOTENCY_KEY, "1" * 64, expected)

    recreated = DatabaseControlPlaneIdempotencyStore(factory, 3600)
    assert await recreated.reserve("operator-a", IDEMPOTENCY_KEY, "1" * 64) == expected
    with pytest.raises(ControlPlaneSecurityError, match="payload conflict"):
        await recreated.reserve("operator-a", IDEMPOTENCY_KEY, "2" * 64)

    pending_key = "pending-command-000001"
    assert await recreated.reserve("operator-a", pending_key, "3" * 64) is None
    async with factory() as session:
        await session.execute(
            update(ApiIdempotencyRecord)
            .where(ApiIdempotencyRecord.idempotency_key == pending_key)
            .values(expires_at=datetime.now(UTC) - timedelta(days=1))
        )
        await session.commit()
    after_restart = DatabaseControlPlaneIdempotencyStore(factory, 3600)
    with pytest.raises(ControlPlaneSecurityError, match="pending reconciliation"):
        await after_restart.reserve("operator-a", pending_key, "3" * 64)
    await engine.dispose()


async def test_database_audit_chain_detects_tampering() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    sink = DatabaseControlPlaneAuditSink(factory)
    for sequence in range(2):
        await sink.append(
            ControlPlaneAuditDraft(
                timestamp=NOW + timedelta(seconds=sequence),
                request_id=f"request-{sequence}",
                actor_id="operator-a",
                actor_roles=("operator",),
                action="POST",
                resource="/scan/command",
                outcome="allowed",
                status_code=200,
                idempotency_key=None,
                request_hash=str(sequence) * 64,
                client_certificate_sha256=FINGERPRINT,
            )
        )
    assert await sink.verify() == 2

    async with factory() as session:
        await session.execute(
            update(ImmutableAuditRecord)
            .where(ImmutableAuditRecord.sequence == 1)
            .values(payload={"tampered": True})
        )
        await session.commit()
    with pytest.raises(ValueError, match="payload hash"):
        await sink.verify()
    await engine.dispose()


class _UnavailableAuditSink:
    async def append(self, event: ControlPlaneAuditDraft) -> ControlPlaneAuditRecord:
        del event
        raise RuntimeError("audit database unavailable")


def test_audit_outage_returns_service_unavailable() -> None:
    policy = _policy()
    app = FastAPI()
    app.add_middleware(
        ControlPlaneMiddleware,
        security=ControlPlaneSecurity(policy),
        audit_sink=_UnavailableAuditSink(),
        idempotency_store=MemoryIdempotencyStore(policy.idempotency_ttl_seconds),
        rate_limiter=MemoryControlPlaneRateLimiter(policy.rate_limit_per_minute),
        token_revocation_store=MemoryTokenRevocationStore(),
    )

    @app.get("/private")
    async def private_read() -> dict[str, bool]:
        return {"ok": True}

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/private", headers=_headers(policy, Role.VIEWER))
    assert response.status_code == 503
    assert response.json()["detail"] == "immutable security audit unavailable"


def test_websocket_requires_same_jwt_mtls_rbac_and_audit() -> None:
    policy = _policy()
    app = FastAPI()
    security = ControlPlaneSecurity(policy)
    audit = MemoryImmutableAuditSink()
    app.state.control_plane_security = security
    app.state.control_plane_audit_sink = audit
    app.state.control_plane_rate_limiter = MemoryControlPlaneRateLimiter(
        policy.rate_limit_per_minute
    )
    app.state.control_plane_token_revocation_store = MemoryTokenRevocationStore()
    app.state.runtime = SimpleNamespace(opportunities=[])
    app.include_router(websocket_router)

    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect("/ws/opportunities"):
                pass
        assert rejected.value.code == 4401

        with client.websocket_connect(
            "/ws/opportunities",
            headers=_headers(policy, Role.VIEWER),
        ) as websocket:
            assert websocket.receive_json() == []

    audit.verify()
    assert [record.outcome for record in audit.records] == ["rejected", "allowed"]


def test_create_app_wires_database_backed_security(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        RUN_MODE="api",
        DATABASE_URL=f"sqlite+aiosqlite:///{(tmp_path / 'control.db').as_posix()}",
        CONTROL_PLANE_SECURITY_ENABLED=True,
        CONTROL_PLANE_JWT_SECRET=SECRET,
        CONTROL_PLANE_MTLS_REQUIRED=True,
        CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS=FINGERPRINT,
    )
    engine = create_async_engine(settings.database_url)

    async def prepare_database() -> None:
        await init_database(engine)
        await engine.dispose()

    asyncio.run(prepare_database())
    app = create_app(settings)
    policy = ControlPlanePolicy.from_settings(settings)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/exchanges").status_code == 401
        assert (
            client.get(
                "/exchanges",
                headers=_headers(policy, Role.VIEWER),
            ).status_code
            == 200
        )
    assert isinstance(
        app.state.control_plane_idempotency_store,
        DatabaseControlPlaneIdempotencyStore,
    )


def _synthetic_client_certificate() -> tuple[str, str]:
    certificate_der = b"synthetic-control-plane-client-certificate"
    certificate_pem = (
        "-----BEGIN CERTIFICATE-----\n"
        + base64.b64encode(certificate_der).decode()
        + "\n-----END CERTIFICATE-----"
    )
    return quote(certificate_pem), hashlib.sha256(certificate_der).hexdigest()


def test_strict_mtls_derives_allowlisted_fingerprint_from_verified_pem() -> None:
    encoded_pem, expected_fingerprint = _synthetic_client_certificate()
    policy = _policy().model_copy(
        update={
            "mtls_certificate_header_required": True,
            "client_certificate_fingerprints": frozenset({expected_fingerprint}),
        }
    )
    security = ControlPlaneSecurity(policy)
    principal, fingerprint = security.authenticate_and_authorize(
        authorization=f"Bearer {_token(policy, Role.VIEWER)}",
        client_host="testclient",
        certificate_fingerprint="f" * 64,
        certificate_pem=encoded_pem,
        method="GET",
        path="/private",
    )

    assert principal.subject == "operator-a"
    assert fingerprint == expected_fingerprint
    assert security.rate_limit_identity("testclient", None, encoded_pem) != (
        security.rate_limit_identity("testclient", None, None)
    )


def test_unauthenticated_requests_are_rate_limited_before_jwt_parsing() -> None:
    policy = _policy(rate_limit=1)
    app, audit, _ = _app(policy)
    with TestClient(app) as client:
        assert client.get("/private").status_code == 401
        assert client.get("/private").status_code == 429
    audit.verify()


def test_chunked_request_body_is_bounded_before_endpoint_execution() -> None:
    policy = _policy().model_copy(update={"maximum_request_bytes": 5})
    app, audit, counter = _app(policy)
    headers = {
        **_headers(policy, Role.OPERATOR),
        "idempotency-key": "oversized-command-000001",
        "transfer-encoding": "chunked",
    }
    with TestClient(app) as client:
        response = client.post(
            "/scan/command",
            headers=headers,
            content=(chunk for chunk in (b"123", b"456")),
        )
    assert response.status_code == 413
    assert counter["writes"] == 0
    audit.verify()


def test_websocket_disconnects_when_jwt_expires() -> None:
    policy = _policy()
    app = FastAPI()
    audit = MemoryImmutableAuditSink()
    app.state.control_plane_security = ControlPlaneSecurity(policy)
    app.state.control_plane_audit_sink = audit
    app.state.control_plane_rate_limiter = MemoryControlPlaneRateLimiter(100)
    app.state.control_plane_token_revocation_store = MemoryTokenRevocationStore()
    app.state.runtime = SimpleNamespace(opportunities=[])
    app.include_router(websocket_router)
    issued_at = datetime.now(UTC) - timedelta(seconds=1)
    token = issue_hs256_token(
        policy,
        subject="operator-a",
        roles=(Role.VIEWER,),
        token_id="short-websocket-token",
        issued_at=issued_at,
        expires_at=datetime.now(UTC) + timedelta(seconds=1.2),
    )
    headers = {
        "authorization": f"Bearer {token}",
        "x-client-cert-sha256": FINGERPRINT,
    }

    with TestClient(app) as client:
        with client.websocket_connect("/ws/opportunities", headers=headers) as websocket:
            assert websocket.receive_json() == []
            with pytest.raises(WebSocketDisconnect) as expired:
                while True:
                    websocket.receive_json()
    assert expired.value.code == 4401
    audit.verify()
    assert audit.records[-1].outcome == "token_expired"


class _FakeRedis:
    def __init__(self, responses: list[int] | None = None, error: Exception | None = None):
        self.responses = responses or []
        self.error = error

    async def eval(self, *args: object) -> int:
        del args
        if self.error is not None:
            raise self.error
        return self.responses.pop(0)

    async def ping(self) -> bool:
        if self.error is not None:
            raise self.error
        return True

    async def aclose(self) -> None:
        return None


async def test_redis_rate_limiter_is_atomic_and_fails_closed() -> None:
    limiter = RedisControlPlaneRateLimiter("redis://unused", 1)
    limiter._redis = _FakeRedis([1, 0])  # type: ignore[assignment]
    await limiter.require("client-a")
    with pytest.raises(ControlPlaneSecurityError, match="rate limit exceeded"):
        await limiter.require("client-a")

    limiter._redis = _FakeRedis(error=RedisError("unavailable"))  # type: ignore[assignment]
    with pytest.raises(ControlPlaneSecurityError, match="storage unavailable"):
        await limiter.require("client-a")
    with pytest.raises(ControlPlaneSecurityError, match="storage unavailable"):
        await limiter.probe()


async def test_sqlite_idempotency_reservation_is_serialized_across_stores(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'concurrent-control.db').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    initial = DatabaseControlPlaneIdempotencyStore(factory, 3600)
    key = "concurrent-command-000001"
    old_hash = "4" * 64
    new_hash = "5" * 64
    assert await initial.reserve("operator-a", key, old_hash) is None
    await initial.complete(
        "operator-a",
        key,
        old_hash,
        CachedHttpResponse(status_code=200, body=b"{}", headers=()),
    )
    async with factory() as session:
        await session.execute(
            update(ApiIdempotencyRecord)
            .where(ApiIdempotencyRecord.idempotency_key == key)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.commit()

    results = await asyncio.gather(
        DatabaseControlPlaneIdempotencyStore(factory, 3600).reserve("operator-a", key, new_hash),
        DatabaseControlPlaneIdempotencyStore(factory, 3600).reserve("operator-a", key, new_hash),
        return_exceptions=True,
    )
    assert sum(result is None for result in results) == 1
    errors = [result for result in results if isinstance(result, Exception)]
    assert len(errors) == 1
    assert isinstance(errors[0], ControlPlaneSecurityError)
    assert "pending reconciliation" in str(errors[0])
    await engine.dispose()


def test_readiness_fails_when_control_plane_schema_is_missing(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        RUN_MODE="api",
        DATABASE_URL=f"sqlite+aiosqlite:///{(tmp_path / 'missing-schema.db').as_posix()}",
        CONTROL_PLANE_SECURITY_ENABLED=True,
        CONTROL_PLANE_JWT_SECRET=SECRET,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["detail"] == "control-plane security storage is unavailable"


def test_admin_can_reconcile_unknown_outcome_without_reexecution() -> None:
    policy = _policy()
    app, audit, counter = _app(policy)
    target_key = "reconcile-target-000001"
    target_headers = {
        **_headers(policy, Role.OPERATOR),
        "idempotency-key": target_key,
    }
    admin = _headers(policy, Role.ADMIN)
    query = {"principal_id": "operator-a", "idempotency_key": target_key}

    with TestClient(app, raise_server_exceptions=False) as client:
        first = client.post("/scan/fail", headers=target_headers, json={"order": "A"})
        assert first.status_code == 500
        assert counter["writes"] == 1

        assert (
            client.get(
                "/control/idempotency", params=query, headers=_headers(policy, Role.VIEWER)
            ).status_code
            == 403
        )
        pending = client.get("/control/idempotency", params=query, headers=admin)
        assert pending.status_code == 200
        pending_payload = pending.json()
        assert pending_payload["state"] == "PENDING"

        wrong_payload = {
            "principal_id": "operator-a",
            "idempotency_key": target_key,
            "request_hash": "0" * 64,
            "status_code": 202,
            "response_body": {"confirmed": True},
        }
        wrong_headers = {
            **admin,
            "idempotency-key": "reconcile-attempt-000001",
        }
        wrong = client.post(
            "/control/idempotency/reconcile",
            headers=wrong_headers,
            json=wrong_payload,
        )
        wrong_replay = client.post(
            "/control/idempotency/reconcile",
            headers=wrong_headers,
            json=wrong_payload,
        )
        assert wrong.status_code == 409
        assert wrong_replay.content == wrong.content

        reconciliation = {
            **wrong_payload,
            "request_hash": pending_payload["request_hash"],
            "response_body": {"confirmed": True, "source": "exchange-ledger"},
        }
        resolved = client.post(
            "/control/idempotency/reconcile",
            headers={**admin, "idempotency-key": "reconcile-attempt-000002"},
            json=reconciliation,
        )
        assert resolved.status_code == 200
        assert resolved.json()["state"] == "COMPLETED"

        original_replay = client.post(
            "/scan/fail",
            headers=target_headers,
            json={"order": "A"},
        )
        assert original_replay.status_code == 202
        assert original_replay.json() == {
            "confirmed": True,
            "source": "exchange-ledger",
        }

    assert counter["writes"] == 1
    audit.verify()


async def test_concurrent_database_reconciliation_accepts_one_outcome(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'reconciliation-race.db').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    key = "reconcile-race-000001"
    request_hash = "6" * 64
    first = DatabaseControlPlaneIdempotencyStore(factory, 3600)
    assert await first.reserve("operator-a", key, request_hash) is None
    response_a = CachedHttpResponse(200, b'{"outcome":"A"}', ())
    response_b = CachedHttpResponse(409, b'{"outcome":"B"}', ())

    results = await asyncio.gather(
        DatabaseControlPlaneIdempotencyStore(factory, 3600).reconcile_pending(
            "operator-a", key, request_hash, response_a
        ),
        DatabaseControlPlaneIdempotencyStore(factory, 3600).reconcile_pending(
            "operator-a", key, request_hash, response_b
        ),
        return_exceptions=True,
    )
    successes = [result for result in results if isinstance(result, CachedHttpResponse)]
    failures = [result for result in results if isinstance(result, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], ControlPlaneSecurityError)
    assert "outcome conflict" in str(failures[0])
    snapshot = await first.inspect("operator-a", key)
    assert snapshot is not None
    assert snapshot.state.value == "COMPLETED"
    assert snapshot.status_code == successes[0].status_code
    await engine.dispose()


def test_admin_revocation_disconnects_active_websocket() -> None:
    policy = _policy()
    app, audit, _ = _app(policy)
    app.state.control_plane_security = ControlPlaneSecurity(policy)
    app.state.control_plane_audit_sink = audit
    app.state.control_plane_rate_limiter = MemoryControlPlaneRateLimiter(100)
    app.state.runtime = SimpleNamespace(opportunities=[])
    app.include_router(websocket_router)
    issued_at = datetime.now(UTC)
    target_token = issue_hs256_token(
        policy,
        subject="viewer-a",
        roles=(Role.VIEWER,),
        token_id="websocket-target-token",
        issued_at=issued_at,
        expires_at=issued_at + timedelta(hours=1),
    )
    websocket_headers = {
        "authorization": f"Bearer {target_token}",
        "x-client-cert-sha256": FINGERPRINT,
    }
    revoke_payload = {
        "token_id": "websocket-target-token",
        "expires_at": (issued_at + timedelta(hours=1)).isoformat(),
    }
    revoke_headers = {
        **_headers(policy, Role.ADMIN),
        "idempotency-key": "revoke-websocket-000001",
    }

    with TestClient(app) as client:
        with client.websocket_connect("/ws/opportunities", headers=websocket_headers) as websocket:
            assert websocket.receive_json() == []
            revoked = client.post(
                "/control/tokens/revoke",
                headers=revoke_headers,
                json=revoke_payload,
            )
            assert revoked.status_code == 200
            with pytest.raises(WebSocketDisconnect) as disconnected:
                while True:
                    websocket.receive_json()
    assert disconnected.value.code == 4401
    audit.verify()
    assert audit.records[-1].outcome == "token_revoked"


class _FakeRevocationRedis:
    def __init__(self, *, exists: int = 0, error: Exception | None = None) -> None:
        self.exists_result = exists
        self.error = error
        self.values: list[tuple[object, ...]] = []

    async def exists(self, *args: object) -> int:
        if self.error is not None:
            raise self.error
        self.values.append(args)
        return self.exists_result

    async def set(self, *args: object, **kwargs: object) -> bool:
        if self.error is not None:
            raise self.error
        self.values.append((*args, kwargs))
        return True

    async def ping(self) -> bool:
        if self.error is not None:
            raise self.error
        return True

    async def aclose(self) -> None:
        return None


async def test_redis_token_revocation_is_shared_and_fails_closed() -> None:
    store = RedisTokenRevocationStore("redis://unused")
    fake = _FakeRevocationRedis(exists=1)
    store._redis = fake  # type: ignore[assignment]
    assert await store.is_revoked("token-a") is True
    await store.revoke("token-a", datetime.now(UTC) + timedelta(minutes=5))
    assert len(fake.values) == 2

    store._redis = _FakeRevocationRedis(  # type: ignore[assignment]
        error=RedisError("unavailable")
    )
    with pytest.raises(ControlPlaneSecurityError, match="revocation storage unavailable"):
        await store.is_revoked("token-a")
    with pytest.raises(ControlPlaneSecurityError, match="revocation storage unavailable"):
        await store.probe()
