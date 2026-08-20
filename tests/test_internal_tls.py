from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any

from funding_arbitrage.config import Settings
from funding_arbitrage.database import session as database_session
from funding_arbitrage.internal_tls import (
    create_internal_ssl_context,
    redis_connection_kwargs,
)


class _FakeContext:
    minimum_version: ssl.TLSVersion | None = None
    check_hostname = False
    verify_mode = ssl.CERT_NONE

    def __init__(self) -> None:
        self.loaded: tuple[str, str] | None = None

    def load_cert_chain(self, *, certfile: str, keyfile: str) -> None:
        self.loaded = (certfile, keyfile)


def _files(tmp_path: Path) -> tuple[Path, Path, Path]:
    ca = tmp_path / "ca.crt"
    cert = tmp_path / "app-client.crt"
    key = tmp_path / "app-client.key"
    for path in (ca, cert, key):
        path.write_text("test", encoding="utf-8")
    return ca, cert, key


def _settings(tmp_path: Path) -> Settings:
    ca, cert, key = _files(tmp_path)
    return Settings(
        _env_file=None,
        INTERNAL_SERVICE_TLS_REQUIRED=True,
        INTERNAL_TLS_CA_FILE=str(ca),
        INTERNAL_TLS_CLIENT_CERT_FILE=str(cert),
        INTERNAL_TLS_CLIENT_KEY_FILE=str(key),
        REDIS_URL="rediss://redis:6379/0",
        REDIS_USERNAME="funding",
        REDIS_PASSWORD="redis-secret-0123456789abcdefabcd",
    )


def test_internal_context_enforces_hostname_ca_tls12_and_client_certificate(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fake = _FakeContext()
    observed: dict[str, object] = {}

    def create_context(purpose: ssl.Purpose, *, cafile: str) -> _FakeContext:
        observed.update({"purpose": purpose, "cafile": cafile})
        return fake

    monkeypatch.setattr(ssl, "create_default_context", create_context)
    settings = _settings(tmp_path)

    assert create_internal_ssl_context(settings) is fake
    assert observed == {
        "purpose": ssl.Purpose.SERVER_AUTH,
        "cafile": settings.internal_tls_ca_file,
    }
    assert fake.minimum_version is ssl.TLSVersion.TLSv1_2
    assert fake.check_hostname is True
    assert fake.verify_mode is ssl.CERT_REQUIRED
    assert fake.loaded == (
        settings.internal_tls_client_cert_file,
        settings.internal_tls_client_key_file,
    )


def test_redis_connection_is_authenticated_and_certificate_validating(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    options = redis_connection_kwargs(settings)

    assert options == {
        "username": "funding",
        "password": "redis-secret-0123456789abcdefabcd",
        "ssl_ca_certs": settings.internal_tls_ca_file,
        "ssl_certfile": settings.internal_tls_client_cert_file,
        "ssl_keyfile": settings.internal_tls_client_key_file,
        "ssl_cert_reqs": "required",
        "ssl_check_hostname": True,
    }
    assert "redis-secret-0123456789abcdefabcd" not in repr(settings)


def test_database_engine_receives_strict_ssl_context(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    settings = _settings(tmp_path)
    fake_context = object()
    observed: dict[str, object] = {}

    def fake_create_engine(url: str, **kwargs: object) -> object:
        observed.update({"url": url, **kwargs})
        return object()

    class _Factory:
        def __call__(self, engine: object, *, expire_on_commit: bool) -> str:
            assert expire_on_commit is False
            return "session-factory"

    monkeypatch.setattr(database_session, "create_internal_ssl_context", lambda _: fake_context)
    monkeypatch.setattr(database_session, "create_async_engine", fake_create_engine)
    monkeypatch.setattr(database_session, "async_sessionmaker", _Factory())

    engine, factory = database_session.create_database(settings)

    assert engine is not None
    assert factory == "session-factory"
    assert observed == {
        "url": settings.database_url,
        "connect_args": {"ssl": fake_context},
        "pool_pre_ping": True,
        "future": True,
    }


def test_missing_or_symlinked_tls_material_fails_closed(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        INTERNAL_SERVICE_TLS_REQUIRED=True,
        INTERNAL_TLS_CA_FILE=str(tmp_path / "missing-ca"),
        INTERNAL_TLS_CLIENT_CERT_FILE=str(tmp_path / "missing-cert"),
        INTERNAL_TLS_CLIENT_KEY_FILE=str(tmp_path / "missing-key"),
    )
    try:
        create_internal_ssl_context(settings)
    except RuntimeError as error:
        assert "regular non-symlink" in str(error)
    else:
        raise AssertionError("missing TLS material was accepted")