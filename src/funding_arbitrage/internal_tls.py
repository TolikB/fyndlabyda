"""Verified TLS client configuration for internal service traffic."""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from funding_arbitrage.config import Settings


def create_internal_ssl_context(settings: Settings) -> ssl.SSLContext | None:
    """Build one strict mTLS context; return None outside secured deployments."""

    if not settings.internal_service_tls_required:
        return None
    return create_client_ssl_context(
        ca_file=settings.internal_tls_ca_file,
        certificate_file=settings.internal_tls_client_cert_file,
        key_file=settings.internal_tls_client_key_file,
    )


def create_client_ssl_context(
    *,
    ca_file: str,
    certificate_file: str,
    key_file: str,
) -> ssl.SSLContext:
    """Create a hostname-validating TLS 1.2+ context from protected files."""

    ca_path = _regular_file(ca_file, "internal TLS CA")
    certificate_path = _regular_file(
        certificate_file, "internal TLS client certificate"
    )
    key_path = _regular_file(key_file, "internal TLS client key", private=True)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(ca_path))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=str(certificate_path), keyfile=str(key_path))
    return context


def redis_connection_kwargs(settings: Settings) -> dict[str, Any]:
    """Return authenticated, certificate-validating Redis client options."""

    kwargs: dict[str, Any] = {}
    username = settings.redis_username.strip()
    password = settings.redis_password.get_secret_value()
    if username:
        kwargs["username"] = username
    if password:
        kwargs["password"] = password
    if settings.internal_service_tls_required:
        kwargs.update(
            {
                "ssl_ca_certs": str(
                    _regular_file(settings.internal_tls_ca_file, "internal TLS CA")
                ),
                "ssl_certfile": str(
                    _regular_file(
                        settings.internal_tls_client_cert_file,
                        "internal TLS client certificate",
                    )
                ),
                "ssl_keyfile": str(
                    _regular_file(
                        settings.internal_tls_client_key_file,
                        "internal TLS client key",
                        private=True,
                    )
                ),
                "ssl_cert_reqs": "required",
                "ssl_check_hostname": True,
            }
        )
    return kwargs


def _regular_file(value: str, label: str, *, private: bool = False) -> Path:
    path = Path(value)
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"{label} must be a regular non-symlink file")
    if private and os.name == "posix" and path.stat().st_mode & 0o077:
        raise RuntimeError(f"{label} must not be group/world-accessible")
    return path