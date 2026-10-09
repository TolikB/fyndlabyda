from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from tests.live_security import live_credential_policy_json


def _live_values() -> dict[str, object]:
    values: dict[str, object] = {
        "APP_ENV": "production",
        "RELEASE_COMMIT_SHA": "a" * 40,
        "RUN_MODE": "live",
        "MARKET_DATA_MODE": "live_public",
        "EXECUTION_MODE": "live",
        "DATABASE_URL": (
            "postgresql+asyncpg://funding:"
            "database-secret-0123456789abcdef@postgres:5432/funding"
        ),
        "REDIS_URL": "rediss://redis:6379/0",
        "REDIS_USERNAME": "funding",
        "REDIS_PASSWORD": "redis-secret-0123456789abcdefabcd",
        "INTERNAL_SERVICE_TLS_REQUIRED": True,
        "INTERNAL_TLS_CA_FILE": "/run/secrets/internal/ca.crt",
        "INTERNAL_TLS_CLIENT_CERT_FILE": "/run/secrets/internal/app.crt",
        "INTERNAL_TLS_CLIENT_KEY_FILE": "/run/secrets/internal/app.key",
        "CONTROL_PLANE_SECURITY_ENABLED": True,
        "CONTROL_PLANE_JWT_SECRET": "0123456789abcdef0123456789abcdef",
        "CONTROL_PLANE_MTLS_REQUIRED": True,
        "CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED": True,
        "CONTROL_PLANE_RATE_LIMIT_BACKEND": "redis",
        "CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS": "a" * 64,
        "LIVE_ARMED": True,
        "LIVE_AUTOTRADE": True,
        "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        "LIVE_VENUES": "bybit,gate,mexc,kucoin,htx",
        "BYBIT_API_KEY": "bybit-key",
        "BYBIT_API_SECRET": "bybit-secret",
        "GATE_API_KEY": "gate-key",
        "GATE_API_SECRET": "gate-secret",
        "MEXC_API_KEY": "mexc-key",
        "MEXC_API_SECRET": "mexc-secret",
        "KUCOIN_API_KEY": "kucoin-key",
        "KUCOIN_API_SECRET": "kucoin-secret",
        "KUCOIN_API_PASSPHRASE": "kucoin-passphrase",
        "HTX_API_KEY": "htx-key",
        "HTX_API_SECRET": "htx-secret",
        "TELEGRAM_ENABLED": True,
        "TELEGRAM_BOT_TOKEN": "telegram-secret",
        "TELEGRAM_CHAT_ID": "123",
        "LIVE_EXPECTED_EGRESS_IP": "203.0.113.10",
    }
    values["LIVE_CREDENTIAL_POLICY_JSON"] = live_credential_policy_json(
        {
            "bybit": "bybit-key",
            "gate": "gate-key",
            "mexc": "mexc-key",
            "kucoin": "kucoin-key",
            "htx": "htx-key",
        }
    )
    return values


def test_live_mode_requires_current_least_privilege_credential_policy() -> None:
    values = _live_values()
    values.pop("LIVE_CREDENTIAL_POLICY_JSON")
    with pytest.raises(ValidationError, match="live credential policy requires"):
        Settings(_env_file=None, **values)

    values = _live_values()
    values["LIVE_CREDENTIAL_POLICY_JSON"] = live_credential_policy_json(
        {
            "bybit": "wrong-key",
            "gate": "gate-key",
            "mexc": "mexc-key",
            "kucoin": "kucoin-key",
            "htx": "htx-key",
        }
    )
    with pytest.raises(ValidationError, match="does not match its policy fingerprint"):
        Settings(_env_file=None, **values)


def test_live_mode_requires_explicit_confirmation_and_credentials() -> None:
    values = _live_values()
    values.pop("LIVE_TRADING_CONFIRM")

    with pytest.raises(ValidationError, match="LIVE_TRADING_CONFIRM"):
        Settings(_env_file=None, **values)

    values = _live_values()
    values.pop("GATE_API_SECRET")
    with pytest.raises(ValidationError, match="missing live credentials for: gate"):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("CONTROL_PLANE_SECURITY_ENABLED", False, "CONTROL_PLANE_SECURITY_ENABLED"),
        ("CONTROL_PLANE_JWT_SECRET", "short", "CONTROL_PLANE_JWT_SECRET"),
        (
            "CONTROL_PLANE_JWT_SECRET",
            "CHANGE_ME_AT_LEAST_32_RANDOM_BYTES",
            "CONTROL_PLANE_JWT_SECRET",
        ),
        ("CONTROL_PLANE_MTLS_REQUIRED", False, "CONTROL_PLANE_MTLS_REQUIRED"),
        (
            "CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS",
            "",
            "CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS",
        ),
    ],
)
def test_live_mode_requires_fail_closed_control_plane(
    field: str, value: object, message: str
) -> None:
    values = _live_values()
    values[field] = value
    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    ("field", "venue"),
    [
        ("KUCOIN_API_PASSPHRASE", "kucoin"),
        ("HTX_API_SECRET", "htx"),
    ],
)
def test_live_mode_requires_complete_kucoin_and_htx_credentials(field: str, venue: str) -> None:
    values = _live_values()
    values.pop(field)

    with pytest.raises(ValidationError, match=f"missing live credentials for: {venue}"):
        Settings(_env_file=None, **values)


def test_live_mode_accepts_complete_minimal_configuration_and_masks_secrets() -> None:
    settings = Settings(_env_file=None, **_live_values())

    assert settings.run_mode == "live"
    assert settings.execution_mode == "live"
    assert settings.live_venue_values == ("bybit", "gate", "mexc", "kucoin", "htx")
    assert settings.live_default_position_size_usd == Decimal("100")
    assert settings.live_credentials("bybit") == {
        "apiKey": "bybit-key",
        "secret": "bybit-secret",
    }
    assert settings.live_credentials("mexc") == {
        "apiKey": "mexc-key",
        "secret": "mexc-secret",
    }
    assert settings.live_credentials("kucoin") == {
        "apiKey": "kucoin-key",
        "secret": "kucoin-secret",
        "password": "kucoin-passphrase",
    }
    assert settings.live_credentials("htx") == {
        "apiKey": "htx-key",
        "secret": "htx-secret",
    }
    assert "bybit-secret" not in repr(settings)
    assert "mexc-secret" not in repr(settings)
    assert "kucoin-passphrase" not in repr(settings)
    assert "htx-secret" not in repr(settings)


def test_limited_live_uses_the_same_strict_live_interlocks() -> None:
    settings = Settings(
        _env_file=None,
        **_live_values(),
        TRADING_MODE="LIMITED_LIVE",
    )

    assert settings.effective_trading_mode.value == "LIMITED_LIVE"
    assert settings.mode_contract.exchange_orders_enabled is True
    assert settings.mode_contract.operator_arming_required is True

def test_live_telegram_alerts_require_both_credentials_and_mask_token() -> None:
    values = _live_values()
    values.pop("TELEGRAM_BOT_TOKEN")
    with pytest.raises(ValidationError, match="live Telegram alerts require"):
        Settings(_env_file=None, **values)

    values["TELEGRAM_BOT_TOKEN"] = "telegram-secret"
    settings = Settings(_env_file=None, **values)
    assert "telegram-secret" not in repr(settings)


def test_live_execution_cannot_be_enabled_from_api_or_paper_mode() -> None:
    with pytest.raises(ValidationError, match="EXECUTION_MODE=live requires RUN_MODE=live"):
        Settings(_env_file=None, EXECUTION_MODE="live")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("APP_ENV", "development", "APP_ENV=production"),
        ("DATABASE_URL", "sqlite+aiosqlite:///live.db", "requires PostgreSQL"),
        (
            "DATABASE_URL",
            "postgresql+asyncpg://funding@postgres:5432/funding",
            "strong authenticated DATABASE_URL",
        ),
        (
            "INTERNAL_SERVICE_TLS_REQUIRED",
            False,
            "INTERNAL_SERVICE_TLS_REQUIRED=true",
        ),
        ("REDIS_URL", "redis://redis:6379/0", "Redis TLS via rediss"),
        ("REDIS_PASSWORD", "short", "strong REDIS_PASSWORD"),
        ("INTERNAL_TLS_CA_FILE", "", "certificate paths are incomplete"),
        ("LIVE_REQUIRE_DEDICATED_ACCOUNTS", False, "dedicated exchange accounts"),
        ("LIVE_MARGIN_MODE", "cross", "requires isolated margin"),
        ("TELEGRAM_ENABLED", False, "requires Telegram safety alerts"),
        (
            "TELEGRAM_API_BASE_URL",
            "https://example.invalid",
            "TELEGRAM_API_BASE_URL must be the official",
        ),
        (
            "MEXC_BASE_URL",
            "https://example.invalid",
            "MEXC_BASE_URL must be the official",
        ),
        (
            "MEXC_FUTURES_BASE_URL",
            "https://example.invalid",
            "MEXC_FUTURES_BASE_URL must be the official",
        ),
        (
            "MEXC_FUTURES_WS_URL",
            "wss://example.invalid/edge",
            "MEXC_FUTURES_WS_URL must be the official",
        ),
        (
            "BYBIT_WS_URL",
            "wss://example.invalid/ws",
            "BYBIT_WS_URL must be the official",
        ),
        (
            "GATE_BASE_URL",
            "https://example.invalid/api/v4",
            "GATE_BASE_URL must be the official",
        ),
        (
            "KUCOIN_FUTURES_BASE_URL",
            "https://example.invalid",
            "KUCOIN_FUTURES_BASE_URL must be the official",
        ),
        (
            "HTX_FUTURES_WS_URL",
            "wss://example.invalid/ws",
            "HTX_FUTURES_WS_URL must be the official",
        ),
    ],
)
def test_live_mode_rejects_unsafe_runtime_boundaries(
    field: str, value: object, message: str
) -> None:
    values = _live_values()
    values[field] = value

    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **values)


def test_database_and_redis_credentials_are_not_in_settings_repr() -> None:
    settings = Settings(
        _env_file=None,
        DATABASE_URL="postgresql+asyncpg://funding:db-secret@localhost/funding",
        REDIS_URL="redis://:url-secret@localhost:6379/0",
        REDIS_PASSWORD="field-secret",
    )

    rendered = repr(settings)
    assert "db-secret" not in rendered
    assert "url-secret" not in rendered
    assert "field-secret" not in rendered


def test_venues_without_supported_live_sandbox_are_rejected() -> None:
    values = _live_values()
    values["LIVE_SANDBOX"] = True

    with pytest.raises(ValidationError, match="live sandbox is not supported for: htx,kucoin,mexc"):
        Settings(_env_file=None, **values)


def test_live_example_is_complete_after_only_secrets_are_supplied() -> None:
    settings = Settings(
        _env_file=".env.live.example",
        RELEASE_COMMIT_SHA="a" * 40,
        POSTGRES_PASSWORD="database-secret-0123456789abcdef",
        DATABASE_URL=(
            "postgresql+asyncpg://funding:"
            "database-secret-0123456789abcdef@postgres:5432/funding"
        ),
        REDIS_PASSWORD="redis-secret-0123456789abcdefabcd",
        CONTROL_PLANE_JWT_SECRET="0123456789abcdef0123456789abcdef",
        CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS="a" * 64,
        BYBIT_API_KEY="key",
        BYBIT_API_SECRET="secret",
        GATE_API_KEY="key",
        GATE_API_SECRET="secret",
        OKX_API_KEY="key",
        OKX_API_SECRET="secret",
        OKX_API_PASSPHRASE="passphrase",
        BINANCE_API_KEY="key",
        BINANCE_API_SECRET="secret",
        HYPERLIQUID_WALLET_ADDRESS="0x" + "1" * 40,
        HYPERLIQUID_PRIVATE_KEY="0x" + "2" * 64,
        MEXC_API_KEY="key",
        MEXC_API_SECRET="secret",
        KUCOIN_API_KEY="key",
        KUCOIN_API_SECRET="secret",
        KUCOIN_API_PASSPHRASE="passphrase",
        HTX_API_KEY="key",
        HTX_API_SECRET="secret",
        LIVE_EXPECTED_EGRESS_IP="203.0.113.10",
        LIVE_CREDENTIAL_POLICY_FILE="",
        LIVE_CREDENTIAL_POLICY_JSON=live_credential_policy_json(
            {
                "bybit": "key",
                "gate": "key",
                "okx": "key",
                "binance": "key",
                "hyperliquid": "0x" + "1" * 40,
                "mexc": "key",
                "kucoin": "key",
                "htx": "key",
            }
        ),
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_CHAT_ID="123",
    )

    assert settings.run_mode == "live"
    assert settings.live_autotrade is False
