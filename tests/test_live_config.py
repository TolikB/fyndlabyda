from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings


def _live_values() -> dict[str, object]:
    return {
        "RUN_MODE": "live",
        "MARKET_DATA_MODE": "live_public",
        "EXECUTION_MODE": "live",
        "LIVE_ARMED": True,
        "LIVE_AUTOTRADE": True,
        "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        "LIVE_VENUES": "bybit,gate",
        "BYBIT_API_KEY": "bybit-key",
        "BYBIT_API_SECRET": "bybit-secret",
        "GATE_API_KEY": "gate-key",
        "GATE_API_SECRET": "gate-secret",
    }


def test_live_mode_requires_explicit_confirmation_and_credentials() -> None:
    values = _live_values()
    values.pop("LIVE_TRADING_CONFIRM")

    with pytest.raises(ValidationError, match="LIVE_TRADING_CONFIRM"):
        Settings(_env_file=None, **values)

    values = _live_values()
    values.pop("GATE_API_SECRET")
    with pytest.raises(ValidationError, match="missing live credentials for: gate"):
        Settings(_env_file=None, **values)


def test_live_mode_accepts_complete_minimal_configuration_and_masks_secrets() -> None:
    settings = Settings(_env_file=None, **_live_values())

    assert settings.run_mode == "live"
    assert settings.execution_mode == "live"
    assert settings.live_venue_values == ("bybit", "gate")
    assert settings.live_default_position_size_usd == Decimal("100")
    assert settings.live_credentials("bybit") == {
        "apiKey": "bybit-key",
        "secret": "bybit-secret",
    }
    assert "bybit-secret" not in repr(settings)


def test_live_telegram_alerts_require_both_credentials_and_mask_token() -> None:
    values = _live_values()
    values["TELEGRAM_ENABLED"] = True
    with pytest.raises(ValidationError, match="live Telegram alerts require"):
        Settings(_env_file=None, **values)

    values["TELEGRAM_BOT_TOKEN"] = "telegram-secret"
    values["TELEGRAM_CHAT_ID"] = "123"
    settings = Settings(_env_file=None, **values)
    assert "telegram-secret" not in repr(settings)


def test_live_execution_cannot_be_enabled_from_api_or_paper_mode() -> None:
    with pytest.raises(ValidationError, match="EXECUTION_MODE=live requires RUN_MODE=live"):
        Settings(_env_file=None, EXECUTION_MODE="live")


def test_live_example_is_complete_after_only_secrets_are_supplied() -> None:
    settings = Settings(
        _env_file=".env.live.example",
        POSTGRES_PASSWORD="database-secret",
        DATABASE_URL=(
            "postgresql+asyncpg://funding:database-secret@postgres:5432/funding"
        ),
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
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_CHAT_ID="123",
    )

    assert settings.run_mode == "live"
    assert settings.live_autotrade is True
