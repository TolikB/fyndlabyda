"""Typed application configuration with environment overrides."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from funding_arbitrage.opportunity.models import FeeSchedule

# Settings removed with simulator v2; their values now live in the series file.
DEPRECATED_ENVIRONMENT = frozenset(
    {
        "PAPER_INITIAL_BALANCE_USD",
        "PAPER_VENUES",
        "PAPER_RESERVE_PERCENT",
        "PAPER_MAX_HOLD_SECONDS",
        "PAPER_POSITION_SIZE_USD",
        "PAPER_MAX_OPEN_POSITIONS",
        "PAPER_SETTLEMENT_INTERVAL_SECONDS",
        "PAPER_HISTORY_REFRESH_SECONDS",
        "PAPER_ORDERBOOK_SYMBOL_LIMIT",
        "REDIS_URL",
    }
)

# The bot is public-data only: any exchange credential in the environment is a
# deployment mistake and must stop the service before it can be misused.
_CREDENTIAL_PATTERN = re.compile(
    r"(API[_-]?KEY|API[_-]?SECRET|SECRET[_-]?KEY|PASSPHRASE|PRIVATE[_-]?KEY|WALLET)", re.I
)
_ALLOWED_SECRET_NAMES = frozenset(
    {"TELEGRAM_BOT_TOKEN", "POSTGRES_PASSWORD", "GF_SECURITY_ADMIN_PASSWORD"}
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_env: str = Field(default="development", alias="APP_ENV")
    run_mode: Literal["api", "paper_test"] = Field(default="api", alias="RUN_MODE")
    market_data_mode: Literal["live_public", "mock"] = Field(
        default="live_public", alias="MARKET_DATA_MODE"
    )
    # There is no live execution path; the literal documents and enforces that.
    execution_mode: Literal["paper"] = Field(default="paper", alias="EXECUTION_MODE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+asyncpg://funding:funding@localhost:5432/funding",
        alias="DATABASE_URL",
    )
    bybit_base_url: str = Field(default="https://api.bybit.com", alias="BYBIT_BASE_URL")
    bybit_ws_url: str = Field(
        default="wss://stream.bybit.com/v5/public/linear", alias="BYBIT_WS_URL"
    )
    bybit_categories: str = Field(default="linear,spot", alias="BYBIT_CATEGORIES")
    gate_base_url: str = Field(default="https://api.gateio.ws/api/v4", alias="GATE_BASE_URL")
    gate_ws_url: str = Field(default="wss://fx-ws.gateio.ws/v4/ws/usdt", alias="GATE_WS_URL")
    gate_settle: str = Field(default="usdt", alias="GATE_SETTLE")
    okx_base_url: str = Field(default="https://www.okx.com", alias="OKX_BASE_URL")
    okx_ws_url: str = Field(default="wss://ws.okx.com:8443/ws/v5/public", alias="OKX_WS_URL")
    # OKX publishes funding per instrument; this many swaps are refreshed per cycle.
    okx_funding_symbol_limit: int = Field(default=30, alias="OKX_FUNDING_SYMBOL_LIMIT")
    binance_spot_base_url: str = Field(
        default="https://api.binance.com", alias="BINANCE_SPOT_BASE_URL"
    )
    binance_futures_base_url: str = Field(
        default="https://fapi.binance.com", alias="BINANCE_FUTURES_BASE_URL"
    )
    binance_ws_url: str = Field(default="wss://fstream.binance.com/ws", alias="BINANCE_WS_URL")
    hyperliquid_base_url: str = Field(
        default="https://api.hyperliquid.xyz", alias="HYPERLIQUID_BASE_URL"
    )
    hyperliquid_ws_url: str = Field(
        default="wss://api.hyperliquid.xyz/ws", alias="HYPERLIQUID_WS_URL"
    )
    enabled_venues: str = Field(
        default="bybit,gate,okx,binance,hyperliquid", alias="ENABLED_VENUES"
    )

    # Market data ---------------------------------------------------------------
    market_data_stale_seconds: int = Field(default=60, alias="MARKET_DATA_STALE_SECONDS")
    market_funding_stale_seconds: int = Field(default=900, alias="MARKET_FUNDING_STALE_SECONDS")
    market_venue_timeout_seconds: float = Field(default=25.0, alias="MARKET_VENUE_TIMEOUT_SECONDS")
    market_instrument_refresh_seconds: int = Field(
        default=3600, alias="MARKET_INSTRUMENT_REFRESH_SECONDS"
    )
    market_history_ttl_seconds: int = Field(default=21_600, alias="MARKET_HISTORY_TTL_SECONDS")
    market_persist_funding_seconds: int = Field(default=900, alias="MARKET_PERSIST_FUNDING_SECONDS")
    market_persist_tickers: bool = Field(default=False, alias="MARKET_PERSIST_TICKERS")
    market_data_retention_days: int = Field(default=30, alias="MARKET_DATA_RETENTION_DAYS")
    opportunity_persist_seconds: int = Field(default=300, alias="OPPORTUNITY_PERSIST_SECONDS")
    mock_funding_interval_seconds: int = Field(
        default=28_800, alias="MOCK_FUNDING_INTERVAL_SECONDS"
    )

    # Paper runner ------------------------------------------------------------------
    paper_series_file: str = Field(default="config/paper_series.yaml", alias="PAPER_SERIES_FILE")
    # false = observe only: collect, scan, fetch books, but open no positions.
    paper_autotrade: bool = Field(default=False, alias="PAPER_AUTOTRADE")
    paper_loop_interval_seconds: float = Field(default=15.0, alias="PAPER_LOOP_INTERVAL_SECONDS")
    paper_confirmation_seconds: int = Field(default=60, alias="PAPER_CONFIRMATION_SECONDS")
    paper_auto_init_database: bool = Field(default=False, alias="PAPER_AUTO_INIT_DATABASE")
    paper_book_depth: int = Field(default=50, alias="PAPER_BOOK_DEPTH")
    paper_max_book_age_seconds: float = Field(default=10.0, alias="PAPER_MAX_BOOK_AGE_SECONDS")
    # Hard per-fill impact limit versus the book mid, in percent.
    paper_max_fill_slippage_percent: Decimal = Field(
        default=Decimal("0.5"), alias="PAPER_MAX_FILL_SLIPPAGE_PERCENT"
    )
    paper_book_candidates_per_cycle: int = Field(default=6, alias="PAPER_BOOK_CANDIDATES_PER_CYCLE")
    paper_history_requests_per_cycle: int = Field(
        default=6, alias="PAPER_HISTORY_REQUESTS_PER_CYCLE"
    )
    paper_funding_grace_seconds: int = Field(default=900, alias="PAPER_FUNDING_GRACE_SECONDS")
    paper_funding_poll_seconds: int = Field(default=3600, alias="PAPER_FUNDING_POLL_SECONDS")
    paper_snapshot_interval_seconds: int = Field(
        default=60, alias="PAPER_SNAPSHOT_INTERVAL_SECONDS"
    )
    paper_close_defer_alert_seconds: int = Field(
        default=600, alias="PAPER_CLOSE_DEFER_ALERT_SECONDS"
    )

    # Telegram -------------------------------------------------------------------
    telegram_enabled: bool = Field(default=False, alias="TELEGRAM_ENABLED")
    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    telegram_api_base_url: str = Field(
        default="https://api.telegram.org", alias="TELEGRAM_API_BASE_URL"
    )
    telegram_timezone: str = Field(default="Europe/Kyiv", alias="TELEGRAM_TIMEZONE")
    telegram_report_hour: int = Field(default=0, alias="TELEGRAM_REPORT_HOUR")
    telegram_report_minute: int = Field(default=5, alias="TELEGRAM_REPORT_MINUTE")

    # Scanner (api mode; paper mode derives filters from the series file) -------------
    scanner_minimum_net_apr: Decimal = Field(
        default=Decimal("0.10"), alias="SCANNER_MINIMUM_NET_APR"
    )
    scanner_minimum_liquidity_score: Decimal = Field(
        default=Decimal("70"), alias="SCANNER_MINIMUM_LIQUIDITY_SCORE"
    )
    scanner_maximum_slippage_percent: Decimal = Field(
        default=Decimal("0.15"), alias="SCANNER_MAXIMUM_SLIPPAGE_PERCENT"
    )
    scanner_maximum_spread_percent: Decimal = Field(
        default=Decimal("0.20"), alias="SCANNER_MAXIMUM_SPREAD_PERCENT"
    )
    scanner_minimum_funding_samples: int = Field(
        default=20, alias="SCANNER_MINIMUM_FUNDING_SAMPLES"
    )
    scanner_minimum_duration_seconds: int = Field(
        default=30, alias="SCANNER_MINIMUM_DURATION_SECONDS"
    )
    # Horizon over which one day of funding must amortize the round-trip costs.
    scanner_expected_holding_hours: Decimal = Field(
        default=Decimal("24"), alias="SCANNER_EXPECTED_HOLDING_HOURS"
    )
    scanner_allow_short_spot: bool = Field(default=False, alias="SCANNER_ALLOW_SHORT_SPOT")
    scanner_max_cross_price_deviation: Decimal = Field(
        default=Decimal("0.015"), alias="SCANNER_MAX_CROSS_PRICE_DEVIATION"
    )
    scanner_max_basis: Decimal = Field(default=Decimal("0.03"), alias="SCANNER_MAX_BASIS")
    scanner_equivalent_quotes: str = Field(default="USDT,USDC", alias="SCANNER_EQUIVALENT_QUOTES")

    # Taker/maker fees (VIP0 defaults; verify against each venue before a series starts).
    bybit_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="BYBIT_MAKER_FEE")
    bybit_taker_fee: Decimal = Field(default=Decimal("0.00055"), alias="BYBIT_TAKER_FEE")
    bybit_spot_taker_fee: Decimal = Field(default=Decimal("0.001"), alias="BYBIT_SPOT_TAKER_FEE")
    gate_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="GATE_MAKER_FEE")
    gate_taker_fee: Decimal = Field(default=Decimal("0.0005"), alias="GATE_TAKER_FEE")
    gate_spot_taker_fee: Decimal = Field(default=Decimal("0.001"), alias="GATE_SPOT_TAKER_FEE")
    okx_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="OKX_MAKER_FEE")
    okx_taker_fee: Decimal = Field(default=Decimal("0.0005"), alias="OKX_TAKER_FEE")
    okx_spot_taker_fee: Decimal = Field(default=Decimal("0.001"), alias="OKX_SPOT_TAKER_FEE")
    binance_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="BINANCE_MAKER_FEE")
    binance_taker_fee: Decimal = Field(default=Decimal("0.0005"), alias="BINANCE_TAKER_FEE")
    binance_spot_taker_fee: Decimal = Field(
        default=Decimal("0.001"), alias="BINANCE_SPOT_TAKER_FEE"
    )
    hyperliquid_maker_fee: Decimal = Field(
        default=Decimal("0.00015"), alias="HYPERLIQUID_MAKER_FEE"
    )
    hyperliquid_taker_fee: Decimal = Field(
        default=Decimal("0.00045"), alias="HYPERLIQUID_TAKER_FEE"
    )
    request_timeout_seconds: float = Field(default=15.0, alias="REQUEST_TIMEOUT_SECONDS")
    rate_limit_requests_per_second: float = Field(
        default=8.0, alias="RATE_LIMIT_REQUESTS_PER_SECOND"
    )
    rate_limit_burst: int = Field(default=8, alias="RATE_LIMIT_BURST")

    @model_validator(mode="after")
    def validate_safe_modes(self) -> Settings:
        _validate_safe_values(self)
        return self

    @property
    def bybit_category_values(self) -> tuple[str, ...]:
        return tuple(value.strip() for value in self.bybit_categories.split(",") if value.strip())

    @property
    def enabled_venue_values(self) -> tuple[str, ...]:
        return tuple(
            value.strip().lower() for value in self.enabled_venues.split(",") if value.strip()
        )

    @property
    def equivalent_quote_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().upper()
            for value in self.scanner_equivalent_quotes.split(",")
            if value.strip()
        )

    @property
    def fee_schedules(self) -> dict[str, FeeSchedule]:
        return {
            "bybit": FeeSchedule(
                maker_fee=self.bybit_maker_fee,
                taker_fee=self.bybit_taker_fee,
                spot_taker_fee=self.bybit_spot_taker_fee,
            ),
            "gate": FeeSchedule(
                maker_fee=self.gate_maker_fee,
                taker_fee=self.gate_taker_fee,
                spot_taker_fee=self.gate_spot_taker_fee,
            ),
            "okx": FeeSchedule(
                maker_fee=self.okx_maker_fee,
                taker_fee=self.okx_taker_fee,
                spot_taker_fee=self.okx_spot_taker_fee,
            ),
            "binance": FeeSchedule(
                maker_fee=self.binance_maker_fee,
                taker_fee=self.binance_taker_fee,
                spot_taker_fee=self.binance_spot_taker_fee,
            ),
            "hyperliquid": FeeSchedule(
                maker_fee=self.hyperliquid_maker_fee, taker_fee=self.hyperliquid_taker_fee
            ),
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process."""

    settings = Settings()
    config_path = Path("config/default.yaml")
    if config_path.exists():
        # YAML supplies local defaults; explicit environment variables remain authoritative.
        with config_path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        sections: dict[str, dict[str, str]] = {
            "app": {
                "environment": "app_env",
                "log_level": "log_level",
                "run_mode": "run_mode",
                "market_data_mode": "market_data_mode",
                "execution_mode": "execution_mode",
            },
            "bybit": {"base_url": "bybit_base_url", "websocket_url": "bybit_ws_url"},
            "gate": {
                "base_url": "gate_base_url",
                "websocket_url": "gate_ws_url",
                "settle": "gate_settle",
            },
            "okx": {
                "base_url": "okx_base_url",
                "websocket_url": "okx_ws_url",
                "funding_symbol_limit": "okx_funding_symbol_limit",
            },
            "binance": {
                "spot_base_url": "binance_spot_base_url",
                "futures_base_url": "binance_futures_base_url",
                "websocket_url": "binance_ws_url",
            },
            "hyperliquid": {
                "base_url": "hyperliquid_base_url",
                "websocket_url": "hyperliquid_ws_url",
            },
            "scanner": {
                "minimum_net_apr": "scanner_minimum_net_apr",
                "minimum_liquidity_score": "scanner_minimum_liquidity_score",
                "maximum_slippage_percent": "scanner_maximum_slippage_percent",
                "maximum_spread_percent": "scanner_maximum_spread_percent",
                "minimum_funding_samples": "scanner_minimum_funding_samples",
                "minimum_opportunity_duration_seconds": "scanner_minimum_duration_seconds",
            },
            "paper_runner": {
                "series_file": "paper_series_file",
                "autotrade": "paper_autotrade",
                "loop_interval_seconds": "paper_loop_interval_seconds",
                "confirmation_seconds": "paper_confirmation_seconds",
                "book_depth": "paper_book_depth",
                "max_book_age_seconds": "paper_max_book_age_seconds",
                "auto_init_database": "paper_auto_init_database",
            },
            "telegram": {
                "enabled": "telegram_enabled",
                "api_base_url": "telegram_api_base_url",
                "timezone": "telegram_timezone",
                "report_hour": "telegram_report_hour",
                "report_minute": "telegram_report_minute",
            },
        }
        for section, fields in sections.items():
            values = raw.get(section, {}) or {}
            for yaml_key, field_name in fields.items():
                if field_name not in settings.model_fields_set and yaml_key in values:
                    setattr(settings, field_name, values[yaml_key])
        _validate_safe_values(settings)
    return settings


def _validate_safe_values(settings: Settings) -> None:
    if settings.run_mode == "paper_test" and settings.execution_mode != "paper":
        raise ValueError("paper_test requires EXECUTION_MODE=paper")
    positive: dict[str, Decimal | float | int] = {
        "SCANNER_EXPECTED_HOLDING_HOURS": settings.scanner_expected_holding_hours,
        "PAPER_LOOP_INTERVAL_SECONDS": settings.paper_loop_interval_seconds,
        "PAPER_BOOK_DEPTH": settings.paper_book_depth,
        "PAPER_MAX_BOOK_AGE_SECONDS": settings.paper_max_book_age_seconds,
        "PAPER_SNAPSHOT_INTERVAL_SECONDS": settings.paper_snapshot_interval_seconds,
        "OKX_FUNDING_SYMBOL_LIMIT": settings.okx_funding_symbol_limit,
        "MARKET_VENUE_TIMEOUT_SECONDS": settings.market_venue_timeout_seconds,
        "MARKET_INSTRUMENT_REFRESH_SECONDS": settings.market_instrument_refresh_seconds,
        "MOCK_FUNDING_INTERVAL_SECONDS": settings.mock_funding_interval_seconds,
    }
    for name, value in positive.items():
        if value <= 0:
            raise ValueError(f"{name} must be positive")
    if not settings.enabled_venue_values:
        raise ValueError("ENABLED_VENUES must contain at least one venue")
    unknown = set(settings.enabled_venue_values) - {
        "bybit",
        "gate",
        "okx",
        "binance",
        "hyperliquid",
    }
    if unknown:
        raise ValueError(f"unknown venues in ENABLED_VENUES: {sorted(unknown)}")
    if not 0 <= settings.telegram_report_hour <= 23:
        raise ValueError("TELEGRAM_REPORT_HOUR must be between 0 and 23")
    if not 0 <= settings.telegram_report_minute <= 59:
        raise ValueError("TELEGRAM_REPORT_MINUTE must be between 0 and 59")


def credential_variables(environ: Mapping[str, str]) -> list[str]:
    """Names (never values) of environment variables that look like exchange credentials."""

    return sorted(
        name
        for name, value in environ.items()
        if value and name.upper() not in _ALLOWED_SECRET_NAMES and _CREDENTIAL_PATTERN.search(name)
    )


def assert_public_data_only(environ: Mapping[str, str]) -> None:
    names = credential_variables(environ)
    if names:
        raise RuntimeError(
            "exchange credentials are not allowed in this paper-only deployment; remove: "
            + ", ".join(names)
        )


def deprecated_variables(environ: Mapping[str, str]) -> list[str]:
    return sorted(name for name in environ if name.upper() in DEPRECATED_ENVIRONMENT)
