"""Typed application configuration with environment overrides."""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_env: str = Field(default="development", alias="APP_ENV")
    run_mode: Literal["api", "paper_test", "live"] = Field(default="api", alias="RUN_MODE")
    market_data_mode: Literal["live_public", "mock"] = Field(
        default="live_public", alias="MARKET_DATA_MODE"
    )
    execution_mode: Literal["paper", "live"] = Field(default="paper", alias="EXECUTION_MODE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+asyncpg://funding:funding@localhost:5432/funding",
        alias="DATABASE_URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
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
    live_armed: bool = Field(default=False, alias="LIVE_ARMED")
    live_trading_confirm: str = Field(default="", alias="LIVE_TRADING_CONFIRM")
    live_autotrade: bool = Field(default=False, alias="LIVE_AUTOTRADE")
    live_sandbox: bool = Field(default=False, alias="LIVE_SANDBOX")
    live_venues: str = Field(
        default="bybit,gate,okx,binance,hyperliquid", alias="LIVE_VENUES"
    )
    live_allowed_assets: str = Field(default="BTC,ETH,SOL", alias="LIVE_ALLOWED_ASSETS")
    live_allowed_strategies: str = Field(
        default="spot_perp,cross_exchange_funding,futures_basis",
        alias="LIVE_ALLOWED_STRATEGIES",
    )
    live_reserve_assets: str = Field(
        default="USD,USDT,USDC",
        alias="LIVE_RESERVE_ASSETS",
    )
    live_client_order_prefix: str = Field(default="fa", alias="LIVE_CLIENT_ORDER_PREFIX")
    live_kill_switch_file: str = Field(
        default=".runtime/LIVE_DISABLED", alias="LIVE_KILL_SWITCH_FILE"
    )
    live_default_position_size_usd: Decimal = Field(
        default=Decimal("100"), alias="LIVE_DEFAULT_POSITION_SIZE_USD"
    )
    live_max_order_notional_usd: Decimal = Field(
        default=Decimal("100"), alias="LIVE_MAX_ORDER_NOTIONAL_USD"
    )
    live_max_total_notional_usd: Decimal = Field(
        default=Decimal("500"), alias="LIVE_MAX_TOTAL_NOTIONAL_USD"
    )
    live_max_asset_notional_usd: Decimal = Field(
        default=Decimal("250"), alias="LIVE_MAX_ASSET_NOTIONAL_USD"
    )
    live_max_venue_notional_usd: Decimal = Field(
        default=Decimal("300"), alias="LIVE_MAX_VENUE_NOTIONAL_USD"
    )
    live_max_strategy_notional_usd: Decimal = Field(
        default=Decimal("300"), alias="LIVE_MAX_STRATEGY_NOTIONAL_USD"
    )
    live_max_correlated_notional_usd: Decimal = Field(
        default=Decimal("300"), alias="LIVE_MAX_CORRELATED_NOTIONAL_USD"
    )
    live_max_open_positions: int = Field(default=2, alias="LIVE_MAX_OPEN_POSITIONS")
    live_max_daily_loss_usd: Decimal = Field(
        default=Decimal("50"), alias="LIVE_MAX_DAILY_LOSS_USD"
    )
    live_max_drawdown_percent: Decimal = Field(
        default=Decimal("0.02"), alias="LIVE_MAX_DRAWDOWN_PERCENT"
    )
    live_max_slippage_percent: Decimal = Field(
        default=Decimal("0.0015"), alias="LIVE_MAX_SLIPPAGE_PERCENT"
    )
    live_max_hedge_drift_percent: Decimal = Field(
        default=Decimal("0.0005"), alias="LIVE_MAX_HEDGE_DRIFT_PERCENT"
    )
    live_max_spot_residual_usd: Decimal = Field(
        default=Decimal("0.25"), alias="LIVE_MAX_SPOT_RESIDUAL_USD"
    )
    live_min_free_balance_percent: Decimal = Field(
        default=Decimal("30"), alias="LIVE_MIN_FREE_BALANCE_PERCENT"
    )
    live_min_expected_profit_usd: Decimal = Field(
        default=Decimal("0.10"), alias="LIVE_MIN_EXPECTED_PROFIT_USD"
    )
    live_min_venue_equity_usd: Decimal = Field(
        default=Decimal("10"), alias="LIVE_MIN_VENUE_EQUITY_USD"
    )
    live_max_adverse_basis_percent: Decimal = Field(
        default=Decimal("0.005"), alias="LIVE_MAX_ADVERSE_BASIS_PERCENT"
    )
    live_order_timeout_seconds: float = Field(
        default=20.0, alias="LIVE_ORDER_TIMEOUT_SECONDS"
    )
    live_reconciliation_interval_seconds: float = Field(
        default=30.0, alias="LIVE_RECONCILIATION_INTERVAL_SECONDS"
    )
    live_loop_interval_seconds: float = Field(
        default=10.0, alias="LIVE_LOOP_INTERVAL_SECONDS"
    )
    live_max_hold_seconds: int = Field(default=32400, alias="LIVE_MAX_HOLD_SECONDS")
    live_exit_edge_miss_cycles: int = Field(
        default=2, alias="LIVE_EXIT_EDGE_MISS_CYCLES"
    )
    live_entry_window_hours: Decimal = Field(
        default=Decimal("2"), alias="LIVE_ENTRY_WINDOW_HOURS"
    )
    live_min_settlement_cost_coverage: Decimal = Field(
        default=Decimal("2"), alias="LIVE_MIN_SETTLEMENT_COST_COVERAGE"
    )
    live_settlement_grace_seconds: int = Field(
        default=90, alias="LIVE_SETTLEMENT_GRACE_SECONDS"
    )
    live_market_persist_interval_seconds: int = Field(
        default=60, alias="LIVE_MARKET_PERSIST_INTERVAL_SECONDS"
    )
    live_account_snapshot_interval_seconds: int = Field(
        default=60, alias="LIVE_ACCOUNT_SNAPSHOT_INTERVAL_SECONDS"
    )
    live_leverage: int = Field(default=1, alias="LIVE_LEVERAGE")
    live_margin_mode: Literal["isolated", "cross"] = Field(
        default="isolated", alias="LIVE_MARGIN_MODE"
    )
    live_require_dedicated_accounts: bool = Field(
        default=True, alias="LIVE_REQUIRE_DEDICATED_ACCOUNTS"
    )
    live_liquidate_on_pause: bool = Field(
        default=True, alias="LIVE_LIQUIDATE_ON_PAUSE"
    )
    bybit_api_key: SecretStr = Field(default=SecretStr(""), alias="BYBIT_API_KEY")
    bybit_api_secret: SecretStr = Field(default=SecretStr(""), alias="BYBIT_API_SECRET")
    gate_api_key: SecretStr = Field(default=SecretStr(""), alias="GATE_API_KEY")
    gate_api_secret: SecretStr = Field(default=SecretStr(""), alias="GATE_API_SECRET")
    okx_api_key: SecretStr = Field(default=SecretStr(""), alias="OKX_API_KEY")
    okx_api_secret: SecretStr = Field(default=SecretStr(""), alias="OKX_API_SECRET")
    okx_api_passphrase: SecretStr = Field(
        default=SecretStr(""), alias="OKX_API_PASSPHRASE"
    )
    binance_api_key: SecretStr = Field(default=SecretStr(""), alias="BINANCE_API_KEY")
    binance_api_secret: SecretStr = Field(
        default=SecretStr(""), alias="BINANCE_API_SECRET"
    )
    hyperliquid_wallet_address: SecretStr = Field(
        default=SecretStr(""), alias="HYPERLIQUID_WALLET_ADDRESS"
    )
    hyperliquid_private_key: SecretStr = Field(
        default=SecretStr(""), alias="HYPERLIQUID_PRIVATE_KEY"
    )
    market_data_stale_seconds: int = Field(default=30, alias="MARKET_DATA_STALE_SECONDS")
    paper_initial_balance_usd: Decimal = Field(
        default=Decimal("15000"), alias="PAPER_INITIAL_BALANCE_USD"
    )
    paper_venues: str = Field(default="bybit,gate,okx,binance,hyperliquid", alias="PAPER_VENUES")
    paper_reserve_percent: Decimal = Field(default=Decimal("20"), alias="PAPER_RESERVE_PERCENT")
    paper_max_single_opportunity_percent: Decimal = Field(
        default=Decimal("20"), alias="PAPER_MAX_SINGLE_OPPORTUNITY_PERCENT"
    )
    paper_max_single_asset_percent: Decimal = Field(
        default=Decimal("30"), alias="PAPER_MAX_SINGLE_ASSET_PERCENT"
    )
    paper_max_single_exchange_percent: Decimal = Field(
        default=Decimal("40"), alias="PAPER_MAX_SINGLE_EXCHANGE_PERCENT"
    )
    paper_max_single_strategy_percent: Decimal = Field(
        default=Decimal("60"), alias="PAPER_MAX_SINGLE_STRATEGY_PERCENT"
    )
    paper_max_correlated_group_percent: Decimal = Field(
        default=Decimal("50"), alias="PAPER_MAX_CORRELATED_GROUP_PERCENT"
    )
    paper_legging_move_percent: Decimal = Field(
        default=Decimal("0.0002"), alias="PAPER_LEGGING_MOVE_PERCENT"
    )
    paper_correlation_groups: str = Field(
        default="BTC,ETH,SOL;DOGE,SHIB,PEPE,WIF,BONK,FLOKI,TUT",
        alias="PAPER_CORRELATION_GROUPS",
    )
    paper_autotrade: bool = Field(default=False, alias="PAPER_AUTOTRADE")
    paper_loop_interval_seconds: float = Field(
        default=10.0, alias="PAPER_LOOP_INTERVAL_SECONDS"
    )
    paper_confirmation_seconds: int = Field(default=30, alias="PAPER_CONFIRMATION_SECONDS")
    paper_max_hold_seconds: int = Field(default=900, alias="PAPER_MAX_HOLD_SECONDS")
    paper_position_size_usd: Decimal = Field(
        default=Decimal("250"), alias="PAPER_POSITION_SIZE_USD"
    )
    paper_max_open_positions: int = Field(default=10, alias="PAPER_MAX_OPEN_POSITIONS")
    paper_settlement_interval_seconds: int = Field(
        default=28800, alias="PAPER_SETTLEMENT_INTERVAL_SECONDS"
    )
    paper_history_refresh_seconds: int = Field(
        default=3600, alias="PAPER_HISTORY_REFRESH_SECONDS"
    )
    paper_orderbook_symbol_limit: int = Field(
        default=10, alias="PAPER_ORDERBOOK_SYMBOL_LIMIT"
    )
    paper_market_asset_limit: int = Field(default=12, alias="PAPER_MARKET_ASSET_LIMIT")
    paper_history_symbol_limit: int = Field(default=5, alias="PAPER_HISTORY_SYMBOL_LIMIT")
    paper_market_persist_interval_seconds: int = Field(
        default=300, alias="PAPER_MARKET_PERSIST_INTERVAL_SECONDS"
    )
    paper_auto_init_database: bool = Field(
        default=False, alias="PAPER_AUTO_INIT_DATABASE"
    )
    paper_simulation_version: str = Field(
        default="v16-oos-candidate", alias="PAPER_SIMULATION_VERSION"
    )
    paper_strategy_profile: Literal["baseline", "candidate"] = Field(
        default="candidate", alias="PAPER_STRATEGY_PROFILE"
    )
    paper_comparison_enabled: bool = Field(
        default=False, alias="PAPER_COMPARISON_ENABLED"
    )
    paper_baseline_simulation_version: str = Field(
        default="v16-oos-baseline", alias="PAPER_BASELINE_SIMULATION_VERSION"
    )
    paper_exit_edge_miss_cycles: int = Field(
        default=2, alias="PAPER_EXIT_EDGE_MISS_CYCLES"
    )
    paper_funding_horizon_hours: Decimal = Field(
        default=Decimal("24"), alias="PAPER_FUNDING_HORIZON_HOURS"
    )
    paper_entry_window_hours: Decimal = Field(
        default=Decimal("2"), alias="PAPER_ENTRY_WINDOW_HOURS"
    )
    paper_min_settlement_cost_coverage: Decimal = Field(
        default=Decimal("2"), alias="PAPER_MIN_SETTLEMENT_COST_COVERAGE"
    )
    paper_max_adverse_basis_percent: Decimal = Field(
        default=Decimal("0.005"), alias="PAPER_MAX_ADVERSE_BASIS_PERCENT"
    )
    telegram_enabled: bool = Field(default=False, alias="TELEGRAM_ENABLED")
    telegram_bot_token: SecretStr = Field(
        default=SecretStr(""), alias="TELEGRAM_BOT_TOKEN"
    )
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    telegram_api_base_url: str = Field(
        default="https://api.telegram.org", alias="TELEGRAM_API_BASE_URL"
    )
    telegram_timezone: str = Field(default="Europe/Kyiv", alias="TELEGRAM_TIMEZONE")
    telegram_report_hour: int = Field(default=0, alias="TELEGRAM_REPORT_HOUR")
    telegram_report_minute: int = Field(default=0, alias="TELEGRAM_REPORT_MINUTE")
    scanner_minimum_net_apr: Decimal = Field(
        default=Decimal("0.10"), alias="SCANNER_MINIMUM_NET_APR"
    )
    scanner_minimum_liquidity_score: Decimal = Field(
        default=Decimal("70"), alias="SCANNER_MINIMUM_LIQUIDITY_SCORE"
    )
    scanner_maximum_slippage_percent: Decimal = Field(
        default=Decimal("0.0015"), alias="SCANNER_MAXIMUM_SLIPPAGE_PERCENT"
    )
    scanner_maximum_spread_percent: Decimal = Field(
        default=Decimal("0.0020"), alias="SCANNER_MAXIMUM_SPREAD_PERCENT"
    )
    scanner_minimum_funding_samples: int = Field(
        default=20, alias="SCANNER_MINIMUM_FUNDING_SAMPLES"
    )
    scanner_minimum_duration_seconds: int = Field(
        default=30, alias="SCANNER_MINIMUM_DURATION_SECONDS"
    )
    scanner_allow_spot_short: bool = Field(
        default=False, alias="SCANNER_ALLOW_SPOT_SHORT"
    )
    scanner_borrowing_cost_daily: Decimal = Field(
        default=Decimal("0"), alias="SCANNER_BORROWING_COST_DAILY"
    )
    bybit_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="BYBIT_MAKER_FEE")
    bybit_taker_fee: Decimal = Field(default=Decimal("0.00055"), alias="BYBIT_TAKER_FEE")
    gate_maker_fee: Decimal = Field(default=Decimal("0.00015"), alias="GATE_MAKER_FEE")
    gate_taker_fee: Decimal = Field(default=Decimal("0.0005"), alias="GATE_TAKER_FEE")
    okx_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="OKX_MAKER_FEE")
    okx_taker_fee: Decimal = Field(default=Decimal("0.0005"), alias="OKX_TAKER_FEE")
    binance_maker_fee: Decimal = Field(default=Decimal("0.0002"), alias="BINANCE_MAKER_FEE")
    binance_taker_fee: Decimal = Field(default=Decimal("0.0004"), alias="BINANCE_TAKER_FEE")
    hyperliquid_maker_fee: Decimal = Field(
        default=Decimal("0.00015"), alias="HYPERLIQUID_MAKER_FEE"
    )
    hyperliquid_taker_fee: Decimal = Field(
        default=Decimal("0.00035"), alias="HYPERLIQUID_TAKER_FEE"
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
    def paper_venue_values(self) -> tuple[str, ...]:
        return tuple(value.strip() for value in self.paper_venues.split(",") if value.strip())

    @property
    def live_venue_values(self) -> tuple[str, ...]:
        return tuple(
            value.strip().lower()
            for value in self.live_venues.split(",")
            if value.strip()
        )

    @property
    def live_allowed_asset_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().upper() for value in self.live_allowed_assets.split(",") if value.strip()
        )

    @property
    def live_allowed_strategy_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().lower()
            for value in self.live_allowed_strategies.split(",")
            if value.strip()
        )

    @property
    def live_reserve_asset_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().upper()
            for value in self.live_reserve_assets.split(",")
            if value.strip()
        )

    def live_credentials(self, venue: str) -> dict[str, str]:
        credentials: dict[str, dict[str, SecretStr]] = {
            "bybit": {"apiKey": self.bybit_api_key, "secret": self.bybit_api_secret},
            "gate": {"apiKey": self.gate_api_key, "secret": self.gate_api_secret},
            "okx": {
                "apiKey": self.okx_api_key,
                "secret": self.okx_api_secret,
                "password": self.okx_api_passphrase,
            },
            "binance": {
                "apiKey": self.binance_api_key,
                "secret": self.binance_api_secret,
            },
            "hyperliquid": {
                "walletAddress": self.hyperliquid_wallet_address,
                "privateKey": self.hyperliquid_private_key,
            },
        }
        return {
            key: value.get_secret_value()
            for key, value in credentials.get(venue, {}).items()
        }

    @property
    def paper_correlation_group_values(self) -> tuple[frozenset[str], ...]:
        return tuple(
            frozenset(
                asset.strip().upper()
                for asset in group.split(",")
                if asset.strip()
            )
            for group in self.paper_correlation_groups.split(";")
            if group.strip()
        )

    @property
    def fee_schedules(self) -> dict[str, tuple[Decimal, Decimal]]:
        return {
            "bybit": (self.bybit_maker_fee, self.bybit_taker_fee),
            "gate": (self.gate_maker_fee, self.gate_taker_fee),
            "okx": (self.okx_maker_fee, self.okx_taker_fee),
            "binance": (self.binance_maker_fee, self.binance_taker_fee),
            "hyperliquid": (self.hyperliquid_maker_fee, self.hyperliquid_taker_fee),
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
        app = raw.get("app", {})
        for yaml_key, field_name in {
            "environment": "app_env",
            "log_level": "log_level",
            "run_mode": "run_mode",
            "market_data_mode": "market_data_mode",
            "execution_mode": "execution_mode",
        }.items():
            if field_name not in settings.model_fields_set and yaml_key in app:
                setattr(settings, field_name, app[yaml_key])
        section_fields = {
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
        }
        for section, fields in section_fields.items():
            values = raw.get(section, {})
            for yaml_key, field_name in fields.items():
                if field_name not in settings.model_fields_set and yaml_key in values:
                    setattr(settings, field_name, values[yaml_key])
        scanner = raw.get("scanner", {})
        scanner_fields = {
            "minimum_net_apr": "scanner_minimum_net_apr",
            "minimum_liquidity_score": "scanner_minimum_liquidity_score",
            "maximum_slippage_percent": "scanner_maximum_slippage_percent",
            "maximum_spread_percent": "scanner_maximum_spread_percent",
            "minimum_funding_samples": "scanner_minimum_funding_samples",
            "minimum_opportunity_duration_seconds": "scanner_minimum_duration_seconds",
            "allow_spot_short": "scanner_allow_spot_short",
            "borrowing_cost_daily": "scanner_borrowing_cost_daily",
        }
        for yaml_key, field_name in scanner_fields.items():
            if field_name not in settings.model_fields_set and yaml_key in scanner:
                setattr(settings, field_name, scanner[yaml_key])
        paper = raw.get("paper_portfolio", {})
        for yaml_key, field_name in {
            "initial_balance_usd": "paper_initial_balance_usd",
            "reserve_percent": "paper_reserve_percent",
            "max_single_opportunity_percent": "paper_max_single_opportunity_percent",
            "max_single_asset_percent": "paper_max_single_asset_percent",
            "max_single_exchange_percent": "paper_max_single_exchange_percent",
            "max_single_strategy_percent": "paper_max_single_strategy_percent",
            "max_correlated_group_percent": "paper_max_correlated_group_percent",
            "legging_move_percent": "paper_legging_move_percent",
            "correlation_groups": "paper_correlation_groups",
            "autotrade": "paper_autotrade",
            "loop_interval_seconds": "paper_loop_interval_seconds",
            "confirmation_seconds": "paper_confirmation_seconds",
            "max_hold_seconds": "paper_max_hold_seconds",
            "position_size_usd": "paper_position_size_usd",
            "max_open_positions": "paper_max_open_positions",
            "settlement_interval_seconds": "paper_settlement_interval_seconds",
            "history_refresh_seconds": "paper_history_refresh_seconds",
            "orderbook_symbol_limit": "paper_orderbook_symbol_limit",
            "market_asset_limit": "paper_market_asset_limit",
            "history_symbol_limit": "paper_history_symbol_limit",
            "market_persist_interval_seconds": "paper_market_persist_interval_seconds",
            "auto_init_database": "paper_auto_init_database",
            "simulation_version": "paper_simulation_version",
            "strategy_profile": "paper_strategy_profile",
            "comparison_enabled": "paper_comparison_enabled",
            "baseline_simulation_version": "paper_baseline_simulation_version",
            "exit_edge_miss_cycles": "paper_exit_edge_miss_cycles",
            "funding_horizon_hours": "paper_funding_horizon_hours",
            "entry_window_hours": "paper_entry_window_hours",
            "min_settlement_cost_coverage": "paper_min_settlement_cost_coverage",
            "max_adverse_basis_percent": "paper_max_adverse_basis_percent",
        }.items():
            if field_name not in settings.model_fields_set and yaml_key in paper:
                setattr(settings, field_name, paper[yaml_key])
        telegram = raw.get("telegram", {})
        for yaml_key, field_name in {
            "enabled": "telegram_enabled",
            "api_base_url": "telegram_api_base_url",
            "timezone": "telegram_timezone",
            "report_hour": "telegram_report_hour",
            "report_minute": "telegram_report_minute",
        }.items():
            if field_name not in settings.model_fields_set and yaml_key in telegram:
                setattr(settings, field_name, telegram[yaml_key])
        _validate_safe_values(settings)
    return settings


def _validate_safe_values(settings: Settings) -> None:
    if settings.run_mode == "paper_test" and settings.execution_mode != "paper":
        raise ValueError("paper_test requires EXECUTION_MODE=paper")
    if settings.run_mode == "paper_test" and settings.market_data_mode not in {
        "mock",
        "live_public",
    }:
        raise ValueError("paper_test requires mock or live_public market data")
    if settings.run_mode == "live":
        if settings.execution_mode != "live":
            raise ValueError("live run mode requires EXECUTION_MODE=live")
        if settings.market_data_mode != "live_public":
            raise ValueError("live run mode requires MARKET_DATA_MODE=live_public")
        if not settings.live_armed:
            raise ValueError("live run mode requires LIVE_ARMED=true")
        if settings.live_trading_confirm != "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS":
            raise ValueError(
                "live run mode requires LIVE_TRADING_CONFIRM="
                "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS"
            )
        supported_venues = {"bybit", "gate", "okx", "binance", "hyperliquid"}
        unknown_venues = set(settings.live_venue_values) - supported_venues
        if unknown_venues:
            raise ValueError(f"unsupported LIVE_VENUES: {sorted(unknown_venues)}")
        if not settings.live_venue_values:
            raise ValueError("LIVE_VENUES must contain at least one venue")
        missing_credentials = [
            venue
            for venue in settings.live_venue_values
            if not settings.live_credentials(venue)
            or not all(settings.live_credentials(venue).values())
        ]
        if missing_credentials:
            raise ValueError(
                "missing live credentials for: " + ",".join(sorted(missing_credentials))
            )
        for venue in settings.live_venue_values:
            for key, value in settings.live_credentials(venue).items():
                if value != value.strip() or any(character in value for character in "\r\n"):
                    raise ValueError(f"invalid whitespace in {venue} credential {key}")
        if "hyperliquid" in settings.live_venue_values:
            wallet = settings.hyperliquid_wallet_address.get_secret_value()
            private_key = settings.hyperliquid_private_key.get_secret_value()
            if not _is_hex_credential(wallet, 40):
                raise ValueError("HYPERLIQUID_WALLET_ADDRESS must be a 20-byte hex address")
            if not _is_hex_credential(private_key, 64):
                raise ValueError("HYPERLIQUID_PRIVATE_KEY must be a 32-byte hex key")
        if not settings.live_allowed_asset_values:
            raise ValueError("LIVE_ALLOWED_ASSETS must contain at least one asset")
        if not settings.live_allowed_strategy_values:
            raise ValueError("LIVE_ALLOWED_STRATEGIES must contain at least one strategy")
        if settings.paper_comparison_enabled:
            raise ValueError("paper comparison cannot run in live mode")
        if settings.telegram_enabled and (
            not settings.telegram_bot_token.get_secret_value()
            or not settings.telegram_chat_id.strip()
        ):
            raise ValueError(
                "live Telegram alerts require TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"
            )
    elif settings.execution_mode == "live":
        raise ValueError("EXECUTION_MODE=live requires RUN_MODE=live")
    if not settings.live_client_order_prefix.isalnum():
        raise ValueError("LIVE_CLIENT_ORDER_PREFIX must be alphanumeric")
    if not 2 <= len(settings.live_client_order_prefix) <= 8:
        raise ValueError("LIVE_CLIENT_ORDER_PREFIX must contain 2 to 8 characters")
    for field_name in (
        "live_default_position_size_usd",
        "live_max_order_notional_usd",
        "live_max_total_notional_usd",
        "live_max_asset_notional_usd",
        "live_max_venue_notional_usd",
        "live_max_strategy_notional_usd",
        "live_max_correlated_notional_usd",
        "live_max_daily_loss_usd",
        "live_min_expected_profit_usd",
        "live_min_venue_equity_usd",
    ):
        if getattr(settings, field_name) <= 0:
            raise ValueError(f"{field_name.upper()} must be positive")
    if settings.live_default_position_size_usd > settings.live_max_order_notional_usd:
        raise ValueError(
            "LIVE_DEFAULT_POSITION_SIZE_USD cannot exceed LIVE_MAX_ORDER_NOTIONAL_USD"
        )
    if settings.live_max_order_notional_usd * 2 > settings.live_max_total_notional_usd:
        raise ValueError(
            "LIVE_MAX_TOTAL_NOTIONAL_USD must cover both legs of one maximum order"
        )
    if not settings.live_reserve_asset_values:
        raise ValueError("LIVE_RESERVE_ASSETS must contain at least one asset")
    if settings.live_max_open_positions <= 0:
        raise ValueError("LIVE_MAX_OPEN_POSITIONS must be positive")
    if not Decimal("0") < settings.live_max_drawdown_percent <= Decimal("0.20"):
        raise ValueError("LIVE_MAX_DRAWDOWN_PERCENT must be between 0 and 0.20")
    if not Decimal("0") < settings.live_max_slippage_percent <= Decimal("0.02"):
        raise ValueError("LIVE_MAX_SLIPPAGE_PERCENT must be between 0 and 0.02")
    if not Decimal("0") < settings.live_max_adverse_basis_percent <= Decimal("0.05"):
        raise ValueError("LIVE_MAX_ADVERSE_BASIS_PERCENT must be between 0 and 0.05")
    if not Decimal("0") <= settings.live_max_hedge_drift_percent <= Decimal("0.01"):
        raise ValueError("LIVE_MAX_HEDGE_DRIFT_PERCENT must be between 0 and 0.01")
    if not Decimal("0") <= settings.live_max_spot_residual_usd <= Decimal("5"):
        raise ValueError("LIVE_MAX_SPOT_RESIDUAL_USD must be between 0 and 5")
    if not Decimal("0") <= settings.live_min_free_balance_percent < Decimal("100"):
        raise ValueError("LIVE_MIN_FREE_BALANCE_PERCENT must be between 0 and 100")
    if settings.live_order_timeout_seconds <= 0:
        raise ValueError("LIVE_ORDER_TIMEOUT_SECONDS must be positive")
    if settings.live_reconciliation_interval_seconds <= 0:
        raise ValueError("LIVE_RECONCILIATION_INTERVAL_SECONDS must be positive")
    if settings.live_loop_interval_seconds <= 0:
        raise ValueError("LIVE_LOOP_INTERVAL_SECONDS must be positive")
    if settings.live_max_hold_seconds <= 0:
        raise ValueError("LIVE_MAX_HOLD_SECONDS must be positive")
    if settings.live_exit_edge_miss_cycles <= 0:
        raise ValueError("LIVE_EXIT_EDGE_MISS_CYCLES must be positive")
    if settings.live_entry_window_hours <= 0:
        raise ValueError("LIVE_ENTRY_WINDOW_HOURS must be positive")
    if settings.live_min_settlement_cost_coverage < 1:
        raise ValueError("LIVE_MIN_SETTLEMENT_COST_COVERAGE must be at least 1")
    if settings.live_settlement_grace_seconds < 0:
        raise ValueError("LIVE_SETTLEMENT_GRACE_SECONDS cannot be negative")
    if settings.live_market_persist_interval_seconds <= 0:
        raise ValueError("LIVE_MARKET_PERSIST_INTERVAL_SECONDS must be positive")
    if settings.live_account_snapshot_interval_seconds <= 0:
        raise ValueError("LIVE_ACCOUNT_SNAPSHOT_INTERVAL_SECONDS must be positive")
    if not 1 <= settings.live_leverage <= 3:
        raise ValueError("LIVE_LEVERAGE must be between 1 and 3")
    if settings.paper_loop_interval_seconds <= 0:
        raise ValueError("PAPER_LOOP_INTERVAL_SECONDS must be positive")
    if settings.paper_settlement_interval_seconds <= 0:
        raise ValueError("PAPER_SETTLEMENT_INTERVAL_SECONDS must be positive")
    if settings.paper_max_hold_seconds <= 0:
        raise ValueError("PAPER_MAX_HOLD_SECONDS must be positive")
    if settings.paper_history_refresh_seconds <= 0:
        raise ValueError("PAPER_HISTORY_REFRESH_SECONDS must be positive")
    if settings.paper_orderbook_symbol_limit <= 0:
        raise ValueError("PAPER_ORDERBOOK_SYMBOL_LIMIT must be positive")
    if settings.paper_market_asset_limit <= 0:
        raise ValueError("PAPER_MARKET_ASSET_LIMIT must be positive")
    if settings.paper_history_symbol_limit <= 0:
        raise ValueError("PAPER_HISTORY_SYMBOL_LIMIT must be positive")
    if settings.paper_market_persist_interval_seconds <= 0:
        raise ValueError("PAPER_MARKET_PERSIST_INTERVAL_SECONDS must be positive")
    if settings.okx_funding_symbol_limit <= 0:
        raise ValueError("OKX_FUNDING_SYMBOL_LIMIT must be positive")
    if not settings.paper_venue_values:
        raise ValueError("PAPER_VENUES must contain at least one venue")
    if settings.paper_position_size_usd <= 0:
        raise ValueError("PAPER_POSITION_SIZE_USD must be positive")
    for field_name in (
        "paper_reserve_percent",
        "paper_max_single_opportunity_percent",
        "paper_max_single_asset_percent",
        "paper_max_single_exchange_percent",
        "paper_max_single_strategy_percent",
        "paper_max_correlated_group_percent",
    ):
        value = getattr(settings, field_name)
        if not Decimal("0") <= value <= Decimal("100"):
            raise ValueError(f"{field_name.upper()} must be between 0 and 100")
    if settings.paper_exit_edge_miss_cycles <= 0:
        raise ValueError("PAPER_EXIT_EDGE_MISS_CYCLES must be positive")
    if not Decimal("0") <= settings.paper_legging_move_percent <= Decimal("0.01"):
        raise ValueError("PAPER_LEGGING_MOVE_PERCENT must be between 0 and 0.01")
    if (
        settings.paper_comparison_enabled
        and settings.paper_baseline_simulation_version == settings.paper_simulation_version
    ):
        raise ValueError("baseline and candidate simulation versions must be distinct")
    if settings.paper_comparison_enabled and settings.paper_strategy_profile != "candidate":
        raise ValueError("shared comparison service must use candidate as the primary profile")
    if settings.paper_funding_horizon_hours <= 0:
        raise ValueError("PAPER_FUNDING_HORIZON_HOURS must be positive")
    if settings.paper_entry_window_hours <= 0:
        raise ValueError("PAPER_ENTRY_WINDOW_HOURS must be positive")
    if settings.paper_min_settlement_cost_coverage < 1:
        raise ValueError("PAPER_MIN_SETTLEMENT_COST_COVERAGE must be at least 1")
    if not Decimal("0") < settings.paper_max_adverse_basis_percent <= Decimal("0.05"):
        raise ValueError("PAPER_MAX_ADVERSE_BASIS_PERCENT must be between 0 and 0.05")
    if settings.scanner_borrowing_cost_daily < 0:
        raise ValueError("SCANNER_BORROWING_COST_DAILY cannot be negative")
    if settings.scanner_allow_spot_short and settings.scanner_borrowing_cost_daily <= 0:
        raise ValueError(
            "SCANNER_BORROWING_COST_DAILY must be positive when spot shorting is enabled"
        )
    if not Decimal("0") <= settings.scanner_maximum_slippage_percent <= Decimal("0.05"):
        raise ValueError("SCANNER_MAXIMUM_SLIPPAGE_PERCENT must be a decimal ratio")
    if not Decimal("0") <= settings.scanner_maximum_spread_percent <= Decimal("0.05"):
        raise ValueError("SCANNER_MAXIMUM_SPREAD_PERCENT must be a decimal ratio")
    if not 0 <= settings.telegram_report_hour <= 23:
        raise ValueError("TELEGRAM_REPORT_HOUR must be between 0 and 23")
    if not 0 <= settings.telegram_report_minute <= 59:
        raise ValueError("TELEGRAM_REPORT_MINUTE must be between 0 and 59")


def _is_hex_credential(value: str, digits: int) -> bool:
    normalized = value[2:] if value.startswith("0x") else value
    if len(normalized) != digits:
        return False
    try:
        int(normalized, 16)
    except ValueError:
        return False
    return True
