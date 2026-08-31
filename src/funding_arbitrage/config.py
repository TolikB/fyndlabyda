"""Typed application configuration with environment overrides."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from funding_arbitrage.credential_policy import (
    load_live_credential_policy,
    verify_live_credential_policy,
)
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.domain.modes import ModeContract, mode_contract


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_env: str = Field(default="development", alias="APP_ENV")
    run_mode: Literal["api", "paper_test", "live"] = Field(default="api", alias="RUN_MODE")
    market_data_mode: Literal["live_public", "mock"] = Field(
        default="live_public", alias="MARKET_DATA_MODE"
    )
    execution_mode: Literal["paper", "live"] = Field(default="paper", alias="EXECUTION_MODE")
    trading_mode: TradingMode | None = Field(default=None, alias="TRADING_MODE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+asyncpg://funding:funding@localhost:5432/funding",
        alias="DATABASE_URL",
        repr=False,
    )
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL", repr=False)
    redis_username: str = Field(default="", alias="REDIS_USERNAME")
    redis_password: SecretStr = Field(default=SecretStr(""), alias="REDIS_PASSWORD")
    internal_service_tls_required: bool = Field(
        default=False, alias="INTERNAL_SERVICE_TLS_REQUIRED"
    )
    internal_tls_ca_file: str = Field(
        default="", alias="INTERNAL_TLS_CA_FILE", repr=False
    )
    internal_tls_client_cert_file: str = Field(
        default="", alias="INTERNAL_TLS_CLIENT_CERT_FILE", repr=False
    )
    internal_tls_client_key_file: str = Field(
        default="", alias="INTERNAL_TLS_CLIENT_KEY_FILE", repr=False
    )
    clickhouse_enabled: bool = Field(default=False, alias="CLICKHOUSE_ENABLED")
    clickhouse_url: str = Field(
        default="https://clickhouse:8443", alias="CLICKHOUSE_URL", repr=False
    )
    clickhouse_database: str = Field(
        default="funding_analytics", alias="CLICKHOUSE_DATABASE"
    )
    clickhouse_username: str = Field(
        default="funding_analytics", alias="CLICKHOUSE_USER"
    )
    clickhouse_password: SecretStr = Field(
        default=SecretStr(""), alias="CLICKHOUSE_PASSWORD"
    )
    clickhouse_request_timeout_seconds: float = Field(
        default=10.0, alias="CLICKHOUSE_REQUEST_TIMEOUT_SECONDS"
    )
    clickhouse_replication_batch_size: int = Field(
        default=500, alias="CLICKHOUSE_REPLICATION_BATCH_SIZE"
    )
    clickhouse_replication_poll_seconds: float = Field(
        default=1.0, alias="CLICKHOUSE_REPLICATION_POLL_SECONDS"
    )
    canonical_event_queue_size: int = Field(
        default=50_000, alias="CANONICAL_EVENT_QUEUE_SIZE"
    )
    canonical_event_batch_size: int = Field(
        default=500, alias="CANONICAL_EVENT_BATCH_SIZE"
    )
    canonical_event_flush_interval_seconds: float = Field(
        default=0.10, alias="CANONICAL_EVENT_FLUSH_INTERVAL_SECONDS"
    )
    multi_regime_enabled: bool = Field(
        default=True, alias="MULTI_REGIME_ENABLED"
    )
    multi_regime_assets: str = Field(
        default="BTC,ETH", alias="MULTI_REGIME_ASSETS"
    )
    multi_regime_source_interval_seconds: int = Field(
        default=60, alias="MULTI_REGIME_SOURCE_INTERVAL_SECONDS"
    )
    multi_regime_strategy_interval_seconds: int = Field(
        default=900, alias="MULTI_REGIME_STRATEGY_INTERVAL_SECONDS"
    )
    multi_regime_regime_interval_seconds: int = Field(
        default=3600, alias="MULTI_REGIME_REGIME_INTERVAL_SECONDS"
    )
    multi_regime_stale_after_seconds: int = Field(
        default=5, alias="MULTI_REGIME_STALE_AFTER_SECONDS"
    )
    multi_regime_restore_hours: int = Field(
        default=168, alias="MULTI_REGIME_RESTORE_HOURS"
    )
    multi_regime_estimated_cost_bps: Decimal = Field(
        default=Decimal("5"), alias="MULTI_REGIME_ESTIMATED_COST_BPS"
    )
    multi_regime_paper_execution_enabled: bool = Field(
        default=True, alias="MULTI_REGIME_PAPER_EXECUTION_ENABLED"
    )
    multi_regime_paper_latency_ms: int = Field(
        default=100, alias="MULTI_REGIME_PAPER_LATENCY_MS"
    )
    multi_regime_paper_maximum_participation_rate: Decimal = Field(
        default=Decimal("0.10"),
        alias="MULTI_REGIME_PAPER_MAXIMUM_PARTICIPATION_RATE",
    )
    multi_regime_paper_impact_coefficient_bps: Decimal = Field(
        default=Decimal("10"),
        alias="MULTI_REGIME_PAPER_IMPACT_COEFFICIENT_BPS",
    )
    public_event_symbol_limit_per_profile: int = Field(
        default=3, alias="PUBLIC_EVENT_SYMBOL_LIMIT_PER_PROFILE"
    )
    public_event_rest_interval_seconds: float = Field(
        default=60.0, alias="PUBLIC_EVENT_REST_INTERVAL_SECONDS"
    )
    public_event_reconnect_initial_seconds: float = Field(
        default=1.0, alias="PUBLIC_EVENT_RECONNECT_INITIAL_SECONDS"
    )
    public_event_reconnect_max_seconds: float = Field(
        default=30.0, alias="PUBLIC_EVENT_RECONNECT_MAX_SECONDS"
    )
    public_metadata_refresh_seconds: float = Field(
        default=3600.0, alias="PUBLIC_METADATA_REFRESH_SECONDS"
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
    mexc_base_url: str = Field(default="https://api.mexc.com", alias="MEXC_BASE_URL")
    mexc_futures_base_url: str = Field(
        default="https://api.mexc.com", alias="MEXC_FUTURES_BASE_URL"
    )
    mexc_futures_ws_url: str = Field(
        default="wss://contract.mexc.com/edge", alias="MEXC_FUTURES_WS_URL"
    )
    mexc_spot_ws_url: str = Field(default="wss://wbs-api.mexc.com/ws", alias="MEXC_SPOT_WS_URL")
    kucoin_spot_base_url: str = Field(
        default="https://api.kucoin.com", alias="KUCOIN_SPOT_BASE_URL"
    )
    kucoin_futures_base_url: str = Field(
        default="https://api-futures.kucoin.com", alias="KUCOIN_FUTURES_BASE_URL"
    )
    kucoin_spot_ws_url: str = Field(
        default="wss://ws-api-spot.kucoin.com", alias="KUCOIN_SPOT_WS_URL"
    )
    kucoin_futures_ws_url: str = Field(
        default="wss://ws-api-futures.kucoin.com", alias="KUCOIN_FUTURES_WS_URL"
    )
    htx_spot_base_url: str = Field(default="https://api.huobi.pro", alias="HTX_SPOT_BASE_URL")
    htx_futures_base_url: str = Field(default="https://api.hbdm.com", alias="HTX_FUTURES_BASE_URL")
    htx_spot_ws_url: str = Field(default="wss://api.huobi.pro/ws", alias="HTX_SPOT_WS_URL")
    htx_futures_ws_url: str = Field(
        default="wss://api.hbdm.com/linear-swap-ws", alias="HTX_FUTURES_WS_URL"
    )
    htx_funding_symbol_limit: int = Field(default=30, alias="HTX_FUNDING_SYMBOL_LIMIT")
    live_armed: bool = Field(default=False, alias="LIVE_ARMED")
    live_trading_confirm: str = Field(default="", alias="LIVE_TRADING_CONFIRM")
    live_autotrade: bool = Field(default=False, alias="LIVE_AUTOTRADE")
    live_sandbox: bool = Field(default=False, alias="LIVE_SANDBOX")
    live_venues: str = Field(
        default="bybit,gate,okx,binance,hyperliquid,mexc,kucoin,htx", alias="LIVE_VENUES"
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
    live_max_daily_loss_usd: Decimal = Field(default=Decimal("50"), alias="LIVE_MAX_DAILY_LOSS_USD")
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
    live_order_timeout_seconds: float = Field(default=20.0, alias="LIVE_ORDER_TIMEOUT_SECONDS")
    live_reconciliation_interval_seconds: float = Field(
        default=30.0, alias="LIVE_RECONCILIATION_INTERVAL_SECONDS"
    )
    live_private_stream_reconciliation_max_age_seconds: float = Field(
        default=90.0,
        alias="LIVE_PRIVATE_STREAM_RECONCILIATION_MAX_AGE_SECONDS",
    )
    live_private_stream_reconnect_initial_seconds: float = Field(
        default=1.0,
        alias="LIVE_PRIVATE_STREAM_RECONNECT_INITIAL_SECONDS",
    )
    live_private_stream_reconnect_max_seconds: float = Field(
        default=30.0,
        alias="LIVE_PRIVATE_STREAM_RECONNECT_MAX_SECONDS",
    )
    live_loop_interval_seconds: float = Field(default=10.0, alias="LIVE_LOOP_INTERVAL_SECONDS")
    live_max_hold_seconds: int = Field(default=32400, alias="LIVE_MAX_HOLD_SECONDS")
    live_exit_edge_miss_cycles: int = Field(default=2, alias="LIVE_EXIT_EDGE_MISS_CYCLES")
    live_entry_window_hours: Decimal = Field(default=Decimal("2"), alias="LIVE_ENTRY_WINDOW_HOURS")
    live_min_settlement_cost_coverage: Decimal = Field(
        default=Decimal("2"), alias="LIVE_MIN_SETTLEMENT_COST_COVERAGE"
    )
    live_settlement_grace_seconds: int = Field(default=90, alias="LIVE_SETTLEMENT_GRACE_SECONDS")
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
    live_expected_egress_ip: str = Field(default="", alias="LIVE_EXPECTED_EGRESS_IP")
    live_credential_policy_json: SecretStr = Field(
        default=SecretStr(""), alias="LIVE_CREDENTIAL_POLICY_JSON"
    )
    live_credential_policy_file: str = Field(
        default="", alias="LIVE_CREDENTIAL_POLICY_FILE", repr=False
    )
    live_credential_max_age_days: int = Field(
        default=90, ge=1, le=365, alias="LIVE_CREDENTIAL_MAX_AGE_DAYS"
    )
    live_credential_attestation_max_age_hours: int = Field(
        default=24,
        ge=1,
        le=168,
        alias="LIVE_CREDENTIAL_ATTESTATION_MAX_AGE_HOURS",
    )
    live_liquidate_on_pause: bool = Field(default=True, alias="LIVE_LIQUIDATE_ON_PAUSE")
    bybit_api_key: SecretStr = Field(default=SecretStr(""), alias="BYBIT_API_KEY")
    bybit_api_secret: SecretStr = Field(default=SecretStr(""), alias="BYBIT_API_SECRET")
    gate_api_key: SecretStr = Field(default=SecretStr(""), alias="GATE_API_KEY")
    gate_api_secret: SecretStr = Field(default=SecretStr(""), alias="GATE_API_SECRET")
    okx_api_key: SecretStr = Field(default=SecretStr(""), alias="OKX_API_KEY")
    okx_api_secret: SecretStr = Field(default=SecretStr(""), alias="OKX_API_SECRET")
    okx_api_passphrase: SecretStr = Field(default=SecretStr(""), alias="OKX_API_PASSPHRASE")
    binance_api_key: SecretStr = Field(default=SecretStr(""), alias="BINANCE_API_KEY")
    binance_api_secret: SecretStr = Field(default=SecretStr(""), alias="BINANCE_API_SECRET")
    hyperliquid_wallet_address: SecretStr = Field(
        default=SecretStr(""), alias="HYPERLIQUID_WALLET_ADDRESS"
    )
    hyperliquid_private_key: SecretStr = Field(
        default=SecretStr(""), alias="HYPERLIQUID_PRIVATE_KEY"
    )
    mexc_api_key: SecretStr = Field(default=SecretStr(""), alias="MEXC_API_KEY")
    mexc_api_secret: SecretStr = Field(default=SecretStr(""), alias="MEXC_API_SECRET")
    kucoin_api_key: SecretStr = Field(default=SecretStr(""), alias="KUCOIN_API_KEY")
    kucoin_api_secret: SecretStr = Field(default=SecretStr(""), alias="KUCOIN_API_SECRET")
    kucoin_api_passphrase: SecretStr = Field(default=SecretStr(""), alias="KUCOIN_API_PASSPHRASE")
    htx_api_key: SecretStr = Field(default=SecretStr(""), alias="HTX_API_KEY")
    htx_api_secret: SecretStr = Field(default=SecretStr(""), alias="HTX_API_SECRET")
    market_data_stale_seconds: int = Field(default=30, alias="MARKET_DATA_STALE_SECONDS")
    orderbook_stream_stale_seconds: int = Field(
        default=120,
        gt=0,
        lt=300,
        alias="ORDERBOOK_STREAM_STALE_SECONDS",
    )
    funding_snapshot_stale_seconds: int = Field(
        default=180,
        gt=0,
        lt=300,
        alias="FUNDING_SNAPSHOT_STALE_SECONDS",
    )
    acceptance_collector_enabled: bool = Field(
        default=False, alias="ACCEPTANCE_COLLECTOR_ENABLED"
    )
    acceptance_window_id: str = Field(default="", alias="ACCEPTANCE_WINDOW_ID")
    acceptance_journal_path: str = Field(
        default="", alias="ACCEPTANCE_JOURNAL_PATH"
    )
    acceptance_sample_interval_seconds: int = Field(
        default=240,
        ge=1,
        le=240,
        alias="ACCEPTANCE_SAMPLE_INTERVAL_SECONDS",
    )
    paper_initial_balance_usd: Decimal = Field(
        default=Decimal("15000"), gt=0, alias="PAPER_INITIAL_BALANCE_USD"
    )
    paper_size_grid_usd: str = Field(
        default="50,100,250,500,1000,2500,5000", alias="PAPER_SIZE_GRID_USD"
    )
    paper_max_funding_capital_usd: Decimal = Field(
        default=Decimal("100"), gt=0, alias="PAPER_MAX_FUNDING_CAPITAL_USD"
    )
    paper_minimum_funding_rate: Decimal = Field(
        default=Decimal("0.0002"),
        gt=0,
        le=Decimal("0.01"),
        alias="PAPER_MINIMUM_FUNDING_RATE",
    )
    paper_venues: str = Field(
        default="bybit,gate,okx,binance,hyperliquid,mexc,kucoin,htx", alias="PAPER_VENUES"
    )
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
    paper_autotrade_start_utc: datetime | None = Field(
        default=None, alias="PAPER_AUTOTRADE_START_UTC"
    )
    paper_loop_interval_seconds: float = Field(default=10.0, alias="PAPER_LOOP_INTERVAL_SECONDS")
    paper_confirmation_seconds: int = Field(default=30, alias="PAPER_CONFIRMATION_SECONDS")
    paper_max_hold_seconds: int = Field(default=900, alias="PAPER_MAX_HOLD_SECONDS")
    paper_position_size_usd: Decimal = Field(
        default=Decimal("50"), gt=0, alias="PAPER_POSITION_SIZE_USD"
    )
    paper_max_open_positions: int = Field(default=8, ge=1, alias="PAPER_MAX_OPEN_POSITIONS")
    paper_settlement_interval_seconds: int = Field(
        default=28800, alias="PAPER_SETTLEMENT_INTERVAL_SECONDS"
    )
    paper_history_refresh_seconds: int = Field(default=3600, alias="PAPER_HISTORY_REFRESH_SECONDS")
    paper_orderbook_symbol_limit: int = Field(default=10, alias="PAPER_ORDERBOOK_SYMBOL_LIMIT")
    paper_market_asset_limit: int = Field(default=12, alias="PAPER_MARKET_ASSET_LIMIT")
    paper_history_symbol_limit: int = Field(default=5, alias="PAPER_HISTORY_SYMBOL_LIMIT")
    paper_market_persist_interval_seconds: int = Field(
        default=300, alias="PAPER_MARKET_PERSIST_INTERVAL_SECONDS"
    )
    paper_auto_init_database: bool = Field(default=False, alias="PAPER_AUTO_INIT_DATABASE")
    paper_simulation_version: str = Field(
        default="v34-cost-gated-candidate", alias="PAPER_SIMULATION_VERSION"
    )
    paper_strategy_profile: Literal["baseline", "candidate"] = Field(
        default="candidate", alias="PAPER_STRATEGY_PROFILE"
    )
    paper_comparison_enabled: bool = Field(default=False, alias="PAPER_COMPARISON_ENABLED")
    paper_baseline_simulation_version: str = Field(
        default="v34-cost-gated-baseline", alias="PAPER_BASELINE_SIMULATION_VERSION"
    )
    paper_exit_edge_miss_cycles: int = Field(default=2, alias="PAPER_EXIT_EDGE_MISS_CYCLES")
    paper_funding_horizon_hours: Decimal = Field(
        default=Decimal("24"), alias="PAPER_FUNDING_HORIZON_HOURS"
    )
    paper_funding_reconciliation_window_seconds: int = Field(
        default=7200, alias="PAPER_FUNDING_RECONCILIATION_WINDOW_SECONDS"
    )
    paper_funding_reconciliation_poll_seconds: int = Field(
        default=60, alias="PAPER_FUNDING_RECONCILIATION_POLL_SECONDS"
    )
    paper_funding_reconciliation_max_post_deadline_attempts: int = Field(
        default=5,
        alias="PAPER_FUNDING_RECONCILIATION_MAX_POST_DEADLINE_ATTEMPTS",
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
    backtest_fill_model_enabled: bool = Field(
        default=True, alias="BACKTEST_FILL_MODEL_ENABLED"
    )
    backtest_order_latency_ms: int = Field(
        default=50, ge=0, le=60_000, alias="BACKTEST_ORDER_LATENCY_MS"
    )
    backtest_cancel_latency_ms: int = Field(
        default=50, ge=0, le=60_000, alias="BACKTEST_CANCEL_LATENCY_MS"
    )
    backtest_maximum_participation_rate: Decimal = Field(
        default=Decimal("0.10"), gt=0, le=1, alias="BACKTEST_MAXIMUM_PARTICIPATION_RATE"
    )
    backtest_passive_fill_ratio: Decimal = Field(
        default=Decimal("0.50"), gt=0, le=1, alias="BACKTEST_PASSIVE_FILL_RATIO"
    )
    backtest_impact_coefficient_bps: Decimal = Field(
        default=Decimal("10"), ge=0, alias="BACKTEST_IMPACT_COEFFICIENT_BPS"
    )
    telegram_enabled: bool = Field(default=False, alias="TELEGRAM_ENABLED")
    telegram_bot_token: SecretStr = Field(default=SecretStr(""), alias="TELEGRAM_BOT_TOKEN")
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
    scanner_allow_spot_short: bool = Field(default=False, alias="SCANNER_ALLOW_SPOT_SHORT")
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
    mexc_maker_fee: Decimal = Field(default=Decimal("0.0006"), alias="MEXC_MAKER_FEE")
    mexc_taker_fee: Decimal = Field(default=Decimal("0.0008"), alias="MEXC_TAKER_FEE")
    kucoin_maker_fee: Decimal = Field(default=Decimal("0.001"), alias="KUCOIN_MAKER_FEE")
    kucoin_taker_fee: Decimal = Field(default=Decimal("0.001"), alias="KUCOIN_TAKER_FEE")
    htx_maker_fee: Decimal = Field(default=Decimal("0.002"), alias="HTX_MAKER_FEE")
    htx_taker_fee: Decimal = Field(default=Decimal("0.002"), alias="HTX_TAKER_FEE")
    request_timeout_seconds: float = Field(default=15.0, alias="REQUEST_TIMEOUT_SECONDS")
    rate_limit_requests_per_second: float = Field(
        default=8.0, alias="RATE_LIMIT_REQUESTS_PER_SECOND"
    )
    rate_limit_burst: int = Field(default=8, alias="RATE_LIMIT_BURST")
    control_plane_security_enabled: bool = Field(
        default=False, alias="CONTROL_PLANE_SECURITY_ENABLED"
    )
    control_plane_jwt_secret: SecretStr = Field(
        default=SecretStr(""), alias="CONTROL_PLANE_JWT_SECRET"
    )
    control_plane_jwt_issuer: str = Field(
        default="funding-arbitrage-operator", alias="CONTROL_PLANE_JWT_ISSUER"
    )
    control_plane_jwt_audience: str = Field(
        default="funding-arbitrage-control", alias="CONTROL_PLANE_JWT_AUDIENCE"
    )
    control_plane_mtls_required: bool = Field(
        default=False, alias="CONTROL_PLANE_MTLS_REQUIRED"
    )
    control_plane_mtls_certificate_header_required: bool = Field(
        default=False,
        alias="CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED",
    )
    control_plane_mtls_trusted_proxies: str = Field(
        default="127.0.0.1,::1,testclient",
        alias="CONTROL_PLANE_MTLS_TRUSTED_PROXIES",
    )
    control_plane_mtls_client_fingerprints: str = Field(
        default="", alias="CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS"
    )
    control_plane_rate_limit_per_minute: int = Field(
        default=120, alias="CONTROL_PLANE_RATE_LIMIT_PER_MINUTE"
    )
    control_plane_rate_limit_backend: Literal["memory", "redis"] = Field(
        default="memory", alias="CONTROL_PLANE_RATE_LIMIT_BACKEND"
    )
    control_plane_max_request_bytes: int = Field(
        default=1_048_576,
        ge=1024,
        le=10_485_760,
        alias="CONTROL_PLANE_MAX_REQUEST_BYTES",
    )
    control_plane_idempotency_ttl_seconds: int = Field(
        default=86400, alias="CONTROL_PLANE_IDEMPOTENCY_TTL_SECONDS"
    )

    @model_validator(mode="after")
    def validate_safe_modes(self) -> Settings:
        _validate_safe_values(self)
        return self

    @property
    def effective_trading_mode(self) -> TradingMode:
        if self.trading_mode is not None:
            return self.trading_mode
        return {
            "api": TradingMode.SAFE_MODE,
            "paper_test": TradingMode.PAPER,
            "live": TradingMode.LIVE,
        }[self.run_mode]

    @property
    def mode_contract(self) -> ModeContract:
        return mode_contract(self.effective_trading_mode)

    @property
    def multi_regime_asset_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().upper()
            for value in self.multi_regime_assets.split(",")
            if value.strip()
        )

    @property
    def bybit_category_values(self) -> tuple[str, ...]:
        return tuple(value.strip() for value in self.bybit_categories.split(",") if value.strip())

    @property
    def paper_venue_values(self) -> tuple[str, ...]:
        return tuple(value.strip() for value in self.paper_venues.split(",") if value.strip())

    @property
    def paper_size_grid_values(self) -> tuple[Decimal, ...]:
        raw_values = [value.strip() for value in self.paper_size_grid_usd.split(",")]
        if not raw_values or any(not value for value in raw_values):
            raise ValueError("PAPER_SIZE_GRID_USD must contain positive decimal values")
        try:
            values = tuple(Decimal(value) for value in raw_values)
        except InvalidOperation as exc:
            raise ValueError(
                "PAPER_SIZE_GRID_USD must contain positive decimal values"
            ) from exc
        if any(value <= 0 for value in values):
            raise ValueError("PAPER_SIZE_GRID_USD must contain positive decimal values")
        return tuple(sorted(set(values)))

    @property
    def live_venue_values(self) -> tuple[str, ...]:
        return tuple(
            value.strip().lower() for value in self.live_venues.split(",") if value.strip()
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
            value.strip().upper() for value in self.live_reserve_assets.split(",") if value.strip()
        )

    @property
    def control_plane_mtls_trusted_proxy_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().lower()
            for value in self.control_plane_mtls_trusted_proxies.split(",")
            if value.strip()
        )

    @property
    def control_plane_mtls_client_fingerprint_values(self) -> frozenset[str]:
        return frozenset(
            value.strip().lower().replace(":", "")
            for value in self.control_plane_mtls_client_fingerprints.split(",")
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
            "mexc": {"apiKey": self.mexc_api_key, "secret": self.mexc_api_secret},
            "kucoin": {
                "apiKey": self.kucoin_api_key,
                "secret": self.kucoin_api_secret,
                "password": self.kucoin_api_passphrase,
            },
            "htx": {"apiKey": self.htx_api_key, "secret": self.htx_api_secret},
        }
        return {key: value.get_secret_value() for key, value in credentials.get(venue, {}).items()}

    @property
    def paper_correlation_group_values(self) -> tuple[frozenset[str], ...]:
        return tuple(
            frozenset(asset.strip().upper() for asset in group.split(",") if asset.strip())
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
            "mexc": (self.mexc_maker_fee, self.mexc_taker_fee),
            "kucoin": (self.kucoin_maker_fee, self.kucoin_taker_fee),
            "htx": (self.htx_maker_fee, self.htx_taker_fee),
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process."""

    settings = Settings()
    config_path = Path("config/default.yaml")
    yaml_defaults: dict[str, object] = {}
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
                yaml_defaults[field_name] = app[yaml_key]
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
            "mexc": {
                "base_url": "mexc_base_url",
                "futures_base_url": "mexc_futures_base_url",
                "futures_websocket_url": "mexc_futures_ws_url",
                "spot_websocket_url": "mexc_spot_ws_url",
            },
            "kucoin": {
                "spot_base_url": "kucoin_spot_base_url",
                "futures_base_url": "kucoin_futures_base_url",
                "spot_websocket_url": "kucoin_spot_ws_url",
                "futures_websocket_url": "kucoin_futures_ws_url",
            },
            "htx": {
                "spot_base_url": "htx_spot_base_url",
                "futures_base_url": "htx_futures_base_url",
                "spot_websocket_url": "htx_spot_ws_url",
                "futures_websocket_url": "htx_futures_ws_url",
            },
        }
        for section, fields in section_fields.items():
            values = raw.get(section, {})
            for yaml_key, field_name in fields.items():
                if field_name not in settings.model_fields_set and yaml_key in values:
                    yaml_defaults[field_name] = values[yaml_key]
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
                yaml_defaults[field_name] = scanner[yaml_key]
        paper = raw.get("paper_portfolio", {})
        for yaml_key, field_name in {
            "initial_balance_usd": "paper_initial_balance_usd",
            "size_grid_usd": "paper_size_grid_usd",
            "max_funding_capital_usd": "paper_max_funding_capital_usd",
            "minimum_funding_rate": "paper_minimum_funding_rate",
            "reserve_percent": "paper_reserve_percent",
            "max_single_opportunity_percent": "paper_max_single_opportunity_percent",
            "max_single_asset_percent": "paper_max_single_asset_percent",
            "max_single_exchange_percent": "paper_max_single_exchange_percent",
            "max_single_strategy_percent": "paper_max_single_strategy_percent",
            "max_correlated_group_percent": "paper_max_correlated_group_percent",
            "legging_move_percent": "paper_legging_move_percent",
            "correlation_groups": "paper_correlation_groups",
            "autotrade": "paper_autotrade",
            "autotrade_start_utc": "paper_autotrade_start_utc",
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
            "funding_reconciliation_window_seconds": "paper_funding_reconciliation_window_seconds",
            "funding_reconciliation_poll_seconds": "paper_funding_reconciliation_poll_seconds",
            "funding_reconciliation_max_post_deadline_attempts": (
                "paper_funding_reconciliation_max_post_deadline_attempts"
            ),
            "entry_window_hours": "paper_entry_window_hours",
            "min_settlement_cost_coverage": "paper_min_settlement_cost_coverage",
            "max_adverse_basis_percent": "paper_max_adverse_basis_percent",
        }.items():
            if field_name not in settings.model_fields_set and yaml_key in paper:
                yaml_defaults[field_name] = paper[yaml_key]
        telegram = raw.get("telegram", {})
        for yaml_key, field_name in {
            "enabled": "telegram_enabled",
            "api_base_url": "telegram_api_base_url",
            "timezone": "telegram_timezone",
            "report_hour": "telegram_report_hour",
            "report_minute": "telegram_report_minute",
        }.items():
            if field_name not in settings.model_fields_set and yaml_key in telegram:
                yaml_defaults[field_name] = telegram[yaml_key]
        if yaml_defaults:
            payload = settings.model_dump(by_alias=True)
            for field_name, value in yaml_defaults.items():
                alias = Settings.model_fields[field_name].alias or field_name
                payload[alias] = value
            settings = Settings.model_validate(payload)
        _validate_safe_values(settings)
    return settings


def _validate_safe_values(settings: Settings) -> None:
    mode = settings.effective_trading_mode
    if settings.funding_snapshot_stale_seconds < settings.market_data_stale_seconds:
        raise ValueError(
            "FUNDING_SNAPSHOT_STALE_SECONDS cannot be lower than "
            "MARKET_DATA_STALE_SECONDS"
        )
    allowed_modes = {
        "api": {TradingMode.BACKTEST, TradingMode.REPLAY, TradingMode.SAFE_MODE},
        "paper_test": {TradingMode.SHADOW, TradingMode.PAPER, TradingMode.SAFE_MODE},
        "live": {TradingMode.LIMITED_LIVE, TradingMode.LIVE},
    }
    if mode not in allowed_modes[settings.run_mode]:
        raise ValueError(
            f"TRADING_MODE={mode.value} is incompatible with RUN_MODE={settings.run_mode}"
        )
    if mode in {
        TradingMode.BACKTEST,
        TradingMode.REPLAY,
        TradingMode.SHADOW,
        TradingMode.SAFE_MODE,
    } and (settings.paper_autotrade or settings.live_autotrade):
        raise ValueError(f"TRADING_MODE={mode.value} forbids autotrade")
    if settings.clickhouse_enabled:
        if not settings.internal_service_tls_required:
            raise ValueError("CLICKHOUSE_ENABLED requires internal mTLS")
        if not settings.clickhouse_url.lower().startswith("https://"):
            raise ValueError("CLICKHOUSE_URL must use HTTPS")
        if (
            not settings.clickhouse_username.strip()
            or not settings.clickhouse_password.get_secret_value()
        ):
            raise ValueError("ClickHouse credentials are required when analytics is enabled")
        if settings.clickhouse_request_timeout_seconds <= 0:
            raise ValueError("CLICKHOUSE_REQUEST_TIMEOUT_SECONDS must be positive")
        if settings.clickhouse_replication_batch_size <= 0:
            raise ValueError("CLICKHOUSE_REPLICATION_BATCH_SIZE must be positive")
        if settings.clickhouse_replication_poll_seconds <= 0:
            raise ValueError("CLICKHOUSE_REPLICATION_POLL_SECONDS must be positive")
    if settings.canonical_event_queue_size <= 0:
        raise ValueError("CANONICAL_EVENT_QUEUE_SIZE must be positive")
    if not 0 < settings.canonical_event_batch_size <= settings.canonical_event_queue_size:
        raise ValueError(
            "CANONICAL_EVENT_BATCH_SIZE must be positive and not exceed queue size"
        )
    if settings.canonical_event_flush_interval_seconds <= 0:
        raise ValueError("CANONICAL_EVENT_FLUSH_INTERVAL_SECONDS must be positive")
    if settings.multi_regime_enabled:
        if not settings.multi_regime_asset_values:
            raise ValueError("MULTI_REGIME_ASSETS cannot be empty")
        intervals = (
            settings.multi_regime_source_interval_seconds,
            settings.multi_regime_strategy_interval_seconds,
            settings.multi_regime_regime_interval_seconds,
        )
        if any(value <= 0 for value in intervals):
            raise ValueError("multi-regime intervals must be positive")
        if (
            settings.multi_regime_strategy_interval_seconds
            % settings.multi_regime_source_interval_seconds
            != 0
            or settings.multi_regime_regime_interval_seconds
            % settings.multi_regime_source_interval_seconds
            != 0
        ):
            raise ValueError(
                "multi-regime strategy/regime intervals must be source multiples"
            )
        if (
            settings.multi_regime_strategy_interval_seconds
            > settings.multi_regime_regime_interval_seconds
        ):
            raise ValueError(
                "MULTI_REGIME_STRATEGY_INTERVAL_SECONDS cannot exceed regime interval"
            )
        if settings.multi_regime_stale_after_seconds <= 0:
            raise ValueError("MULTI_REGIME_STALE_AFTER_SECONDS must be positive")
        if settings.multi_regime_restore_hours <= 0:
            raise ValueError("MULTI_REGIME_RESTORE_HOURS must be positive")
        if settings.multi_regime_estimated_cost_bps < 0:
            raise ValueError("MULTI_REGIME_ESTIMATED_COST_BPS cannot be negative")
        if settings.multi_regime_paper_latency_ms < 0:
            raise ValueError("MULTI_REGIME_PAPER_LATENCY_MS cannot be negative")
        if not (
            Decimal("0")
            < settings.multi_regime_paper_maximum_participation_rate
            <= Decimal("1")
        ):
            raise ValueError(
                "MULTI_REGIME_PAPER_MAXIMUM_PARTICIPATION_RATE must be in (0, 1]"
            )
        if settings.multi_regime_paper_impact_coefficient_bps < 0:
            raise ValueError(
                "MULTI_REGIME_PAPER_IMPACT_COEFFICIENT_BPS cannot be negative"
            )
    if settings.public_event_symbol_limit_per_profile <= 0:
        raise ValueError("PUBLIC_EVENT_SYMBOL_LIMIT_PER_PROFILE must be positive")
    if settings.public_event_rest_interval_seconds <= 0:
        raise ValueError("PUBLIC_EVENT_REST_INTERVAL_SECONDS must be positive")
    if settings.public_metadata_refresh_seconds <= 0:
        raise ValueError("PUBLIC_METADATA_REFRESH_SECONDS must be positive")
    if not (
        0 < settings.public_event_reconnect_initial_seconds
        <= settings.public_event_reconnect_max_seconds
    ):
        raise ValueError("public event reconnect bounds are invalid")
    if (
        settings.live_private_stream_reconciliation_max_age_seconds
        <= settings.live_reconciliation_interval_seconds
    ):
        raise ValueError(
            "LIVE_PRIVATE_STREAM_RECONCILIATION_MAX_AGE_SECONDS must exceed "
            "LIVE_RECONCILIATION_INTERVAL_SECONDS"
        )
    if not (
        0 < settings.live_private_stream_reconnect_initial_seconds
        <= settings.live_private_stream_reconnect_max_seconds
    ):
        raise ValueError("live private stream reconnect bounds are invalid")
    size_grid = settings.paper_size_grid_values
    if settings.paper_max_funding_capital_usd > settings.paper_initial_balance_usd:
        raise ValueError(
            "PAPER_MAX_FUNDING_CAPITAL_USD cannot exceed PAPER_INITIAL_BALANCE_USD"
        )
    if size_grid[0] * Decimal("2") > settings.paper_max_funding_capital_usd:
        raise ValueError(
            "PAPER_SIZE_GRID_USD must include a two-leg size within "
            "PAPER_MAX_FUNDING_CAPITAL_USD"
        )
    if settings.run_mode == "paper_test" and settings.execution_mode != "paper":
        raise ValueError("paper_test requires EXECUTION_MODE=paper")
    if settings.run_mode == "paper_test" and settings.market_data_mode not in {
        "mock",
        "live_public",
    }:
        raise ValueError("paper_test requires mock or live_public market data")
    if settings.run_mode == "live":
        if settings.app_env != "production":
            raise ValueError("live run mode requires APP_ENV=production")
        if not settings.database_url.startswith("postgresql+asyncpg://"):
            raise ValueError("live run mode requires PostgreSQL via postgresql+asyncpg")
        database_password = urlsplit(settings.database_url).password or ""
        if len(database_password) < 24 or database_password.upper().startswith(
            ("CHANGE_ME", "REPLACE_ME")
        ):
            raise ValueError("live PostgreSQL requires a strong authenticated DATABASE_URL")
        if not settings.internal_service_tls_required:
            raise ValueError("live run mode requires INTERNAL_SERVICE_TLS_REQUIRED=true")
        if not settings.redis_url.startswith("rediss://"):
            raise ValueError("live run mode requires Redis TLS via rediss")
        redis_password = settings.redis_password.get_secret_value()
        if (
            not settings.redis_username.strip()
            or len(redis_password) < 32
            or redis_password.upper().startswith(("CHANGE_ME", "REPLACE_ME"))
        ):
            raise ValueError(
                "live Redis requires authenticated REDIS_USERNAME and a strong REDIS_PASSWORD"
            )
        if any(
            not value.strip()
            for value in (
                settings.internal_tls_ca_file,
                settings.internal_tls_client_cert_file,
                settings.internal_tls_client_key_file,
            )
        ):
            raise ValueError("live internal service TLS certificate paths are incomplete")
        if settings.execution_mode != "live":
            raise ValueError("live run mode requires EXECUTION_MODE=live")
        if settings.market_data_mode != "live_public":
            raise ValueError("live run mode requires MARKET_DATA_MODE=live_public")
        if not settings.control_plane_security_enabled:
            raise ValueError("live run mode requires CONTROL_PLANE_SECURITY_ENABLED=true")
        jwt_secret = settings.control_plane_jwt_secret.get_secret_value()
        if len(jwt_secret) < 32 or jwt_secret.strip().upper().startswith(
            ("CHANGE_ME", "REPLACE_ME")
        ):
            raise ValueError("live run mode requires a 32-byte CONTROL_PLANE_JWT_SECRET")
        if not settings.control_plane_jwt_issuer.strip():
            raise ValueError("CONTROL_PLANE_JWT_ISSUER cannot be empty")
        if not settings.control_plane_jwt_audience.strip():
            raise ValueError("CONTROL_PLANE_JWT_AUDIENCE cannot be empty")
        if not settings.control_plane_mtls_required:
            raise ValueError("live run mode requires CONTROL_PLANE_MTLS_REQUIRED=true")
        if not settings.control_plane_mtls_certificate_header_required:
            raise ValueError(
                "live run mode requires CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED=true"
            )
        if settings.control_plane_rate_limit_backend != "redis":
            raise ValueError("live run mode requires CONTROL_PLANE_RATE_LIMIT_BACKEND=redis")
        if not settings.control_plane_mtls_trusted_proxy_values:
            raise ValueError("CONTROL_PLANE_MTLS_TRUSTED_PROXIES cannot be empty")
        if not settings.control_plane_mtls_client_fingerprint_values or any(
            not _is_hex_credential(value, 64)
            for value in settings.control_plane_mtls_client_fingerprint_values
        ):
            raise ValueError(
                "live run mode requires valid CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS"
            )
        if not settings.live_armed:
            raise ValueError("live run mode requires LIVE_ARMED=true")
        if settings.live_trading_confirm != "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS":
            raise ValueError(
                "live run mode requires LIVE_TRADING_CONFIRM=I_UNDERSTAND_THIS_SENDS_REAL_ORDERS"
            )
        supported_venues = {
            "bybit",
            "gate",
            "okx",
            "binance",
            "hyperliquid",
            "mexc",
            "kucoin",
            "htx",
        }
        unknown_venues = set(settings.live_venue_values) - supported_venues
        if unknown_venues:
            raise ValueError(f"unsupported LIVE_VENUES: {sorted(unknown_venues)}")
        if not settings.live_venue_values:
            raise ValueError("LIVE_VENUES must contain at least one venue")
        unsupported_sandbox_venues = sorted(
            set(settings.live_venue_values) & {"mexc", "kucoin", "htx"}
        )
        if settings.live_sandbox and unsupported_sandbox_venues:
            raise ValueError(
                "live sandbox is not supported for: "
                + ",".join(unsupported_sandbox_venues)
                + "; use paper_test with live_public data"
            )
        if not settings.live_require_dedicated_accounts:
            raise ValueError("live run mode requires dedicated exchange accounts")
        if settings.live_margin_mode != "isolated":
            raise ValueError("live run mode requires isolated margin")
        if not settings.telegram_enabled:
            raise ValueError("live run mode requires Telegram safety alerts")
        _require_official_https_url(
            settings.telegram_api_base_url,
            host="api.telegram.org",
            label="TELEGRAM_API_BASE_URL",
        )
        official_market_endpoints: dict[str, tuple[tuple[str, str, str], ...]] = {
            "bybit": (
                (settings.bybit_base_url, "https://api.bybit.com", "BYBIT_BASE_URL"),
                (
                    settings.bybit_ws_url,
                    "wss://stream.bybit.com/v5/public/linear",
                    "BYBIT_WS_URL",
                ),
            ),
            "gate": (
                (
                    settings.gate_base_url,
                    "https://api.gateio.ws/api/v4",
                    "GATE_BASE_URL",
                ),
                (
                    settings.gate_ws_url,
                    "wss://fx-ws.gateio.ws/v4/ws/usdt",
                    "GATE_WS_URL",
                ),
            ),
            "okx": (
                (settings.okx_base_url, "https://www.okx.com", "OKX_BASE_URL"),
                (
                    settings.okx_ws_url,
                    "wss://ws.okx.com:8443/ws/v5/public",
                    "OKX_WS_URL",
                ),
            ),
            "binance": (
                (
                    settings.binance_spot_base_url,
                    "https://api.binance.com",
                    "BINANCE_SPOT_BASE_URL",
                ),
                (
                    settings.binance_futures_base_url,
                    "https://fapi.binance.com",
                    "BINANCE_FUTURES_BASE_URL",
                ),
                (
                    settings.binance_ws_url,
                    "wss://fstream.binance.com/ws",
                    "BINANCE_WS_URL",
                ),
            ),
            "hyperliquid": (
                (
                    settings.hyperliquid_base_url,
                    "https://api.hyperliquid.xyz",
                    "HYPERLIQUID_BASE_URL",
                ),
                (
                    settings.hyperliquid_ws_url,
                    "wss://api.hyperliquid.xyz/ws",
                    "HYPERLIQUID_WS_URL",
                ),
            ),
            "mexc": (
                (settings.mexc_base_url, "https://api.mexc.com", "MEXC_BASE_URL"),
                (
                    settings.mexc_futures_base_url,
                    "https://api.mexc.com",
                    "MEXC_FUTURES_BASE_URL",
                ),
                (
                    settings.mexc_futures_ws_url,
                    "wss://contract.mexc.com/edge",
                    "MEXC_FUTURES_WS_URL",
                ),
                (
                    settings.mexc_spot_ws_url,
                    "wss://wbs-api.mexc.com/ws",
                    "MEXC_SPOT_WS_URL",
                ),
            ),
            "kucoin": (
                (
                    settings.kucoin_spot_base_url,
                    "https://api.kucoin.com",
                    "KUCOIN_SPOT_BASE_URL",
                ),
                (
                    settings.kucoin_futures_base_url,
                    "https://api-futures.kucoin.com",
                    "KUCOIN_FUTURES_BASE_URL",
                ),
                (
                    settings.kucoin_spot_ws_url,
                    "wss://ws-api-spot.kucoin.com",
                    "KUCOIN_SPOT_WS_URL",
                ),
                (
                    settings.kucoin_futures_ws_url,
                    "wss://ws-api-futures.kucoin.com",
                    "KUCOIN_FUTURES_WS_URL",
                ),
            ),
            "htx": (
                (
                    settings.htx_spot_base_url,
                    "https://api.huobi.pro",
                    "HTX_SPOT_BASE_URL",
                ),
                (
                    settings.htx_futures_base_url,
                    "https://api.hbdm.com",
                    "HTX_FUTURES_BASE_URL",
                ),
                (
                    settings.htx_spot_ws_url,
                    "wss://api.huobi.pro/ws",
                    "HTX_SPOT_WS_URL",
                ),
                (
                    settings.htx_futures_ws_url,
                    "wss://api.hbdm.com/linear-swap-ws",
                    "HTX_FUTURES_WS_URL",
                ),
            ),
        }
        for venue in settings.live_venue_values:
            for value, expected, label in official_market_endpoints[venue]:
                _require_exact_endpoint(value, expected=expected, label=label)
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
        credential_identifiers = {
            venue: (
                settings.live_credentials(venue).get("walletAddress", "")
                if venue == "hyperliquid"
                else settings.live_credentials(venue).get("apiKey", "")
            )
            for venue in settings.live_venue_values
        }
        credential_policy = load_live_credential_policy(
            policy_json=settings.live_credential_policy_json.get_secret_value(),
            policy_file=settings.live_credential_policy_file,
        )
        verify_live_credential_policy(
            credential_policy,
            venues=settings.live_venue_values,
            credential_identifiers=credential_identifiers,
            expected_egress_ip=settings.live_expected_egress_ip,
            maximum_age_days=settings.live_credential_max_age_days,
            maximum_attestation_age_hours=(
                settings.live_credential_attestation_max_age_hours
            ),
        )
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
        if (
            not settings.telegram_bot_token.get_secret_value()
            or not settings.telegram_chat_id.strip()
        ):
            raise ValueError("live Telegram alerts require TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
    elif settings.execution_mode == "live":
        raise ValueError("EXECUTION_MODE=live requires RUN_MODE=live")
    if not settings.live_client_order_prefix.isalnum():
        raise ValueError("LIVE_CLIENT_ORDER_PREFIX must be alphanumeric")
    if not 2 <= len(settings.live_client_order_prefix) <= 8:
        raise ValueError("LIVE_CLIENT_ORDER_PREFIX must contain 2 to 8 characters")
    if settings.control_plane_rate_limit_per_minute <= 0:
        raise ValueError("CONTROL_PLANE_RATE_LIMIT_PER_MINUTE must be positive")
    if settings.control_plane_idempotency_ttl_seconds < 60:
        raise ValueError("CONTROL_PLANE_IDEMPOTENCY_TTL_SECONDS must be at least 60")
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
        raise ValueError("LIVE_DEFAULT_POSITION_SIZE_USD cannot exceed LIVE_MAX_ORDER_NOTIONAL_USD")
    if settings.live_max_order_notional_usd * 2 > settings.live_max_total_notional_usd:
        raise ValueError("LIVE_MAX_TOTAL_NOTIONAL_USD must cover both legs of one maximum order")
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
    if (
        settings.paper_autotrade_start_utc is not None
        and settings.paper_autotrade_start_utc.utcoffset() is None
    ):
        raise ValueError("PAPER_AUTOTRADE_START_UTC must include a timezone")
    if settings.paper_autotrade_start_utc is not None:
        settings.paper_autotrade_start_utc = settings.paper_autotrade_start_utc.astimezone(UTC)
    if not settings.paper_simulation_version.strip():
        raise ValueError("PAPER_SIMULATION_VERSION must not be blank")
    if not settings.paper_baseline_simulation_version.strip():
        raise ValueError("PAPER_BASELINE_SIMULATION_VERSION must not be blank")
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
    if settings.htx_funding_symbol_limit <= 0:
        raise ValueError("HTX_FUNDING_SYMBOL_LIMIT must be positive")
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
    if not 0 < settings.paper_funding_reconciliation_window_seconds <= 21_600:
        raise ValueError(
            "PAPER_FUNDING_RECONCILIATION_WINDOW_SECONDS must be between 1 and 21600"
        )
    if not (
        5
        <= settings.paper_funding_reconciliation_poll_seconds
        <= settings.paper_funding_reconciliation_window_seconds
    ):
        raise ValueError(
            "PAPER_FUNDING_RECONCILIATION_POLL_SECONDS must be between 5 and "
            "the reconciliation window"
        )
    if not 1 <= settings.paper_funding_reconciliation_max_post_deadline_attempts <= 60:
        raise ValueError(
            "PAPER_FUNDING_RECONCILIATION_MAX_POST_DEADLINE_ATTEMPTS must be "
            "between 1 and 60"
        )
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
    if settings.acceptance_collector_enabled:
        acceptance_venues = (
            "binance",
            "bybit",
            "gate",
            "htx",
            "hyperliquid",
            "kucoin",
            "mexc",
            "okx",
        )
        if settings.run_mode != "paper_test":
            raise ValueError("ACCEPTANCE_COLLECTOR_ENABLED requires RUN_MODE=paper_test")
        if mode not in {TradingMode.SHADOW, TradingMode.PAPER}:
            raise ValueError("acceptance collection requires SHADOW or PAPER mode")
        if settings.market_data_mode != "live_public":
            raise ValueError("acceptance collection requires live_public market data")
        if settings.execution_mode != "paper":
            raise ValueError("acceptance collection forbids live execution")
        if tuple(sorted(settings.paper_venue_values)) != acceptance_venues:
            raise ValueError("acceptance collection requires the exact eight-venue set")
        if settings.paper_loop_interval_seconds > 10:
            raise ValueError("acceptance collection requires a cycle interval of at most 10s")
        if settings.paper_comparison_enabled:
            raise ValueError("acceptance collection requires one isolated paper namespace")
        if settings.paper_auto_init_database:
            raise ValueError("acceptance collection requires a migrated dedicated database")
        if not settings.acceptance_window_id.strip():
            raise ValueError("ACCEPTANCE_WINDOW_ID is required for acceptance collection")
        if re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
            settings.acceptance_window_id,
        ) is None:
            raise ValueError("ACCEPTANCE_WINDOW_ID has an invalid format")
        if not settings.acceptance_journal_path.strip() or not Path(
            settings.acceptance_journal_path
        ).is_absolute():
            raise ValueError("ACCEPTANCE_JOURNAL_PATH must be an absolute path")
        if mode is TradingMode.PAPER and not settings.paper_autotrade:
            raise ValueError("PAPER acceptance collection requires PAPER_AUTOTRADE=true")
        if mode is TradingMode.PAPER and (
            not settings.telegram_enabled
            or not settings.telegram_bot_token.get_secret_value()
            or not settings.telegram_chat_id.strip()
        ):
            raise ValueError("PAPER acceptance collection requires daily Telegram reporting")
        if any(
            value
            for venue in acceptance_venues
            for value in settings.live_credentials(venue).values()
        ):
            raise ValueError("acceptance collection forbids private exchange credentials")
        if (
            settings.live_armed
            or settings.live_autotrade
            or settings.live_trading_confirm.strip()
        ):
            raise ValueError("acceptance collection forbids live-trading authorization")
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


def _require_official_https_url(value: str, *, host: str, label: str) -> None:
    _require_exact_endpoint(value, expected=f"https://{host}", label=label)


def _require_exact_endpoint(value: str, *, expected: str, label: str) -> None:
    if value.rstrip("/") != expected.rstrip("/"):
        raise ValueError(f"{label} must be the official {expected} endpoint")
