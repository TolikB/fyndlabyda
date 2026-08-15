"""SQLAlchemy persistence models for market data, research, and paper trading."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all persisted records."""


class InstrumentRecord(Base):
    __tablename__ = "instruments"
    __table_args__ = (
        UniqueConstraint(
            "exchange",
            "exchange_symbol",
            "instrument_type",
            name="uq_instrument_exchange_symbol_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    exchange_symbol: Mapped[str] = mapped_column(String(128))
    canonical_id: Mapped[str] = mapped_column(String(128), index=True)
    base_asset: Mapped[str] = mapped_column(String(32), index=True)
    quote_asset: Mapped[str] = mapped_column(String(32))
    instrument_type: Mapped[str] = mapped_column(String(16), index=True)
    settlement_asset: Mapped[str | None] = mapped_column(String(32), nullable=True)
    contract_size: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    tick_size: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    step_size: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    min_order_size: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    funding_interval: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expiry: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class TickerSnapshotRecord(Base):
    __tablename__ = "ticker_snapshots"
    __table_args__ = (
        Index("ix_ticker_exchange_symbol_timestamp", "exchange", "symbol", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str] = mapped_column(String(16))
    last_price: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    index_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    volume_24h: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    open_interest: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class FundingSnapshotRecord(Base):
    __tablename__ = "funding_snapshots"
    __table_args__ = (
        Index("ix_funding_exchange_symbol_timestamp", "exchange", "symbol", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    funding_rate: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    funding_interval_hours: Mapped[Decimal] = mapped_column(Numeric(18, 8))
    next_funding_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    index_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class FundingHistoryRecord(Base):
    __tablename__ = "funding_history"
    __table_args__ = (
        UniqueConstraint(
            "exchange", "symbol", "funding_timestamp", name="uq_funding_history_event"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    funding_rate: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    funding_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)


class MarketCandleRecord(Base):
    __tablename__ = "market_candles"
    __table_args__ = (
        UniqueConstraint(
            "exchange",
            "symbol",
            "instrument_type",
            "interval_minutes",
            "open_time",
            name="uq_market_candle_identity",
        ),
        Index(
            "ix_market_candle_lookup",
            "exchange",
            "symbol",
            "instrument_type",
            "open_time",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str] = mapped_column(String(16), index=True)
    interval_minutes: Mapped[int] = mapped_column(Integer)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    open: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    high: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    low: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    close: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    volume: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    is_closed: Mapped[bool] = mapped_column(Boolean, default=True)


class ExchangeRecord(Base):
    """Configured venue and last observed health state."""

    __tablename__ = "exchanges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(24), default="UNKNOWN")
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class OrderBookSnapshotRecord(Base):
    """Depth snapshot retained for conservative execution analysis."""

    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_orderbook_exchange_symbol_timestamp", "exchange", "symbol", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str] = mapped_column(String(16), default="PERPETUAL")
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    bids: Mapped[list[list[str]]] = mapped_column(JSON)
    asks: Mapped[list[list[str]]] = mapped_column(JSON)


class OpportunityRecord(Base):
    """Immutable opportunity history, including opportunities not paper-traded."""

    __tablename__ = "opportunities"
    __table_args__ = (
        Index("ix_opportunities_created_strategy_asset", "created_at", "strategy", "asset"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    opportunity_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    strategy: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    venue_a: Mapped[str] = mapped_column(String(32), index=True)
    venue_b: Mapped[str | None] = mapped_column(String(32), nullable=True)
    gross_edge: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    net_edge: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    net_apr: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    opportunity_score: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    status: Mapped[str] = mapped_column(String(16), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperPositionRecord(Base):
    """Paper position state and full PnL breakdown."""

    __tablename__ = "paper_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    opportunity_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    state: Mapped[str] = mapped_column(String(16), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    capital: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    simulation_version: Mapped[str] = mapped_column(String(32), default="v1-legacy", index=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperFillRecord(Base):
    """Simulated fills; no live exchange order identifiers are stored."""

    __tablename__ = "paper_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fill_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    position_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    side: Mapped[str] = mapped_column(String(8))
    filled_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    fee: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    slippage: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    status: Mapped[str] = mapped_column(String(16))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperFundingPaymentRecord(Base):
    """Actual historical/live funding events applied to a paper position."""

    __tablename__ = "paper_funding_payments"
    __table_args__ = (
        UniqueConstraint(
            "position_id",
            "exchange",
            "symbol",
            "funding_timestamp",
            name="uq_paper_funding_position_exchange_symbol_event",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[str] = mapped_column(String(64), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    funding_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    funding_rate: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))


class PortfolioSnapshotRecord(Base):
    """Point-in-time virtual equity, balances, and PnL totals."""

    __tablename__ = "portfolio_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    simulation_version: Mapped[str] = mapped_column(String(32), default="v1-legacy", index=True)
    equity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    cash: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    locked_capital: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    total_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    funding_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    fees: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    balances: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperRuntimeIncidentRecord(Base):
    """Restart-safe evidence of a paper runner failure or process epoch."""

    __tablename__ = "paper_runtime_incidents"
    __table_args__ = (
        Index(
            "ix_paper_runtime_incident_version_timestamp",
            "simulation_version",
            "occurred_at",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    simulation_version: Mapped[str] = mapped_column(String(32), index=True)
    category: Mapped[str] = mapped_column(String(32), index=True)
    error_type: Mapped[str] = mapped_column(String(128))


class BacktestRunRecord(Base):
    """Reproducible backtest metadata and deterministic configuration hash."""

    __tablename__ = "backtest_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    config_hash: Mapped[str] = mapped_column(String(64))
    dataset_version: Mapped[str] = mapped_column(String(128))
    git_commit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="completed")
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class BacktestResultRecord(Base):
    """Stored metrics and monthly distribution for later API/dashboard use."""

    __tablename__ = "backtest_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON)
    monthly_distribution: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class TelegramDailyReportRecord(Base):
    """Idempotency ledger for one Telegram report per local calendar day."""

    __tablename__ = "telegram_daily_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str] = mapped_column(String(4096))
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class LiveIntentRecord(Base):
    """Durable strategy intent persisted before either real order is submitted."""

    __tablename__ = "live_intents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    intent_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    opportunity_id: Mapped[str] = mapped_column(String(64), index=True)
    strategy: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(32), index=True)
    capital_per_leg: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    failure_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class LiveOrderRecord(Base):
    """Acknowledgements and terminal state for every authenticated order request."""

    __tablename__ = "live_orders"
    __table_args__ = (
        UniqueConstraint("exchange", "client_order_id", name="uq_live_order_client"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    intent_id: Mapped[str] = mapped_column(String(64), index=True)
    position_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    leg: Mapped[str] = mapped_column(String(16))
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    exchange_symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str] = mapped_column(String(16), index=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    client_order_id: Mapped[str] = mapped_column(String(64), index=True)
    side: Mapped[str] = mapped_column(String(8))
    requested_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    filled_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    average_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    fee: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    fee_currency: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    reduce_only: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class LivePositionRecord(Base):
    """Bot-owned two-leg real position used for restart reconciliation."""

    __tablename__ = "live_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    intent_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    opportunity_id: Mapped[str] = mapped_column(String(64), index=True)
    opportunity_key: Mapped[str] = mapped_column(String(512), index=True)
    strategy: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(32), index=True)
    capital_per_leg: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class LiveAccountSnapshotRecord(Base):
    """Venue balance snapshots used for actual equity-delta PnL reporting."""

    __tablename__ = "live_account_snapshots"
    __table_args__ = (
        Index("ix_live_account_snapshot_exchange_time", "exchange", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    free_collateral_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    balances: Mapped[dict[str, Any]] = mapped_column(JSON)


class LiveReconciliationRecord(Base):
    """Immutable evidence for each startup and continuous reconciliation pass."""

    __tablename__ = "live_reconciliations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON)


class LiveDailyReportRecord(Base):
    """Idempotency ledger for actual-account daily Telegram reports."""

    __tablename__ = "live_daily_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str] = mapped_column(String(4096))
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class LiveFundingPaymentRecord(Base):
    """Actual venue funding cashflows from authenticated account history."""

    __tablename__ = "live_funding_payments"
    __table_args__ = (
        UniqueConstraint("exchange", "external_id", name="uq_live_funding_payment"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    external_id: Mapped[str] = mapped_column(String(128), index=True)
    exchange_symbol: Mapped[str] = mapped_column(String(128), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    currency: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class CanonicalEventRecord(Base):
    """Append-only canonical event journal used by runtime and deterministic replay."""

    __tablename__ = "canonical_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_canonical_event_id"),
        Index(
            "ix_canonical_events_replay",
            "exchange_timestamp",
            "monotonic_ns",
            "event_id",
        ),
        Index("ix_canonical_events_source_sequence", "source", "sequence_id"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    event_id: Mapped[str] = mapped_column(String(80), nullable=False)
    kind: Mapped[str] = mapped_column(String(40), index=True)
    source: Mapped[str] = mapped_column(String(128), index=True)
    sequence_id: Mapped[str] = mapped_column(String(128))
    correlation_id: Mapped[str] = mapped_column(String(128), index=True)
    payload_version: Mapped[int] = mapped_column(Integer)
    quality: Mapped[str] = mapped_column(String(24), index=True)
    exchange_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    receive_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    monotonic_ns: Mapped[int] = mapped_column(BigInteger)
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
