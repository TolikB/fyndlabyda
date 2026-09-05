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
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
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
    snapshot_scope: Mapped[str] = mapped_column(String(16), default="legacy", index=True)
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

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
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


class MarketReplayJobRecord(Base):
    """Durable, leased market-replay job state for restart-safe execution."""

    __tablename__ = "market_replay_jobs"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    job_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    stage: Mapped[str] = mapped_column(String(64))
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


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
    __table_args__ = (UniqueConstraint("exchange", "client_order_id", name="uq_live_order_client"),)

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
    __table_args__ = (Index("ix_live_account_snapshot_exchange_time", "exchange", "timestamp"),)

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
    __table_args__ = (UniqueConstraint("exchange", "external_id", name="uq_live_funding_payment"),)

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
        Index(
            "ix_canonical_events_source_sequence",
            "source",
            "native_sequence",
            "sequence_id",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    event_id: Mapped[str] = mapped_column(String(80), nullable=False)
    kind: Mapped[str] = mapped_column(String(40), index=True)
    source: Mapped[str] = mapped_column(String(128), index=True)
    sequence_id: Mapped[str] = mapped_column(String(128))
    native_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    correlation_id: Mapped[str] = mapped_column(String(128), index=True)
    payload_version: Mapped[int] = mapped_column(Integer)
    quality: Mapped[str] = mapped_column(String(24), index=True)
    exchange_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    receive_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    monotonic_ns: Mapped[int] = mapped_column(BigInteger)
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class CanonicalJournalProfileRecord(Base):
    """Immutable boundary describing how subsequent canonical rows were recorded."""

    __tablename__ = "canonical_journal_profiles"
    __table_args__ = (
        UniqueConstraint("boundary_id", name="uq_canonical_journal_profile_boundary"),
        Index(
            "ix_canonical_journal_profiles_event_boundary",
            "after_event_row_id",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    boundary_id: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    after_event_row_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile: Mapped[str] = mapped_column(String(16), index=True)
    high_frequency_events_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    minimum_interval_seconds: Mapped[str] = mapped_column(String(32), nullable=False)
    simulation_versions: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    config_sha256: Mapped[str] = mapped_column(String(64), index=True, nullable=False)


class MultiRegimeDecisionRecord(Base):
    """Durable feature, regime, signal, risk, and hypothetical-plan decision batch."""

    __tablename__ = "multi_regime_decision_batches"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    batch_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    source_event_id: Mapped[str] = mapped_column(String(80), index=True)
    instrument_id: Mapped[str] = mapped_column(String(256), index=True)
    mode: Mapped[str] = mapped_column(String(24), index=True)
    regime: Mapped[str] = mapped_column(String(32), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class MultiRegimePaperCheckpointRecord(Base):
    """Exact durable cursor for deterministic multi-regime paper replay."""

    __tablename__ = "multi_regime_paper_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    event_row_id: Mapped[int] = mapped_column(BigInteger, index=True)
    event_id: Mapped[str] = mapped_column(String(80), index=True)
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    journal_profile_boundary_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("canonical_journal_profiles.boundary_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    journal_profile_config_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AnalyticsReplicationCheckpointRecord(Base):
    """Durable cursor from authoritative PostgreSQL events into ClickHouse."""

    __tablename__ = "analytics_replication_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    last_event_row_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RiskDecisionRecord(Base):
    """Authoritative risk authorization consumed by every execution path."""

    __tablename__ = "risk_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    decision_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    signal_id: Mapped[str] = mapped_column(String(80), index=True)
    approved: Mapped[bool] = mapped_column(Boolean, index=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    approved_risk_usdt: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    approved_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    approved_notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class OMSOrderStateRecord(Base):
    """Latest durable OMS projection; immutable transitions remain in audit/event logs."""

    __tablename__ = "oms_order_states"
    __table_args__ = (
        UniqueConstraint("venue", "client_order_id", name="uq_oms_order_venue_client"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_order_id: Mapped[str] = mapped_column(String(80), index=True)
    simulation_version: Mapped[str] = mapped_column(String(64), default="v1-legacy", index=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(160), nullable=True)
    risk_decision_id: Mapped[str] = mapped_column(
        ForeignKey("risk_decisions.decision_id"), index=True
    )
    signal_id: Mapped[str] = mapped_column(String(80), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    instrument_id: Mapped[str] = mapped_column(String(256), index=True)
    side: Mapped[str] = mapped_column(String(8))
    order_type: Mapped[str] = mapped_column(String(24))
    status: Mapped[str] = mapped_column(String(32), index=True)
    requested_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    filled_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    reduce_only: Mapped[bool] = mapped_column(Boolean)
    version: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class ExecutionFillRecord(Base):
    """Venue fill truth, unique by venue fill ID and linked to the OMS client ID."""

    __tablename__ = "execution_fills"
    __table_args__ = (UniqueConstraint("venue", "fill_id", name="uq_execution_fill_venue_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fill_id: Mapped[str] = mapped_column(String(160), index=True)
    simulation_version: Mapped[str] = mapped_column(String(64), default="v1-legacy", index=True)
    client_order_id: Mapped[str] = mapped_column(String(80), index=True)
    exchange_order_id: Mapped[str] = mapped_column(String(160), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    instrument_id: Mapped[str] = mapped_column(String(256), index=True)
    side: Mapped[str] = mapped_column(String(8))
    price: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    fee_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    fee_asset: Mapped[str] = mapped_column(String(32))
    liquidity_role: Mapped[str] = mapped_column(String(16))
    exchange_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    receive_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PositionStateRecord(Base):
    """Current position projection with realized/unrealized and collateral attribution."""

    __tablename__ = "position_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    simulation_version: Mapped[str] = mapped_column(String(64), default="v1-legacy", index=True)
    strategy_id: Mapped[str] = mapped_column(String(80), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    instrument_id: Mapped[str] = mapped_column(String(256), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    signed_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    entry_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    collateral: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class BalanceStateRecord(Base):
    """Latest authenticated balance projection for continuous reconciliation."""

    __tablename__ = "balance_states"
    __table_args__ = (UniqueConstraint("venue", "asset", name="uq_balance_state_venue_asset"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    total: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    available: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    locked: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    borrowed: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class LedgerTransactionRecord(Base):
    """Immutable hash-chained double-entry transaction header."""

    __tablename__ = "ledger_transactions"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    sequence: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    transaction_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    reference_type: Mapped[str] = mapped_column(String(64), index=True)
    reference_id: Mapped[str] = mapped_column(String(160), index=True)
    description: Mapped[str] = mapped_column(String(512))
    previous_hash: Mapped[str] = mapped_column(String(64))
    transaction_hash: Mapped[str] = mapped_column(String(64), unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class LedgerPostingRecord(Base):
    """One debit-positive posting linked to an immutable ledger transaction."""

    __tablename__ = "ledger_postings"
    __table_args__ = (
        UniqueConstraint(
            "transaction_id",
            "posting_index",
            name="uq_ledger_posting_transaction_index",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    transaction_id: Mapped[str] = mapped_column(
        ForeignKey("ledger_transactions.transaction_id"), index=True
    )
    posting_index: Mapped[int] = mapped_column(Integer)
    account: Mapped[str] = mapped_column(String(256), index=True)
    account_kind: Mapped[str] = mapped_column(String(16), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    venue: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    strategy_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    position_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)


class ReconciliationAuditRecord(Base):
    """Immutable classified local-versus-venue reconciliation result."""

    __tablename__ = "reconciliation_audits"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    sequence: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    run_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    passed: Mapped[bool] = mapped_column(Boolean, index=True)
    critical_count: Mapped[int] = mapped_column(Integer)
    warning_count: Mapped[int] = mapped_column(Integer)
    input_hash: Mapped[str] = mapped_column(String(64))
    previous_hash: Mapped[str] = mapped_column(String(64))
    audit_hash: Mapped[str] = mapped_column(String(64), unique=True)
    issues: Mapped[list[dict[str, Any]]] = mapped_column(JSON)


class WithdrawalStateRecord(Base):
    """Latest disabled-by-default withdrawal authorization and venue state."""

    __tablename__ = "withdrawal_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    client_withdrawal_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    source_venue: Mapped[str] = mapped_column(String(32), index=True)
    destination_id: Mapped[str] = mapped_column(String(80), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    network: Mapped[str] = mapped_column(String(64))
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    amount_usdt: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    maximum_fee_usdt: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    requested_by: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    exchange_withdrawal_id: Mapped[str | None] = mapped_column(String(160), nullable=True)
    transaction_hash: Mapped[str | None] = mapped_column(String(160), nullable=True)
    confirmations: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class ApiIdempotencyRecord(Base):
    """Durable response cache and replay guard for authenticated control writes."""

    __tablename__ = "api_idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "principal_id",
            "idempotency_key",
            name="uq_api_idempotency_principal_key",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    principal_id: Mapped[str] = mapped_column(String(128), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16), index=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    response_headers: Mapped[dict[str, str] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ImmutableAuditRecord(Base):
    """Hash-chained control-plane and operator action audit record."""

    __tablename__ = "immutable_audit_log"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    sequence: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    audit_event_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    actor_id: Mapped[str] = mapped_column(String(128), index=True)
    actor_role: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(128), index=True)
    resource_type: Mapped[str] = mapped_column(String(64), index=True)
    resource_id: Mapped[str] = mapped_column(String(160), index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    outcome: Mapped[str] = mapped_column(String(32), index=True)
    payload_hash: Mapped[str] = mapped_column(String(64))
    previous_hash: Mapped[str] = mapped_column(String(64))
    audit_hash: Mapped[str] = mapped_column(String(64), unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
