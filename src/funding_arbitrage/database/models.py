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

LEGACY_SERIES_ID = "legacy"


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
    """Depth snapshot retained as evidence for simulated fills."""

    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_orderbook_exchange_symbol_timestamp", "exchange", "symbol", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    instrument_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    # Venue update ids and OKX millisecond timestamps exceed 32 bits.
    sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    bids: Mapped[list[list[str]]] = mapped_column(JSON)
    asks: Mapped[list[list[str]]] = mapped_column(JSON)


class OpportunityRecord(Base):
    """Opportunity history, including opportunities not paper-traded."""

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


class PaperSeriesRecord(Base):
    """One independently accounted paper series (candidate, baseline, legacy)."""

    __tablename__ = "paper_series"

    series_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(32), index=True)
    simulator_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    config: Mapped[dict[str, Any]] = mapped_column(JSON)
    initial_balance: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PaperPositionRecord(Base):
    """Paper position state and full PnL breakdown."""

    __tablename__ = "paper_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    series_id: Mapped[str] = mapped_column(String(64), index=True, default=LEGACY_SERIES_ID)
    opportunity_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    strategy: Mapped[str | None] = mapped_column(String(32), nullable=True)
    state: Mapped[str] = mapped_column(String(16), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    capital: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    close_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    exposure: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    booked_pnl: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    funding_pnl: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    fees: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    slippage: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperFillRecord(Base):
    """Simulated fills; no live exchange order identifiers are stored."""

    __tablename__ = "paper_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fill_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    series_id: Mapped[str] = mapped_column(String(64), index=True, default=LEGACY_SERIES_ID)
    position_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    purpose: Mapped[str | None] = mapped_column(String(8), nullable=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    side: Mapped[str] = mapped_column(String(8))
    filled_quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
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
            "funding_timestamp",
            name="uq_paper_funding_position_exchange_event",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[str] = mapped_column(String(64), index=True, default=LEGACY_SERIES_ID)
    position_id: Mapped[str] = mapped_column(String(64), index=True)
    leg_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(128), index=True)
    funding_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    funding_rate: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    rate_source: Mapped[str | None] = mapped_column(String(16), nullable=True)
    price_source: Mapped[str | None] = mapped_column(String(24), nullable=True)


class PaperLedgerEntryRecord(Base):
    """Append-only cash movements; the verifiable source of every paper balance."""

    __tablename__ = "paper_ledger_entries"
    __table_args__ = (
        UniqueConstraint("series_id", "reference", name="uq_paper_ledger_series_reference"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    series_id: Mapped[str] = mapped_column(String(64), index=True)
    position_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    entry_type: Mapped[str] = mapped_column(String(24), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    reference: Mapped[str] = mapped_column(String(160))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class PortfolioSnapshotRecord(Base):
    """Point-in-time virtual equity, balances, and PnL totals of one series."""

    __tablename__ = "portfolio_snapshots"
    __table_args__ = (Index("ix_portfolio_snapshots_series_timestamp", "series_id", "timestamp"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[str] = mapped_column(String(64), default=LEGACY_SERIES_ID)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    equity: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    cash: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    locked_capital: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    unrealized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    total_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    funding_pnl: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    fees: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    slippage: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    exposure: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    open_positions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    invariant_diff: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    balances: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperCycleRecord(Base):
    """One runner cycle; gaps and errors between rows are the data-quality record."""

    __tablename__ = "paper_cycles"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), index=True)
    stage: Mapped[str | None] = mapped_column(String(24), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    venues_ok: Mapped[list[str]] = mapped_column(JSON)
    venues_failed: Mapped[list[str]] = mapped_column(JSON)
    opportunities: Mapped[int] = mapped_column(Integer, default=0)
    books_fetched: Mapped[int] = mapped_column(Integer, default=0)
    autotrade: Mapped[bool] = mapped_column(Boolean, default=False)
    incidents: Mapped[list[str]] = mapped_column(JSON)


class PaperRunnerSessionRecord(Base):
    """Runner process lifetime, for start/stop notices and crash detection."""

    __tablename__ = "paper_runner_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    clean_stop: Mapped[bool] = mapped_column(Boolean, default=False)
    simulator_version: Mapped[str] = mapped_column(String(32))
    release_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    autotrade: Mapped[bool] = mapped_column(Boolean, default=False)
    series: Mapped[list[str]] = mapped_column(JSON)


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
    """Idempotency ledger for one Telegram report per series and local calendar day."""

    __tablename__ = "telegram_daily_reports"
    __table_args__ = (
        UniqueConstraint("series_id", "report_date", name="uq_telegram_series_report_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[str] = mapped_column(String(64), index=True, default=LEGACY_SERIES_ID)
    report_date: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str] = mapped_column(String(4096))
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)
