"""Persistence mapping from normalized models to SQLAlchemy records."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    NormalizedInstrument,
    Ticker,
)
from funding_arbitrage.execution.base import PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot
from funding_arbitrage.portfolio.position import PaperPosition

from ..models import (
    BacktestResultRecord,
    BacktestRunRecord,
    ExchangeRecord,
    FundingHistoryRecord,
    FundingSnapshotRecord,
    InstrumentRecord,
    MarketCandleRecord,
    OpportunityRecord,
    OrderBookSnapshotRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PaperRuntimeIncidentRecord,
    PortfolioSnapshotRecord,
    TickerSnapshotRecord,
)


async def save_instruments(session: AsyncSession, instruments: list[NormalizedInstrument]) -> None:
    for item in instruments:
        record = await session.scalar(
            select(InstrumentRecord).where(
                InstrumentRecord.exchange == item.exchange,
                InstrumentRecord.exchange_symbol == item.exchange_symbol,
                InstrumentRecord.instrument_type == item.instrument_type.value,
            )
        )
        values = {
            "exchange": item.exchange,
            "exchange_symbol": item.exchange_symbol,
            "canonical_id": item.canonical_id,
            "base_asset": item.base_asset,
            "quote_asset": item.quote_asset,
            "instrument_type": item.instrument_type.value,
            "settlement_asset": item.settlement_asset,
            "contract_size": item.contract_size,
            "tick_size": item.tick_size,
            "step_size": item.step_size,
            "min_order_size": item.min_order_size,
            "funding_interval": item.funding_interval,
            "expiry": item.expiry,
            "is_active": item.is_active,
        }
        if record is None:
            session.add(InstrumentRecord(**values))
        else:
            for field, value in values.items():
                setattr(record, field, value)
    await session.commit()


async def save_tickers(session: AsyncSession, tickers: list[Ticker]) -> None:
    session.add_all(
        [
            TickerSnapshotRecord(
                exchange=item.exchange,
                symbol=item.symbol,
                instrument_type=item.instrument_type.value,
                last_price=item.last_price,
                mark_price=item.mark_price,
                index_price=item.index_price,
                best_bid=item.best_bid,
                best_ask=item.best_ask,
                volume_24h=item.volume_24h,
                open_interest=item.open_interest,
                timestamp=item.timestamp,
            )
            for item in tickers
        ]
    )
    await session.commit()


async def save_funding_snapshots(session: AsyncSession, snapshots: list[FundingSnapshot]) -> None:
    session.add_all(
        [
            FundingSnapshotRecord(
                exchange=item.exchange,
                symbol=item.symbol,
                funding_rate=item.funding_rate,
                funding_interval_hours=item.funding_interval_hours,
                next_funding_time=item.next_funding_time,
                mark_price=item.mark_price,
                index_price=item.index_price,
                timestamp=item.timestamp,
            )
            for item in snapshots
        ]
    )
    await session.commit()


async def _upsert_funding_history(
    session: AsyncSession, points: list[FundingHistoryPoint]
) -> None:
    deduplicated = {
        (item.exchange, item.symbol, item.funding_timestamp): item for item in points
    }
    rows: list[dict[str, Any]] = [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "funding_rate": item.funding_rate,
            "funding_timestamp": item.funding_timestamp,
            "mark_price": item.mark_price,
        }
        for item in deduplicated.values()
    ]
    if not rows:
        return
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        for index in range(0, len(rows), 1000):
            statement = pg_insert(FundingHistoryRecord).values(rows[index : index + 1000])
            statement = statement.on_conflict_do_update(
                constraint="uq_funding_history_event",
                set_={
                    "funding_rate": statement.excluded.funding_rate,
                    "mark_price": statement.excluded.mark_price,
                },
            )
            await session.execute(statement)
    else:
        for row in rows:
            record = await session.scalar(
                select(FundingHistoryRecord).where(
                    FundingHistoryRecord.exchange == row["exchange"],
                    FundingHistoryRecord.symbol == row["symbol"],
                    FundingHistoryRecord.funding_timestamp == row["funding_timestamp"],
                )
            )
            if record is None:
                session.add(FundingHistoryRecord(**row))
            else:
                record.funding_rate = row["funding_rate"]
                record.mark_price = row["mark_price"]


async def save_funding_history(session: AsyncSession, points: list[FundingHistoryPoint]) -> None:
    await _upsert_funding_history(session, points)
    await session.commit()


async def save_candles(session: AsyncSession, candles: list[Candle]) -> None:
    if not candles:
        return
    rows = [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "instrument_type": item.instrument_type.value,
            "interval_minutes": item.interval_minutes,
            "open_time": item.open_time,
            "close_time": item.close_time,
            "open": item.open,
            "high": item.high,
            "low": item.low,
            "close": item.close,
            "volume": item.volume,
            "is_closed": item.is_closed,
        }
        for item in candles
    ]
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        for index in range(0, len(rows), 1000):
            statement = pg_insert(MarketCandleRecord).values(rows[index : index + 1000])
            statement = statement.on_conflict_do_update(
                constraint="uq_market_candle_identity",
                set_={
                    "close_time": statement.excluded.close_time,
                    "open": statement.excluded.open,
                    "high": statement.excluded.high,
                    "low": statement.excluded.low,
                    "close": statement.excluded.close,
                    "volume": statement.excluded.volume,
                    "is_closed": statement.excluded.is_closed,
                },
            )
            await session.execute(statement)
    else:
        for row in rows:
            record = await session.scalar(
                select(MarketCandleRecord).where(
                    MarketCandleRecord.exchange == row["exchange"],
                    MarketCandleRecord.symbol == row["symbol"],
                    MarketCandleRecord.instrument_type == row["instrument_type"],
                    MarketCandleRecord.interval_minutes == row["interval_minutes"],
                    MarketCandleRecord.open_time == row["open_time"],
                )
            )
            if record is None:
                session.add(MarketCandleRecord(**row))
            else:
                for field, value in row.items():
                    setattr(record, field, value)
    await session.commit()


async def save_market_snapshot(
    session: AsyncSession,
    snapshot: MarketSnapshot,
    *,
    include_history: bool = True,
) -> None:
    """Persist one normalized snapshot, including depth used for cost estimates."""

    await save_instruments(session, snapshot.instruments)
    await save_tickers(session, snapshot.tickers)
    await save_funding_snapshots(session, snapshot.funding)
    for exchange in sorted(
        {
            item.exchange for item in snapshot.instruments + snapshot.tickers + snapshot.funding
        }
    ):
        record = await session.scalar(select(ExchangeRecord).where(ExchangeRecord.name == exchange))
        if record is None:
            session.add(
                ExchangeRecord(
                    name=exchange,
                    enabled=True,
                    status="ONLINE",
                    last_seen_at=snapshot.captured_at,
                    metadata_json={"source": "public_read_only"},
                )
            )
        else:
            record.status = "ONLINE"
            record.last_seen_at = snapshot.captured_at
    if include_history and snapshot.funding_history:
        await save_funding_history(
            session,
            [point for points in snapshot.funding_history.values() for point in points],
        )
    session.add_all(
        [
            OrderBookSnapshotRecord(
                exchange=book.exchange,
                symbol=book.symbol,
                instrument_type=book.instrument_type.value,
                timestamp=book.timestamp,
                sequence=book.sequence,
                bids=[[str(level.price), str(level.quantity)] for level in book.bids],
                asks=[[str(level.price), str(level.quantity)] for level in book.asks],
            )
            for book in snapshot.orderbooks.values()
        ]
    )
    await session.commit()


async def save_opportunities(
    session: AsyncSession, opportunities: Iterable[Opportunity]
) -> None:
    """Upsert scanner output so unused opportunities remain available for research."""

    for item in opportunities:
        record = await session.scalar(
            select(OpportunityRecord).where(OpportunityRecord.opportunity_id == item.id)
        )
        values: dict[str, Any] = {
            "opportunity_id": item.id,
            "strategy": str(item.strategy),
            "asset": item.asset,
            "venue_a": item.venue_a,
            "venue_b": item.venue_b,
            "gross_edge": item.gross_edge,
            "net_edge": item.net_edge,
            "net_apr": item.net_apr,
            "opportunity_score": item.opportunity_score,
            "status": str(item.status),
            "created_at": item.created_at,
            "expires_at": item.expires_at,
            "payload": item.model_dump(mode="json"),
        }
        if record is None:
            session.add(OpportunityRecord(**values))
        else:
            for field, value in values.items():
                setattr(record, field, value)
    await session.commit()


async def save_portfolio_snapshot(session: AsyncSession, snapshot: PortfolioSnapshot) -> None:
    session.add(
        PortfolioSnapshotRecord(
            timestamp=snapshot.timestamp,
            simulation_version=snapshot.simulation_version,
            equity=snapshot.equity,
            cash=snapshot.cash,
            locked_capital=snapshot.locked_capital,
            total_pnl=snapshot.total_pnl,
            funding_pnl=snapshot.funding_pnl,
            fees=snapshot.fees,
            balances={key: str(value) for key, value in snapshot.balances.items()},
        )
    )
    await session.commit()


async def save_paper_runtime_incident(
    session: AsyncSession,
    simulation_versions: Iterable[str],
    category: str,
    error_type: str,
    occurred_at: datetime,
) -> None:
    """Persist one redacted runtime event for every affected paper ledger."""

    for simulation_version in dict.fromkeys(simulation_versions):
        session.add(
            PaperRuntimeIncidentRecord(
                occurred_at=occurred_at,
                simulation_version=simulation_version,
                category=category,
                error_type=error_type[:128],
            )
        )
    await session.commit()


async def save_backtest_result(
    session: AsyncSession,
    run_id: str,
    result: Any,
    started_at: datetime,
    config: object | None = None,
) -> None:
    """Persist reproducibility metadata and metrics for an event-driven run."""

    finished_at = datetime.now(UTC)
    session.add(
        BacktestRunRecord(
            run_id=run_id,
            config_hash=result.config_hash,
            dataset_version=result.dataset_version,
            git_commit=result.git_commit,
            started_at=started_at,
            finished_at=finished_at,
            status="completed",
            config_json=config if isinstance(config, dict) else None,
        )
    )
    session.add(
        BacktestResultRecord(
            run_id=run_id,
            metrics=result.metrics.model_dump(mode="json"),
            monthly_distribution={
                "monthly_returns": result.metrics.model_dump(mode="json")[
                    "monthly_returns"
                ],
            },
            created_at=finished_at,
        )
    )
    await session.commit()


async def save_paper_position(session: AsyncSession, position: PaperPosition) -> None:
    """Upsert paper position state and its complete PnL payload."""

    values: dict[str, Any] = {
        "position_id": position.id,
        "opportunity_id": position.opportunity_id,
        "state": str(position.state),
        "asset": position.asset,
        "capital": position.capital,
        "simulation_version": position.simulation_version,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "payload": position.model_dump(mode="json"),
    }
    record = await session.scalar(
        select(PaperPositionRecord).where(PaperPositionRecord.position_id == position.id)
    )
    if record is None:
        session.add(PaperPositionRecord(**values))
    else:
        for field, value in values.items():
            setattr(record, field, value)
    for fill in (
        position.leg_a,
        position.leg_b,
        position.close_leg_a,
        position.close_leg_b,
    ):
        if fill is not None:
            await save_paper_fill(session, fill, position.id)
    await session.commit()


async def save_paper_fill(
    session: AsyncSession, fill: PaperFill, position_id: str | None = None
) -> None:
    values: dict[str, Any] = {
        "fill_id": fill.fill_id,
        "position_id": position_id,
        "exchange": fill.exchange,
        "symbol": fill.symbol,
        "instrument_type": fill.instrument_type.value if fill.instrument_type else None,
        "side": fill.side,
        "filled_quantity": fill.filled_quantity,
        "price": fill.price,
        "fee": fill.fee,
        "slippage": fill.slippage,
        "status": str(fill.status),
        "timestamp": fill.timestamp,
        "payload": fill.model_dump(mode="json"),
    }
    record = await session.scalar(
        select(PaperFillRecord).where(PaperFillRecord.fill_id == fill.fill_id)
    )
    if record is None:
        session.add(PaperFillRecord(**values))
    else:
        for field, value in values.items():
            setattr(record, field, value)


async def save_paper_funding_payment(
    session: AsyncSession,
    position_id: str,
    funding: FundingSnapshot,
    notional: Decimal,
    pnl: Decimal,
    *,
    history_event: FundingHistoryPoint | None = None,
) -> None:
    # A live paper payment must never exist without its authoritative raw
    # exchange event. Upsert both in the same transaction; mock settlements do
    # not pass a history event and therefore cannot pollute the public ledger.
    if history_event is not None:
        await _upsert_funding_history(session, [history_event])
    session.add(
        PaperFundingPaymentRecord(
            position_id=position_id,
            exchange=funding.exchange,
            symbol=funding.symbol,
            funding_timestamp=funding.timestamp,
            funding_rate=funding.funding_rate,
            notional=notional,
            pnl=pnl,
        )
    )
    await session.commit()
