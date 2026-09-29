"""Persistence mapping from normalized models to SQLAlchemy records.

Writes are set-based (one statement per chunk) so a cycle never issues one
round trip per instrument or ticker.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import Opportunity

from ..models import (
    BacktestResultRecord,
    BacktestRunRecord,
    ExchangeRecord,
    FundingHistoryRecord,
    FundingSnapshotRecord,
    InstrumentRecord,
    OpportunityRecord,
    OrderBookSnapshotRecord,
    TickerSnapshotRecord,
)

# asyncpg accepts at most 32767 bind parameters per statement.
_CHUNK = 1000


def _chunks(rows: Sequence[dict[str, Any]]) -> Iterable[Sequence[dict[str, Any]]]:
    for start in range(0, len(rows), _CHUNK):
        yield rows[start : start + _CHUNK]


async def upsert_instruments(
    session: AsyncSession, instruments: list[NormalizedInstrument]
) -> None:
    rows = list(
        {
            (item.exchange, item.exchange_symbol, item.instrument_type.value): {
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
            for item in instruments
        }.values()
    )
    for chunk in _chunks(rows):
        statement = pg_insert(InstrumentRecord).values(list(chunk))
        excluded = statement.excluded
        await session.execute(
            statement.on_conflict_do_update(
                constraint="uq_instrument_exchange_symbol_type",
                set_={
                    "canonical_id": excluded.canonical_id,
                    "base_asset": excluded.base_asset,
                    "quote_asset": excluded.quote_asset,
                    "settlement_asset": excluded.settlement_asset,
                    "contract_size": excluded.contract_size,
                    "tick_size": excluded.tick_size,
                    "step_size": excluded.step_size,
                    "min_order_size": excluded.min_order_size,
                    "funding_interval": excluded.funding_interval,
                    "expiry": excluded.expiry,
                    "is_active": excluded.is_active,
                },
            )
        )


async def insert_tickers(session: AsyncSession, tickers: list[Ticker]) -> None:
    rows = [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "instrument_type": item.instrument_type.value,
            "last_price": item.last_price,
            "mark_price": item.mark_price,
            "index_price": item.index_price,
            "best_bid": item.best_bid,
            "best_ask": item.best_ask,
            "volume_24h": item.volume_24h,
            "open_interest": item.open_interest,
            "timestamp": item.timestamp,
        }
        for item in tickers
    ]
    for chunk in _chunks(rows):
        await session.execute(insert(TickerSnapshotRecord), list(chunk))


async def insert_funding_snapshots(session: AsyncSession, snapshots: list[FundingSnapshot]) -> None:
    rows = [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "funding_rate": item.funding_rate,
            "funding_interval_hours": item.funding_interval_hours,
            "next_funding_time": item.next_funding_time,
            "mark_price": item.mark_price,
            "index_price": item.index_price,
            "timestamp": item.timestamp,
        }
        for item in snapshots
    ]
    for chunk in _chunks(rows):
        await session.execute(insert(FundingSnapshotRecord), list(chunk))


async def upsert_funding_history(session: AsyncSession, points: list[FundingHistoryPoint]) -> None:
    rows = list(
        {
            (item.exchange, item.symbol, item.funding_timestamp): {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "funding_rate": item.funding_rate,
                "funding_timestamp": item.funding_timestamp,
                "mark_price": item.mark_price,
            }
            for item in points
        }.values()
    )
    for chunk in _chunks(rows):
        statement = pg_insert(FundingHistoryRecord).values(list(chunk))
        await session.execute(
            statement.on_conflict_do_update(
                constraint="uq_funding_history_event",
                set_={
                    "funding_rate": statement.excluded.funding_rate,
                    "mark_price": statement.excluded.mark_price,
                },
            )
        )


async def insert_orderbooks(session: AsyncSession, books: Iterable[OrderBook]) -> None:
    rows = [
        {
            "exchange": book.exchange,
            "symbol": book.symbol,
            "instrument_type": book.instrument_type.value,
            "timestamp": book.timestamp,
            "sequence": book.sequence,
            "bids": [[str(level.price), str(level.quantity)] for level in book.bids],
            "asks": [[str(level.price), str(level.quantity)] for level in book.asks],
        }
        for book in books
    ]
    for chunk in _chunks(rows):
        await session.execute(insert(OrderBookSnapshotRecord), list(chunk))


async def upsert_exchanges(session: AsyncSession, snapshot: MarketSnapshot) -> None:
    rows = [
        {
            "name": name,
            "enabled": True,
            "status": str(state.status),
            "last_seen_at": state.last_success_at,
            "metadata_json": {"source": "public_read_only", "last_error": state.last_error},
        }
        for name, state in snapshot.venues.items()
    ]
    if not rows:
        return
    statement = pg_insert(ExchangeRecord).values(rows)
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=["name"],
            set_={
                "status": statement.excluded.status,
                "last_seen_at": statement.excluded.last_seen_at,
                "metadata_json": statement.excluded.metadata_json,
            },
        )
    )


async def upsert_opportunities(session: AsyncSession, opportunities: Iterable[Opportunity]) -> None:
    """Upsert scanner output so unused opportunities remain available for research."""

    rows = [
        {
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
        for item in opportunities
    ]
    for chunk in _chunks(rows):
        statement = pg_insert(OpportunityRecord).values(list(chunk))
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=["opportunity_id"],
                set_={
                    "status": statement.excluded.status,
                    "net_apr": statement.excluded.net_apr,
                    "opportunity_score": statement.excluded.opportunity_score,
                    "payload": statement.excluded.payload,
                },
            )
        )


async def save_market_snapshot(
    session: AsyncSession,
    snapshot: MarketSnapshot,
    *,
    instruments: bool = True,
    tickers: bool = True,
    funding: bool = True,
    orderbooks: bool = True,
) -> None:
    """Persist one normalized snapshot in a single transaction."""

    if instruments:
        await upsert_instruments(session, snapshot.instruments)
    if tickers:
        await insert_tickers(session, snapshot.tickers)
    if funding:
        await insert_funding_snapshots(session, snapshot.funding)
    await upsert_exchanges(session, snapshot)
    if snapshot.funding_history:
        await upsert_funding_history(
            session,
            [point for points in snapshot.funding_history.values() for point in points],
        )
    if orderbooks:
        await insert_orderbooks(session, snapshot.orderbooks.values())
    await session.commit()


async def save_opportunities(session: AsyncSession, opportunities: Iterable[Opportunity]) -> None:
    await upsert_opportunities(session, opportunities)
    await session.commit()


async def prune_market_data(session: AsyncSession, older_than: datetime) -> dict[str, int]:
    """Delete high-volume market rows older than the retention horizon."""

    removed: dict[str, int] = {}
    for name, table, column in (
        ("tickers", TickerSnapshotRecord, TickerSnapshotRecord.timestamp),
        ("funding_snapshots", FundingSnapshotRecord, FundingSnapshotRecord.timestamp),
        ("orderbooks", OrderBookSnapshotRecord, OrderBookSnapshotRecord.timestamp),
        ("opportunities", OpportunityRecord, OpportunityRecord.created_at),
    ):
        result = await session.execute(delete(table).where(column < older_than))
        removed[name] = int(getattr(result, "rowcount", 0) or 0)
    await session.commit()
    return removed


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
                "monthly_returns": result.metrics.monthly_returns,
            },
            created_at=finished_at,
        )
    )
    await session.commit()
