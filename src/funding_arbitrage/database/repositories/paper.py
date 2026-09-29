"""Transactional persistence and restore of paper series state."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.portfolio.ledger import LedgerEntryType, LedgerTotals
from funding_arbitrage.portfolio.portfolio import AccountSnapshot, PaperAccount
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.services.series import changed_keys

from ..models import (
    PaperCycleRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperLedgerEntryRecord,
    PaperPositionRecord,
    PaperRunnerSessionRecord,
    PaperSeriesRecord,
    PortfolioSnapshotRecord,
)


class SeriesConfigMismatch(RuntimeError):
    """A stored series was started with a different configuration or simulator."""


@dataclass
class CycleRecord:
    started_at: datetime
    finished_at: datetime
    status: str
    venues_ok: list[str] = field(default_factory=list)
    venues_failed: list[str] = field(default_factory=list)
    opportunities: int = 0
    books_fetched: int = 0
    autotrade: bool = False
    incidents: list[str] = field(default_factory=list)
    stage: str | None = None
    error: str | None = None

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)


@dataclass
class RestoredSeries:
    totals: LedgerTotals
    open_positions: list[PaperPosition]
    closed_positions_pnl: Decimal
    closed_slippage: Decimal


# ------------------------------------------------------------------------ series
async def ensure_series(
    session: AsyncSession,
    *,
    series_id: str,
    name: str,
    simulator_version: str,
    config_hash: str,
    config: dict[str, Any],
    initial_balance: Decimal,
    now: datetime,
) -> bool:
    """Create the series row or verify it was started with the same settings.

    Returns True when the series is new.
    """

    record = await session.get(PaperSeriesRecord, series_id)
    if record is None:
        session.add(
            PaperSeriesRecord(
                series_id=series_id,
                name=name,
                simulator_version=simulator_version,
                config_hash=config_hash,
                config=config,
                initial_balance=initial_balance,
                status="active",
                created_at=now,
            )
        )
        return True
    if record.status == "legacy":
        raise SeriesConfigMismatch(f"series '{series_id}' is reserved for legacy data")
    if record.config_hash != config_hash or record.simulator_version != simulator_version:
        changes = changed_keys(record.config, config)[:10]
        raise SeriesConfigMismatch(
            f"series '{series_id}' was started with simulator {record.simulator_version} and "
            f"config {record.config_hash[:12]}; the current settings differ "
            f"(simulator {simulator_version}, config {config_hash[:12]}; changed: "
            f"{', '.join(changes) or 'unknown'}). Give the series a new label to start a "
            "separate statistic instead of mixing results."
        )
    return False


async def load_series_state(session: AsyncSession, series_id: str) -> RestoredSeries:
    totals = LedgerTotals()
    rows = await session.execute(
        select(PaperLedgerEntryRecord.entry_type, func.sum(PaperLedgerEntryRecord.amount))
        .where(PaperLedgerEntryRecord.series_id == series_id)
        .group_by(PaperLedgerEntryRecord.entry_type)
    )
    for entry_type, amount in rows.all():
        value = Decimal(str(amount or 0))
        kind = LedgerEntryType(entry_type)
        if kind in (LedgerEntryType.COLLATERAL_LOCK, LedgerEntryType.COLLATERAL_RELEASE):
            totals.collateral += value
        elif kind is LedgerEntryType.FEE:
            totals.fees += value
        elif kind is LedgerEntryType.FUNDING:
            totals.funding += value
        else:
            totals.realized_price_pnl += value
    open_rows = await session.execute(
        select(PaperPositionRecord.payload)
        .where(
            PaperPositionRecord.series_id == series_id,
            PaperPositionRecord.state == PositionState.OPEN.value,
        )
        .order_by(PaperPositionRecord.id)
    )
    payloads: list[dict[str, Any]] = list(open_rows.scalars().all())
    positions = [PaperPosition.model_validate(payload) for payload in payloads]
    closed = await session.execute(
        select(
            func.coalesce(func.sum(PaperPositionRecord.booked_pnl), 0),
            func.coalesce(func.sum(PaperPositionRecord.slippage), 0),
        ).where(
            PaperPositionRecord.series_id == series_id,
            PaperPositionRecord.state == PositionState.CLOSED.value,
        )
    )
    closed_pnl, closed_slippage = closed.one()
    return RestoredSeries(
        totals=totals,
        open_positions=positions,
        closed_positions_pnl=Decimal(str(closed_pnl)),
        closed_slippage=Decimal(str(closed_slippage)),
    )


# ------------------------------------------------------------------ persistence
def _position_row(position: PaperPosition, now: datetime) -> dict[str, Any]:
    return {
        "position_id": position.id,
        "series_id": position.series_id,
        "opportunity_id": position.opportunity_id,
        "strategy": position.strategy,
        "state": position.state.value,
        "asset": position.asset,
        "capital": position.capital,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "close_reason": position.close_reason,
        "exposure": position.exposure,
        "booked_pnl": position.booked_pnl,
        "funding_pnl": position.funding_pnl,
        "fees": position.fees,
        "slippage": position.slippage,
        "updated_at": now,
        "payload": position.model_dump(mode="json"),
    }


async def persist_account_changes(
    session: AsyncSession, account: PaperAccount, now: datetime
) -> None:
    """Write one account's pending changes; the caller owns the transaction."""

    positions = [_position_row(position, now) for position in account.dirty_positions.values()]
    positions += [
        _position_row(position, now)
        for position in account.positions.values()
        if position.id not in account.dirty_positions
    ]
    if positions:
        statement = pg_insert(PaperPositionRecord).values(positions)
        excluded = statement.excluded
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=["position_id"],
                set_={
                    column: getattr(excluded, column)
                    for column in (
                        "state",
                        "capital",
                        "closed_at",
                        "close_reason",
                        "exposure",
                        "booked_pnl",
                        "funding_pnl",
                        "fees",
                        "slippage",
                        "updated_at",
                        "payload",
                    )
                },
            )
        )
    if account.pending_fills:
        statement = pg_insert(PaperFillRecord).values(
            [
                {
                    "fill_id": fill.fill_id,
                    "series_id": fill.series_id,
                    "position_id": fill.position_id,
                    "purpose": fill.purpose.value,
                    "exchange": fill.exchange,
                    "symbol": fill.symbol,
                    "side": fill.side,
                    "filled_quantity": fill.quantity,
                    "price": fill.price,
                    "notional": fill.notional,
                    "fee": fill.fee,
                    "slippage": fill.slippage,
                    "status": fill.status.value,
                    "timestamp": fill.timestamp,
                    "payload": fill.model_dump(mode="json"),
                }
                for fill in account.pending_fills
            ]
        )
        await session.execute(statement.on_conflict_do_nothing(index_elements=["fill_id"]))
    if account.pending_funding:
        statement = pg_insert(PaperFundingPaymentRecord).values(
            [
                {
                    "series_id": payment.series_id,
                    "position_id": payment.position_id,
                    "leg_index": payment.leg_index,
                    "exchange": payment.exchange,
                    "symbol": payment.symbol,
                    "funding_timestamp": payment.funding_timestamp,
                    "funding_rate": payment.funding_rate,
                    "quantity": payment.quantity,
                    "mark_price": payment.mark_price,
                    "notional": payment.notional,
                    "pnl": payment.amount,
                    "rate_source": payment.rate_source.value,
                    "price_source": payment.price_source.value,
                }
                for payment in account.pending_funding
            ]
        )
        await session.execute(
            statement.on_conflict_do_nothing(constraint="uq_paper_funding_position_exchange_event")
        )
    if account.pending_ledger:
        statement = pg_insert(PaperLedgerEntryRecord).values(
            [
                {
                    "series_id": entry.series_id,
                    "position_id": entry.position_id,
                    "entry_type": entry.entry_type.value,
                    "amount": entry.amount,
                    "reference": entry.reference,
                    "timestamp": entry.timestamp,
                }
                for entry in account.pending_ledger
            ]
        )
        await session.execute(
            statement.on_conflict_do_nothing(constraint="uq_paper_ledger_series_reference")
        )


def snapshot_row(snapshot: AccountSnapshot) -> dict[str, Any]:
    return {
        "series_id": snapshot.series_id,
        "timestamp": snapshot.timestamp,
        "equity": snapshot.equity,
        "cash": snapshot.cash,
        "locked_capital": snapshot.locked_capital,
        "unrealized_pnl": snapshot.unrealized_pnl,
        "realized_pnl": snapshot.realized_pnl,
        "total_pnl": snapshot.total_pnl,
        "funding_pnl": snapshot.funding_pnl,
        "fees": snapshot.fees,
        "slippage": snapshot.slippage,
        "exposure": snapshot.exposure,
        "open_positions": snapshot.open_positions,
        "invariant_diff": snapshot.invariant_diff,
        "balances": {"cash": str(snapshot.cash), "locked": str(snapshot.locked_capital)},
    }


async def insert_account_snapshots(
    session: AsyncSession, snapshots: Iterable[AccountSnapshot]
) -> None:
    rows = [snapshot_row(snapshot) for snapshot in snapshots]
    if rows:
        await session.execute(pg_insert(PortfolioSnapshotRecord).values(rows))


async def insert_cycle(session: AsyncSession, cycle: CycleRecord) -> None:
    session.add(
        PaperCycleRecord(
            started_at=cycle.started_at,
            finished_at=cycle.finished_at,
            duration_ms=cycle.duration_ms,
            status=cycle.status,
            stage=cycle.stage,
            error=(cycle.error or "")[:512] or None,
            venues_ok=cycle.venues_ok,
            venues_failed=cycle.venues_failed,
            opportunities=cycle.opportunities,
            books_fetched=cycle.books_fetched,
            autotrade=cycle.autotrade,
            incidents=cycle.incidents[:50],
        )
    )


# ------------------------------------------------------------------- sessions
async def start_runner_session(
    session: AsyncSession,
    *,
    now: datetime,
    simulator_version: str,
    release_hash: str | None,
    autotrade: bool,
    series: list[str],
) -> tuple[int, PaperRunnerSessionRecord | None]:
    """Open a session row and return it with the previous session, if any."""

    previous = await session.scalar(
        select(PaperRunnerSessionRecord).order_by(PaperRunnerSessionRecord.id.desc()).limit(1)
    )
    record = PaperRunnerSessionRecord(
        started_at=now,
        last_heartbeat_at=now,
        simulator_version=simulator_version,
        release_hash=release_hash,
        autotrade=autotrade,
        series=series,
        clean_stop=False,
    )
    session.add(record)
    await session.flush()
    return record.id, previous


async def heartbeat_runner_session(session: AsyncSession, session_id: int, now: datetime) -> None:
    await session.execute(
        update(PaperRunnerSessionRecord)
        .where(PaperRunnerSessionRecord.id == session_id)
        .values(last_heartbeat_at=now)
    )


async def stop_runner_session(session: AsyncSession, session_id: int, now: datetime) -> None:
    await session.execute(
        update(PaperRunnerSessionRecord)
        .where(PaperRunnerSessionRecord.id == session_id)
        .values(stopped_at=now, clean_stop=True, last_heartbeat_at=now)
    )
