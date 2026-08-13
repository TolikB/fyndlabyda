"""Deterministic replay dataset built from durable paper-trading ledgers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.backtest.events import (
    BacktestEvent,
    FillEvent,
    FundingEvent,
    OpportunityEvent,
    PositionEvent,
)
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PaperRuntimeIncidentRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.execution.base import PaperFill
from funding_arbitrage.portfolio.position import PaperPosition


@dataclass(frozen=True)
class PaperReplayDataset:
    events: list[BacktestEvent]
    dataset_version: str
    attribution: dict[str, dict[str, dict[str, Decimal]]]
    position_count: int
    observation_start: datetime | None = None
    observation_end: datetime | None = None
    snapshot_timestamps: tuple[datetime, ...] = ()
    max_snapshot_gap_seconds: Decimal = Decimal("0")
    max_accounting_invariant_error: Decimal = Decimal("0")
    snapshot_pnl_delta: Decimal | None = None
    runtime_incident_count: int = 0


class DatabasePaperReplay:
    async def load(
        self,
        session: AsyncSession,
        simulation_version: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> PaperReplayDataset:
        prior_snapshot = None
        if start is not None:
            prior_snapshot = await session.scalar(
                select(PortfolioSnapshotRecord)
                .where(
                    PortfolioSnapshotRecord.simulation_version == simulation_version,
                    PortfolioSnapshotRecord.timestamp < start,
                )
                .order_by(
                    PortfolioSnapshotRecord.timestamp.desc(),
                    PortfolioSnapshotRecord.id.desc(),
                )
                .limit(1)
            )
        incident_statement = select(func.count(PaperRuntimeIncidentRecord.id)).where(
            PaperRuntimeIncidentRecord.simulation_version == simulation_version
        )
        if start is not None:
            incident_statement = incident_statement.where(
                PaperRuntimeIncidentRecord.occurred_at >= start
            )
        if end is not None:
            incident_statement = incident_statement.where(
                PaperRuntimeIncidentRecord.occurred_at < end
            )
        runtime_incident_count = int(await session.scalar(incident_statement) or 0)
        snapshot_statement = (
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.simulation_version == simulation_version)
            .order_by(PortfolioSnapshotRecord.timestamp, PortfolioSnapshotRecord.id)
        )
        if start is not None:
            snapshot_statement = snapshot_statement.where(
                PortfolioSnapshotRecord.timestamp >= start
            )
        if end is not None:
            snapshot_statement = snapshot_statement.where(
                PortfolioSnapshotRecord.timestamp < end
            )
        snapshots = list((await session.execute(snapshot_statement)).scalars())
        snapshot_timestamps = tuple(row.timestamp for row in snapshots)
        max_snapshot_gap = max(
            (
                Decimal(str((current - previous).total_seconds()))
                for previous, current in zip(
                    snapshot_timestamps, snapshot_timestamps[1:], strict=False
                )
            ),
            default=Decimal("0"),
        )
        max_invariant_error = max(
            (
                abs(
                    row.equity
                    - (row.cash + row.locked_capital + row.total_pnl)
                )
                for row in snapshots
            ),
            default=Decimal("0"),
        )
        snapshot_pnl_delta = (
            snapshots[-1].total_pnl
            - (prior_snapshot.total_pnl if prior_snapshot is not None else Decimal("0"))
            if snapshots
            else Decimal("0")
        )
        statement = select(PaperPositionRecord).where(
            PaperPositionRecord.simulation_version == simulation_version,
            PaperPositionRecord.opened_at.is_not(None),
        )
        if start is not None:
            statement = statement.where(PaperPositionRecord.opened_at >= start)
        if end is not None:
            statement = statement.where(PaperPositionRecord.opened_at < end)
        rows = list((await session.execute(statement)).scalars())
        rows = [
            row
            for row in rows
            if row.opened_at is not None
            and (start is None or row.opened_at >= start)
            and (end is None or row.opened_at < end)
        ]
        positions = {row.position_id: PaperPosition.model_validate(row.payload) for row in rows}
        if not positions:
            return PaperReplayDataset(
                events=[],
                dataset_version=self._version(
                    simulation_version,
                    start,
                    end,
                    0,
                    0,
                    0,
                    len(snapshots),
                    runtime_incident_count,
                ),
                attribution={"strategy": {}, "exchange": {}, "asset": {}},
                position_count=0,
                observation_start=(snapshot_timestamps[0] if snapshot_timestamps else None),
                observation_end=(snapshot_timestamps[-1] if snapshot_timestamps else None),
                snapshot_timestamps=snapshot_timestamps,
                max_snapshot_gap_seconds=max_snapshot_gap,
                max_accounting_invariant_error=max_invariant_error,
                snapshot_pnl_delta=snapshot_pnl_delta,
                runtime_incident_count=runtime_incident_count,
            )

        position_ids = tuple(positions)
        fill_statement = (
            select(PaperFillRecord)
            .where(PaperFillRecord.position_id.in_(position_ids))
            .order_by(PaperFillRecord.timestamp, PaperFillRecord.id)
        )
        if start is not None:
            fill_statement = fill_statement.where(PaperFillRecord.timestamp >= start)
        if end is not None:
            fill_statement = fill_statement.where(PaperFillRecord.timestamp < end)
        fills = list((await session.execute(fill_statement)).scalars())
        fills = [
            row
            for row in fills
            if (start is None or row.timestamp >= start)
            and (end is None or row.timestamp < end)
        ]
        funding_statement = (
            select(PaperFundingPaymentRecord)
            .where(PaperFundingPaymentRecord.position_id.in_(position_ids))
            .order_by(
                PaperFundingPaymentRecord.funding_timestamp,
                PaperFundingPaymentRecord.id,
            )
        )
        if start is not None:
            funding_statement = funding_statement.where(
                PaperFundingPaymentRecord.funding_timestamp >= start
            )
        if end is not None:
            funding_statement = funding_statement.where(
                PaperFundingPaymentRecord.funding_timestamp < end
            )
        funding = list((await session.execute(funding_statement)).scalars())
        funding = [
            row
            for row in funding
            if (start is None or row.funding_timestamp >= start)
            and (end is None or row.funding_timestamp < end)
        ]

        events: list[BacktestEvent] = []
        attribution: dict[str, dict[str, dict[str, Decimal]]] = {
            "strategy": {},
            "exchange": {},
            "asset": {},
        }
        fills_by_position: dict[str, list[PaperFillRecord]] = {}
        parsed_fills_by_position: dict[str, list[PaperFill]] = {}
        funding_by_position: dict[str, list[PaperFundingPaymentRecord]] = {}
        for fill in fills:
            if fill.position_id is not None:
                fills_by_position.setdefault(fill.position_id, []).append(fill)
            payload = PaperFill.model_validate(fill.payload)
            if fill.position_id is not None:
                parsed_fills_by_position.setdefault(fill.position_id, []).append(payload)
            events.append(
                FillEvent(
                    event_id=f"fill:{fill.id}",
                    timestamp=fill.timestamp,
                    position_id=fill.position_id or "unknown",
                    notional=(fill.price or Decimal("0")) * fill.filled_quantity,
                    fee=fill.fee,
                    spread=payload.spread,
                    slippage=fill.slippage,
                )
            )
        for payment in funding:
            funding_by_position.setdefault(payment.position_id, []).append(payment)
            events.append(
                FundingEvent(
                    event_id=f"funding:{payment.id}",
                    timestamp=payment.funding_timestamp,
                    exchange=payment.exchange,
                    symbol=payment.symbol,
                    rate=payment.funding_rate,
                    notional=payment.notional,
                    pnl=payment.pnl,
                )
            )

        for position_id, position in positions.items():
            if position.opened_at is None:
                continue
            closed_in_window = position.closed_at is not None and (
                end is None or position.closed_at < end
            )
            basis_pnl = position.pnl.basis_pnl if closed_in_window else Decimal("0")
            price_mismatch_pnl = (
                position.pnl.price_pnl_leg_a + position.pnl.price_pnl_leg_b
                if closed_in_window
                else Decimal("0")
            )
            borrow_cost = position.pnl.borrow_cost
            if not closed_in_window and position.state == "CLOSED":
                borrow_cost = Decimal("0")
            market_pnl = (
                basis_pnl
                + price_mismatch_pnl
                - borrow_cost
                - position.pnl.legging_cost
            )
            valuation_at = (
                position.closed_at
                if closed_in_window
                else snapshot_timestamps[-1]
                if snapshot_timestamps
                else position.opened_at
            )
            events.extend(
                [
                    OpportunityEvent(
                        event_id=f"opportunity:{position_id}",
                        timestamp=position.opened_at,
                        opportunity_id=position.opportunity_id,
                        net_edge=position.entry_net_edge,
                    ),
                    PositionEvent(
                        event_id=f"position-open:{position_id}",
                        timestamp=position.opened_at,
                        position_id=position_id,
                        state="OPEN",
                    ),
                    PositionEvent(
                        event_id=f"position-close:{position_id}",
                        timestamp=valuation_at,
                        position_id=position_id,
                        state="CLOSED" if closed_in_window else "MARKED",
                        pnl=market_pnl,
                    ),
                ]
            )
            fees_total = sum(
                (fill.fee for fill in fills_by_position.get(position_id, [])), Decimal("0")
            )
            spread_total = sum(
                (
                    fill.spread
                    for fill in parsed_fills_by_position.get(position_id, [])
                ),
                Decimal("0"),
            )
            slippage_total = sum(
                (fill.slippage for fill in fills_by_position.get(position_id, [])),
                Decimal("0"),
            )
            funding_total = sum(
                (payment.pnl for payment in funding_by_position.get(position_id, [])),
                Decimal("0"),
            )
            components = {
                "funding_pnl": funding_total,
                "basis_pnl": basis_pnl,
                "price_mismatch_pnl": price_mismatch_pnl,
                "fees": fees_total,
                "spread": spread_total,
                "slippage": slippage_total,
                "borrow_cost": borrow_cost,
                "legging_cost": position.pnl.legging_cost,
                "net_pnl": (
                    basis_pnl
                    + price_mismatch_pnl
                    + funding_total
                    - fees_total
                    - spread_total
                    - slippage_total
                    - borrow_cost
                    - position.pnl.legging_cost
                ),
            }
            self._attribute(
                attribution["strategy"], position.strategy or "unknown", components
            )
            self._attribute(attribution["asset"], position.asset, components)
            venues = tuple(
                dict.fromkeys(
                    fill.exchange for fill in fills_by_position.get(position_id, [])
                )
            ) or position.allocated_venues
            divisor = Decimal(max(1, len(venues)))
            for venue in venues:
                self._attribute(
                    attribution["exchange"],
                    venue,
                    {key: value / divisor for key, value in components.items()},
                )

        return PaperReplayDataset(
            events=events,
            dataset_version=self._version(
                simulation_version,
                start,
                end,
                len(rows),
                len(fills),
                len(funding),
                len(snapshots),
                runtime_incident_count,
            ),
            attribution=attribution,
            position_count=len(rows),
            observation_start=(snapshot_timestamps[0] if snapshot_timestamps else None),
            observation_end=(snapshot_timestamps[-1] if snapshot_timestamps else None),
            snapshot_timestamps=snapshot_timestamps,
            max_snapshot_gap_seconds=max_snapshot_gap,
            max_accounting_invariant_error=max_invariant_error,
            snapshot_pnl_delta=snapshot_pnl_delta,
            runtime_incident_count=runtime_incident_count,
        )

    @staticmethod
    def _attribute(
        target: dict[str, dict[str, Decimal]],
        key: str,
        components: dict[str, Decimal],
    ) -> None:
        bucket = target.setdefault(key, {})
        for component, value in components.items():
            bucket[component] = bucket.get(component, Decimal("0")) + value

    @staticmethod
    def _version(
        simulation_version: str,
        start: datetime | None,
        end: datetime | None,
        positions: int,
        fills: int,
        funding: int,
        snapshots: int,
        runtime_incidents: int,
    ) -> str:
        return ":".join(
            (
                "paper-db",
                simulation_version,
                start.isoformat() if start else "begin",
                end.isoformat() if end else "latest",
                (
                    f"p{positions}-f{fills}-u{funding}-s{snapshots}"
                    f"-i{runtime_incidents}"
                ),
            )
        )
