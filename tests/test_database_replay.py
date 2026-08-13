from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.database_replay import DatabasePaperReplay
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.base import FillStatus, PaperFill
from funding_arbitrage.portfolio.position import PaperPosition, PnLBreakdown, PositionState


class _Rows:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def scalars(self) -> list[object]:
        return self.values


class _ReplaySession:
    def __init__(
        self,
        results: list[list[object]],
        scalar_values: list[object | None] | None = None,
    ) -> None:
        self.results = results
        self.scalar_values = scalar_values or []

    async def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    async def execute(self, _statement: object) -> _Rows:
        return _Rows(self.results.pop(0))


def _fill(
    fill_id: str,
    position_id: str,
    timestamp: datetime,
    side: str,
    *,
    fee: str = "1",
    spread: str = "0.5",
    slippage: str = "0.2",
) -> PaperFillRecord:
    payload = PaperFill(
        fill_id=fill_id,
        client_order_id=f"client-{fill_id}",
        exchange="gate",
        symbol="TUT_USDT",
        instrument_type=InstrumentType.PERPETUAL,
        side=side,
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("100"),
        reference_price=Decimal("100"),
        fee=Decimal(fee),
        spread=Decimal(spread),
        slippage=Decimal(slippage),
        status=FillStatus.FILLED,
        timestamp=timestamp,
    )
    return PaperFillRecord(
        fill_id=fill_id,
        position_id=position_id,
        exchange="gate",
        symbol="TUT_USDT",
        instrument_type=InstrumentType.PERPETUAL.value,
        side=side,
        filled_quantity=Decimal("1"),
        price=Decimal("100"),
        fee=Decimal(fee),
        slippage=Decimal(slippage),
        status=FillStatus.FILLED.value,
        timestamp=timestamp,
        payload=payload.model_dump(mode="json"),
    )


@pytest.mark.asyncio
async def test_database_replay_marks_open_position_and_reconciles_snapshot_pnl() -> None:
    start = datetime(2026, 8, 11, tzinfo=UTC)
    end = start + timedelta(hours=1)
    position_id = "open-position"
    position = PaperPosition(
        id=position_id,
        opportunity_id="opportunity",
        asset="TUT",
        capital=Decimal("100"),
        simulation_version="candidate",
        state=PositionState.OPEN,
        opened_at=start + timedelta(minutes=1),
        allocated_venues=("gate", "gate"),
        pnl=PnLBreakdown(legging_cost=Decimal("0.1")),
    )
    position_row = PaperPositionRecord(
        position_id=position_id,
        opportunity_id="opportunity",
        state=PositionState.OPEN.value,
        asset="TUT",
        capital=Decimal("100"),
        simulation_version="candidate",
        opened_at=position.opened_at,
        payload=position.model_dump(mode="json"),
    )
    snapshots = [
        PortfolioSnapshotRecord(
            timestamp=start,
            simulation_version="candidate",
            equity=Decimal("1000"),
            cash=Decimal("1000"),
            locked_capital=Decimal("0"),
            total_pnl=Decimal("0"),
            funding_pnl=Decimal("0"),
            fees=Decimal("0"),
            balances={},
        ),
        PortfolioSnapshotRecord(
            timestamp=end,
            simulation_version="candidate",
            equity=Decimal("996.5"),
            cash=Decimal("800"),
            locked_capital=Decimal("200"),
            total_pnl=Decimal("-3.5"),
            funding_pnl=Decimal("0"),
            fees=Decimal("2"),
            balances={},
        ),
    ]
    fills = [
        _fill("entry-a", position_id, start + timedelta(seconds=1), "BUY"),
        _fill("entry-b", position_id, start + timedelta(seconds=2), "SELL"),
    ]
    session = _ReplaySession([snapshots, [position_row], fills, []])

    dataset = await DatabasePaperReplay().load(  # type: ignore[arg-type]
        session, "candidate", start
    )
    result = BacktestEngine().run(dataset.events, Decimal("1000"), {}, "test")
    comparison = compare_paper_datasets(dataset, dataset, Decimal("1000"))

    assert dataset.position_count == 1
    assert dataset.snapshot_pnl_delta == Decimal("-3.5")
    assert result.metrics.net_profit_after_costs == Decimal("-3.5")
    assert comparison["checks"]["accounting_reconciled"] is True
    assert comparison["observation"]["candidate_replay_pnl_error"] == "0.0"


@pytest.mark.asyncio
async def test_database_replay_loads_restart_safe_runtime_incident_count() -> None:
    start = datetime(2026, 8, 11, tzinfo=UTC)
    snapshots = [
        PortfolioSnapshotRecord(
            timestamp=start,
            simulation_version="candidate",
            equity=Decimal("1000"),
            cash=Decimal("1000"),
            locked_capital=Decimal("0"),
            total_pnl=Decimal("0"),
            funding_pnl=Decimal("0"),
            fees=Decimal("0"),
            balances={},
        )
    ]
    session = _ReplaySession(
        [snapshots, []],
        scalar_values=[None, 2],
    )

    dataset = await DatabasePaperReplay().load(  # type: ignore[arg-type]
        session, "candidate", start
    )

    assert dataset.runtime_incident_count == 2
    assert dataset.dataset_version.endswith("-i2-c0")


@pytest.mark.asyncio
async def test_database_replay_detects_position_carried_across_boundary() -> None:
    start = datetime(2026, 8, 11, tzinfo=UTC)
    snapshots = [
        PortfolioSnapshotRecord(
            timestamp=start,
            simulation_version="candidate",
            equity=Decimal("1000"),
            cash=Decimal("1000"),
            locked_capital=Decimal("0"),
            total_pnl=Decimal("0"),
            funding_pnl=Decimal("0"),
            fees=Decimal("0"),
            balances={},
        )
    ]
    session = _ReplaySession(
        [snapshots, []],
        scalar_values=[None, 0, 1],
    )

    dataset = await DatabasePaperReplay().load(  # type: ignore[arg-type]
        session, "candidate", start
    )

    assert dataset.carry_in_position_count == 1
    assert dataset.dataset_version.endswith("-i0-c1")


@pytest.mark.asyncio
async def test_database_replay_includes_boundary_to_first_snapshot_gap() -> None:
    start = datetime(2026, 8, 11, tzinfo=UTC)
    first_snapshot = start + timedelta(seconds=301)
    snapshots = [
        PortfolioSnapshotRecord(
            timestamp=first_snapshot,
            simulation_version="candidate",
            equity=Decimal("1000"),
            cash=Decimal("1000"),
            locked_capital=Decimal("0"),
            total_pnl=Decimal("0"),
            funding_pnl=Decimal("0"),
            fees=Decimal("0"),
            balances={},
        )
    ]
    session = _ReplaySession(
        [snapshots, []],
        scalar_values=[None, 0, 0],
    )

    dataset = await DatabasePaperReplay().load(  # type: ignore[arg-type]
        session, "candidate", start
    )

    assert dataset.max_snapshot_gap_seconds == Decimal("301.0")


@pytest.mark.asyncio
async def test_database_replay_excludes_close_data_after_requested_cutoff() -> None:
    start = datetime(2026, 8, 11, tzinfo=UTC)
    end = start + timedelta(hours=1)
    position_id = "future-close"
    position = PaperPosition(
        id=position_id,
        opportunity_id="opportunity",
        asset="TUT",
        capital=Decimal("100"),
        simulation_version="candidate",
        state=PositionState.CLOSED,
        opened_at=start + timedelta(minutes=1),
        closed_at=end + timedelta(hours=1),
        allocated_venues=("gate", "gate"),
        pnl=PnLBreakdown(price_pnl_leg_a=Decimal("500")),
    )
    position_row = PaperPositionRecord(
        position_id=position_id,
        opportunity_id="opportunity",
        state=PositionState.CLOSED.value,
        asset="TUT",
        capital=Decimal("100"),
        simulation_version="candidate",
        opened_at=position.opened_at,
        closed_at=position.closed_at,
        payload=position.model_dump(mode="json"),
    )
    snapshots = [
        PortfolioSnapshotRecord(
            timestamp=start,
            simulation_version="candidate",
            equity=Decimal("1000"),
            cash=Decimal("1000"),
            locked_capital=Decimal("0"),
            total_pnl=Decimal("0"),
            funding_pnl=Decimal("0"),
            fees=Decimal("0"),
            balances={},
        ),
        PortfolioSnapshotRecord(
            timestamp=end - timedelta(seconds=1),
            simulation_version="candidate",
            equity=Decimal("996.6"),
            cash=Decimal("800"),
            locked_capital=Decimal("200"),
            total_pnl=Decimal("-3.4"),
            funding_pnl=Decimal("0"),
            fees=Decimal("2"),
            balances={},
        ),
    ]
    fills = [
        _fill("entry-a", position_id, start + timedelta(seconds=1), "BUY"),
        _fill("entry-b", position_id, start + timedelta(seconds=2), "SELL"),
        _fill("future-a", position_id, end + timedelta(minutes=1), "SELL"),
        _fill("future-b", position_id, end + timedelta(minutes=2), "BUY"),
    ]
    session = _ReplaySession([snapshots, [position_row], fills, []])

    dataset = await DatabasePaperReplay().load(  # type: ignore[arg-type]
        session, "candidate", start, end
    )
    result = BacktestEngine().run(dataset.events, Decimal("1000"), {}, "test")

    assert result.metrics.net_profit_after_costs == Decimal("-3.4")
    assert dataset.attribution["strategy"]["unknown"]["price_mismatch_pnl"] == 0
