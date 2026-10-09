from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    Side,
)
from funding_arbitrage.strategies import (
    FundingHarvestPositionState,
    FundingHarvestStateMachine,
    FundingLegExecution,
    FundingLegExecutionState,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(instrument_type: InstrumentType) -> InstrumentKey:
    return InstrumentKey(
        venue="BYBIT",
        exchange_symbol=f"BTCUSDT-{instrument_type.value}",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=instrument_type,
    )


def _machine() -> FundingHarvestStateMachine:
    return FundingHarvestStateMachine(
        (
            FundingLegExecution(
                instrument=_instrument(InstrumentType.SPOT),
                side=Side.BUY,
                requested_quantity=Decimal("1"),
            ),
            FundingLegExecution(
                instrument=_instrument(InstrumentType.PERPETUAL),
                side=Side.SELL,
                requested_quantity=Decimal("1"),
            ),
        ),
        legging_timeout_seconds=3,
    )


def test_two_leg_state_machine_requires_exact_open_and_close_quantities() -> None:
    machine = _machine()
    machine.start_opening(NOW)
    machine.acknowledge(0, "order-a", NOW)
    machine.acknowledge(1, "order-b", NOW)

    assert machine.apply_open_fill(0, "fill-a1", Decimal("0.4"), NOW) is True
    assert machine.apply_open_fill(0, "fill-a1", Decimal("0.4"), NOW) is False
    assert machine.legs[0].state is FundingLegExecutionState.PARTIALLY_FILLED
    machine.apply_open_fill(0, "fill-a2", Decimal("0.6"), NOW)
    machine.apply_open_fill(1, "fill-b1", Decimal("1"), NOW)

    assert machine.state is FundingHarvestPositionState.HEDGED
    assert all(leg.state is FundingLegExecutionState.HEDGED for leg in machine.legs)
    machine.mark_open(NOW)
    machine.start_exit(NOW + timedelta(hours=8))
    machine.apply_close_fill(0, "close-a1", Decimal("0.5"), NOW + timedelta(hours=8))
    machine.apply_close_fill(0, "close-a2", Decimal("0.5"), NOW + timedelta(hours=8))
    machine.apply_close_fill(1, "close-b1", Decimal("1"), NOW + timedelta(hours=8))

    assert machine.state is FundingHarvestPositionState.CLOSED
    assert all(leg.closed_quantity == leg.filled_quantity for leg in machine.legs)
    assert [event.sequence for event in machine.transitions] == list(
        range(1, len(machine.transitions) + 1)
    )


def test_legging_timeout_demands_full_compensating_unwind() -> None:
    machine = _machine()
    machine.start_opening(NOW)
    machine.acknowledge(0, "order-a", NOW)
    machine.acknowledge(1, "order-b", NOW)
    machine.apply_open_fill(0, "fill-a", Decimal("1"), NOW)

    assert machine.check_legging_timeout(NOW + timedelta(seconds=2)) is False
    assert machine.check_legging_timeout(NOW + timedelta(seconds=3)) is True
    assert machine.state is FundingHarvestPositionState.UNWIND_REQUIRED
    with pytest.raises(ValueError, match="every filled unit"):
        machine.complete_compensating_unwind(
            (Decimal("0.9"), Decimal("0")),
            NOW + timedelta(seconds=4),
        )
    machine.complete_compensating_unwind(
        (Decimal("1"), Decimal("0")),
        NOW + timedelta(seconds=4),
    )

    assert machine.state is FundingHarvestPositionState.FAILED
    assert [leg.state for leg in machine.legs] == [
        FundingLegExecutionState.FAILED,
        FundingLegExecutionState.FAILED,
    ]


def test_state_machine_rejects_overfills_and_invalid_transitions() -> None:
    machine = _machine()
    with pytest.raises(ValueError, match="position must be"):
        machine.mark_open(NOW)
    machine.start_opening(NOW)
    machine.acknowledge(0, "order-a", NOW)
    machine.acknowledge(1, "order-b", NOW)
    with pytest.raises(ValueError, match="exceeds requested"):
        machine.apply_open_fill(0, "overfill", Decimal("1.1"), NOW)
