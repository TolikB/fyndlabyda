"""Every open runtime position must carry tracked, reconciled protection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.backtest.fills import (
    SimulatedFill,
    SimulatedOrderState,
    SimulatedOrderType,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderType,
    Side,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalExitReason,
    DirectionalPaperOrder,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
)
from funding_arbitrage.execution.protective import (
    ProtectiveStopStatus,
    VenueProtectiveOrder,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.runtime_protective import (
    RuntimeProtectiveStopCoordinator,
)

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _position(
    *,
    status: DirectionalPaperStatus = DirectionalPaperStatus.OPEN,
    exit_reason: DirectionalExitReason | None = None,
    position_id: str = "protective_position_1",
) -> DirectionalPaperPosition:
    timestamp = NOW + timedelta(seconds=1)
    fill = SimulatedFill(
        timestamp=timestamp,
        quantity=Decimal("1"),
        price=Decimal("100.5"),
        notional=Decimal("100.5"),
        fee=Decimal("0.05"),
        spread_cost=Decimal("0.25"),
        impact_cost=Decimal("0"),
        liquidity_role=LiquidityRole.TAKER,
    )
    entry = DirectionalPaperOrder(
        client_order_id=f"{position_id}_entry",
        side=Side.BUY,
        order_type=SimulatedOrderType.LIMIT,
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        limit_price=Decimal("101"),
        submitted_at=NOW,
        expires_at=NOW + timedelta(seconds=15),
        state=SimulatedOrderState.FILLED,
        fills=(fill,),
        version=2,
    )
    exit_order = None
    if status is DirectionalPaperStatus.CLOSED:
        exit_order = DirectionalPaperOrder(
            client_order_id=f"{position_id}_exit",
            side=Side.SELL,
            order_type=SimulatedOrderType.MARKET,
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            submitted_at=timestamp,
            expires_at=timestamp + timedelta(seconds=15),
            state=SimulatedOrderState.FILLED,
            fills=(fill,),
            version=2,
        )
    return DirectionalPaperPosition(
        position_id=position_id,
        plan_id=f"{position_id}_plan",
        signal_id=f"{position_id}_signal",
        risk_decision_id=f"{position_id}_risk",
        strategy_id="orderflow-breakout-v1",
        instrument=INSTRUMENT,
        side=Side.BUY,
        approved_notional=Decimal("101"),
        structural_stop=Decimal("98"),
        target_price=Decimal("103"),
        expected_exit_at=NOW + timedelta(minutes=30),
        status=status,
        entry_order=entry,
        exit_order=exit_order,
        exit_order_history=(exit_order,) if exit_order is not None else (),
        exit_reason=exit_reason,
        mark_price=Decimal("100"),
        opened_at=timestamp,
        closed_at=(
            timestamp + timedelta(seconds=1)
            if status is DirectionalPaperStatus.CLOSED
            else None
        ),
        created_at=NOW,
        updated_at=timestamp,
    )


def _coordinator(tmp_path: Path) -> RuntimeProtectiveStopCoordinator:
    return RuntimeProtectiveStopCoordinator(tmp_path / "nested" / "protective.jsonl")


def test_protection_is_registered_and_activated_for_an_open_position(
    tmp_path: Path,
) -> None:
    coordinator = _coordinator(tmp_path)
    position = _position()
    result = coordinator.synchronize((position,), NOW)

    stop = coordinator.protection_for(position.position_id)
    assert stop is not None
    assert stop.status is ProtectiveStopStatus.ACTIVE
    assert stop.reduce_only is True
    assert stop.exchange_hosted is True
    assert stop.order_type is OrderType.STOP
    # A long position is protected by a reduce-only sell at the structural stop.
    assert stop.side is Side.SELL
    assert stop.stop_price == Decimal("98")
    assert stop.quantity == Decimal("1")
    assert result.safe is True
    assert result.active_count == 1
    assert coordinator.interlock_engaged is False


def test_protection_survives_a_restart_from_the_journal(tmp_path: Path) -> None:
    journal = tmp_path / "protective.jsonl"
    first = RuntimeProtectiveStopCoordinator(journal)
    position = _position()
    first.synchronize((position,), NOW)

    recovered = RuntimeProtectiveStopCoordinator(journal)
    stop = recovered.protection_for(position.position_id)
    assert stop is not None
    assert stop.status is ProtectiveStopStatus.ACTIVE
    assert stop.stop_price == Decimal("98")


def test_synchronizing_is_idempotent(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    position = _position()
    coordinator.synchronize((position,), NOW)
    before = coordinator.protection_for(position.position_id)
    result = coordinator.synchronize((position,), NOW + timedelta(seconds=5))
    after = coordinator.protection_for(position.position_id)

    assert before is not None and after is not None
    assert after.protective_order_id == before.protective_order_id
    assert result.safe is True
    assert len(coordinator.manager.stops) == 1


def test_a_stopped_out_position_marks_its_protection_triggered(
    tmp_path: Path,
) -> None:
    coordinator = _coordinator(tmp_path)
    position = _position()
    coordinator.synchronize((position,), NOW)

    closed = _position(
        status=DirectionalPaperStatus.CLOSED,
        exit_reason=DirectionalExitReason.STOP,
    )
    result = coordinator.synchronize((closed,), NOW + timedelta(seconds=10))

    stop = coordinator.protection_for(closed.position_id)
    assert stop is not None
    assert stop.status is ProtectiveStopStatus.TRIGGERED
    assert result.safe is True


def test_a_target_exit_cancels_its_protection_without_an_interlock(
    tmp_path: Path,
) -> None:
    coordinator = _coordinator(tmp_path)
    coordinator.synchronize((_position(),), NOW)

    closed = _position(
        status=DirectionalPaperStatus.CLOSED,
        exit_reason=DirectionalExitReason.TARGET,
    )
    result = coordinator.synchronize((closed,), NOW + timedelta(seconds=10))

    stop = coordinator.protection_for(closed.position_id)
    assert stop is not None
    assert stop.status is ProtectiveStopStatus.CANCELLED
    assert result.safe is True
    assert coordinator.interlock_engaged is False


def test_missing_venue_protection_blocks_the_stop_and_engages_the_interlock(
    tmp_path: Path,
) -> None:
    coordinator = _coordinator(tmp_path)
    position = _position()
    coordinator.synchronize((position,), NOW)

    # The venue reports nothing while the position is still open.
    result = coordinator.manager.reconcile((), NOW + timedelta(seconds=10))

    assert result.safe is False
    assert any("missing_exchange_protection" in issue for issue in result.issues)
    assert coordinator.interlock_engaged is True
    stop = coordinator.protection_for(position.position_id)
    assert stop is not None
    assert stop.status is ProtectiveStopStatus.BLOCKED


def test_a_mismatched_venue_stop_price_engages_the_interlock(tmp_path: Path) -> None:
    coordinator = _coordinator(tmp_path)
    position = _position()
    coordinator.synchronize((position,), NOW)
    stop = coordinator.protection_for(position.position_id)
    assert stop is not None

    tampered = VenueProtectiveOrder(
        protective_order_id=stop.protective_order_id,
        exchange_order_id=stop.exchange_order_id or "venue-1",
        instrument=stop.instrument,
        side=stop.side,
        quantity=stop.quantity,
        stop_price=Decimal("50"),
        limit_price=None,
        order_type=stop.order_type,
        reduce_only=True,
        status=ProtectiveStopStatus.ACTIVE,
    )
    result = coordinator.manager.reconcile((tampered,), NOW + timedelta(seconds=10))

    assert result.safe is False
    assert any("stop_price_mismatch" in issue for issue in result.issues)
    assert coordinator.interlock_engaged is True


def test_runtime_state_blocks_entries_while_the_interlock_is_engaged() -> None:
    state = RuntimeState(
        Settings(_env_file=None, RUN_MODE="paper_test"),
        {},
        emit_metrics=False,
    )
    assert state.entries_allowed() is True

    state.engage_protective_interlock("protective_stop_missing_exchange_protection")
    assert state.entries_allowed() is False
    assert state.entry_block_reason() == (
        "protective_stop_missing_exchange_protection"
    )

    state.clear_protective_interlock()
    assert state.entries_allowed() is True
    assert state.protective_interlock_reason is None


def test_protective_interlock_requires_a_reason() -> None:
    state = RuntimeState(
        Settings(_env_file=None, RUN_MODE="paper_test"),
        {},
        emit_metrics=False,
    )
    with pytest.raises(ValueError, match="requires a reason"):
        state.engage_protective_interlock("   ")
