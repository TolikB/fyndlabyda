from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, OrderType, Side
from funding_arbitrage.execution.protective import (
    JsonlProtectiveJournal,
    ProtectiveStopManager,
    ProtectiveStopSnapshot,
    ProtectiveStopStatus,
    VenueProtectiveOrder,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(venue: str = "bybit") -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _register(
    manager: ProtectiveStopManager,
    *,
    signed_quantity: str = "2",
    limit_price: str | None = None,
) -> ProtectiveStopSnapshot:
    return manager.register_stop(
        position_id="position-1",
        instrument=_instrument(),
        signed_position_quantity=Decimal(signed_quantity),
        stop_price=Decimal("95"),
        limit_price=Decimal(limit_price) if limit_price is not None else None,
        timestamp=NOW,
    )


def _venue(
    stop: ProtectiveStopSnapshot,
    *,
    status: ProtectiveStopStatus = ProtectiveStopStatus.ACTIVE,
    reduce_only: bool = True,
    quantity: str | None = None,
) -> VenueProtectiveOrder:
    return VenueProtectiveOrder(
        protective_order_id=stop.protective_order_id,
        exchange_order_id=stop.exchange_order_id or "venue-stop-1",
        instrument=stop.instrument,
        side=stop.side,
        quantity=Decimal(quantity) if quantity is not None else stop.quantity,
        stop_price=stop.stop_price,
        limit_price=stop.limit_price,
        order_type=stop.order_type,
        reduce_only=reduce_only,
        status=status,
    )


def test_exchange_hosted_reduce_only_stop_is_persisted_before_submit_and_recovers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "protective.jsonl"
    manager = ProtectiveStopManager(JsonlProtectiveJournal(path))
    registered = _register(manager, signed_quantity="2")
    prepared = manager.prepare_submit(
        registered.protective_order_id,
        NOW + timedelta(seconds=1),
    )

    assert registered.side is Side.SELL
    assert registered.order_type is OrderType.STOP
    assert registered.reduce_only is True
    assert registered.exchange_hosted is True
    assert prepared.status is ProtectiveStopStatus.SUBMITTING
    events = [
        json.loads(line)["event_type"]
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["REGISTERED", "SUBMIT_PREPARED"]
    assert ProtectiveStopManager(JsonlProtectiveJournal(path)).stops[
        registered.protective_order_id
    ] == prepared


def test_acknowledged_stop_reconciles_exactly_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "protective.jsonl"
    manager = ProtectiveStopManager(JsonlProtectiveJournal(path))
    stop = _register(manager, signed_quantity="-1", limit_price="94.5")
    manager.prepare_submit(stop.protective_order_id, NOW + timedelta(seconds=1))
    active = manager.acknowledge(
        stop.protective_order_id,
        "venue-stop-1",
        NOW + timedelta(seconds=2),
    )

    assert active.side is Side.BUY
    assert active.order_type is OrderType.STOP_LIMIT
    recovered = ProtectiveStopManager(JsonlProtectiveJournal(path))
    result = recovered.reconcile(
        (_venue(active),),
        NOW + timedelta(seconds=3),
    )
    assert result.safe is True
    assert result.active_count == 1
    assert result.interlock_engaged is False


def test_missing_stop_engages_persistent_interlock_until_replaced(tmp_path: Path) -> None:
    path = tmp_path / "protective.jsonl"
    manager = ProtectiveStopManager(JsonlProtectiveJournal(path))
    stop = _register(manager)
    manager.prepare_submit(stop.protective_order_id, NOW + timedelta(seconds=1))
    manager.acknowledge(
        stop.protective_order_id,
        "venue-stop-1",
        NOW + timedelta(seconds=2),
    )

    result = manager.reconcile((), NOW + timedelta(seconds=3))
    assert result.safe is False
    assert result.interlock_engaged is True
    assert manager.stops[stop.protective_order_id].status is ProtectiveStopStatus.BLOCKED

    recovered = ProtectiveStopManager(JsonlProtectiveJournal(path))
    assert recovered.interlock_engaged is True
    with pytest.raises(ValueError, match="dual approval"):
        recovered.clear_interlock(
            NOW + timedelta(seconds=4),
            operator_approved=True,
            risk_approved=False,
        )
    with pytest.raises(ValueError, match="remain unresolved"):
        recovered.clear_interlock(
            NOW + timedelta(seconds=4),
            operator_approved=True,
            risk_approved=True,
        )

    recovered.prepare_replacement(stop.protective_order_id, NOW + timedelta(seconds=5))
    replacement = recovered.acknowledge(
        stop.protective_order_id,
        "venue-stop-2",
        NOW + timedelta(seconds=6),
    )
    recovered.clear_interlock(
        NOW + timedelta(seconds=7),
        operator_approved=True,
        risk_approved=True,
    )
    assert replacement.status is ProtectiveStopStatus.ACTIVE
    assert recovered.interlock_engaged is False


@pytest.mark.parametrize(
    ("reduce_only", "quantity", "issue"),
    [
        (False, None, "reduce_only_mismatch"),
        (True, "1.9", "quantity_mismatch"),
    ],
)
def test_mismatched_exchange_protection_fails_closed(
    tmp_path: Path,
    reduce_only: bool,
    quantity: str | None,
    issue: str,
) -> None:
    manager = ProtectiveStopManager(JsonlProtectiveJournal(tmp_path / "protective.jsonl"))
    stop = _register(manager)
    manager.prepare_submit(stop.protective_order_id, NOW + timedelta(seconds=1))
    active = manager.acknowledge(
        stop.protective_order_id,
        "venue-stop-1",
        NOW + timedelta(seconds=2),
    )
    observed = _venue(active, reduce_only=reduce_only, quantity=quantity)

    result = manager.reconcile((observed,), NOW + timedelta(seconds=3))
    assert result.safe is False
    assert any(issue in item for item in result.issues)
    assert result.interlock_engaged is True


def test_trigger_and_flat_position_cancellation_are_reconciled(tmp_path: Path) -> None:
    manager = ProtectiveStopManager(JsonlProtectiveJournal(tmp_path / "protective.jsonl"))
    first = _register(manager)
    manager.prepare_submit(first.protective_order_id, NOW + timedelta(seconds=1))
    active = manager.acknowledge(
        first.protective_order_id,
        "venue-stop-1",
        NOW + timedelta(seconds=2),
    )
    triggered = manager.reconcile(
        (_venue(active, status=ProtectiveStopStatus.TRIGGERED),),
        NOW + timedelta(seconds=3),
    )
    assert triggered.safe is True
    assert triggered.triggered_count == 1

    second = manager.register_stop(
        position_id="position-2",
        instrument=_instrument("gate"),
        signed_position_quantity=Decimal("1"),
        stop_price=Decimal("95"),
        limit_price=None,
        timestamp=NOW,
    )
    manager.prepare_submit(second.protective_order_id, NOW + timedelta(seconds=4))
    active_second = manager.acknowledge(
        second.protective_order_id,
        "venue-stop-2",
        NOW + timedelta(seconds=5),
    )
    with pytest.raises(ValueError, match="position is open"):
        manager.prepare_cancel(
            second.protective_order_id,
            NOW + timedelta(seconds=6),
            position_is_flat=False,
        )
    pending = manager.prepare_cancel(
        second.protective_order_id,
        NOW + timedelta(seconds=6),
        position_is_flat=True,
    )
    cancelled = manager.reconcile(
        (_venue(active_second, status=ProtectiveStopStatus.CANCELLED),),
        NOW + timedelta(seconds=7),
    )
    assert pending.status is ProtectiveStopStatus.CANCEL_PENDING
    assert cancelled.safe is True
    assert manager.stops[second.protective_order_id].status is (
        ProtectiveStopStatus.CANCELLED
    )


def test_orphan_exchange_stop_and_flat_registration_fail_closed(tmp_path: Path) -> None:
    manager = ProtectiveStopManager(JsonlProtectiveJournal(tmp_path / "protective.jsonl"))
    with pytest.raises(ValueError, match="flat position"):
        _register(manager, signed_quantity="0")

    stop = _register(manager)
    orphan = VenueProtectiveOrder(
        **{
            **_venue(stop).model_dump(),
            "protective_order_id": "unknown-protection",
        }
    )
    result = manager.reconcile((orphan,), NOW + timedelta(seconds=1))
    assert result.safe is False
    assert "orphan_exchange_protection" in result.issues[-1]
