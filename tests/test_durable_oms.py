from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.decisions import ExecutionReport, RiskDecision
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    Side,
)
from funding_arbitrage.execution.oms import DurableOMS, JsonlOMSJournal, OMSOrderSnapshot

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _decision(*, approved: bool = True) -> RiskDecision:
    return RiskDecision(
        signal_id="signal-1",
        decision_id="risk-1",
        decided_at=NOW,
        approved=approved,
        rejection_reason=None if approved else "risk limit",
        approved_risk_usdt=Decimal("25") if approved else Decimal("0"),
        approved_quantity=Decimal("2") if approved else Decimal("0"),
        approved_notional=Decimal("200") if approved else Decimal("0"),
        max_slippage_bps=Decimal("12"),
        max_execution_seconds=10,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _instrument() -> InstrumentKey:
    return InstrumentKey(
        venue="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _create(oms: DurableOMS, *, quantity: str = "2") -> OMSOrderSnapshot:
    return oms.create_order(
        _decision(),
        leg_index=0,
        instrument=_instrument(),
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal(quantity),
        limit_price=Decimal("100"),
        reduce_only=False,
        timestamp=NOW,
    )


def _report(
    client_order_id: str,
    status: OrderStatus,
    filled: str,
    *,
    offset: int,
) -> ExecutionReport:
    fill = Decimal(filled)
    return ExecutionReport(
        client_order_id=client_order_id,
        exchange_order_id="venue-order-7",
        status=status,
        requested_quantity=Decimal("2"),
        filled_quantity=fill,
        average_fill_price=Decimal("100.1") if fill else None,
        fee=Decimal("0.02") if fill else Decimal("0"),
        fee_asset="USDT" if fill else None,
        liquidity_role=LiquidityRole.TAKER,
        exchange_timestamp=NOW + timedelta(seconds=offset),
        receive_timestamp=NOW + timedelta(seconds=offset, milliseconds=10),
        reject_code="RISK" if status is OrderStatus.REJECTED else None,
    )


def test_persist_before_submit_and_restart_recovery(tmp_path: Path) -> None:
    path = tmp_path / "oms.jsonl"
    oms = DurableOMS(JsonlOMSJournal(path))
    created = _create(oms)
    prepared = oms.prepare_submit(created.client_order_id, NOW + timedelta(seconds=1))

    entries = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["event_type"] for line in entries] == [
        "CREATED",
        "SUBMIT_PREPARED",
    ]
    assert prepared.status is OrderStatus.SUBMITTING

    recovered = DurableOMS(JsonlOMSJournal(path))
    assert recovered.orders[created.client_order_id] == prepared


def test_partial_fill_full_fill_and_duplicate_report_are_idempotent(
    tmp_path: Path,
) -> None:
    oms = DurableOMS(JsonlOMSJournal(tmp_path / "oms.jsonl"))
    order = _create(oms)
    oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    acknowledged = oms.apply_report(
        _report(order.client_order_id, OrderStatus.ACKNOWLEDGED, "0", offset=2)
    )
    partial_report = _report(
        order.client_order_id,
        OrderStatus.PARTIALLY_FILLED,
        "0.75",
        offset=3,
    )
    partial = oms.apply_report(partial_report)
    duplicate = oms.apply_report(partial_report)
    filled = oms.apply_report(
        _report(order.client_order_id, OrderStatus.FILLED, "2", offset=4)
    )

    assert acknowledged.status is OrderStatus.ACKNOWLEDGED
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    assert partial.filled_quantity == Decimal("0.75")
    assert duplicate == partial
    assert duplicate.version == partial.version
    assert filled.status is OrderStatus.FILLED
    assert filled.filled_quantity == Decimal("2")


def test_fill_wins_cancel_race(tmp_path: Path) -> None:
    oms = DurableOMS(JsonlOMSJournal(tmp_path / "oms.jsonl"))
    order = _create(oms)
    oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    oms.apply_report(
        _report(order.client_order_id, OrderStatus.ACKNOWLEDGED, "0", offset=2)
    )
    oms.apply_report(
        _report(order.client_order_id, OrderStatus.PARTIALLY_FILLED, "0.4", offset=3)
    )
    pending = oms.prepare_cancel(order.client_order_id, NOW + timedelta(seconds=4))
    filled = oms.apply_report(
        _report(order.client_order_id, OrderStatus.FILLED, "2", offset=5)
    )

    assert pending.status is OrderStatus.CANCEL_PENDING
    assert pending.cancel_requested is True
    assert filled.status is OrderStatus.FILLED
    assert filled.filled_quantity == Decimal("2")


def test_unknown_order_reconciles_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "oms.jsonl"
    oms = DurableOMS(JsonlOMSJournal(path))
    order = _create(oms)
    oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    unknown = oms.mark_unknown(
        order.client_order_id,
        NOW + timedelta(seconds=2),
        "submit timeout",
    )
    assert unknown.status is OrderStatus.UNKNOWN

    recovered = DurableOMS(JsonlOMSJournal(path))
    reconciling = recovered.start_reconciliation(
        order.client_order_id,
        NOW + timedelta(seconds=3),
    )
    final = recovered.apply_reconciliation(
        order.client_order_id,
        status=OrderStatus.ACKNOWLEDGED,
        filled_quantity=Decimal("0.5"),
        exchange_order_id="venue-order-7",
        timestamp=NOW + timedelta(seconds=4),
    )

    assert reconciling.status is OrderStatus.RECONCILING
    assert final.status is OrderStatus.PARTIALLY_FILLED
    assert final.filled_quantity == Decimal("0.5")
    assert DurableOMS(JsonlOMSJournal(path)).orders[order.client_order_id] == final


def test_rejection_expiry_authorization_and_collision_guards(tmp_path: Path) -> None:
    oms = DurableOMS(JsonlOMSJournal(tmp_path / "oms.jsonl"))
    with pytest.raises(ValueError, match="approved risk decision"):
        oms.create_order(
            _decision(approved=False),
            leg_index=0,
            instrument=_instrument(),
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1"),
            limit_price=None,
            reduce_only=False,
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="exceeds risk authorization"):
        _create(oms, quantity="2.1")

    expiring = _create(oms)
    assert _create(oms) == expiring
    with pytest.raises(ValueError, match="collision"):
        oms.create_order(
            _decision(),
            leg_index=0,
            instrument=_instrument(),
            side=Side.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("2"),
            limit_price=Decimal("100"),
            reduce_only=False,
            timestamp=NOW,
        )
    assert oms.expire(expiring.client_order_id, NOW + timedelta(seconds=1)).status is (
        OrderStatus.EXPIRED
    )

    other = oms.create_order(
        RiskDecision(**{**_decision().model_dump(), "decision_id": "risk-2"}),
        leg_index=0,
        instrument=_instrument(),
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("2"),
        limit_price=Decimal("100"),
        reduce_only=False,
        timestamp=NOW,
    )
    oms.prepare_submit(other.client_order_id, NOW + timedelta(seconds=2))
    rejected = oms.apply_report(
        _report(other.client_order_id, OrderStatus.REJECTED, "0", offset=3)
    )
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.rejection_reason == "RISK"


def test_journal_rejects_sequence_and_order_version_corruption(tmp_path: Path) -> None:
    path = tmp_path / "oms.jsonl"
    oms = DurableOMS(JsonlOMSJournal(path))
    order = _create(oms)
    oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[1]["sequence"] = 3
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="sequence"):
        DurableOMS(JsonlOMSJournal(path))
