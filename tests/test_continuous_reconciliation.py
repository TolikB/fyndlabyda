from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    OrderStatus,
    Side,
)
from funding_arbitrage.portfolio.reconciliation import (
    BalanceReconState,
    ContinuousReconciler,
    FillReconState,
    FundingReconState,
    JsonlReconciliationAudit,
    OrderReconState,
    PositionReconState,
    ReconciliationCategory,
    ReconciliationInput,
    ReconciliationSeverity,
    ReconciliationTolerance,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument() -> InstrumentKey:
    return InstrumentKey(
        venue="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _order() -> OrderReconState:
    return OrderReconState(
        venue="bybit",
        client_order_id="client-1",
        exchange_order_id="exchange-1",
        status=OrderStatus.FILLED,
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        updated_at=NOW,
    )


def _fill() -> FillReconState:
    return FillReconState(
        venue="bybit",
        fill_id="fill-1",
        client_order_id="client-1",
        instrument=_instrument(),
        side=Side.BUY,
        price=Decimal("100"),
        quantity=Decimal("1"),
        fee_amount=Decimal("0.1"),
        fee_asset="USDT",
        timestamp=NOW - timedelta(seconds=5),
    )


def _balance() -> BalanceReconState:
    return BalanceReconState(
        venue="bybit",
        asset="USDT",
        total=Decimal("1000"),
        available=Decimal("900"),
        locked=Decimal("100"),
        borrowed=Decimal("0"),
        timestamp=NOW,
    )


def _position() -> PositionReconState:
    return PositionReconState(
        instrument=_instrument(),
        signed_quantity=Decimal("-1"),
        entry_price=Decimal("100"),
        realized_pnl=Decimal("2"),
        unrealized_pnl=Decimal("1"),
        timestamp=NOW,
    )


def _funding() -> FundingReconState:
    return FundingReconState(
        venue="bybit",
        external_id="funding-1",
        instrument=_instrument(),
        asset="USDT",
        amount=Decimal("0.5"),
        settlement_timestamp=NOW - timedelta(hours=8),
    )


def _input(**updates: object) -> ReconciliationInput:
    values: dict[str, object] = {
        "as_of": NOW,
        "source_health": {"BYBIT": True},
        "local_orders": (_order(),),
        "venue_orders": (_order(),),
        "local_fills": (_fill(),),
        "venue_fills": (_fill(),),
        "local_balances": (_balance(),),
        "venue_balances": (_balance(),),
        "local_positions": (_position(),),
        "venue_positions": (_position(),),
        "local_funding": (_funding(),),
        "venue_funding": (_funding(),),
    }
    values.update(updates)
    return ReconciliationInput.model_validate(values)


def _reconciler(path: Path) -> ContinuousReconciler:
    return ContinuousReconciler(
        ReconciliationTolerance(
            quantity_absolute=Decimal("0.00000001"),
            money_absolute=Decimal("0.01"),
            balance_relative=Decimal("0.000001"),
            propagation_grace_seconds=10,
            maximum_snapshot_age_seconds=30,
        ),
        JsonlReconciliationAudit(path),
    )


def test_exact_order_fill_balance_position_and_funding_reconciliation_passes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reconciliation.jsonl"
    reconciler = _reconciler(path)
    result = reconciler.reconcile(_input())

    assert result.passed is True
    assert result.critical_count == 0
    assert result.warning_count == 0
    assert result.interlock_engaged is False
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
    recovered = _reconciler(path)
    assert recovered.sequence == 1
    assert recovered.head_hash == result.audit_hash


def test_every_reconciliation_dimension_is_classified_and_audited(
    tmp_path: Path,
) -> None:
    orphan_order = _order().model_copy(
        update={"client_order_id": "orphan", "exchange_order_id": "orphan-exchange"}
    )
    mismatched_fill = _fill().model_copy(update={"fee_amount": Decimal("0.2")})
    mismatched_balance = _balance().model_copy(update={"total": Decimal("999")})
    mismatched_position = _position().model_copy(
        update={"signed_quantity": Decimal("-0.9")}
    )
    mismatched_funding = _funding().model_copy(update={"amount": Decimal("0.4")})
    data = _input(
        source_health={"BYBIT": False},
        venue_orders=(_order(), orphan_order),
        venue_fills=(mismatched_fill,),
        venue_balances=(mismatched_balance,),
        venue_positions=(mismatched_position,),
        venue_funding=(mismatched_funding,),
    )

    result = _reconciler(tmp_path / "reconciliation.jsonl").reconcile(data)
    categories = {issue.category for issue in result.issues}
    codes = {issue.code for issue in result.issues}
    assert result.passed is False
    assert result.interlock_engaged is True
    assert categories == {
        ReconciliationCategory.CONNECTIVITY,
        ReconciliationCategory.ORDER,
        ReconciliationCategory.FILL,
        ReconciliationCategory.BALANCE,
        ReconciliationCategory.POSITION,
        ReconciliationCategory.FUNDING,
    }
    assert "ORPHAN_VENUE_ORDER" in codes
    assert "FILL_DETAILS_MISMATCH" in codes
    assert "BALANCE_TOTAL_MISMATCH" in codes
    assert "POSITION_DETAILS_MISMATCH" in codes
    assert "FUNDING_DETAILS_MISMATCH" in codes


def test_fill_and_funding_propagation_grace_warns_then_becomes_critical(
    tmp_path: Path,
) -> None:
    recent_fill = _fill().model_copy(update={"timestamp": NOW - timedelta(seconds=5)})
    recent_funding = _funding().model_copy(
        update={"settlement_timestamp": NOW - timedelta(seconds=5)}
    )
    warning = _reconciler(tmp_path / "warning.jsonl").reconcile(
        _input(
            local_fills=(),
            venue_fills=(recent_fill,),
            local_funding=(),
            venue_funding=(recent_funding,),
        )
    )
    assert warning.passed is True
    assert warning.warning_count == 2
    assert {
        issue.severity for issue in warning.issues
    } == {ReconciliationSeverity.WARNING}

    old_fill = recent_fill.model_copy(update={"timestamp": NOW - timedelta(seconds=11)})
    old_funding = recent_funding.model_copy(
        update={"settlement_timestamp": NOW - timedelta(seconds=11)}
    )
    critical = _reconciler(tmp_path / "critical.jsonl").reconcile(
        _input(
            local_fills=(),
            venue_fills=(old_fill,),
            local_funding=(),
            venue_funding=(old_funding,),
        )
    )
    assert critical.passed is False
    assert critical.critical_count == 2
    assert critical.interlock_engaged is True


def test_stale_current_private_snapshot_is_critical_but_old_history_is_valid(
    tmp_path: Path,
) -> None:
    stale_balance = _balance().model_copy(
        update={"timestamp": NOW - timedelta(seconds=31)}
    )
    result = _reconciler(tmp_path / "stale.jsonl").reconcile(
        _input(
            local_balances=(stale_balance,),
            venue_balances=(stale_balance,),
        )
    )
    assert result.passed is False
    assert any(issue.code == "STALE_PRIVATE_SNAPSHOT" for issue in result.issues)
    assert not any(
        issue.identity == _funding().key and issue.code == "STALE_PRIVATE_SNAPSHOT"
        for issue in result.issues
    )


def test_reconciliation_audit_hash_chain_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "reconciliation.jsonl"
    reconciler = _reconciler(path)
    reconciler.reconcile(_input())
    reconciler.reconcile(_input(as_of=NOW + timedelta(seconds=1)))

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[0]["passed"] = False
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        _reconciler(path)
