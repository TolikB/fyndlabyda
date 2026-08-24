from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

import funding_arbitrage.execution.oms as oms_module
from funding_arbitrage.domain.decisions import ExecutionReport, RiskDecision
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    Side,
)
from funding_arbitrage.execution.oms import (
    DurableOMS,
    InMemoryOMSJournal,
    JsonlOMSJournal,
    OMSEventType,
    OMSJournalEntry,
    OMSOrderSnapshot,
)

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


def test_jsonl_journal_syncs_each_entry_and_closes_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "durable.jsonl"
    syncs: list[int] = []
    original_sync = oms_module._sync_data

    def record_sync(descriptor: int) -> None:
        syncs.append(descriptor)
        original_sync(descriptor)

    monkeypatch.setattr(oms_module, "_sync_data", record_sync)
    journal = JsonlOMSJournal(path)
    syncs.clear()
    oms = DurableOMS(journal)
    order = _create(oms)
    assert len(syncs) == 1
    if oms_module.os.name == "posix":
        assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text(encoding="utf-8"))["event_type"] == "CREATED"

    journal.close()
    journal.close()
    assert journal.closed is True
    with pytest.raises(RuntimeError, match="journal is closed"):
        oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))


def test_jsonl_journal_context_cleanup_does_not_change_replay(
    tmp_path: Path,
) -> None:
    path = tmp_path / "replay.jsonl"
    journal = JsonlOMSJournal(path)
    order = _create(DurableOMS(journal))
    journal.close()

    recovered_journal = JsonlOMSJournal(path)
    recovered = DurableOMS(recovered_journal)
    assert recovered.orders[order.client_order_id] == order
    recovered_journal.close()


def test_jsonl_journal_io_failure_poison_requires_restart_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "io-failure.jsonl"
    journal = JsonlOMSJournal(path)
    oms = DurableOMS(journal)
    order = _create(oms)

    def fail_sync(_descriptor: int) -> None:
        raise OSError("simulated durability failure")

    monkeypatch.setattr(oms_module, "_sync_data", fail_sync)
    with pytest.raises(OSError, match="simulated durability failure"):
        oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    assert journal.closed is True
    assert oms.orders[order.client_order_id].status is OrderStatus.NEW
    with pytest.raises(RuntimeError, match="journal is closed"):
        oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))

    monkeypatch.undo()
    recovered_journal = JsonlOMSJournal(path)
    recovered = DurableOMS(recovered_journal)
    assert recovered.orders[order.client_order_id].status is OrderStatus.SUBMITTING
    assert [entry.sequence for entry in recovered_journal.load()] == [1, 2]
    recovered_journal.close()


def test_jsonl_journal_repairs_only_torn_final_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "partial-write.jsonl"
    journal = JsonlOMSJournal(path)
    oms = DurableOMS(journal)
    order = _create(oms)
    original_write = oms_module.os.write
    writes = 0

    def partial_then_fail(descriptor: int, payload: bytes | memoryview) -> int:
        nonlocal writes
        writes += 1
        if writes == 1:
            prefix = payload[: max(1, len(payload) // 2)]
            return original_write(descriptor, prefix)
        raise OSError("simulated partial journal write")

    monkeypatch.setattr(oms_module.os, "write", partial_then_fail)
    with pytest.raises(OSError, match="simulated partial journal write"):
        oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    assert journal.closed is True
    assert not path.read_bytes().endswith(b"\n")

    monkeypatch.undo()
    recovered_journal = JsonlOMSJournal(path)
    recovered = DurableOMS(recovered_journal)
    assert recovered.orders[order.client_order_id].status is OrderStatus.NEW
    assert [entry.sequence for entry in recovered_journal.load()] == [1]
    repaired = path.read_bytes()
    assert repaired.endswith(b"\n")
    assert len(repaired.splitlines()) == 1
    recovered_journal.close()


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

def test_snapshot_and_in_memory_journal_reject_corrupt_state(tmp_path: Path) -> None:
    order = _create(DurableOMS(JsonlOMSJournal(tmp_path / "snapshot.jsonl")))

    with pytest.raises(ValidationError, match="cannot exceed requested"):
        OMSOrderSnapshot.model_validate(
            order.model_dump() | {"filled_quantity": Decimal("2.01")}
        )

    journal = InMemoryOMSJournal()
    entry = OMSJournalEntry(
        sequence=2,
        event_id="event-2",
        event_type=OMSEventType.CREATED,
        timestamp=NOW,
        snapshot=order,
    )
    with pytest.raises(ValueError, match="sequence is not contiguous"):
        journal.append(entry)


def test_oms_rejects_invalid_submit_report_cancel_and_expiry_states(
    tmp_path: Path,
) -> None:
    oms = DurableOMS(JsonlOMSJournal(tmp_path / "lifecycle.jsonl"))
    with pytest.raises(ValueError, match="limit order requires limit price"):
        oms.create_order(
            _decision(),
            leg_index=1,
            instrument=_instrument(),
            side=Side.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("1"),
            limit_price=None,
            reduce_only=False,
            timestamp=NOW,
        )

    order = _create(oms)
    prepared = oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))
    assert oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=2)) == prepared
    with pytest.raises(ValueError, match="order is not cancellable"):
        oms.prepare_cancel(order.client_order_id, NOW + timedelta(seconds=2))

    mismatched = _report(
        order.client_order_id,
        OrderStatus.ACKNOWLEDGED,
        "0",
        offset=2,
    ).model_copy(update={"requested_quantity": Decimal("3")})
    with pytest.raises(ValueError, match="requested quantity mismatch"):
        oms.apply_report(mismatched)

    acknowledged = oms.apply_report(
        _report(order.client_order_id, OrderStatus.ACKNOWLEDGED, "0", offset=3)
    )
    with pytest.raises(ValueError, match="only NEW"):
        oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=4))

    partial = oms.apply_report(
        _report(
            order.client_order_id,
            OrderStatus.ACKNOWLEDGED,
            "0.5",
            offset=5,
        )
    )
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    backwards = _report(
        order.client_order_id,
        OrderStatus.PARTIALLY_FILLED,
        "0.4",
        offset=6,
    )
    with pytest.raises(ValueError, match="moved backwards"):
        oms.apply_report(backwards)

    invalid_transition = _report(
        order.client_order_id,
        OrderStatus.ACKNOWLEDGED,
        "0.5",
        offset=7,
    )
    with pytest.raises(ValueError, match="invalid OMS report transition"):
        oms.apply_report(invalid_transition)

    pending = oms.prepare_cancel(order.client_order_id, NOW + timedelta(seconds=8))
    assert oms.prepare_cancel(order.client_order_id, NOW + timedelta(seconds=9)) == pending
    with pytest.raises(ValueError, match="order cannot expire"):
        oms.expire(order.client_order_id, NOW + timedelta(seconds=10))
    assert acknowledged.exchange_order_id == "venue-order-7"

    with pytest.raises(ValueError, match="unknown OMS client order ID"):
        oms.prepare_submit("missing", NOW)


def test_unknown_and_reconciliation_are_strict_and_terminal_safe(tmp_path: Path) -> None:
    terminal_oms = DurableOMS(JsonlOMSJournal(tmp_path / "terminal.jsonl"))
    terminal = _create(terminal_oms)
    terminal_oms.prepare_submit(terminal.client_order_id, NOW + timedelta(seconds=1))
    terminal_oms.apply_report(
        _report(terminal.client_order_id, OrderStatus.FILLED, "2", offset=2)
    )
    with pytest.raises(ValueError, match="terminal order cannot become unknown"):
        terminal_oms.mark_unknown(terminal.client_order_id, NOW, "late timeout")

    oms = DurableOMS(JsonlOMSJournal(tmp_path / "reconcile.jsonl"))
    order = _create(oms)
    with pytest.raises(ValueError, match="only unknown/cancel-pending"):
        oms.start_reconciliation(order.client_order_id, NOW)
    with pytest.raises(ValueError, match="order is not reconciling"):
        oms.apply_reconciliation(
            order.client_order_id,
            status=OrderStatus.ACKNOWLEDGED,
            filled_quantity=Decimal("0"),
            exchange_order_id=None,
            timestamp=NOW,
        )

    oms.prepare_submit(order.client_order_id, NOW)
    oms.mark_unknown(order.client_order_id, NOW, "timeout")
    oms.start_reconciliation(order.client_order_id, NOW)
    for status in (
        OrderStatus.UNKNOWN,
        OrderStatus.RECONCILING,
        OrderStatus.SUBMITTING,
    ):
        with pytest.raises(ValueError, match="observable venue state"):
            oms.apply_reconciliation(
                order.client_order_id,
                status=status,
                filled_quantity=Decimal("0"),
                exchange_order_id=None,
                timestamp=NOW,
            )
    with pytest.raises(ValueError, match="fill quantity is invalid"):
        oms.apply_reconciliation(
            order.client_order_id,
            status=OrderStatus.ACKNOWLEDGED,
            filled_quantity=Decimal("2.01"),
            exchange_order_id=None,
            timestamp=NOW,
        )

    filled = oms.apply_reconciliation(
        order.client_order_id,
        status=OrderStatus.ACKNOWLEDGED,
        filled_quantity=Decimal("2"),
        exchange_order_id="venue-reconciled",
        timestamp=NOW,
    )
    assert filled.status is OrderStatus.FILLED
    assert filled.exchange_order_id == "venue-reconciled"


def test_recovery_rejects_sequence_first_version_and_version_gaps() -> None:
    valid = InMemoryOMSJournal()
    oms = DurableOMS(valid)
    order = _create(oms)
    oms.prepare_submit(order.client_order_id, NOW + timedelta(seconds=1))

    sequence_gap = InMemoryOMSJournal()
    sequence_gap.entries = [
        valid.entries[0].model_copy(update={"sequence": 2})
    ]
    with pytest.raises(ValueError, match="replay sequence gap"):
        DurableOMS(sequence_gap)

    first_version_gap = InMemoryOMSJournal()
    first_version_gap.entries = [
        valid.entries[0].model_copy(
            update={
                "snapshot": valid.entries[0].snapshot.model_copy(update={"version": 2})
            }
        )
    ]
    with pytest.raises(ValueError, match="first order version"):
        DurableOMS(first_version_gap)

    order_version_gap = InMemoryOMSJournal()
    order_version_gap.entries = [
        valid.entries[0],
        valid.entries[1].model_copy(
            update={
                "snapshot": valid.entries[1].snapshot.model_copy(update={"version": 3})
            }
        ),
    ]
    with pytest.raises(ValueError, match="order version gap"):
        DurableOMS(order_version_gap)
