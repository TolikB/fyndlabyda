from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_arbitrage.domain.events import Side
from funding_arbitrage.portfolio.ledger import (
    GENESIS_HASH,
    DoubleEntryLedger,
    JsonlLedgerJournal,
    LedgerAccountKind,
    LedgerPosting,
    LedgerTransaction,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _ledger(path: Path) -> DoubleEntryLedger:
    return DoubleEntryLedger(JsonlLedgerJournal(path))


def test_every_transaction_balances_per_asset_and_is_idempotent(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path / "ledger.jsonl")
    first = ledger.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="usdt",
        amount=Decimal("1000"),
        timestamp=NOW,
    )
    duplicate = ledger.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="usdt",
        amount=Decimal("1000"),
        timestamp=NOW,
    )
    assert duplicate == first
    assert ledger.sequence == 1
    assert ledger.snapshot().trial_balance == {"USDT": Decimal("0")}

    with pytest.raises(ValueError, match="collision"):
        ledger.deposit(
            transaction_id="deposit-1",
            venue="gate",
            asset="usdt",
            amount=Decimal("1000"),
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="unbalanced"):
        ledger.post(
            transaction_id="bad",
            timestamp=NOW,
            reference_type="TEST",
            reference_id="bad",
            description="unbalanced transaction",
            postings=(
                LedgerPosting(
                    account="ASSET:CASH:BYBIT",
                    account_kind=LedgerAccountKind.ASSET,
                    asset="USDT",
                    amount=Decimal("1"),
                ),
                LedgerPosting(
                    account="EXPENSE:FEES:BYBIT",
                    account_kind=LedgerAccountKind.EXPENSE,
                    asset="USDT",
                    amount=Decimal("1"),
                ),
            ),
        )


def test_spot_round_trip_tracks_inventory_cash_fees_and_realized_pnl(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path / "ledger.jsonl")
    ledger.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("1000"),
        timestamp=NOW,
    )
    ledger.book_spot_fill(
        transaction_id="fill-entry",
        fill_id="fill-1",
        venue="bybit",
        position_id="position-1",
        strategy_id="stat-arb",
        side=Side.BUY,
        base_asset="BTC",
        quote_asset="USDT",
        quantity=Decimal("1"),
        price=Decimal("100"),
        fee_amount=Decimal("0.1"),
        fee_asset="USDT",
        timestamp=NOW + timedelta(seconds=1),
    )
    ledger.book_spot_fill(
        transaction_id="fill-exit",
        fill_id="fill-2",
        venue="bybit",
        position_id="position-1",
        strategy_id="stat-arb",
        side=Side.SELL,
        base_asset="BTC",
        quote_asset="USDT",
        quantity=Decimal("1"),
        price=Decimal("110"),
        fee_amount=Decimal("0.1"),
        fee_asset="USDT",
        timestamp=NOW + timedelta(seconds=2),
    )
    ledger.realize_spot_clearing(
        transaction_id="realize-1",
        venue="bybit",
        position_id="position-1",
        strategy_id="stat-arb",
        quote_asset="USDT",
        timestamp=NOW + timedelta(seconds=3),
    )

    assert ledger.balance("ASSET:INVENTORY:BYBIT:POSITION-1", "BTC") == 0
    assert ledger.balance("ASSET:TRADE_CLEARING:BYBIT:POSITION-1", "BTC") == 0
    assert ledger.balance("ASSET:TRADE_CLEARING:BYBIT:POSITION-1", "USDT") == 0
    snapshot = ledger.snapshot()
    assert snapshot.cash_by_asset == {"USDT": Decimal("1009.8")}
    assert snapshot.realized_pnl_by_asset == {"USDT": Decimal("10")}
    assert snapshot.fees_by_asset == {"USDT": Decimal("0.2")}
    assert snapshot.trial_balance.get("BTC", Decimal("0")) == 0
    assert snapshot.trial_balance["USDT"] == 0


def test_derivative_positions_marks_funding_and_realized_loss_are_attributed(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path / "ledger.jsonl")
    ledger.deposit(
        transaction_id="deposit-1",
        venue="gate",
        asset="USDT",
        amount=Decimal("500"),
        timestamp=NOW,
    )
    ledger.move_collateral(
        transaction_id="collateral-lock",
        venue="gate",
        asset="USDT",
        amount=Decimal("100"),
        lock=True,
        position_id="perp-1",
        timestamp=NOW,
    )
    ledger.book_derivative_fill(
        transaction_id="perp-open",
        fill_id="perp-fill-1",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        contract_asset="BTC-PERP",
        side=Side.SELL,
        quantity=Decimal("2"),
        timestamp=NOW + timedelta(seconds=1),
    )
    ledger.mark_unrealized_pnl(
        transaction_id="mark-1",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        asset="USDT",
        target_unrealized_pnl=Decimal("5"),
        timestamp=NOW + timedelta(seconds=2),
    )
    ledger.mark_unrealized_pnl(
        transaction_id="mark-2",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        asset="USDT",
        target_unrealized_pnl=Decimal("-2"),
        timestamp=NOW + timedelta(seconds=3),
    )
    ledger.post_funding(
        transaction_id="funding-1",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        asset="USDT",
        amount=Decimal("2.5"),
        timestamp=NOW + timedelta(seconds=4),
    )
    ledger.post_realized_pnl(
        transaction_id="realized-1",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        asset="USDT",
        amount=Decimal("-3"),
        timestamp=NOW + timedelta(seconds=5),
    )
    ledger.book_derivative_fill(
        transaction_id="perp-close",
        fill_id="perp-fill-2",
        venue="gate",
        position_id="perp-1",
        strategy_id="funding",
        contract_asset="BTC-PERP",
        side=Side.BUY,
        quantity=Decimal("2"),
        timestamp=NOW + timedelta(seconds=6),
    )
    ledger.move_collateral(
        transaction_id="collateral-release",
        venue="gate",
        asset="USDT",
        amount=Decimal("100"),
        lock=False,
        position_id="perp-1",
        timestamp=NOW + timedelta(seconds=7),
    )

    snapshot = ledger.snapshot()
    assert ledger.balance("ASSET:POSITION_QUANTITY:GATE:PERP-1", "BTC-PERP") == 0
    assert snapshot.collateral_by_asset.get("USDT", Decimal("0")) == 0
    assert snapshot.unrealized_pnl_by_asset == {"USDT": Decimal("-2")}
    assert snapshot.funding_by_asset == {"USDT": Decimal("2.5")}
    assert snapshot.realized_pnl_by_asset == {"USDT": Decimal("-3")}


def test_borrow_gas_and_transfer_costs_reconcile_without_residual_clearing(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path / "ledger.jsonl")
    ledger.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("1000"),
        timestamp=NOW,
    )
    ledger.post_borrow_principal(
        transaction_id="borrow-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("50"),
        borrow=True,
        timestamp=NOW,
    )
    ledger.post_expense(
        transaction_id="borrow-cost-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("0.5"),
        component="borrow_cost",
        timestamp=NOW,
    )
    ledger.post_borrow_principal(
        transaction_id="borrow-repay-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("50"),
        borrow=False,
        timestamp=NOW,
    )
    ledger.start_transfer(
        transaction_id="transfer-start-1",
        transfer_id="transfer-1",
        source_venue="bybit",
        asset="USDT",
        amount=Decimal("100"),
        timestamp=NOW,
    )
    ledger.complete_transfer(
        transaction_id="transfer-complete-1",
        transfer_id="transfer-1",
        source_venue="bybit",
        destination_venue="gate",
        asset="USDT",
        amount_sent=Decimal("100"),
        amount_received=Decimal("99"),
        timestamp=NOW,
    )
    ledger.post_expense(
        transaction_id="gas-1",
        venue="gate",
        asset="USDT",
        amount=Decimal("0.25"),
        component="gas",
        timestamp=NOW,
    )

    snapshot = ledger.snapshot()
    assert ledger.balance("LIABILITY:BORROWED:BYBIT", "USDT") == 0
    assert ledger.balance("ASSET:TRANSFER_IN_TRANSIT:BYBIT:TRANSFER-1", "USDT") == 0
    assert snapshot.borrow_cost_by_asset == {"USDT": Decimal("0.5")}
    assert snapshot.transfer_cost_by_asset == {"USDT": Decimal("1")}
    assert snapshot.gas_cost_by_asset == {"USDT": Decimal("0.25")}
    assert snapshot.cash_by_asset == {"USDT": Decimal("998.25")}


def test_hash_chain_restart_and_tamper_detection(tmp_path: Path) -> None:
    path = tmp_path / "ledger.jsonl"
    ledger = _ledger(path)
    ledger.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("100"),
        timestamp=NOW,
    )
    ledger.post_expense(
        transaction_id="fee-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("1"),
        component="fees",
        timestamp=NOW + timedelta(seconds=1),
    )
    recovered = _ledger(path)
    assert recovered.sequence == 2
    assert recovered.head_hash == ledger.head_hash
    assert recovered.snapshot() == ledger.snapshot()

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[0]["description"] = "tampered"
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        _ledger(path)

def _balanced_postings() -> tuple[LedgerPosting, LedgerPosting]:
    return (
        LedgerPosting(
            account="ASSET:CASH:BYBIT",
            account_kind=LedgerAccountKind.ASSET,
            asset="USDT",
            amount=Decimal("1"),
        ),
        LedgerPosting(
            account="EQUITY:CONTRIBUTED",
            account_kind=LedgerAccountKind.EQUITY,
            asset="USDT",
            amount=Decimal("-1"),
        ),
    )


def test_ledger_models_reject_blank_zero_prefix_and_reference_values() -> None:
    base = {
        "account": "ASSET:CASH",
        "account_kind": LedgerAccountKind.ASSET,
        "asset": "USDT",
        "amount": Decimal("1"),
    }
    for update, message in (
        ({"account": "  "}, "identity cannot be blank"),
        ({"amount": Decimal("0")}, "amount cannot be zero"),
        ({"account": "EXPENSE:FEES"}, "prefix disagrees"),
    ):
        with pytest.raises(ValidationError, match=message):
            LedgerPosting.model_validate(base | update)

    with pytest.raises(ValidationError, match="reference cannot be blank"):
        LedgerTransaction(
            sequence=1,
            transaction_id="transaction-1",
            timestamp=NOW,
            reference_type=" ",
            reference_id="reference-1",
            description="invalid blank reference",
            postings=_balanced_postings(),
            previous_hash=GENESIS_HASH,
            transaction_hash=GENESIS_HASH,
        )


def test_ledger_operations_reject_non_economic_inputs(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path / "invalid-operations.jsonl")

    with pytest.raises(ValueError, match="deposit must be positive"):
        ledger.deposit(
            transaction_id="zero-deposit",
            venue="bybit",
            asset="USDT",
            amount=Decimal("0"),
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="fill fee cannot be negative"):
        ledger.book_spot_fill(
            transaction_id="negative-fee",
            fill_id="fill-negative",
            venue="bybit",
            position_id="position-1",
            strategy_id="strategy-1",
            side=Side.BUY,
            base_asset="BTC",
            quote_asset="USDT",
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee_amount=Decimal("-0.01"),
            fee_asset="USDT",
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="no PnL to realize"):
        ledger.realize_spot_clearing(
            transaction_id="empty-clearing",
            venue="bybit",
            position_id="position-1",
            strategy_id="strategy-1",
            quote_asset="USDT",
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="funding cashflow cannot be zero"):
        ledger.post_funding(
            transaction_id="zero-funding",
            venue="bybit",
            position_id="position-1",
            strategy_id="strategy-1",
            asset="USDT",
            amount=Decimal("0"),
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="realized PnL cannot be zero"):
        ledger.post_realized_pnl(
            transaction_id="zero-pnl",
            venue="bybit",
            position_id="position-1",
            strategy_id="strategy-1",
            asset="USDT",
            amount=Decimal("0"),
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="unsupported ledger expense"):
        ledger.post_expense(
            transaction_id="bad-expense",
            venue="bybit",
            asset="USDT",
            amount=Decimal("1"),
            component="unknown",
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="cannot exceed sent amount"):
        ledger.complete_transfer(
            transaction_id="bad-transfer",
            transfer_id="transfer-1",
            source_venue="bybit",
            destination_venue="gate",
            asset="USDT",
            amount_sent=Decimal("10"),
            amount_received=Decimal("11"),
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="mark is unchanged"):
        ledger.mark_unrealized_pnl(
            transaction_id="unchanged-mark",
            venue="bybit",
            position_id="position-1",
            strategy_id="strategy-1",
            asset="USDT",
            target_unrealized_pnl=Decimal("0"),
            timestamp=NOW,
        )


def test_ledger_snapshot_and_apply_reject_in_memory_corruption(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path / "snapshot.jsonl")
    ledger.balances[("ASSET:CASH:BYBIT", "USDT")] = Decimal("1")
    with pytest.raises(ValueError, match="global trial balance failed"):
        ledger.snapshot()

    source = _ledger(tmp_path / "source.jsonl")
    transaction = source.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("10"),
        timestamp=NOW,
    )
    sequence_target = _ledger(tmp_path / "sequence-target.jsonl")
    with pytest.raises(ValueError, match="apply sequence gap"):
        sequence_target._apply(transaction.model_copy(update={"sequence": 2}))

    with pytest.raises(ValueError, match="apply hash chain mismatch"):
        source._apply(
            transaction.model_copy(
                update={"sequence": 2, "previous_hash": "f" * 64}
            )
        )


class DuplicateReplayJournal:
    def __init__(self, transaction: LedgerTransaction) -> None:
        self.transaction = transaction

    def load(self) -> tuple[LedgerTransaction, ...]:
        return self.transaction, self.transaction

    def append(self, transaction: LedgerTransaction) -> None:
        raise AssertionError("replay journal must not append")


def test_ledger_replay_rejects_duplicate_transaction_id(tmp_path: Path) -> None:
    source = _ledger(tmp_path / "duplicate-source.jsonl")
    transaction = source.deposit(
        transaction_id="deposit-1",
        venue="bybit",
        asset="USDT",
        amount=Decimal("10"),
        timestamp=NOW,
    )

    with pytest.raises(ValueError, match="duplicate ledger transaction ID"):
        DoubleEntryLedger(DuplicateReplayJournal(transaction))  # type: ignore[arg-type]


def test_jsonl_ledger_rejects_sequence_and_previous_hash_corruption(
    tmp_path: Path,
) -> None:
    def _two_transactions(path: Path) -> list[dict[str, object]]:
        ledger = _ledger(path)
        ledger.deposit(
            transaction_id="deposit-1",
            venue="bybit",
            asset="USDT",
            amount=Decimal("10"),
            timestamp=NOW,
        )
        ledger.post_expense(
            transaction_id="fee-1",
            venue="bybit",
            asset="USDT",
            amount=Decimal("1"),
            component="fees",
            timestamp=NOW + timedelta(seconds=1),
        )
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]

    sequence_path = tmp_path / "bad-sequence.jsonl"
    sequence_rows = _two_transactions(sequence_path)
    sequence_rows[1]["sequence"] = 3
    sequence_path.write_text(
        "\n".join(json.dumps(row) for row in sequence_rows) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="sequence is not contiguous"):
        JsonlLedgerJournal(sequence_path).load()

    hash_path = tmp_path / "bad-previous-hash.jsonl"
    hash_rows = _two_transactions(hash_path)
    hash_rows[1]["previous_hash"] = GENESIS_HASH
    hash_path.write_text(
        "\n".join(json.dumps(row) for row in hash_rows) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="previous hash mismatch"):
        JsonlLedgerJournal(hash_path).load()
