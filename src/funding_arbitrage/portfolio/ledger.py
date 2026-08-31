"""Hash-chained multi-asset double-entry trading ledger."""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import Side

ZERO = Decimal("0")
GENESIS_HASH = "0" * 64


class LedgerAccountKind(StrEnum):
    ASSET = "ASSET"
    LIABILITY = "LIABILITY"
    EQUITY = "EQUITY"
    REVENUE = "REVENUE"
    EXPENSE = "EXPENSE"


class LedgerPosting(BaseModel):
    """Debit-positive posting; every asset must balance to zero per transaction."""

    model_config = ConfigDict(frozen=True)

    account: str = Field(min_length=1)
    account_kind: LedgerAccountKind
    asset: str = Field(min_length=1)
    amount: Decimal
    venue: str | None = None
    strategy_id: str | None = None
    position_id: str | None = None

    @field_validator("account", "asset", "venue", "strategy_id", "position_id")
    @classmethod
    def normalize_identity(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("ledger identity cannot be blank")
        return normalized

    @model_validator(mode="after")
    def reject_zero_posting(self) -> LedgerPosting:
        if self.amount == 0:
            raise ValueError("ledger posting amount cannot be zero")
        if not self.account.startswith(f"{self.account_kind.value}:"):
            raise ValueError("ledger account prefix disagrees with account kind")
        return self


class LedgerTransaction(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    transaction_id: str = Field(min_length=1)
    timestamp: datetime
    reference_type: str = Field(min_length=1)
    reference_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    postings: tuple[LedgerPosting, ...] = Field(min_length=2)
    previous_hash: str = Field(min_length=64, max_length=64)
    transaction_hash: str = Field(min_length=64, max_length=64)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("reference_type", "reference_id")
    @classmethod
    def normalize_reference(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("ledger reference cannot be blank")
        return normalized

    @model_validator(mode="after")
    def validate_trial_balance(self) -> LedgerTransaction:
        trial: dict[str, Decimal] = defaultdict(Decimal)
        for posting in self.postings:
            trial[posting.asset] += posting.amount
        unbalanced = {asset: amount for asset, amount in trial.items() if amount != 0}
        if unbalanced:
            raise ValueError(f"ledger transaction is unbalanced by asset: {unbalanced}")
        return self


class LedgerSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(ge=0)
    head_hash: str
    balances: dict[str, dict[str, Decimal]]
    trial_balance: dict[str, Decimal]
    cash_by_asset: dict[str, Decimal]
    collateral_by_asset: dict[str, Decimal]
    liabilities_by_asset: dict[str, Decimal]
    realized_pnl_by_asset: dict[str, Decimal]
    unrealized_pnl_by_asset: dict[str, Decimal]
    fees_by_asset: dict[str, Decimal]
    funding_by_asset: dict[str, Decimal]
    borrow_cost_by_asset: dict[str, Decimal]
    gas_cost_by_asset: dict[str, Decimal]
    transfer_cost_by_asset: dict[str, Decimal]


class JsonlLedgerJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, transaction: LedgerTransaction) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(transaction.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[LedgerTransaction, ...]:
        if not self.path.exists():
            return ()
        transactions = tuple(
            LedgerTransaction.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        previous_hash = GENESIS_HASH
        for sequence, transaction in enumerate(transactions, start=1):
            if transaction.sequence != sequence:
                raise ValueError("ledger journal sequence is not contiguous")
            if transaction.previous_hash != previous_hash:
                raise ValueError("ledger hash chain previous hash mismatch")
            if transaction.transaction_hash != ledger_transaction_hash(transaction):
                raise ValueError("ledger transaction hash mismatch")
            previous_hash = transaction.transaction_hash
        return transactions


class DoubleEntryLedger:
    def __init__(self, journal: JsonlLedgerJournal) -> None:
        self.journal = journal
        self.transactions: dict[str, LedgerTransaction] = {}
        self.balances: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
        self.sequence = 0
        self.head_hash = GENESIS_HASH
        self._recover()

    def post(
        self,
        *,
        transaction_id: str,
        timestamp: datetime,
        reference_type: str,
        reference_id: str,
        description: str,
        postings: tuple[LedgerPosting, ...],
    ) -> LedgerTransaction:
        existing = self.transactions.get(transaction_id)
        if existing is not None:
            identity = (
                _utc(timestamp),
                reference_type.upper(),
                reference_id.upper(),
                description,
                postings,
            )
            existing_identity = (
                existing.timestamp,
                existing.reference_type,
                existing.reference_id,
                existing.description,
                existing.postings,
            )
            if identity != existing_identity:
                raise ValueError("ledger transaction ID collision")
            return existing
        transaction = build_ledger_transaction(
            sequence=self.sequence + 1,
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type=reference_type,
            reference_id=reference_id,
            description=description,
            postings=postings,
            previous_hash=self.head_hash,
        )
        self.journal.append(transaction)
        self._apply(transaction)
        return transaction

    def deposit(
        self,
        *,
        transaction_id: str,
        venue: str,
        asset: str,
        amount: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(amount, "deposit")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="DEPOSIT",
            reference_id=transaction_id,
            description="External capital deposit",
            postings=(
                _posting("ASSET:CASH", LedgerAccountKind.ASSET, asset, amount, venue),
                _posting(
                    "EQUITY:CONTRIBUTED",
                    LedgerAccountKind.EQUITY,
                    asset,
                    -amount,
                ),
            ),
        )

    def move_collateral(
        self,
        *,
        transaction_id: str,
        venue: str,
        asset: str,
        amount: Decimal,
        lock: bool,
        timestamp: datetime,
        position_id: str | None = None,
    ) -> LedgerTransaction:
        _positive(amount, "collateral movement")
        cash_amount = -amount if lock else amount
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="COLLATERAL",
            reference_id=position_id or transaction_id,
            description="Lock collateral" if lock else "Release collateral",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    cash_amount,
                    venue,
                    position_id=position_id,
                ),
                _posting(
                    "ASSET:COLLATERAL",
                    LedgerAccountKind.ASSET,
                    asset,
                    -cash_amount,
                    venue,
                    position_id=position_id,
                ),
            ),
        )

    def book_spot_fill(
        self,
        *,
        transaction_id: str,
        fill_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        side: Side,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        price: Decimal,
        fee_amount: Decimal,
        fee_asset: str,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(quantity, "fill quantity")
        _positive(price, "fill price")
        if fee_amount < 0:
            raise ValueError("fill fee cannot be negative")
        direction = Decimal("1") if side is Side.BUY else Decimal("-1")
        quote_notional = quantity * price
        postings = [
            _posting(
                "ASSET:INVENTORY",
                LedgerAccountKind.ASSET,
                base_asset,
                direction * quantity,
                venue,
                strategy_id,
                position_id,
            ),
            _posting(
                "ASSET:TRADE_CLEARING",
                LedgerAccountKind.ASSET,
                base_asset,
                -direction * quantity,
                venue,
                strategy_id,
                position_id,
            ),
            _posting(
                "ASSET:CASH",
                LedgerAccountKind.ASSET,
                quote_asset,
                -direction * quote_notional,
                venue,
                strategy_id,
                position_id,
            ),
            _posting(
                "ASSET:TRADE_CLEARING",
                LedgerAccountKind.ASSET,
                quote_asset,
                direction * quote_notional,
                venue,
                strategy_id,
                position_id,
            ),
        ]
        if fee_amount > 0:
            postings.extend(
                (
                    _posting(
                        "ASSET:CASH",
                        LedgerAccountKind.ASSET,
                        fee_asset,
                        -fee_amount,
                        venue,
                        strategy_id,
                        position_id,
                    ),
                    _posting(
                        "EXPENSE:FEES",
                        LedgerAccountKind.EXPENSE,
                        fee_asset,
                        fee_amount,
                        venue,
                        strategy_id,
                        position_id,
                    ),
                )
            )
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="FILL",
            reference_id=fill_id,
            description=f"{side.value} spot fill",
            postings=tuple(postings),
        )

    def realize_spot_clearing(
        self,
        *,
        transaction_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        quote_asset: str,
        timestamp: datetime,
    ) -> LedgerTransaction:
        clearing_account = _account("ASSET:TRADE_CLEARING", venue, position_id)
        balance = self.balance(clearing_account, quote_asset)
        if balance == 0:
            raise ValueError("spot clearing has no PnL to realize")
        pnl_account = "REVENUE:REALIZED_PNL" if balance < 0 else "EXPENSE:REALIZED_LOSS"
        pnl_kind = (
            LedgerAccountKind.REVENUE if balance < 0 else LedgerAccountKind.EXPENSE
        )
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="POSITION_CLOSE",
            reference_id=position_id,
            description="Realize spot round-trip clearing PnL",
            postings=(
                _posting(
                    "ASSET:TRADE_CLEARING",
                    LedgerAccountKind.ASSET,
                    quote_asset,
                    -balance,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    pnl_account,
                    pnl_kind,
                    quote_asset,
                    balance,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def post_funding(
        self,
        *,
        transaction_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        asset: str,
        amount: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        if amount == 0:
            raise ValueError("funding cashflow cannot be zero")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="FUNDING",
            reference_id=transaction_id,
            description="Funding settlement",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    "REVENUE:FUNDING",
                    LedgerAccountKind.REVENUE,
                    asset,
                    -amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def book_derivative_fill(
        self,
        *,
        transaction_id: str,
        fill_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        contract_asset: str,
        side: Side,
        quantity: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(quantity, "derivative fill quantity")
        direction = Decimal("1") if side is Side.BUY else Decimal("-1")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="DERIVATIVE_FILL",
            reference_id=fill_id,
            description=f"{side.value} derivative fill",
            postings=(
                _posting(
                    "ASSET:POSITION_QUANTITY",
                    LedgerAccountKind.ASSET,
                    contract_asset,
                    direction * quantity,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    "ASSET:POSITION_CLEARING",
                    LedgerAccountKind.ASSET,
                    contract_asset,
                    -direction * quantity,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def post_realized_pnl(
        self,
        *,
        transaction_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        asset: str,
        amount: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        if amount == 0:
            raise ValueError("realized PnL cannot be zero")
        pnl_account = "REVENUE:REALIZED_PNL" if amount > 0 else "EXPENSE:REALIZED_LOSS"
        pnl_kind = (
            LedgerAccountKind.REVENUE if amount > 0 else LedgerAccountKind.EXPENSE
        )
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="REALIZED_PNL",
            reference_id=position_id,
            description="Settle realized derivative PnL",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    pnl_account,
                    pnl_kind,
                    asset,
                    -amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def post_expense(
        self,
        *,
        transaction_id: str,
        venue: str,
        asset: str,
        amount: Decimal,
        component: str,
        timestamp: datetime,
        position_id: str | None = None,
        strategy_id: str | None = None,
    ) -> LedgerTransaction:
        _positive(amount, "expense")
        normalized = component.strip().upper()
        allowed = {"FEES", "BORROW_COST", "GAS", "TRANSFER_COST"}
        if normalized not in allowed:
            raise ValueError("unsupported ledger expense component")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type=normalized,
            reference_id=transaction_id,
            description=f"{normalized} expense",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    -amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    f"EXPENSE:{normalized}",
                    LedgerAccountKind.EXPENSE,
                    asset,
                    amount,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def post_borrow_principal(
        self,
        *,
        transaction_id: str,
        venue: str,
        asset: str,
        amount: Decimal,
        borrow: bool,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(amount, "borrow principal")
        cash_amount = amount if borrow else -amount
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="BORROW" if borrow else "BORROW_REPAYMENT",
            reference_id=transaction_id,
            description="Borrow principal" if borrow else "Repay borrow principal",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    cash_amount,
                    venue,
                ),
                _posting(
                    "LIABILITY:BORROWED",
                    LedgerAccountKind.LIABILITY,
                    asset,
                    -cash_amount,
                    venue,
                ),
            ),
        )

    def start_transfer(
        self,
        *,
        transaction_id: str,
        transfer_id: str,
        source_venue: str,
        asset: str,
        amount: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(amount, "transfer")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="TRANSFER_OUT",
            reference_id=transfer_id,
            description="Move funds into transfer clearing",
            postings=(
                _posting(
                    "ASSET:CASH",
                    LedgerAccountKind.ASSET,
                    asset,
                    -amount,
                    source_venue,
                ),
                _posting(
                    "ASSET:TRANSFER_IN_TRANSIT",
                    LedgerAccountKind.ASSET,
                    asset,
                    amount,
                    source_venue,
                    position_id=transfer_id,
                ),
            ),
        )

    def complete_transfer(
        self,
        *,
        transaction_id: str,
        transfer_id: str,
        source_venue: str,
        destination_venue: str,
        asset: str,
        amount_sent: Decimal,
        amount_received: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        _positive(amount_sent, "sent transfer")
        _positive(amount_received, "received transfer")
        if amount_received > amount_sent:
            raise ValueError("received transfer cannot exceed sent amount")
        fee = amount_sent - amount_received
        postings = [
            _posting(
                "ASSET:TRANSFER_IN_TRANSIT",
                LedgerAccountKind.ASSET,
                asset,
                -amount_sent,
                source_venue,
                position_id=transfer_id,
            ),
            _posting(
                "ASSET:CASH",
                LedgerAccountKind.ASSET,
                asset,
                amount_received,
                destination_venue,
            ),
        ]
        if fee > 0:
            postings.append(
                _posting(
                    "EXPENSE:TRANSFER_COST",
                    LedgerAccountKind.EXPENSE,
                    asset,
                    fee,
                    source_venue,
                    position_id=transfer_id,
                )
            )
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="TRANSFER_IN",
            reference_id=transfer_id,
            description="Settle transfer clearing",
            postings=tuple(postings),
        )

    def mark_unrealized_pnl(
        self,
        *,
        transaction_id: str,
        venue: str,
        position_id: str,
        strategy_id: str,
        asset: str,
        target_unrealized_pnl: Decimal,
        timestamp: datetime,
    ) -> LedgerTransaction:
        account = _account("ASSET:UNREALIZED_PNL", venue, position_id)
        current = self.balance(account, asset)
        delta = target_unrealized_pnl - current
        if delta == 0:
            raise ValueError("unrealized PnL mark is unchanged")
        return self.post(
            transaction_id=transaction_id,
            timestamp=timestamp,
            reference_type="MARK_TO_MARKET",
            reference_id=position_id,
            description="Update unrealized PnL",
            postings=(
                _posting(
                    "ASSET:UNREALIZED_PNL",
                    LedgerAccountKind.ASSET,
                    asset,
                    delta,
                    venue,
                    strategy_id,
                    position_id,
                ),
                _posting(
                    "REVENUE:UNREALIZED_PNL",
                    LedgerAccountKind.REVENUE,
                    asset,
                    -delta,
                    venue,
                    strategy_id,
                    position_id,
                ),
            ),
        )

    def balance(self, account: str, asset: str) -> Decimal:
        return self.balances[(_normalize(account), _normalize(asset))]

    def snapshot(self) -> LedgerSnapshot:
        grouped: dict[str, dict[str, Decimal]] = defaultdict(dict)
        trial: dict[str, Decimal] = defaultdict(Decimal)
        for (account, asset), amount in sorted(self.balances.items()):
            if amount == 0:
                continue
            grouped[account][asset] = amount
            trial[asset] += amount
        if any(amount != 0 for amount in trial.values()):
            raise ValueError("ledger global trial balance failed")
        return LedgerSnapshot(
            sequence=self.sequence,
            head_hash=self.head_hash,
            balances=dict(grouped),
            trial_balance=dict(trial),
            cash_by_asset=self._sum_accounts("ASSET:CASH"),
            collateral_by_asset=self._sum_accounts("ASSET:COLLATERAL"),
            liabilities_by_asset=self._sum_accounts("LIABILITY:BORROWED"),
            realized_pnl_by_asset=_net_component(
                self.balances,
                ("REVENUE:REALIZED_PNL", "EXPENSE:REALIZED_LOSS"),
            ),
            unrealized_pnl_by_asset=_net_component(
                self.balances,
                ("REVENUE:UNREALIZED_PNL",),
            ),
            fees_by_asset=self._sum_accounts("EXPENSE:FEES"),
            funding_by_asset=_net_component(
                self.balances,
                ("REVENUE:FUNDING",),
            ),
            borrow_cost_by_asset=self._sum_accounts("EXPENSE:BORROW_COST"),
            gas_cost_by_asset=self._sum_accounts("EXPENSE:GAS"),
            transfer_cost_by_asset=self._sum_accounts("EXPENSE:TRANSFER_COST"),
        )

    def _sum_accounts(self, prefix: str) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = defaultdict(Decimal)
        normalized = _normalize(prefix)
        for (account, asset), amount in self.balances.items():
            if account.startswith(normalized):
                totals[asset] += amount
        return dict(totals)

    def _recover(self) -> None:
        for transaction in self.journal.load():
            if transaction.transaction_id in self.transactions:
                raise ValueError("duplicate ledger transaction ID during replay")
            self._apply(transaction)

    def _apply(self, transaction: LedgerTransaction) -> None:
        if transaction.sequence != self.sequence + 1:
            raise ValueError("ledger apply sequence gap")
        if transaction.previous_hash != self.head_hash:
            raise ValueError("ledger apply hash chain mismatch")
        for posting in transaction.postings:
            self.balances[(posting.account, posting.asset)] += posting.amount
        self.transactions[transaction.transaction_id] = transaction
        self.sequence = transaction.sequence
        self.head_hash = transaction.transaction_hash


def _posting(
    account_prefix: str,
    kind: LedgerAccountKind,
    asset: str,
    amount: Decimal,
    venue: str | None = None,
    strategy_id: str | None = None,
    position_id: str | None = None,
) -> LedgerPosting:
    return LedgerPosting(
        account=_account(account_prefix, venue, position_id),
        account_kind=kind,
        asset=asset,
        amount=amount,
        venue=venue,
        strategy_id=strategy_id,
        position_id=position_id,
    )


def _account(prefix: str, venue: str | None, position_id: str | None) -> str:
    parts = [_normalize(prefix)]
    if venue:
        parts.append(_normalize(venue))
    if position_id:
        parts.append(_normalize(position_id))
    return ":".join(parts)


def _net_component(
    balances: dict[tuple[str, str], Decimal],
    prefixes: tuple[str, ...],
) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(Decimal)
    normalized = tuple(_normalize(prefix) for prefix in prefixes)
    for (account, asset), amount in balances.items():
        if account.startswith(normalized):
            kind = account.split(":", 1)[0]
            totals[asset] += -amount if kind == LedgerAccountKind.REVENUE.value else -amount
    return dict(totals)


def build_ledger_transaction(
    *,
    sequence: int,
    transaction_id: str,
    timestamp: datetime,
    reference_type: str,
    reference_id: str,
    description: str,
    postings: tuple[LedgerPosting, ...],
    previous_hash: str,
) -> LedgerTransaction:
    """Build one validated hash-chained transaction for any durable journal."""

    candidate = LedgerTransaction(
        sequence=sequence,
        transaction_id=transaction_id,
        timestamp=timestamp,
        reference_type=reference_type,
        reference_id=reference_id,
        description=description,
        postings=postings,
        previous_hash=previous_hash,
        transaction_hash=GENESIS_HASH,
    )
    return candidate.model_copy(
        update={"transaction_hash": ledger_transaction_hash(candidate)}
    )


def ledger_transaction_hash(transaction: LedgerTransaction) -> str:
    """Return the canonical content hash used by file and database journals."""

    payload = transaction.model_dump(mode="json", exclude={"transaction_hash"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _normalize(value: str) -> str:
    return value.strip().upper()


def _positive(value: Decimal, label: str) -> None:
    if value <= 0:
        raise ValueError(f"{label} must be positive")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
