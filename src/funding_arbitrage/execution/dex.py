"""Fail-closed DEX transaction planning, nonce control, replacement, and finality."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import RiskDecision

ZERO = Decimal("0")
BPS = Decimal("10000")
GWEI = Decimal("1000000000")


class DexTransactionKind(StrEnum):
    APPROVAL = "APPROVAL"
    SWAP = "SWAP"


class DexTransactionStatus(StrEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    UNKNOWN = "UNKNOWN"
    INCLUDED = "INCLUDED"
    CONFIRMED = "CONFIRMED"
    REVERTED = "REVERTED"
    REORGED = "REORGED"
    REPLACED = "REPLACED"
    DROPPED = "DROPPED"


class DexEventType(StrEnum):
    PLAN_PREPARED = "PLAN_PREPARED"
    SUBMISSION_PREPARED = "SUBMISSION_PREPARED"
    SUBMITTED = "SUBMITTED"
    UNKNOWN_MARKED = "UNKNOWN_MARKED"
    RECEIPT_OBSERVED = "RECEIPT_OBSERVED"
    FINALIZED = "FINALIZED"
    REORG_DETECTED = "REORG_DETECTED"
    REPLACEMENT_PREPARED = "REPLACEMENT_PREPARED"
    DROPPED = "DROPPED"
    INTERLOCK_ENGAGED = "INTERLOCK_ENGAGED"


class DexExecutionPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    chain_id: int = Field(gt=0)
    required_confirmations: int = Field(default=12, gt=0)
    maximum_quote_age_seconds: int = Field(default=15, gt=0)
    maximum_calldata_lifetime_seconds: int = Field(default=60, gt=0)
    maximum_slippage_bps: Decimal = Field(default=Decimal("50"), ge=0)
    maximum_price_impact_bps: Decimal = Field(default=Decimal("100"), ge=0)
    gas_limit_buffer: Decimal = Field(default=Decimal("1.20"), ge=1)
    base_fee_multiplier: Decimal = Field(default=Decimal("2"), ge=1)
    maximum_fee_per_gas_gwei: Decimal = Field(gt=0)
    maximum_priority_fee_gwei: Decimal = Field(gt=0)
    maximum_gas_cost_wei: int = Field(gt=0)
    replacement_bump_bps: Decimal = Field(default=Decimal("1250"), gt=0)
    maximum_replacements: int = Field(default=3, ge=0)


class TokenAllowance(BaseModel):
    model_config = ConfigDict(frozen=True)

    chain_id: int = Field(gt=0)
    owner: str = Field(min_length=1)
    token: str = Field(min_length=1)
    spender: str = Field(min_length=1)
    amount: Decimal = Field(ge=0)
    block_number: int = Field(ge=0)
    block_hash: str = Field(min_length=1)


class DexSwapQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    quote_id: str = Field(min_length=1)
    chain_id: int = Field(gt=0)
    account: str = Field(min_length=1)
    token_in: str = Field(min_length=1)
    token_out: str = Field(min_length=1)
    router: str = Field(min_length=1)
    allowance_spender: str = Field(min_length=1)
    amount_in: Decimal = Field(gt=0)
    expected_amount_out: Decimal = Field(gt=0)
    input_notional_usdt: Decimal = Field(gt=0)
    price_impact_bps: Decimal = Field(ge=0)
    swap_calldata: str = Field(min_length=1)
    calldata_minimum_amount_out: Decimal = Field(gt=0)
    calldata_deadline: datetime
    exact_approval_calldata: str = Field(min_length=1)
    native_value_wei: int = Field(default=0, ge=0)
    swap_gas_estimate: int = Field(gt=0)
    approval_gas_estimate: int = Field(gt=0)
    base_fee_gwei: Decimal = Field(gt=0)
    priority_fee_gwei: Decimal = Field(gt=0)
    quote_block_number: int = Field(ge=0)
    quote_block_hash: str = Field(min_length=1)
    quoted_at: datetime

    @field_validator("quoted_at", "calldata_deadline")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_assets(self) -> DexSwapQuote:
        if self.token_in.lower() == self.token_out.lower():
            raise ValueError("DEX swap assets must differ")
        if self.calldata_deadline <= self.quoted_at:
            raise ValueError("DEX calldata deadline must follow quote timestamp")
        return self


class DexTransactionSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    client_transaction_id: str
    parent_transaction_id: str | None = None
    swap_id: str
    risk_decision_id: str
    chain_id: int = Field(gt=0)
    account: str
    nonce: int = Field(ge=0)
    kind: DexTransactionKind
    to: str
    calldata: str
    value_wei: int = Field(ge=0)
    gas_limit: int = Field(gt=0)
    maximum_fee_per_gas_gwei: Decimal = Field(gt=0)
    maximum_priority_fee_gwei: Decimal = Field(gt=0)
    maximum_gas_cost_wei: int = Field(gt=0)
    depends_on_transaction_id: str | None = None
    token_in: str
    token_out: str
    amount_in: Decimal = Field(gt=0)
    minimum_amount_out: Decimal = Field(ge=0)
    calldata_deadline: datetime | None = None
    status: DexTransactionStatus
    transaction_hash: str | None = None
    included_block_number: int | None = Field(default=None, ge=0)
    included_block_hash: str | None = None
    gas_used: int | None = Field(default=None, ge=0)
    effective_gas_price_gwei: Decimal | None = Field(default=None, ge=0)
    replacement_attempt: int = Field(default=0, ge=0)
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("calldata_deadline")
    @classmethod
    def normalize_optional_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None


class DexSwapPlan(BaseModel):
    model_config = ConfigDict(frozen=True)

    swap_id: str
    quote_id: str
    risk_decision_id: str
    minimum_amount_out: Decimal = Field(gt=0)
    exact_approval_required: bool
    approval: DexTransactionSnapshot | None
    swap: DexTransactionSnapshot

    @model_validator(mode="after")
    def validate_dependency(self) -> DexSwapPlan:
        if self.exact_approval_required != (self.approval is not None):
            raise ValueError("DEX approval marker disagrees with transaction plan")
        if self.approval is not None and (
            self.swap.depends_on_transaction_id
            != self.approval.client_transaction_id
        ):
            raise ValueError("DEX swap dependency does not reference approval")
        return self


class DexJournalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    event_id: str
    event_type: DexEventType
    timestamp: datetime
    snapshots: tuple[DexTransactionSnapshot, ...] = ()
    reasons: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class JsonlDexJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: DexJournalEntry) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(entry.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[DexJournalEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            DexJournalEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        if tuple(entry.sequence for entry in entries) != tuple(
            range(1, len(entries) + 1)
        ):
            raise ValueError("DEX journal sequence is not contiguous")
        return entries


class DexExecutionEngine:
    """Creates durable unsigned intents; signing stays behind an external signer."""

    def __init__(
        self,
        policy: DexExecutionPolicy,
        journal: JsonlDexJournal,
        *,
        chain_pending_nonce: int,
    ) -> None:
        if chain_pending_nonce < 0:
            raise ValueError("chain pending nonce cannot be negative")
        self.policy = policy
        self.journal = journal
        self.transactions: dict[str, DexTransactionSnapshot] = {}
        self.interlock_engaged = False
        self.interlock_reasons: tuple[str, ...] = ()
        self._sequence = 0
        self._recover()
        journal_next = max(
            (transaction.nonce + 1 for transaction in self.transactions.values()),
            default=0,
        )
        self.next_nonce = max(chain_pending_nonce, journal_next)

    def plan_swap(
        self,
        risk_decision: RiskDecision,
        quote: DexSwapQuote,
        allowance: TokenAllowance,
        *,
        as_of: datetime,
        canonical_quote_block_hash: str,
    ) -> DexSwapPlan:
        self._validate_plan(
            risk_decision,
            quote,
            allowance,
            as_of,
            canonical_quote_block_hash,
        )
        slippage_limit = min(
            self.policy.maximum_slippage_bps,
            risk_decision.max_slippage_bps,
        )
        minimum_out = quote.expected_amount_out * (BPS - slippage_limit) / BPS
        if quote.calldata_minimum_amount_out < minimum_out:
            raise ValueError("DEX calldata minimum output is weaker than risk policy")
        max_fee, priority = self._network_fees(
            quote.base_fee_gwei,
            quote.priority_fee_gwei,
        )
        now = _utc(as_of)
        swap_id = _identifier("dexswap", quote.quote_id, risk_decision.decision_id)
        approval: DexTransactionSnapshot | None = None
        snapshots: list[DexTransactionSnapshot] = []
        nonce = self.next_nonce
        approval_required = allowance.amount < quote.amount_in
        if approval_required:
            approval = self._transaction(
                transaction_id=_identifier("dextx", swap_id, "approval", str(nonce)),
                parent_transaction_id=None,
                swap_id=swap_id,
                risk_decision_id=risk_decision.decision_id,
                quote=quote,
                nonce=nonce,
                kind=DexTransactionKind.APPROVAL,
                to=quote.token_in,
                calldata=quote.exact_approval_calldata,
                value_wei=0,
                gas_estimate=quote.approval_gas_estimate,
                maximum_fee=max_fee,
                priority_fee=priority,
                depends_on=None,
                minimum_out=ZERO,
                calldata_deadline=None,
                now=now,
            )
            snapshots.append(approval)
            nonce += 1
        swap = self._transaction(
            transaction_id=_identifier("dextx", swap_id, "swap", str(nonce)),
            parent_transaction_id=None,
            swap_id=swap_id,
            risk_decision_id=risk_decision.decision_id,
            quote=quote,
            nonce=nonce,
            kind=DexTransactionKind.SWAP,
            to=quote.router,
            calldata=quote.swap_calldata,
            value_wei=quote.native_value_wei,
            gas_estimate=quote.swap_gas_estimate,
            maximum_fee=max_fee,
            priority_fee=priority,
            depends_on=(approval.client_transaction_id if approval is not None else None),
            minimum_out=minimum_out,
            calldata_deadline=quote.calldata_deadline,
            now=now,
        )
        snapshots.append(swap)
        if any(item.client_transaction_id in self.transactions for item in snapshots):
            raise ValueError("duplicate DEX swap plan identity")
        self._persist(DexEventType.PLAN_PREPARED, now, snapshots=tuple(snapshots))
        self.next_nonce = nonce + 1
        return DexSwapPlan(
            swap_id=swap_id,
            quote_id=quote.quote_id,
            risk_decision_id=risk_decision.decision_id,
            minimum_amount_out=minimum_out,
            exact_approval_required=approval_required,
            approval=approval,
            swap=swap,
        )

    def prepare_submission(
        self,
        client_transaction_id: str,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if self.interlock_engaged and transaction.parent_transaction_id is None:
            raise ValueError("DEX execution interlock is engaged")
        if transaction.status is DexTransactionStatus.SUBMITTING:
            return transaction
        if transaction.status is not DexTransactionStatus.PREPARED:
            raise ValueError("only prepared DEX transaction can submit")
        if transaction.depends_on_transaction_id is not None:
            dependency = self._transaction_by_id(transaction.depends_on_transaction_id)
            if dependency.status is not DexTransactionStatus.CONFIRMED:
                raise ValueError("DEX transaction dependency is not final")
        if (
            transaction.kind is DexTransactionKind.SWAP
            and transaction.calldata_deadline is not None
            and _utc(timestamp) > transaction.calldata_deadline
        ):
            raise ValueError("DEX swap calldata deadline has expired")
        updated = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.SUBMITTING,
        )
        self._persist(DexEventType.SUBMISSION_PREPARED, timestamp, snapshots=(updated,))
        return updated

    def mark_submitted(
        self,
        client_transaction_id: str,
        transaction_hash: str,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        if not transaction_hash:
            raise ValueError("submitted DEX transaction requires hash")
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status is DexTransactionStatus.SUBMITTED:
            if transaction.transaction_hash != transaction_hash:
                raise ValueError("DEX transaction hash changed")
            return transaction
        if transaction.status is not DexTransactionStatus.SUBMITTING:
            raise ValueError("DEX transaction was not persisted before submit")
        updated = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.SUBMITTED,
            transaction_hash=transaction_hash,
        )
        self._persist(DexEventType.SUBMITTED, timestamp, snapshots=(updated,))
        return updated

    def mark_unknown(
        self,
        client_transaction_id: str,
        timestamp: datetime,
        reason: str,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status not in {
            DexTransactionStatus.SUBMITTING,
            DexTransactionStatus.SUBMITTED,
        }:
            raise ValueError("DEX transaction cannot become unknown")
        updated = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.UNKNOWN,
        )
        self._persist(
            DexEventType.UNKNOWN_MARKED,
            timestamp,
            snapshots=(updated,),
            reasons=(reason,),
        )
        return updated

    def observe_receipt(
        self,
        client_transaction_id: str,
        *,
        transaction_hash: str,
        block_number: int,
        block_hash: str,
        success: bool,
        gas_used: int,
        effective_gas_price_gwei: Decimal,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status not in {
            DexTransactionStatus.SUBMITTED,
            DexTransactionStatus.UNKNOWN,
        }:
            raise ValueError("DEX receipt is invalid for transaction state")
        if transaction.transaction_hash not in {None, transaction_hash}:
            raise ValueError("DEX receipt hash mismatch")
        if block_number < 0 or not block_hash or gas_used < 0:
            raise ValueError("DEX receipt fields are invalid")
        status = DexTransactionStatus.INCLUDED if success else DexTransactionStatus.REVERTED
        updated = self._update(
            transaction,
            timestamp,
            status=status,
            transaction_hash=transaction_hash,
            included_block_number=block_number,
            included_block_hash=block_hash,
            gas_used=gas_used,
            effective_gas_price_gwei=effective_gas_price_gwei,
        )
        self._persist(DexEventType.RECEIPT_OBSERVED, timestamp, snapshots=(updated,))
        if not success:
            self._engage_interlock(timestamp, (f"{client_transaction_id}:reverted",))
        return updated

    def observe_canonical_head(
        self,
        client_transaction_id: str,
        *,
        head_block_number: int,
        canonical_inclusion_hash: str | None,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status is not DexTransactionStatus.INCLUDED:
            raise ValueError("only included DEX transaction can advance finality")
        assert transaction.included_block_number is not None
        assert transaction.included_block_hash is not None
        if canonical_inclusion_hash != transaction.included_block_hash:
            updated = self._update(
                transaction,
                timestamp,
                status=DexTransactionStatus.REORGED,
            )
            self._persist(DexEventType.REORG_DETECTED, timestamp, snapshots=(updated,))
            self._engage_interlock(
                timestamp,
                (f"{client_transaction_id}:inclusion_reorged",),
            )
            return updated
        confirmations = head_block_number - transaction.included_block_number + 1
        if confirmations < 0:
            raise ValueError("canonical head precedes inclusion block")
        if confirmations < self.policy.required_confirmations:
            return transaction
        updated = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.CONFIRMED,
        )
        self._persist(DexEventType.FINALIZED, timestamp, snapshots=(updated,))
        return updated

    def prepare_replacement(
        self,
        client_transaction_id: str,
        *,
        chain_pending_nonce: int,
        base_fee_gwei: Decimal,
        priority_fee_gwei: Decimal,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status not in {
            DexTransactionStatus.SUBMITTED,
            DexTransactionStatus.UNKNOWN,
            DexTransactionStatus.REORGED,
            DexTransactionStatus.DROPPED,
        }:
            raise ValueError("DEX transaction cannot be replaced")
        if chain_pending_nonce > transaction.nonce:
            self._engage_interlock(
                timestamp,
                (f"{client_transaction_id}:nonce_already_consumed",),
            )
            raise ValueError("DEX nonce was consumed by an unknown transaction")
        attempt = transaction.replacement_attempt + 1
        if attempt > self.policy.maximum_replacements:
            raise ValueError("DEX replacement attempt limit exceeded")
        bump = (BPS + self.policy.replacement_bump_bps) / BPS
        network_max_fee, network_priority = self._network_fees(
            base_fee_gwei,
            priority_fee_gwei,
        )
        maximum_fee = max(transaction.maximum_fee_per_gas_gwei * bump, network_max_fee)
        priority = max(transaction.maximum_priority_fee_gwei * bump, network_priority)
        self._validate_fee_caps(maximum_fee, priority, transaction.gas_limit)
        replaced = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.REPLACED,
        )
        replacement = transaction.model_copy(
            update={
                "client_transaction_id": _identifier(
                    "dextx",
                    transaction.client_transaction_id,
                    "replacement",
                    str(attempt),
                ),
                "parent_transaction_id": transaction.client_transaction_id,
                "maximum_fee_per_gas_gwei": maximum_fee,
                "maximum_priority_fee_gwei": priority,
                "maximum_gas_cost_wei": _gas_cost_wei(
                    transaction.gas_limit,
                    maximum_fee,
                ),
                "status": DexTransactionStatus.PREPARED,
                "transaction_hash": None,
                "included_block_number": None,
                "included_block_hash": None,
                "gas_used": None,
                "effective_gas_price_gwei": None,
                "replacement_attempt": attempt,
                "version": 1,
                "created_at": _utc(timestamp),
                "updated_at": _utc(timestamp),
            }
        )
        self._persist(
            DexEventType.REPLACEMENT_PREPARED,
            timestamp,
            snapshots=(replaced, replacement),
        )
        return replacement

    def mark_dropped(
        self,
        client_transaction_id: str,
        timestamp: datetime,
    ) -> DexTransactionSnapshot:
        transaction = self._transaction_by_id(client_transaction_id)
        if transaction.status not in {
            DexTransactionStatus.SUBMITTED,
            DexTransactionStatus.UNKNOWN,
        }:
            raise ValueError("DEX transaction cannot be marked dropped")
        updated = self._update(
            transaction,
            timestamp,
            status=DexTransactionStatus.DROPPED,
        )
        self._persist(DexEventType.DROPPED, timestamp, snapshots=(updated,))
        return updated

    def reconcile_chain_nonce(self, chain_pending_nonce: int, timestamp: datetime) -> int:
        if chain_pending_nonce < 0:
            raise ValueError("chain pending nonce cannot be negative")
        unresolved = [
            transaction
            for transaction in self.transactions.values()
            if transaction.nonce < chain_pending_nonce
            and transaction.status
            not in {
                DexTransactionStatus.CONFIRMED,
                DexTransactionStatus.REVERTED,
                DexTransactionStatus.REPLACED,
            }
        ]
        if unresolved:
            reasons = tuple(
                f"{transaction.client_transaction_id}:nonce_consumed_without_finality"
                for transaction in unresolved
            )
            self._engage_interlock(timestamp, reasons)
            raise ValueError("chain nonce advanced over unresolved DEX transactions")
        self.next_nonce = max(self.next_nonce, chain_pending_nonce)
        return self.next_nonce

    def _validate_plan(
        self,
        risk_decision: RiskDecision,
        quote: DexSwapQuote,
        allowance: TokenAllowance,
        as_of: datetime,
        canonical_quote_block_hash: str,
    ) -> None:
        if self.interlock_engaged:
            raise ValueError("DEX execution interlock is engaged")
        if not risk_decision.approved:
            raise ValueError("DEX execution requires approved risk decision")
        if quote.input_notional_usdt > risk_decision.approved_notional:
            raise ValueError("DEX quote exceeds risk-authorized notional")
        if quote.chain_id != self.policy.chain_id or allowance.chain_id != quote.chain_id:
            raise ValueError("DEX chain ID mismatch")
        if (
            allowance.owner.lower() != quote.account.lower()
            or allowance.token.lower() != quote.token_in.lower()
            or allowance.spender.lower() != quote.allowance_spender.lower()
        ):
            raise ValueError("DEX allowance identity mismatch")
        if (
            quote.quote_block_number != allowance.block_number
            or quote.quote_block_hash != allowance.block_hash
            or quote.quote_block_hash != canonical_quote_block_hash
        ):
            raise ValueError("DEX quote or allowance block is not canonical")
        age = _utc(as_of) - quote.quoted_at
        if age < timedelta(0) or age > timedelta(
            seconds=self.policy.maximum_quote_age_seconds
        ):
            raise ValueError("DEX quote is stale or from the future")
        if quote.price_impact_bps > self.policy.maximum_price_impact_bps:
            raise ValueError("DEX quote price impact exceeds policy")
        if quote.calldata_deadline > quote.quoted_at + timedelta(
            seconds=self.policy.maximum_calldata_lifetime_seconds
        ):
            raise ValueError("DEX calldata deadline exceeds policy lifetime")
        self._network_fees(quote.base_fee_gwei, quote.priority_fee_gwei)

    def _transaction(
        self,
        *,
        transaction_id: str,
        parent_transaction_id: str | None,
        swap_id: str,
        risk_decision_id: str,
        quote: DexSwapQuote,
        nonce: int,
        kind: DexTransactionKind,
        to: str,
        calldata: str,
        value_wei: int,
        gas_estimate: int,
        maximum_fee: Decimal,
        priority_fee: Decimal,
        depends_on: str | None,
        minimum_out: Decimal,
        calldata_deadline: datetime | None,
        now: datetime,
    ) -> DexTransactionSnapshot:
        gas_limit = int(
            (Decimal(gas_estimate) * self.policy.gas_limit_buffer).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
        self._validate_fee_caps(maximum_fee, priority_fee, gas_limit)
        return DexTransactionSnapshot(
            client_transaction_id=transaction_id,
            parent_transaction_id=parent_transaction_id,
            swap_id=swap_id,
            risk_decision_id=risk_decision_id,
            chain_id=quote.chain_id,
            account=quote.account,
            nonce=nonce,
            kind=kind,
            to=to,
            calldata=calldata,
            value_wei=value_wei,
            gas_limit=gas_limit,
            maximum_fee_per_gas_gwei=maximum_fee,
            maximum_priority_fee_gwei=priority_fee,
            maximum_gas_cost_wei=_gas_cost_wei(gas_limit, maximum_fee),
            depends_on_transaction_id=depends_on,
            token_in=quote.token_in,
            token_out=quote.token_out,
            amount_in=quote.amount_in,
            minimum_amount_out=minimum_out,
            calldata_deadline=calldata_deadline,
            status=DexTransactionStatus.PREPARED,
            version=1,
            created_at=now,
            updated_at=now,
        )

    def _network_fees(
        self,
        base_fee_gwei: Decimal,
        priority_fee_gwei: Decimal,
    ) -> tuple[Decimal, Decimal]:
        priority = min(priority_fee_gwei, self.policy.maximum_priority_fee_gwei)
        maximum_fee = base_fee_gwei * self.policy.base_fee_multiplier + priority
        if priority_fee_gwei > self.policy.maximum_priority_fee_gwei:
            raise ValueError("DEX priority fee exceeds policy")
        if maximum_fee > self.policy.maximum_fee_per_gas_gwei:
            raise ValueError("DEX maximum fee per gas exceeds policy")
        return maximum_fee, priority

    def _validate_fee_caps(
        self,
        maximum_fee_gwei: Decimal,
        priority_fee_gwei: Decimal,
        gas_limit: int,
    ) -> None:
        if maximum_fee_gwei > self.policy.maximum_fee_per_gas_gwei:
            raise ValueError("DEX replacement maximum fee exceeds policy")
        if priority_fee_gwei > self.policy.maximum_priority_fee_gwei:
            raise ValueError("DEX replacement priority fee exceeds policy")
        if _gas_cost_wei(gas_limit, maximum_fee_gwei) > self.policy.maximum_gas_cost_wei:
            raise ValueError("DEX maximum gas cost exceeds policy")

    def _recover(self) -> None:
        for entry in self.journal.load():
            if entry.sequence != self._sequence + 1:
                raise ValueError("DEX journal replay sequence gap")
            for snapshot in entry.snapshots:
                current = self.transactions.get(snapshot.client_transaction_id)
                expected_version = 1 if current is None else current.version + 1
                if snapshot.version != expected_version:
                    raise ValueError("DEX transaction version gap during replay")
                self.transactions[snapshot.client_transaction_id] = snapshot
            if entry.event_type is DexEventType.INTERLOCK_ENGAGED:
                self.interlock_engaged = True
                self.interlock_reasons = entry.reasons
            self._sequence = entry.sequence

    def _engage_interlock(self, timestamp: datetime, reasons: tuple[str, ...]) -> None:
        self._persist(DexEventType.INTERLOCK_ENGAGED, timestamp, reasons=reasons)
        self.interlock_engaged = True
        self.interlock_reasons = reasons

    def _persist(
        self,
        event_type: DexEventType,
        timestamp: datetime,
        *,
        snapshots: tuple[DexTransactionSnapshot, ...] = (),
        reasons: tuple[str, ...] = (),
    ) -> None:
        sequence = self._sequence + 1
        identities = ",".join(snapshot.client_transaction_id for snapshot in snapshots)
        event_id = _identifier(
            "dexevt",
            str(sequence),
            event_type.value,
            identities,
            _utc(timestamp).isoformat(),
        )
        entry = DexJournalEntry(
            sequence=sequence,
            event_id=event_id,
            event_type=event_type,
            timestamp=timestamp,
            snapshots=snapshots,
            reasons=reasons,
        )
        self.journal.append(entry)
        for snapshot in snapshots:
            self.transactions[snapshot.client_transaction_id] = snapshot
        self._sequence = sequence

    def _transaction_by_id(self, client_transaction_id: str) -> DexTransactionSnapshot:
        try:
            return self.transactions[client_transaction_id]
        except KeyError as exc:
            raise ValueError("unknown DEX client transaction ID") from exc

    @staticmethod
    def _update(
        transaction: DexTransactionSnapshot,
        timestamp: datetime,
        **updates: object,
    ) -> DexTransactionSnapshot:
        return transaction.model_copy(
            update={
                **updates,
                "version": transaction.version + 1,
                "updated_at": _utc(timestamp),
            }
        )


def _gas_cost_wei(gas_limit: int, maximum_fee_gwei: Decimal) -> int:
    return int(
        (Decimal(gas_limit) * maximum_fee_gwei * GWEI).to_integral_value(
            rounding=ROUND_CEILING
        )
    )


def _identifier(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
