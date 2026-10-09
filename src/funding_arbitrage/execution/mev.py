"""Private-relay MEV bundle execution with simulation and hard loss bounds."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import RiskDecision

ZERO = Decimal("0")


class MevBundleStatus(StrEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    INCLUDED = "INCLUDED"
    CONFIRMED = "CONFIRMED"
    EXPIRED = "EXPIRED"
    REORGED = "REORGED"
    FAILED = "FAILED"
    REPLACED = "REPLACED"


class MevEventType(StrEnum):
    PREPARED = "PREPARED"
    SUBMISSION_PREPARED = "SUBMISSION_PREPARED"
    SUBMITTED = "SUBMITTED"
    INCLUDED = "INCLUDED"
    FINALIZED = "FINALIZED"
    EXPIRED = "EXPIRED"
    REORGED = "REORGED"
    RETRY_PREPARED = "RETRY_PREPARED"
    INTERLOCK_ENGAGED = "INTERLOCK_ENGAGED"


class MevExecutionPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    chain_id: int = Field(gt=0)
    private_relay_ids: tuple[str, ...] = Field(min_length=1)
    required_confirmations: int = Field(default=12, gt=0)
    minimum_independent_simulators: int = Field(default=2, gt=0)
    maximum_simulation_age_seconds: int = Field(default=3, gt=0)
    maximum_target_window_blocks: int = Field(default=3, gt=0)
    minimum_expected_profit_usdt: Decimal = Field(gt=0)
    maximum_loss_usdt: Decimal = Field(gt=0)
    maximum_capital_at_risk_usdt: Decimal = Field(gt=0)
    maximum_gas_cost_usdt: Decimal = Field(gt=0)
    maximum_builder_payment_usdt: Decimal = Field(ge=0)
    maximum_simulation_profit_dispersion_usdt: Decimal = Field(ge=0)
    maximum_reorg_retries: int = Field(default=1, ge=0)

    @field_validator("private_relay_ids")
    @classmethod
    def normalize_relays(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in value if item.strip())
        if len(normalized) != len(value) or len(set(normalized)) != len(normalized):
            raise ValueError("MEV private relay IDs must be unique and non-empty")
        return normalized


class MevBundleTransaction(BaseModel):
    model_config = ConfigDict(frozen=True)

    transaction_hash: str = Field(min_length=1)
    signed_payload_digest: str = Field(min_length=1)
    can_revert: bool = False

    @model_validator(mode="after")
    def forbid_optional_reverts(self) -> MevBundleTransaction:
        if self.can_revert:
            raise ValueError("V1 MEV bundles do not permit reverting transactions")
        return self


class MevBundleCandidate(BaseModel):
    model_config = ConfigDict(frozen=True)

    opportunity_id: str = Field(min_length=1)
    chain_id: int = Field(gt=0)
    account: str = Field(min_length=1)
    transactions: tuple[MevBundleTransaction, ...] = Field(min_length=1)
    base_block_number: int = Field(ge=0)
    base_block_hash: str = Field(min_length=1)
    target_block_number: int = Field(gt=0)
    maximum_block_number: int = Field(gt=0)
    capital_at_risk_usdt: Decimal = Field(gt=0)
    expected_gross_profit_usdt: Decimal = Field(gt=0)
    maximum_gas_cost_usdt: Decimal = Field(ge=0)
    maximum_builder_payment_usdt: Decimal = Field(ge=0)
    candidate_maximum_loss_usdt: Decimal = Field(gt=0)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_block_window(self) -> MevBundleCandidate:
        if self.target_block_number <= self.base_block_number:
            raise ValueError("MEV target block must follow simulation base block")
        if self.maximum_block_number < self.target_block_number:
            raise ValueError("MEV maximum block cannot precede target block")
        hashes = [transaction.transaction_hash for transaction in self.transactions]
        if len(hashes) != len(set(hashes)):
            raise ValueError("MEV bundle transaction hashes must be unique")
        return self

    @property
    def payload_hash(self) -> str:
        return _hash("mevpayload", *(item.signed_payload_digest for item in self.transactions))


class MevBundleSimulation(BaseModel):
    model_config = ConfigDict(frozen=True)

    simulator_id: str = Field(min_length=1)
    payload_hash: str = Field(min_length=1)
    base_block_number: int = Field(ge=0)
    base_block_hash: str = Field(min_length=1)
    success: bool
    reverting_transaction_hashes: tuple[str, ...] = ()
    gas_cost_usdt: Decimal = Field(ge=0)
    builder_payment_usdt: Decimal = Field(ge=0)
    gross_profit_usdt: Decimal
    net_profit_usdt: Decimal
    worst_case_loss_usdt: Decimal = Field(ge=0)
    state_diff_hash: str = Field(min_length=1)
    simulated_at: datetime

    @field_validator("simulated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_profit_accounting(self) -> MevBundleSimulation:
        expected_net = (
            self.gross_profit_usdt
            - self.gas_cost_usdt
            - self.builder_payment_usdt
        )
        if self.net_profit_usdt != expected_net:
            raise ValueError("MEV simulation net profit accounting mismatch")
        return self


class MevBundleSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    bundle_id: str
    parent_bundle_id: str | None = None
    opportunity_id: str
    risk_decision_id: str
    operator_authorization_id: str
    chain_id: int = Field(gt=0)
    account: str
    transactions: tuple[MevBundleTransaction, ...]
    payload_hash: str
    base_block_number: int = Field(ge=0)
    base_block_hash: str
    target_block_number: int = Field(gt=0)
    maximum_block_number: int = Field(gt=0)
    expected_net_profit_usdt: Decimal
    maximum_loss_usdt: Decimal = Field(gt=0)
    maximum_gas_cost_usdt: Decimal = Field(ge=0)
    maximum_builder_payment_usdt: Decimal = Field(ge=0)
    simulation_ids: tuple[str, ...]
    simulation_state_diff_hash: str
    relay_id: str | None = None
    relay_submission_id: str | None = None
    status: MevBundleStatus
    included_block_number: int | None = Field(default=None, ge=0)
    included_block_hash: str | None = None
    realized_net_profit_usdt: Decimal | None = None
    reorg_attempt: int = Field(default=0, ge=0)
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class MevJournalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    event_id: str
    event_type: MevEventType
    timestamp: datetime
    snapshots: tuple[MevBundleSnapshot, ...] = ()
    reasons: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class JsonlMevJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: MevJournalEntry) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(entry.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[MevJournalEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            MevJournalEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        if tuple(entry.sequence for entry in entries) != tuple(
            range(1, len(entries) + 1)
        ):
            raise ValueError("MEV journal sequence is not contiguous")
        return entries


class MevExecutionEngine:
    """Persists simulated private bundles before any relay side effect."""

    def __init__(self, policy: MevExecutionPolicy, journal: JsonlMevJournal) -> None:
        self.policy = policy
        self.journal = journal
        self.bundles: dict[str, MevBundleSnapshot] = {}
        self.interlock_engaged = False
        self.interlock_reasons: tuple[str, ...] = ()
        self._sequence = 0
        self._recover()

    def prepare_bundle(
        self,
        risk_decision: RiskDecision,
        candidate: MevBundleCandidate,
        simulations: tuple[MevBundleSimulation, ...],
        *,
        operator_authorization_id: str,
        canonical_base_block_hash: str,
        as_of: datetime,
        parent_bundle_id: str | None = None,
    ) -> MevBundleSnapshot:
        attempt = self._validate_candidate(
            risk_decision,
            candidate,
            simulations,
            operator_authorization_id,
            canonical_base_block_hash,
            as_of,
            parent_bundle_id,
        )
        expected_net = min(simulation.net_profit_usdt for simulation in simulations)
        loss_bound = min(
            self.policy.maximum_loss_usdt,
            risk_decision.approved_risk_usdt,
            candidate.candidate_maximum_loss_usdt,
        )
        bundle_id = _hash(
            "mevbundle",
            candidate.opportunity_id,
            candidate.payload_hash,
            str(candidate.target_block_number),
            str(attempt),
        )
        if bundle_id in self.bundles:
            return self.bundles[bundle_id]
        now = _utc(as_of)
        snapshot = MevBundleSnapshot(
            bundle_id=bundle_id,
            parent_bundle_id=parent_bundle_id,
            opportunity_id=candidate.opportunity_id,
            risk_decision_id=risk_decision.decision_id,
            operator_authorization_id=operator_authorization_id,
            chain_id=candidate.chain_id,
            account=candidate.account,
            transactions=candidate.transactions,
            payload_hash=candidate.payload_hash,
            base_block_number=candidate.base_block_number,
            base_block_hash=candidate.base_block_hash,
            target_block_number=candidate.target_block_number,
            maximum_block_number=candidate.maximum_block_number,
            expected_net_profit_usdt=expected_net,
            maximum_loss_usdt=loss_bound,
            maximum_gas_cost_usdt=candidate.maximum_gas_cost_usdt,
            maximum_builder_payment_usdt=candidate.maximum_builder_payment_usdt,
            simulation_ids=tuple(sorted(item.simulator_id for item in simulations)),
            simulation_state_diff_hash=simulations[0].state_diff_hash,
            status=MevBundleStatus.PREPARED,
            reorg_attempt=attempt,
            version=1,
            created_at=now,
            updated_at=now,
        )
        event_type = (
            MevEventType.RETRY_PREPARED
            if parent_bundle_id is not None
            else MevEventType.PREPARED
        )
        if parent_bundle_id is not None:
            parent = self.bundles[parent_bundle_id]
            replaced = self._update(parent, now, status=MevBundleStatus.REPLACED)
            self._persist(event_type, now, snapshots=(replaced, snapshot))
        else:
            self._persist(event_type, now, snapshots=(snapshot,))
        return snapshot

    def prepare_private_submission(
        self,
        bundle_id: str,
        *,
        relay_id: str,
        current_block_number: int,
        timestamp: datetime,
    ) -> MevBundleSnapshot:
        bundle = self._bundle(bundle_id)
        if relay_id not in self.policy.private_relay_ids:
            raise ValueError("MEV submission requires an allowlisted private relay")
        if self.interlock_engaged and bundle.parent_bundle_id is None:
            raise ValueError("MEV execution interlock is engaged")
        if bundle.status is MevBundleStatus.SUBMITTING:
            return bundle
        if bundle.status is not MevBundleStatus.PREPARED:
            raise ValueError("only prepared MEV bundle can submit")
        if current_block_number > bundle.maximum_block_number:
            raise ValueError("MEV bundle target window has expired")
        updated = self._update(
            bundle,
            timestamp,
            status=MevBundleStatus.SUBMITTING,
            relay_id=relay_id,
        )
        self._persist(MevEventType.SUBMISSION_PREPARED, timestamp, snapshots=(updated,))
        return updated

    def mark_private_submitted(
        self,
        bundle_id: str,
        *,
        relay_submission_id: str,
        timestamp: datetime,
    ) -> MevBundleSnapshot:
        if not relay_submission_id:
            raise ValueError("MEV relay submission ID is required")
        bundle = self._bundle(bundle_id)
        if bundle.status is MevBundleStatus.SUBMITTED:
            if bundle.relay_submission_id != relay_submission_id:
                raise ValueError("MEV relay submission ID changed")
            return bundle
        if bundle.status is not MevBundleStatus.SUBMITTING:
            raise ValueError("MEV bundle was not persisted before relay submission")
        updated = self._update(
            bundle,
            timestamp,
            status=MevBundleStatus.SUBMITTED,
            relay_submission_id=relay_submission_id,
        )
        self._persist(MevEventType.SUBMITTED, timestamp, snapshots=(updated,))
        return updated

    def observe_inclusion(
        self,
        bundle_id: str,
        *,
        block_number: int,
        block_hash: str,
        included_transaction_hashes: tuple[str, ...],
        gross_profit_usdt: Decimal,
        gas_cost_usdt: Decimal,
        builder_payment_usdt: Decimal,
        timestamp: datetime,
    ) -> MevBundleSnapshot:
        bundle = self._bundle(bundle_id)
        if bundle.status is not MevBundleStatus.SUBMITTED:
            raise ValueError("MEV inclusion requires submitted bundle")
        if not bundle.target_block_number <= block_number <= bundle.maximum_block_number:
            raise ValueError("MEV inclusion occurred outside authorized block window")
        expected_hashes = tuple(item.transaction_hash for item in bundle.transactions)
        if included_transaction_hashes != expected_hashes:
            self._engage_interlock(timestamp, (f"{bundle_id}:non_atomic_inclusion",))
            raise ValueError("MEV included transaction set/order mismatch")
        if gas_cost_usdt > bundle.maximum_gas_cost_usdt:
            self._engage_interlock(timestamp, (f"{bundle_id}:gas_cost_exceeded",))
            raise ValueError("MEV realized gas cost exceeded bound")
        if builder_payment_usdt > bundle.maximum_builder_payment_usdt:
            self._engage_interlock(timestamp, (f"{bundle_id}:builder_payment_exceeded",))
            raise ValueError("MEV builder payment exceeded bound")
        realized_net = gross_profit_usdt - gas_cost_usdt - builder_payment_usdt
        realized_loss = max(-realized_net, ZERO)
        status = (
            MevBundleStatus.FAILED
            if realized_loss > bundle.maximum_loss_usdt
            else MevBundleStatus.INCLUDED
        )
        updated = self._update(
            bundle,
            timestamp,
            status=status,
            included_block_number=block_number,
            included_block_hash=block_hash,
            realized_net_profit_usdt=realized_net,
        )
        self._persist(MevEventType.INCLUDED, timestamp, snapshots=(updated,))
        if status is MevBundleStatus.FAILED:
            self._engage_interlock(timestamp, (f"{bundle_id}:loss_bound_exceeded",))
        return updated

    def observe_canonical_head(
        self,
        bundle_id: str,
        *,
        head_block_number: int,
        canonical_inclusion_hash: str | None,
        timestamp: datetime,
    ) -> MevBundleSnapshot:
        bundle = self._bundle(bundle_id)
        if bundle.status is not MevBundleStatus.INCLUDED:
            raise ValueError("only included MEV bundle can advance finality")
        assert bundle.included_block_number is not None
        assert bundle.included_block_hash is not None
        if canonical_inclusion_hash != bundle.included_block_hash:
            updated = self._update(bundle, timestamp, status=MevBundleStatus.REORGED)
            self._persist(MevEventType.REORGED, timestamp, snapshots=(updated,))
            self._engage_interlock(timestamp, (f"{bundle_id}:inclusion_reorged",))
            return updated
        confirmations = head_block_number - bundle.included_block_number + 1
        if confirmations < 0:
            raise ValueError("MEV canonical head precedes inclusion")
        if confirmations < self.policy.required_confirmations:
            return bundle
        updated = self._update(bundle, timestamp, status=MevBundleStatus.CONFIRMED)
        self._persist(MevEventType.FINALIZED, timestamp, snapshots=(updated,))
        return updated

    def expire_if_past_window(
        self,
        bundle_id: str,
        *,
        current_block_number: int,
        timestamp: datetime,
    ) -> MevBundleSnapshot:
        bundle = self._bundle(bundle_id)
        if bundle.status not in {
            MevBundleStatus.PREPARED,
            MevBundleStatus.SUBMITTING,
            MevBundleStatus.SUBMITTED,
        }:
            return bundle
        if current_block_number <= bundle.maximum_block_number:
            return bundle
        updated = self._update(bundle, timestamp, status=MevBundleStatus.EXPIRED)
        self._persist(MevEventType.EXPIRED, timestamp, snapshots=(updated,))
        return updated

    def _validate_candidate(
        self,
        risk_decision: RiskDecision,
        candidate: MevBundleCandidate,
        simulations: tuple[MevBundleSimulation, ...],
        operator_authorization_id: str,
        canonical_base_block_hash: str,
        as_of: datetime,
        parent_bundle_id: str | None,
    ) -> int:
        if not self.policy.enabled:
            raise ValueError("MEV execution is disabled")
        if not operator_authorization_id:
            raise ValueError("MEV execution requires explicit operator authorization")
        if self.interlock_engaged and parent_bundle_id is None:
            raise ValueError("MEV execution interlock is engaged")
        if not risk_decision.approved:
            raise ValueError("MEV execution requires approved risk decision")
        if candidate.chain_id != self.policy.chain_id:
            raise ValueError("MEV chain ID mismatch")
        if candidate.capital_at_risk_usdt > min(
            self.policy.maximum_capital_at_risk_usdt,
            risk_decision.approved_notional,
        ):
            raise ValueError("MEV capital at risk exceeds authorization")
        if candidate.maximum_gas_cost_usdt > self.policy.maximum_gas_cost_usdt:
            raise ValueError("MEV gas bound exceeds policy")
        if (
            candidate.maximum_builder_payment_usdt
            > self.policy.maximum_builder_payment_usdt
        ):
            raise ValueError("MEV builder payment exceeds policy")
        if (
            candidate.maximum_block_number - candidate.target_block_number + 1
            > self.policy.maximum_target_window_blocks
        ):
            raise ValueError("MEV target block window exceeds policy")
        if candidate.base_block_hash != canonical_base_block_hash:
            raise ValueError("MEV simulation base block is not canonical")
        if len(simulations) < self.policy.minimum_independent_simulators:
            raise ValueError("MEV independent simulation quorum is missing")
        simulator_ids = [item.simulator_id for item in simulations]
        if len(simulator_ids) != len(set(simulator_ids)):
            raise ValueError("MEV simulations are not independent")
        now = _utc(as_of)
        for simulation in simulations:
            age = now - simulation.simulated_at
            if age < timedelta(0) or age > timedelta(
                seconds=self.policy.maximum_simulation_age_seconds
            ):
                raise ValueError("MEV simulation is stale or from the future")
            if (
                simulation.payload_hash != candidate.payload_hash
                or simulation.base_block_number != candidate.base_block_number
                or simulation.base_block_hash != candidate.base_block_hash
            ):
                raise ValueError("MEV simulation does not match candidate state")
            if not simulation.success or simulation.reverting_transaction_hashes:
                raise ValueError("MEV simulation failed or contains a revert")
            if simulation.gas_cost_usdt > candidate.maximum_gas_cost_usdt:
                raise ValueError("MEV simulated gas exceeds candidate bound")
            if (
                simulation.builder_payment_usdt
                > candidate.maximum_builder_payment_usdt
            ):
                raise ValueError("MEV simulated builder payment exceeds bound")
        state_diffs = {item.state_diff_hash for item in simulations}
        if len(state_diffs) != 1:
            raise ValueError("MEV simulations disagree on state transition")
        profits = [item.net_profit_usdt for item in simulations]
        if max(profits) - min(profits) > (
            self.policy.maximum_simulation_profit_dispersion_usdt
        ):
            raise ValueError("MEV simulation profit dispersion exceeds policy")
        if min(profits) < self.policy.minimum_expected_profit_usdt:
            raise ValueError("MEV expected net profit is below policy")
        loss_bound = min(
            self.policy.maximum_loss_usdt,
            risk_decision.approved_risk_usdt,
            candidate.candidate_maximum_loss_usdt,
        )
        if max(item.worst_case_loss_usdt for item in simulations) > loss_bound:
            raise ValueError("MEV simulated worst-case loss exceeds authorization")
        if parent_bundle_id is None:
            return 0
        parent = self._bundle(parent_bundle_id)
        if parent.status is not MevBundleStatus.REORGED:
            raise ValueError("MEV retry requires a reorged parent bundle")
        if parent.opportunity_id != candidate.opportunity_id:
            raise ValueError("MEV retry opportunity identity changed")
        attempt = parent.reorg_attempt + 1
        if attempt > self.policy.maximum_reorg_retries:
            raise ValueError("MEV reorg retry limit exceeded")
        return attempt

    def _recover(self) -> None:
        for entry in self.journal.load():
            if entry.sequence != self._sequence + 1:
                raise ValueError("MEV journal replay sequence gap")
            for snapshot in entry.snapshots:
                current = self.bundles.get(snapshot.bundle_id)
                expected_version = 1 if current is None else current.version + 1
                if snapshot.version != expected_version:
                    raise ValueError("MEV bundle version gap during replay")
                self.bundles[snapshot.bundle_id] = snapshot
            if entry.event_type is MevEventType.INTERLOCK_ENGAGED:
                self.interlock_engaged = True
                self.interlock_reasons = entry.reasons
            self._sequence = entry.sequence

    def _engage_interlock(self, timestamp: datetime, reasons: tuple[str, ...]) -> None:
        self._persist(MevEventType.INTERLOCK_ENGAGED, timestamp, reasons=reasons)
        self.interlock_engaged = True
        self.interlock_reasons = reasons

    def _persist(
        self,
        event_type: MevEventType,
        timestamp: datetime,
        *,
        snapshots: tuple[MevBundleSnapshot, ...] = (),
        reasons: tuple[str, ...] = (),
    ) -> None:
        sequence = self._sequence + 1
        event_id = _hash(
            "mevevt",
            str(sequence),
            event_type.value,
            *(snapshot.bundle_id for snapshot in snapshots),
            _utc(timestamp).isoformat(),
        )
        entry = MevJournalEntry(
            sequence=sequence,
            event_id=event_id,
            event_type=event_type,
            timestamp=timestamp,
            snapshots=snapshots,
            reasons=reasons,
        )
        self.journal.append(entry)
        for snapshot in snapshots:
            self.bundles[snapshot.bundle_id] = snapshot
        self._sequence = sequence

    def _bundle(self, bundle_id: str) -> MevBundleSnapshot:
        try:
            return self.bundles[bundle_id]
        except KeyError as exc:
            raise ValueError("unknown MEV bundle ID") from exc

    @staticmethod
    def _update(
        bundle: MevBundleSnapshot,
        timestamp: datetime,
        **updates: object,
    ) -> MevBundleSnapshot:
        return bundle.model_copy(
            update={
                **updates,
                "version": bundle.version + 1,
                "updated_at": _utc(timestamp),
            }
        )


def _hash(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
