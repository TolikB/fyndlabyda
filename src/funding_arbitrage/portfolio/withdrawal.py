"""Disabled-by-default, allowlisted, multi-approved withdrawal state machine."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

GENESIS_HASH = "0" * 64


class WithdrawalApprovalRole(StrEnum):
    OPERATOR = "OPERATOR"
    RISK = "RISK"
    SECURITY = "SECURITY"


class WithdrawalStatus(StrEnum):
    AWAITING_APPROVALS = "AWAITING_APPROVALS"
    APPROVED = "APPROVED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    UNKNOWN = "UNKNOWN"
    CONFIRMING = "CONFIRMING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class WithdrawalEventType(StrEnum):
    REQUESTED = "REQUESTED"
    APPROVAL_ADDED = "APPROVAL_ADDED"
    SUBMISSION_PREPARED = "SUBMISSION_PREPARED"
    SUBMITTED = "SUBMITTED"
    UNKNOWN_MARKED = "UNKNOWN_MARKED"
    CONFIRMING = "CONFIRMING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class WithdrawalDestination(BaseModel):
    model_config = ConfigDict(frozen=True)

    destination_id: str = Field(min_length=1)
    asset: str = Field(min_length=1)
    network: str = Field(min_length=1)
    address: str = Field(min_length=1)
    memo: str | None = None
    allowed_source_venues: tuple[str, ...] = Field(min_length=1)
    not_before: datetime
    expires_at: datetime | None = None
    maximum_single_amount: Decimal = Field(gt=0)
    enabled: bool = True

    @field_validator("destination_id", "asset", "network", "allowed_source_venues")
    @classmethod
    def normalize_identity(
        cls,
        value: str | tuple[str, ...],
    ) -> str | tuple[str, ...]:
        if isinstance(value, tuple):
            normalized = tuple(item.strip().upper() for item in value if item.strip())
            if len(normalized) != len(value) or len(set(normalized)) != len(normalized):
                raise ValueError("withdrawal source venues must be unique and non-empty")
            return normalized
        normalized_value = value.strip().upper()
        if not normalized_value:
            raise ValueError("withdrawal destination identity cannot be blank")
        return normalized_value

    @field_validator("not_before", "expires_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_window(self) -> WithdrawalDestination:
        if self.expires_at is not None and self.expires_at <= self.not_before:
            raise ValueError("withdrawal allowlist expiry must follow activation")
        return self


class WithdrawalPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    required_approval_roles: tuple[WithdrawalApprovalRole, ...] = (
        WithdrawalApprovalRole.RISK,
        WithdrawalApprovalRole.SECURITY,
    )
    maximum_single_usdt: Decimal = Field(gt=0)
    maximum_daily_usdt: Decimal = Field(gt=0)
    maximum_fee_usdt: Decimal = Field(ge=0)
    minimum_confirmations: int = Field(default=12, gt=0)

    @field_validator("required_approval_roles")
    @classmethod
    def require_multiple_roles(
        cls,
        value: tuple[WithdrawalApprovalRole, ...],
    ) -> tuple[WithdrawalApprovalRole, ...]:
        if len(value) < 2 or len(value) != len(set(value)):
            raise ValueError("withdrawals require at least two unique approval roles")
        return value


class WithdrawalApproval(BaseModel):
    model_config = ConfigDict(frozen=True)

    approver_id: str = Field(min_length=1)
    role: WithdrawalApprovalRole
    authorization_id: str = Field(min_length=1)
    approved_at: datetime

    @field_validator("approver_id", "authorization_id")
    @classmethod
    def normalize_principal(cls, value: str) -> str:
        return _principal(value)

    @field_validator("approved_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class WithdrawalSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str = Field(min_length=1)
    client_withdrawal_id: str = Field(min_length=1)
    source_venue: str
    asset: str
    network: str
    destination_id: str
    address: str
    memo: str | None = None
    amount: Decimal = Field(gt=0)
    amount_usdt: Decimal = Field(gt=0)
    maximum_fee_usdt: Decimal = Field(ge=0)
    requested_by: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    requested_date: date
    approvals: tuple[WithdrawalApproval, ...] = ()
    status: WithdrawalStatus
    exchange_withdrawal_id: str | None = None
    transaction_hash: str | None = None
    confirmations: int = Field(default=0, ge=0)
    failure_reason: str | None = None
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("source_venue", "asset", "network", "destination_id")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("withdrawal identity cannot be blank")
        return normalized

    @field_validator("requested_by")
    @classmethod
    def normalize_requester(cls, value: str) -> str:
        return _principal(value)

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class WithdrawalJournalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    event_id: str
    event_type: WithdrawalEventType
    timestamp: datetime
    snapshot: WithdrawalSnapshot
    previous_hash: str
    entry_hash: str

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class JsonlWithdrawalJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: WithdrawalJournalEntry) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(entry.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[WithdrawalJournalEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            WithdrawalJournalEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        previous = GENESIS_HASH
        for sequence, entry in enumerate(entries, start=1):
            if entry.sequence != sequence:
                raise ValueError("withdrawal journal sequence is not contiguous")
            if entry.previous_hash != previous or entry.entry_hash != _entry_hash(entry):
                raise ValueError("withdrawal journal hash chain mismatch")
            previous = entry.entry_hash
        return entries


class WithdrawalManager:
    """Owns authorization state; external venue calls occur only after SUBMITTING."""

    def __init__(
        self,
        policy: WithdrawalPolicy,
        destinations: tuple[WithdrawalDestination, ...],
        journal: JsonlWithdrawalJournal,
    ) -> None:
        self.policy = policy
        self.destinations = {item.destination_id: item for item in destinations}
        if len(self.destinations) != len(destinations):
            raise ValueError("duplicate withdrawal destination IDs")
        self.journal = journal
        self.requests: dict[str, WithdrawalSnapshot] = {}
        self.sequence = 0
        self.head_hash = GENESIS_HASH
        self._recover()

    def request(
        self,
        *,
        request_id: str,
        source_venue: str,
        destination_id: str,
        asset: str,
        network: str,
        address: str,
        memo: str | None,
        amount: Decimal,
        amount_usdt: Decimal,
        maximum_fee_usdt: Decimal,
        requested_by: str,
        reason: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        if not self.policy.enabled:
            raise ValueError("automated withdrawals are disabled")
        request_id = request_id.strip()
        reason = reason.strip()
        requested_by = _principal(requested_by)
        if not request_id or not reason:
            raise ValueError("withdrawal request ID and reason are required")
        if amount <= 0 or amount_usdt <= 0:
            raise ValueError("withdrawal amount must be positive")
        if maximum_fee_usdt < 0 or maximum_fee_usdt > self.policy.maximum_fee_usdt:
            raise ValueError("withdrawal fee bound exceeds policy")
        destination = self._validate_destination(
            destination_id,
            source_venue,
            asset,
            network,
            address,
            memo,
            amount,
            timestamp,
        )
        total_usdt = amount_usdt + maximum_fee_usdt
        if total_usdt > self.policy.maximum_single_usdt:
            raise ValueError("withdrawal exceeds single-transfer policy cap")
        existing = self.requests.get(request_id)
        identity = (
            source_venue.upper(),
            destination.destination_id,
            asset.upper(),
            network.upper(),
            address,
            memo,
            amount,
            amount_usdt,
            maximum_fee_usdt,
            requested_by,
            reason,
            _utc(timestamp),
        )
        if existing is not None:
            existing_identity = (
                existing.source_venue,
                existing.destination_id,
                existing.asset,
                existing.network,
                existing.address,
                existing.memo,
                existing.amount,
                existing.amount_usdt,
                existing.maximum_fee_usdt,
                existing.requested_by,
                existing.reason,
                existing.created_at,
            )
            if identity != existing_identity:
                raise ValueError("withdrawal request ID collision")
            return existing
        request_date = _utc(timestamp).date()
        committed = sum(
            (
                item.amount_usdt + item.maximum_fee_usdt
                for item in self.requests.values()
                if item.requested_date == request_date
                and item.status
                not in {
                    WithdrawalStatus.REJECTED,
                    WithdrawalStatus.CANCELLED,
                    WithdrawalStatus.FAILED,
                }
            ),
            Decimal("0"),
        )
        if committed + total_usdt > self.policy.maximum_daily_usdt:
            raise ValueError("withdrawal exceeds daily policy cap")
        now = _utc(timestamp)
        snapshot = WithdrawalSnapshot(
            request_id=request_id,
            client_withdrawal_id=_client_withdrawal_id(request_id),
            source_venue=source_venue,
            asset=asset,
            network=network,
            destination_id=destination.destination_id,
            address=address,
            memo=memo,
            amount=amount,
            amount_usdt=amount_usdt,
            maximum_fee_usdt=maximum_fee_usdt,
            requested_by=requested_by,
            reason=reason,
            requested_date=request_date,
            status=WithdrawalStatus.AWAITING_APPROVALS,
            version=1,
            created_at=now,
            updated_at=now,
        )
        self._persist(WithdrawalEventType.REQUESTED, snapshot, now)
        return snapshot

    def approve(
        self,
        request_id: str,
        *,
        approver_id: str,
        role: WithdrawalApprovalRole,
        authorization_id: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        request = self._request(request_id)
        if request.status not in {
            WithdrawalStatus.AWAITING_APPROVALS,
            WithdrawalStatus.APPROVED,
        }:
            raise ValueError("withdrawal is not awaiting approval")
        approver_id = _principal(approver_id)
        authorization_id = _principal(authorization_id)
        if role not in self.policy.required_approval_roles:
            raise ValueError("withdrawal approval role is not authorized")
        if approver_id == request.requested_by:
            raise ValueError("withdrawal requester cannot approve own request")
        existing_principals = {item.approver_id for item in request.approvals}
        existing_roles = {item.role for item in request.approvals}
        if approver_id in existing_principals:
            existing = next(item for item in request.approvals if item.approver_id == approver_id)
            if existing.role is role and existing.authorization_id == authorization_id:
                return request
            raise ValueError("withdrawal approver identity was reused")
        if role in existing_roles:
            raise ValueError("withdrawal approval role already satisfied")
        approval = WithdrawalApproval(
            approver_id=approver_id,
            role=role,
            authorization_id=authorization_id,
            approved_at=timestamp,
        )
        approvals = (*request.approvals, approval)
        approved_roles = {item.role for item in approvals}
        status = (
            WithdrawalStatus.APPROVED
            if set(self.policy.required_approval_roles) <= approved_roles
            else WithdrawalStatus.AWAITING_APPROVALS
        )
        updated = self._update(request, timestamp, approvals=approvals, status=status)
        self._persist(WithdrawalEventType.APPROVAL_ADDED, updated, timestamp)
        return updated

    def prepare_submit(self, request_id: str, timestamp: datetime) -> WithdrawalSnapshot:
        request = self._request(request_id)
        if not self.policy.enabled:
            raise ValueError("automated withdrawals are disabled")
        if request.status is WithdrawalStatus.SUBMITTING:
            return request
        if request.status is not WithdrawalStatus.APPROVED:
            raise ValueError("withdrawal lacks required approvals")
        self._validate_destination(
            request.destination_id,
            request.source_venue,
            request.asset,
            request.network,
            request.address,
            request.memo,
            request.amount,
            timestamp,
        )
        updated = self._update(request, timestamp, status=WithdrawalStatus.SUBMITTING)
        self._persist(WithdrawalEventType.SUBMISSION_PREPARED, updated, timestamp)
        return updated

    def mark_submitted(
        self,
        request_id: str,
        *,
        exchange_withdrawal_id: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        if not exchange_withdrawal_id:
            raise ValueError("exchange withdrawal ID is required")
        request = self._request(request_id)
        if request.status is WithdrawalStatus.SUBMITTED:
            if request.exchange_withdrawal_id != exchange_withdrawal_id:
                raise ValueError("exchange withdrawal ID changed")
            return request
        if request.status is not WithdrawalStatus.SUBMITTING:
            raise ValueError("withdrawal was not persisted before submit")
        updated = self._update(
            request,
            timestamp,
            status=WithdrawalStatus.SUBMITTED,
            exchange_withdrawal_id=exchange_withdrawal_id,
        )
        self._persist(WithdrawalEventType.SUBMITTED, updated, timestamp)
        return updated

    def mark_unknown(
        self,
        request_id: str,
        *,
        reason: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        request = self._request(request_id)
        if request.status not in {
            WithdrawalStatus.SUBMITTING,
            WithdrawalStatus.SUBMITTED,
        }:
            raise ValueError("withdrawal cannot become unknown")
        updated = self._update(
            request,
            timestamp,
            status=WithdrawalStatus.UNKNOWN,
            failure_reason=reason,
        )
        self._persist(WithdrawalEventType.UNKNOWN_MARKED, updated, timestamp)
        return updated

    def observe_transaction(
        self,
        request_id: str,
        *,
        transaction_hash: str,
        confirmations: int,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        if not transaction_hash or confirmations < 0:
            raise ValueError("withdrawal transaction observation is invalid")
        request = self._request(request_id)
        if request.status not in {
            WithdrawalStatus.SUBMITTED,
            WithdrawalStatus.UNKNOWN,
            WithdrawalStatus.CONFIRMING,
        }:
            raise ValueError("withdrawal cannot enter confirmation state")
        if request.transaction_hash not in {None, transaction_hash}:
            raise ValueError("withdrawal transaction hash changed")
        status = (
            WithdrawalStatus.COMPLETED
            if confirmations >= self.policy.minimum_confirmations
            else WithdrawalStatus.CONFIRMING
        )
        updated = self._update(
            request,
            timestamp,
            status=status,
            transaction_hash=transaction_hash,
            confirmations=confirmations,
            failure_reason=None,
        )
        event = (
            WithdrawalEventType.COMPLETED
            if status is WithdrawalStatus.COMPLETED
            else WithdrawalEventType.CONFIRMING
        )
        self._persist(event, updated, timestamp)
        return updated

    def reject_or_fail(
        self,
        request_id: str,
        *,
        rejected: bool,
        reason: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        request = self._request(request_id)
        if request.status in {
            WithdrawalStatus.COMPLETED,
            WithdrawalStatus.CANCELLED,
        }:
            raise ValueError("terminal withdrawal cannot fail")
        status = WithdrawalStatus.REJECTED if rejected else WithdrawalStatus.FAILED
        updated = self._update(
            request,
            timestamp,
            status=status,
            failure_reason=reason,
        )
        event = WithdrawalEventType.REJECTED if rejected else WithdrawalEventType.FAILED
        self._persist(event, updated, timestamp)
        return updated

    def cancel(
        self,
        request_id: str,
        *,
        cancelled_by: str,
        timestamp: datetime,
    ) -> WithdrawalSnapshot:
        request = self._request(request_id)
        if request.status not in {
            WithdrawalStatus.AWAITING_APPROVALS,
            WithdrawalStatus.APPROVED,
        }:
            raise ValueError("submitted withdrawal cannot be locally cancelled")
        updated = self._update(
            request,
            timestamp,
            status=WithdrawalStatus.CANCELLED,
            failure_reason=f"cancelled_by:{cancelled_by}",
        )
        self._persist(WithdrawalEventType.CANCELLED, updated, timestamp)
        return updated

    def _validate_destination(
        self,
        destination_id: str,
        source_venue: str,
        asset: str,
        network: str,
        address: str,
        memo: str | None,
        amount: Decimal,
        timestamp: datetime,
    ) -> WithdrawalDestination:
        try:
            destination = self.destinations[destination_id.upper()]
        except KeyError as exc:
            raise ValueError("withdrawal destination is not allowlisted") from exc
        now = _utc(timestamp)
        if not destination.enabled:
            raise ValueError("withdrawal destination is disabled")
        if now < destination.not_before:
            raise ValueError("withdrawal destination cooldown is not complete")
        if destination.expires_at is not None and now >= destination.expires_at:
            raise ValueError("withdrawal destination allowlist entry expired")
        identity_matches = (
            source_venue.upper() in destination.allowed_source_venues
            and asset.upper() == destination.asset
            and network.upper() == destination.network
            and address == destination.address
            and memo == destination.memo
        )
        if not identity_matches:
            raise ValueError("withdrawal destination details do not exactly match allowlist")
        if amount > destination.maximum_single_amount:
            raise ValueError("withdrawal exceeds destination amount cap")
        return destination

    def _recover(self) -> None:
        for entry in self.journal.load():
            current = self.requests.get(entry.snapshot.request_id)
            expected_version = 1 if current is None else current.version + 1
            if entry.snapshot.version != expected_version:
                raise ValueError("withdrawal version gap during replay")
            self.requests[entry.snapshot.request_id] = entry.snapshot
            self.sequence = entry.sequence
            self.head_hash = entry.entry_hash

    def _persist(
        self,
        event_type: WithdrawalEventType,
        snapshot: WithdrawalSnapshot,
        timestamp: datetime,
    ) -> None:
        candidate = WithdrawalJournalEntry(
            sequence=self.sequence + 1,
            event_id="withdraw_evt_"
            + hashlib.sha256(
                (
                    f"{self.sequence + 1}|{event_type}|"
                    f"{snapshot.request_id}|{snapshot.version}"
                ).encode()
            ).hexdigest()[:32],
            event_type=event_type,
            timestamp=timestamp,
            snapshot=snapshot,
            previous_hash=self.head_hash,
            entry_hash=GENESIS_HASH,
        )
        entry = candidate.model_copy(update={"entry_hash": _entry_hash(candidate)})
        self.journal.append(entry)
        self.requests[snapshot.request_id] = snapshot
        self.sequence = entry.sequence
        self.head_hash = entry.entry_hash

    def _request(self, request_id: str) -> WithdrawalSnapshot:
        try:
            return self.requests[request_id]
        except KeyError as exc:
            raise ValueError("unknown withdrawal request ID") from exc

    @staticmethod
    def _update(
        request: WithdrawalSnapshot,
        timestamp: datetime,
        **updates: object,
    ) -> WithdrawalSnapshot:
        return request.model_copy(
            update={
                **updates,
                "version": request.version + 1,
                "updated_at": _utc(timestamp),
            }
        )


def _entry_hash(entry: WithdrawalJournalEntry) -> str:
    payload = entry.model_dump(mode="json", exclude={"entry_hash"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _client_withdrawal_id(request_id: str) -> str:
    return "withdraw_" + hashlib.sha256(request_id.encode()).hexdigest()[:24]


def _principal(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("withdrawal principal identity cannot be blank")
    return normalized


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
