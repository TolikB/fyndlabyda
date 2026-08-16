"""Classified continuous reconciliation across local and authenticated venue state."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import InstrumentKey, OrderStatus, Side

GENESIS_HASH = "0" * 64


class ReconciliationCategory(StrEnum):
    CONNECTIVITY = "CONNECTIVITY"
    ORDER = "ORDER"
    FILL = "FILL"
    BALANCE = "BALANCE"
    POSITION = "POSITION"
    FUNDING = "FUNDING"


class ReconciliationSeverity(StrEnum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class OrderReconState(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    client_order_id: str
    exchange_order_id: str | None = None
    status: OrderStatus
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    updated_at: datetime

    @field_validator("venue", "client_order_id", "exchange_order_id")
    @classmethod
    def normalize_identity(cls, value: str | None) -> str | None:
        return _identity(value)

    @field_validator("updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_fill(self) -> OrderReconState:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("reconciliation order overfilled")
        return self

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.client_order_id}"


class FillReconState(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    fill_id: str
    client_order_id: str
    instrument: InstrumentKey
    side: Side
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    fee_amount: Decimal = Field(ge=0)
    fee_asset: str
    timestamp: datetime

    @field_validator("venue", "fill_id", "client_order_id", "fee_asset")
    @classmethod
    def normalize_identity(cls, value: str | None) -> str | None:
        return _identity(value)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.fill_id}"


class BalanceReconState(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    asset: str
    total: Decimal
    available: Decimal
    locked: Decimal
    borrowed: Decimal = Decimal("0")
    timestamp: datetime

    @field_validator("venue", "asset")
    @classmethod
    def normalize_identity(cls, value: str | None) -> str | None:
        return _identity(value)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.asset}"


class PositionReconState(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    signed_quantity: Decimal
    entry_price: Decimal | None = Field(default=None, gt=0)
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> str:
        return self.instrument.canonical_id


class FundingReconState(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    external_id: str
    instrument: InstrumentKey
    asset: str
    amount: Decimal
    settlement_timestamp: datetime

    @field_validator("venue", "external_id", "asset")
    @classmethod
    def normalize_identity(cls, value: str | None) -> str | None:
        return _identity(value)

    @field_validator("settlement_timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.external_id}"


ReconItem = (
    OrderReconState
    | FillReconState
    | BalanceReconState
    | PositionReconState
    | FundingReconState
)


class ReconciliationInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    as_of: datetime
    source_health: dict[str, bool]
    local_orders: tuple[OrderReconState, ...] = ()
    venue_orders: tuple[OrderReconState, ...] = ()
    local_fills: tuple[FillReconState, ...] = ()
    venue_fills: tuple[FillReconState, ...] = ()
    local_balances: tuple[BalanceReconState, ...] = ()
    venue_balances: tuple[BalanceReconState, ...] = ()
    local_positions: tuple[PositionReconState, ...] = ()
    venue_positions: tuple[PositionReconState, ...] = ()
    local_funding: tuple[FundingReconState, ...] = ()
    venue_funding: tuple[FundingReconState, ...] = ()

    @field_validator("as_of")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def reject_duplicate_keys(self) -> ReconciliationInput:
        collections = (
            self.local_orders,
            self.venue_orders,
            self.local_fills,
            self.venue_fills,
            self.local_balances,
            self.venue_balances,
            self.local_positions,
            self.venue_positions,
            self.local_funding,
            self.venue_funding,
        )
        for items in collections:
            keys = [item.key for item in items]
            if len(keys) != len(set(keys)):
                raise ValueError("reconciliation input contains duplicate identity")
        return self


class ReconciliationTolerance(BaseModel):
    model_config = ConfigDict(frozen=True)

    quantity_absolute: Decimal = Field(default=Decimal("0.00000001"), ge=0)
    money_absolute: Decimal = Field(default=Decimal("0.01"), ge=0)
    balance_relative: Decimal = Field(default=Decimal("0.000001"), ge=0)
    propagation_grace_seconds: int = Field(default=10, ge=0)
    maximum_snapshot_age_seconds: int = Field(default=30, gt=0)


class ReconciliationIssue(BaseModel):
    model_config = ConfigDict(frozen=True)

    issue_id: str
    category: ReconciliationCategory
    severity: ReconciliationSeverity
    code: str
    identity: str
    local_value: str | None = None
    venue_value: str | None = None
    action: str


class ReconciliationAuditEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    run_id: str
    timestamp: datetime
    passed: bool
    issue_counts: dict[str, int]
    issues: tuple[ReconciliationIssue, ...]
    input_hash: str
    previous_hash: str
    audit_hash: str

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class ReconciliationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    passed: bool
    issues: tuple[ReconciliationIssue, ...]
    critical_count: int = Field(ge=0)
    warning_count: int = Field(ge=0)
    interlock_engaged: bool
    audit_hash: str


class JsonlReconciliationAudit:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: ReconciliationAuditEntry) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(entry.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[ReconciliationAuditEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            ReconciliationAuditEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        previous = GENESIS_HASH
        for sequence, entry in enumerate(entries, start=1):
            if entry.sequence != sequence:
                raise ValueError("reconciliation audit sequence is not contiguous")
            if entry.previous_hash != previous:
                raise ValueError("reconciliation audit hash chain mismatch")
            if entry.audit_hash != _audit_hash(entry):
                raise ValueError("reconciliation audit entry hash mismatch")
            previous = entry.audit_hash
        return entries


class ContinuousReconciler:
    def __init__(
        self,
        tolerance: ReconciliationTolerance,
        audit: JsonlReconciliationAudit,
    ) -> None:
        self.tolerance = tolerance
        self.audit = audit
        entries = audit.load()
        self.sequence = len(entries)
        self.head_hash = entries[-1].audit_hash if entries else GENESIS_HASH
        self.interlock_engaged = any(not entry.passed for entry in entries)

    def reconcile(self, reconciliation_input: ReconciliationInput) -> ReconciliationResult:
        issues: list[ReconciliationIssue] = []
        for venue, healthy in sorted(reconciliation_input.source_health.items()):
            if not healthy:
                issues.append(
                    _issue(
                        ReconciliationCategory.CONNECTIVITY,
                        ReconciliationSeverity.CRITICAL,
                        "PRIVATE_SOURCE_UNAVAILABLE",
                        venue,
                        None,
                        "unavailable",
                        "pause venue and restore authenticated source",
                    )
                )
        self._check_freshness(reconciliation_input, issues)
        self._check_orders(reconciliation_input, issues)
        self._check_fills(reconciliation_input, issues)
        self._check_balances(reconciliation_input, issues)
        self._check_positions(reconciliation_input, issues)
        self._check_funding(reconciliation_input, issues)
        issues.sort(key=lambda item: (item.severity, item.category, item.identity, item.code))
        critical = sum(
            issue.severity is ReconciliationSeverity.CRITICAL for issue in issues
        )
        warning = len(issues) - critical
        passed = critical == 0
        if not passed:
            self.interlock_engaged = True
        input_hash = hashlib.sha256(
            reconciliation_input.model_dump_json().encode()
        ).hexdigest()
        run_id = "recon_" + hashlib.sha256(
            f"{self.sequence + 1}|{input_hash}".encode()
        ).hexdigest()[:32]
        counts = Counter(f"{item.severity.value}:{item.category.value}" for item in issues)
        candidate = ReconciliationAuditEntry(
            sequence=self.sequence + 1,
            run_id=run_id,
            timestamp=reconciliation_input.as_of,
            passed=passed,
            issue_counts=dict(counts),
            issues=tuple(issues),
            input_hash=input_hash,
            previous_hash=self.head_hash,
            audit_hash=GENESIS_HASH,
        )
        entry = candidate.model_copy(update={"audit_hash": _audit_hash(candidate)})
        self.audit.append(entry)
        self.sequence = entry.sequence
        self.head_hash = entry.audit_hash
        return ReconciliationResult(
            run_id=run_id,
            passed=passed,
            issues=tuple(issues),
            critical_count=critical,
            warning_count=warning,
            interlock_engaged=self.interlock_engaged,
            audit_hash=entry.audit_hash,
        )

    def _check_freshness(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        all_snapshots: tuple[ReconItem, ...] = (
            *data.venue_orders,
            *data.venue_balances,
            *data.venue_positions,
        )
        maximum_age = timedelta(seconds=self.tolerance.maximum_snapshot_age_seconds)
        for item in all_snapshots:
            timestamp = _item_timestamp(item)
            age = data.as_of - timestamp
            if age < timedelta(0) or age > maximum_age:
                issues.append(
                    _issue(
                        ReconciliationCategory.CONNECTIVITY,
                        ReconciliationSeverity.CRITICAL,
                        "STALE_PRIVATE_SNAPSHOT",
                        item.key,
                        None,
                        timestamp.isoformat(),
                        "pause venue and refresh authenticated snapshot",
                    )
                )

    def _check_orders(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        local = {item.key: item for item in data.local_orders}
        venue = {item.key: item for item in data.venue_orders}
        for key in sorted(set(local) | set(venue)):
            expected = local.get(key)
            actual = venue.get(key)
            if expected is None:
                issues.append(
                    _issue(
                        ReconciliationCategory.ORDER,
                        ReconciliationSeverity.CRITICAL,
                        "ORPHAN_VENUE_ORDER",
                        key,
                        None,
                        actual.status.value if actual else None,
                        "cancel or adopt venue order under incident procedure",
                    )
                )
                continue
            if actual is None:
                issues.append(
                    _issue(
                        ReconciliationCategory.ORDER,
                        ReconciliationSeverity.CRITICAL,
                        "LOCAL_ORDER_MISSING_AT_VENUE",
                        key,
                        expected.status.value,
                        None,
                        "query order history and reconcile unknown outcome",
                    )
                )
                continue
            comparisons = (
                ("ORDER_STATUS_MISMATCH", expected.status.value, actual.status.value),
                (
                    "ORDER_REQUESTED_QUANTITY_MISMATCH",
                    expected.requested_quantity,
                    actual.requested_quantity,
                ),
                (
                    "ORDER_FILLED_QUANTITY_MISMATCH",
                    expected.filled_quantity,
                    actual.filled_quantity,
                ),
                (
                    "ORDER_EXCHANGE_ID_MISMATCH",
                    expected.exchange_order_id,
                    actual.exchange_order_id,
                ),
            )
            for code, wanted, observed in comparisons:
                if wanted != observed:
                    issues.append(
                        _issue(
                            ReconciliationCategory.ORDER,
                            ReconciliationSeverity.CRITICAL,
                            code,
                            key,
                            str(wanted),
                            str(observed),
                            "pause order scope and reconcile venue history",
                        )
                    )

    def _check_fills(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        local = {item.key: item for item in data.local_fills}
        venue = {item.key: item for item in data.venue_fills}
        for key in sorted(set(local) | set(venue)):
            expected = local.get(key)
            actual = venue.get(key)
            if expected is None or actual is None:
                present = expected or actual
                assert present is not None
                severity = self._propagation_severity(data.as_of, present.timestamp)
                code = (
                    "VENUE_FILL_MISSING_LOCALLY"
                    if expected is None
                    else "LOCAL_FILL_NOT_CONFIRMED"
                )
                issues.append(
                    _issue(
                        ReconciliationCategory.FILL,
                        severity,
                        code,
                        key,
                        "present" if expected else None,
                        "present" if actual else None,
                        "ingest fill or query authenticated trade history",
                    )
                )
                continue
            fields = (
                expected.client_order_id == actual.client_order_id,
                expected.instrument == actual.instrument,
                expected.side == actual.side,
                expected.price == actual.price,
                expected.quantity == actual.quantity,
                expected.fee_amount == actual.fee_amount,
                expected.fee_asset == actual.fee_asset,
                expected.timestamp == actual.timestamp,
            )
            if not all(fields):
                issues.append(
                    _issue(
                        ReconciliationCategory.FILL,
                        ReconciliationSeverity.CRITICAL,
                        "FILL_DETAILS_MISMATCH",
                        key,
                        expected.model_dump_json(),
                        actual.model_dump_json(),
                        "freeze accounting and replay raw venue fills",
                    )
                )

    def _check_balances(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        local = {item.key: item for item in data.local_balances}
        venue = {item.key: item for item in data.venue_balances}
        for key in sorted(set(local) | set(venue)):
            expected = local.get(key)
            actual = venue.get(key)
            if expected is None or actual is None:
                issues.append(
                    _issue(
                        ReconciliationCategory.BALANCE,
                        ReconciliationSeverity.CRITICAL,
                        "BALANCE_IDENTITY_MISSING",
                        key,
                        expected.model_dump_json() if expected else None,
                        actual.model_dump_json() if actual else None,
                        "pause venue and reconcile account ledger",
                    )
                )
                continue
            tolerance = max(
                self.tolerance.money_absolute,
                abs(expected.total) * self.tolerance.balance_relative,
            )
            fields = (
                ("total", expected.total, actual.total),
                ("available", expected.available, actual.available),
                ("locked", expected.locked, actual.locked),
                ("borrowed", expected.borrowed, actual.borrowed),
            )
            for field, wanted, observed in fields:
                if abs(wanted - observed) > tolerance:
                    issues.append(
                        _issue(
                            ReconciliationCategory.BALANCE,
                            ReconciliationSeverity.CRITICAL,
                            f"BALANCE_{field.upper()}_MISMATCH",
                            key,
                            str(wanted),
                            str(observed),
                            "pause venue and reconcile cash/collateral movements",
                        )
                    )

    def _check_positions(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        local = {item.key: item for item in data.local_positions}
        venue = {item.key: item for item in data.venue_positions}
        for key in sorted(set(local) | set(venue)):
            expected = local.get(key)
            actual = venue.get(key)
            if expected is None or actual is None:
                issues.append(
                    _issue(
                        ReconciliationCategory.POSITION,
                        ReconciliationSeverity.CRITICAL,
                        "POSITION_IDENTITY_MISSING",
                        key,
                        expected.model_dump_json() if expected else None,
                        actual.model_dump_json() if actual else None,
                        "freeze strategy and flatten or adopt venue exposure",
                    )
                )
                continue
            mismatched = (
                abs(expected.signed_quantity - actual.signed_quantity)
                > self.tolerance.quantity_absolute
                or abs(expected.realized_pnl - actual.realized_pnl)
                > self.tolerance.money_absolute
                or abs(expected.unrealized_pnl - actual.unrealized_pnl)
                > self.tolerance.money_absolute
                or expected.entry_price != actual.entry_price
            )
            if mismatched:
                issues.append(
                    _issue(
                        ReconciliationCategory.POSITION,
                        ReconciliationSeverity.CRITICAL,
                        "POSITION_DETAILS_MISMATCH",
                        key,
                        expected.model_dump_json(),
                        actual.model_dump_json(),
                        "pause asset scope and reconcile fills and marks",
                    )
                )

    def _check_funding(
        self,
        data: ReconciliationInput,
        issues: list[ReconciliationIssue],
    ) -> None:
        local = {item.key: item for item in data.local_funding}
        venue = {item.key: item for item in data.venue_funding}
        for key in sorted(set(local) | set(venue)):
            expected = local.get(key)
            actual = venue.get(key)
            if expected is None or actual is None:
                present = expected or actual
                assert present is not None
                severity = self._propagation_severity(
                    data.as_of,
                    present.settlement_timestamp,
                )
                code = (
                    "VENUE_FUNDING_MISSING_LOCALLY"
                    if expected is None
                    else "LOCAL_FUNDING_NOT_CONFIRMED"
                )
                issues.append(
                    _issue(
                        ReconciliationCategory.FUNDING,
                        severity,
                        code,
                        key,
                        "present" if expected else None,
                        "present" if actual else None,
                        "ingest exact funding payment history",
                    )
                )
                continue
            if (
                expected.instrument != actual.instrument
                or expected.asset != actual.asset
                or expected.amount != actual.amount
                or expected.settlement_timestamp != actual.settlement_timestamp
            ):
                issues.append(
                    _issue(
                        ReconciliationCategory.FUNDING,
                        ReconciliationSeverity.CRITICAL,
                        "FUNDING_DETAILS_MISMATCH",
                        key,
                        expected.model_dump_json(),
                        actual.model_dump_json(),
                        "freeze PnL reporting and replay venue funding history",
                    )
                )

    def _propagation_severity(
        self,
        as_of: datetime,
        event_time: datetime,
    ) -> ReconciliationSeverity:
        age = as_of - event_time
        if age <= timedelta(seconds=self.tolerance.propagation_grace_seconds):
            return ReconciliationSeverity.WARNING
        return ReconciliationSeverity.CRITICAL


def _item_timestamp(item: ReconItem) -> datetime:
    if isinstance(item, OrderReconState):
        return item.updated_at
    if isinstance(item, FillReconState):
        return item.timestamp
    if isinstance(item, (BalanceReconState, PositionReconState)):
        return item.timestamp
    return item.settlement_timestamp


def _issue(
    category: ReconciliationCategory,
    severity: ReconciliationSeverity,
    code: str,
    identity: str,
    local_value: str | None,
    venue_value: str | None,
    action: str,
) -> ReconciliationIssue:
    issue_id = "reconissue_" + hashlib.sha256(
        f"{category}|{severity}|{code}|{identity}|{local_value}|{venue_value}".encode()
    ).hexdigest()[:32]
    return ReconciliationIssue(
        issue_id=issue_id,
        category=category,
        severity=severity,
        code=code,
        identity=identity,
        local_value=local_value,
        venue_value=venue_value,
        action=action,
    )


def _audit_hash(entry: ReconciliationAuditEntry) -> str:
    payload = entry.model_dump(mode="json", exclude={"audit_hash"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _identity(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("reconciliation identity cannot be blank")
    return normalized


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
