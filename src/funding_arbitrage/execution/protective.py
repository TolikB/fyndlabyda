"""Durable exchange-hosted reduce-only protective stop control."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import InstrumentKey, OrderType, Side


class ProtectiveStopStatus(StrEnum):
    REGISTERED = "REGISTERED"
    SUBMITTING = "SUBMITTING"
    ACTIVE = "ACTIVE"
    TRIGGERED = "TRIGGERED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    BLOCKED = "BLOCKED"


class ProtectiveEventType(StrEnum):
    REGISTERED = "REGISTERED"
    SUBMIT_PREPARED = "SUBMIT_PREPARED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    UNKNOWN_MARKED = "UNKNOWN_MARKED"
    CANCEL_PREPARED = "CANCEL_PREPARED"
    TERMINAL_APPLIED = "TERMINAL_APPLIED"
    RECONCILED = "RECONCILED"
    REPLACEMENT_PREPARED = "REPLACEMENT_PREPARED"
    INTERLOCK_ENGAGED = "INTERLOCK_ENGAGED"
    INTERLOCK_CLEARED = "INTERLOCK_CLEARED"


class ProtectiveStopSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    protective_order_id: str
    position_id: str = Field(min_length=1)
    instrument: InstrumentKey
    side: Side
    quantity: Decimal = Field(gt=0)
    stop_price: Decimal = Field(gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    order_type: OrderType
    reduce_only: bool = True
    exchange_hosted: bool = True
    status: ProtectiveStopStatus
    exchange_order_id: str | None = None
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def require_exchange_protection(self) -> ProtectiveStopSnapshot:
        if not self.reduce_only or not self.exchange_hosted:
            raise ValueError("protective stops must be exchange-hosted and reduce-only")
        if self.order_type not in {OrderType.STOP, OrderType.STOP_LIMIT}:
            raise ValueError("protective order must be STOP or STOP_LIMIT")
        if self.order_type is OrderType.STOP_LIMIT and self.limit_price is None:
            raise ValueError("STOP_LIMIT protection requires limit price")
        if self.order_type is OrderType.STOP and self.limit_price is not None:
            raise ValueError("STOP protection cannot carry a limit price")
        return self


class ProtectiveJournalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    event_id: str
    event_type: ProtectiveEventType
    timestamp: datetime
    snapshot: ProtectiveStopSnapshot | None = None
    reasons: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class VenueProtectiveOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    protective_order_id: str
    exchange_order_id: str
    instrument: InstrumentKey
    side: Side
    quantity: Decimal = Field(gt=0)
    stop_price: Decimal = Field(gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    order_type: OrderType
    reduce_only: bool
    status: ProtectiveStopStatus

    @model_validator(mode="after")
    def validate_observable_status(self) -> VenueProtectiveOrder:
        if self.status not in {
            ProtectiveStopStatus.ACTIVE,
            ProtectiveStopStatus.TRIGGERED,
            ProtectiveStopStatus.CANCELLED,
            ProtectiveStopStatus.REJECTED,
        }:
            raise ValueError("venue protection must report an observable status")
        return self


class ProtectiveReconciliationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    safe: bool
    active_count: int = Field(ge=0)
    triggered_count: int = Field(ge=0)
    issues: tuple[str, ...]
    interlock_engaged: bool


class JsonlProtectiveJournal:
    """Fsync journal that is authoritative across process restarts."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: ProtectiveJournalEntry) -> None:
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(entry.model_dump_json() + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[ProtectiveJournalEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            ProtectiveJournalEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        if tuple(entry.sequence for entry in entries) != tuple(
            range(1, len(entries) + 1)
        ):
            raise ValueError("protective journal sequence is not contiguous")
        return entries


class ProtectiveStopManager:
    """Owns expected exchange-side protection and blocks risk on divergence."""

    def __init__(self, journal: JsonlProtectiveJournal) -> None:
        self.journal = journal
        self.stops: dict[str, ProtectiveStopSnapshot] = {}
        self.interlock_engaged = False
        self.interlock_reasons: tuple[str, ...] = ()
        self._sequence = 0
        self._recover()

    def register_stop(
        self,
        *,
        position_id: str,
        instrument: InstrumentKey,
        signed_position_quantity: Decimal,
        stop_price: Decimal,
        limit_price: Decimal | None,
        timestamp: datetime,
    ) -> ProtectiveStopSnapshot:
        if not position_id:
            raise ValueError("protective stop requires position ID")
        if signed_position_quantity == 0:
            raise ValueError("flat position cannot create protective stop")
        if stop_price <= 0 or (limit_price is not None and limit_price <= 0):
            raise ValueError("protective prices must be positive")
        side = Side.SELL if signed_position_quantity > 0 else Side.BUY
        quantity = abs(signed_position_quantity)
        order_type = OrderType.STOP_LIMIT if limit_price is not None else OrderType.STOP
        protective_id = _protective_id(position_id, instrument)
        existing = self.stops.get(protective_id)
        if existing is not None:
            identity = (
                instrument,
                side,
                quantity,
                stop_price,
                limit_price,
                order_type,
            )
            expected = (
                existing.instrument,
                existing.side,
                existing.quantity,
                existing.stop_price,
                existing.limit_price,
                existing.order_type,
            )
            if identity != expected:
                raise ValueError("protective order identity collision")
            return existing
        now = _utc(timestamp)
        snapshot = ProtectiveStopSnapshot(
            protective_order_id=protective_id,
            position_id=position_id,
            instrument=instrument,
            side=side,
            quantity=quantity,
            stop_price=stop_price,
            limit_price=limit_price,
            order_type=order_type,
            status=ProtectiveStopStatus.REGISTERED,
            version=1,
            created_at=now,
            updated_at=now,
        )
        self._persist(ProtectiveEventType.REGISTERED, now, snapshot=snapshot)
        return snapshot

    def prepare_submit(
        self,
        protective_order_id: str,
        timestamp: datetime,
    ) -> ProtectiveStopSnapshot:
        stop = self._stop(protective_order_id)
        if stop.status is ProtectiveStopStatus.SUBMITTING:
            return stop
        if stop.status is not ProtectiveStopStatus.REGISTERED:
            raise ValueError("only registered stop can be submitted")
        updated = self._update(stop, timestamp, status=ProtectiveStopStatus.SUBMITTING)
        self._persist(ProtectiveEventType.SUBMIT_PREPARED, timestamp, snapshot=updated)
        return updated

    def acknowledge(
        self,
        protective_order_id: str,
        exchange_order_id: str,
        timestamp: datetime,
    ) -> ProtectiveStopSnapshot:
        if not exchange_order_id:
            raise ValueError("protective acknowledgement requires exchange order ID")
        stop = self._stop(protective_order_id)
        if stop.status is ProtectiveStopStatus.ACTIVE:
            if stop.exchange_order_id != exchange_order_id:
                raise ValueError("protective exchange order ID changed")
            return stop
        if stop.status is not ProtectiveStopStatus.SUBMITTING:
            raise ValueError("only submitting protection can be acknowledged")
        updated = self._update(
            stop,
            timestamp,
            status=ProtectiveStopStatus.ACTIVE,
            exchange_order_id=exchange_order_id,
        )
        self._persist(ProtectiveEventType.ACKNOWLEDGED, timestamp, snapshot=updated)
        return updated

    def mark_unknown(
        self,
        protective_order_id: str,
        timestamp: datetime,
        reason: str,
    ) -> ProtectiveStopSnapshot:
        stop = self._stop(protective_order_id)
        if stop.status not in {
            ProtectiveStopStatus.SUBMITTING,
            ProtectiveStopStatus.ACTIVE,
            ProtectiveStopStatus.CANCEL_PENDING,
        }:
            raise ValueError("protective stop cannot become unknown")
        updated = self._update(stop, timestamp, status=ProtectiveStopStatus.UNKNOWN)
        self._persist(
            ProtectiveEventType.UNKNOWN_MARKED,
            timestamp,
            snapshot=updated,
            reasons=(reason,),
        )
        return updated

    def prepare_cancel(
        self,
        protective_order_id: str,
        timestamp: datetime,
        *,
        position_is_flat: bool,
    ) -> ProtectiveStopSnapshot:
        if not position_is_flat:
            raise ValueError("protection cannot be cancelled while position is open")
        stop = self._stop(protective_order_id)
        if stop.status is ProtectiveStopStatus.CANCEL_PENDING:
            return stop
        if stop.status not in {
            ProtectiveStopStatus.ACTIVE,
            ProtectiveStopStatus.UNKNOWN,
        }:
            raise ValueError("protective stop is not cancellable")
        updated = self._update(
            stop,
            timestamp,
            status=ProtectiveStopStatus.CANCEL_PENDING,
        )
        self._persist(ProtectiveEventType.CANCEL_PREPARED, timestamp, snapshot=updated)
        return updated

    def apply_terminal(
        self,
        protective_order_id: str,
        status: ProtectiveStopStatus,
        timestamp: datetime,
    ) -> ProtectiveStopSnapshot:
        if status not in {
            ProtectiveStopStatus.TRIGGERED,
            ProtectiveStopStatus.CANCELLED,
            ProtectiveStopStatus.REJECTED,
        }:
            raise ValueError("invalid terminal protective status")
        stop = self._stop(protective_order_id)
        if status is ProtectiveStopStatus.CANCELLED and stop.status is not (
            ProtectiveStopStatus.CANCEL_PENDING
        ):
            raise ValueError("unsolicited protective cancellation")
        if status is ProtectiveStopStatus.TRIGGERED and stop.status not in {
            ProtectiveStopStatus.ACTIVE,
            ProtectiveStopStatus.CANCEL_PENDING,
        }:
            raise ValueError("inactive protective stop cannot trigger")
        updated = self._update(stop, timestamp, status=status)
        self._persist(ProtectiveEventType.TERMINAL_APPLIED, timestamp, snapshot=updated)
        return updated

    def reconcile(
        self,
        venue_orders: tuple[VenueProtectiveOrder, ...],
        timestamp: datetime,
    ) -> ProtectiveReconciliationResult:
        observed = {order.protective_order_id: order for order in venue_orders}
        if len(observed) != len(venue_orders):
            raise ValueError("duplicate venue protective order IDs")
        expected_statuses = {
            ProtectiveStopStatus.SUBMITTING,
            ProtectiveStopStatus.ACTIVE,
            ProtectiveStopStatus.UNKNOWN,
            ProtectiveStopStatus.CANCEL_PENDING,
        }
        issues: list[str] = []
        active_count = 0
        triggered_count = 0
        for protective_id, stop in tuple(self.stops.items()):
            if stop.status not in expected_statuses:
                continue
            venue = observed.pop(protective_id, None)
            if venue is None:
                issues.append(f"{protective_id}:missing_exchange_protection")
                self._block(stop, timestamp)
                continue
            mismatch = _protection_mismatch(stop, venue)
            if mismatch:
                issues.append(f"{protective_id}:{mismatch}")
                self._block(stop, timestamp)
                continue
            if (
                venue.status is ProtectiveStopStatus.CANCELLED
                and stop.status is ProtectiveStopStatus.CANCEL_PENDING
            ):
                reconciled = self._update(
                    stop,
                    timestamp,
                    status=ProtectiveStopStatus.CANCELLED,
                    exchange_order_id=venue.exchange_order_id,
                )
                self._persist(
                    ProtectiveEventType.RECONCILED,
                    timestamp,
                    snapshot=reconciled,
                )
                continue
            if venue.status in {
                ProtectiveStopStatus.CANCELLED,
                ProtectiveStopStatus.REJECTED,
            }:
                issues.append(
                    f"{protective_id}:unexpected_{venue.status.value.lower()}"
                )
                self._block(stop, timestamp)
                continue
            reconciled = self._update(
                stop,
                timestamp,
                status=venue.status,
                exchange_order_id=venue.exchange_order_id,
            )
            self._persist(
                ProtectiveEventType.RECONCILED,
                timestamp,
                snapshot=reconciled,
            )
            if venue.status is ProtectiveStopStatus.ACTIVE:
                active_count += 1
            else:
                triggered_count += 1
        for protective_id in sorted(observed):
            issues.append(f"{protective_id}:orphan_exchange_protection")
        if issues:
            self._engage_interlock(timestamp, tuple(issues))
        return ProtectiveReconciliationResult(
            safe=not issues,
            active_count=active_count,
            triggered_count=triggered_count,
            issues=tuple(issues),
            interlock_engaged=self.interlock_engaged,
        )

    def prepare_replacement(
        self,
        protective_order_id: str,
        timestamp: datetime,
    ) -> ProtectiveStopSnapshot:
        stop = self._stop(protective_order_id)
        if stop.status is not ProtectiveStopStatus.BLOCKED:
            raise ValueError("only blocked protection can be replaced")
        updated = self._update(
            stop,
            timestamp,
            status=ProtectiveStopStatus.SUBMITTING,
            exchange_order_id=None,
        )
        self._persist(
            ProtectiveEventType.REPLACEMENT_PREPARED,
            timestamp,
            snapshot=updated,
        )
        return updated

    def clear_interlock(
        self,
        timestamp: datetime,
        *,
        operator_approved: bool,
        risk_approved: bool,
    ) -> None:
        if not operator_approved or not risk_approved:
            raise ValueError("protective interlock requires dual approval")
        if any(stop.status is ProtectiveStopStatus.BLOCKED for stop in self.stops.values()):
            raise ValueError("blocked protective orders remain unresolved")
        self._persist(ProtectiveEventType.INTERLOCK_CLEARED, timestamp)
        self.interlock_engaged = False
        self.interlock_reasons = ()

    def _recover(self) -> None:
        for entry in self.journal.load():
            if entry.sequence != self._sequence + 1:
                raise ValueError("protective journal replay sequence gap")
            if entry.snapshot is not None:
                current = self.stops.get(entry.snapshot.protective_order_id)
                expected_version = 1 if current is None else current.version + 1
                if entry.snapshot.version != expected_version:
                    raise ValueError("protective stop version gap during replay")
                self.stops[entry.snapshot.protective_order_id] = entry.snapshot
            if entry.event_type is ProtectiveEventType.INTERLOCK_ENGAGED:
                self.interlock_engaged = True
                self.interlock_reasons = entry.reasons
            elif entry.event_type is ProtectiveEventType.INTERLOCK_CLEARED:
                self.interlock_engaged = False
                self.interlock_reasons = ()
            self._sequence = entry.sequence

    def _block(self, stop: ProtectiveStopSnapshot, timestamp: datetime) -> None:
        updated = self._update(stop, timestamp, status=ProtectiveStopStatus.BLOCKED)
        self._persist(ProtectiveEventType.RECONCILED, timestamp, snapshot=updated)

    def _engage_interlock(self, timestamp: datetime, reasons: tuple[str, ...]) -> None:
        self._persist(
            ProtectiveEventType.INTERLOCK_ENGAGED,
            timestamp,
            reasons=reasons,
        )
        self.interlock_engaged = True
        self.interlock_reasons = reasons

    def _persist(
        self,
        event_type: ProtectiveEventType,
        timestamp: datetime,
        *,
        snapshot: ProtectiveStopSnapshot | None = None,
        reasons: tuple[str, ...] = (),
    ) -> None:
        sequence = self._sequence + 1
        identity = snapshot.protective_order_id if snapshot is not None else "GLOBAL"
        version = snapshot.version if snapshot is not None else 0
        event_id = "protectevt_" + hashlib.sha256(
            f"{sequence}|{event_type}|{identity}|{version}|{_utc(timestamp).isoformat()}".encode()
        ).hexdigest()[:32]
        entry = ProtectiveJournalEntry(
            sequence=sequence,
            event_id=event_id,
            event_type=event_type,
            timestamp=timestamp,
            snapshot=snapshot,
            reasons=reasons,
        )
        self.journal.append(entry)
        if snapshot is not None:
            self.stops[snapshot.protective_order_id] = snapshot
        self._sequence = sequence

    def _stop(self, protective_order_id: str) -> ProtectiveStopSnapshot:
        try:
            return self.stops[protective_order_id]
        except KeyError as exc:
            raise ValueError("unknown protective order ID") from exc

    @staticmethod
    def _update(
        stop: ProtectiveStopSnapshot,
        timestamp: datetime,
        **updates: object,
    ) -> ProtectiveStopSnapshot:
        return stop.model_copy(
            update={
                **updates,
                "version": stop.version + 1,
                "updated_at": _utc(timestamp),
            }
        )


def _protective_id(position_id: str, instrument: InstrumentKey) -> str:
    digest = hashlib.sha256(f"{position_id}|{instrument.canonical_id}".encode()).hexdigest()
    return "protect_" + digest[:24]


def _protection_mismatch(
    expected: ProtectiveStopSnapshot,
    observed: VenueProtectiveOrder,
) -> str | None:
    fields = (
        ("instrument", expected.instrument, observed.instrument),
        ("side", expected.side, observed.side),
        ("quantity", expected.quantity, observed.quantity),
        ("stop_price", expected.stop_price, observed.stop_price),
        ("limit_price", expected.limit_price, observed.limit_price),
        ("order_type", expected.order_type, observed.order_type),
        ("reduce_only", True, observed.reduce_only),
    )
    for name, wanted, actual in fields:
        if wanted != actual:
            return f"{name}_mismatch"
    if expected.exchange_order_id is not None and (
        expected.exchange_order_id != observed.exchange_order_id
    ):
        return "exchange_order_id_mismatch"
    return None


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
