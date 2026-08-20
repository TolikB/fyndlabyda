"""Durable venue-independent OMS with restart replay and cancel-race handling."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import ExecutionReport, RiskDecision
from funding_arbitrage.domain.events import (
    InstrumentKey,
    OrderStatus,
    OrderType,
    Side,
)

ZERO = Decimal("0")


class OMSEventType(StrEnum):
    CREATED = "CREATED"
    SUBMIT_PREPARED = "SUBMIT_PREPARED"
    EXECUTION_REPORT = "EXECUTION_REPORT"
    CANCEL_PREPARED = "CANCEL_PREPARED"
    UNKNOWN_MARKED = "UNKNOWN_MARKED"
    RECONCILIATION_STARTED = "RECONCILIATION_STARTED"
    RECONCILIATION_APPLIED = "RECONCILIATION_APPLIED"
    EXPIRED = "EXPIRED"


class OMSOrderSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    client_order_id: str
    signal_id: str
    risk_decision_id: str
    leg_index: int = Field(ge=0)
    instrument: InstrumentKey
    side: Side
    order_type: OrderType
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(default=ZERO, ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    reduce_only: bool = False
    status: OrderStatus
    exchange_order_id: str | None = None
    cancel_requested: bool = False
    rejection_reason: str | None = None
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_quantities(self) -> OMSOrderSnapshot:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("OMS filled quantity cannot exceed requested quantity")
        return self


class OMSJournalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    event_id: str
    event_type: OMSEventType
    timestamp: datetime
    snapshot: OMSOrderSnapshot
    report_fingerprint: str | None = None
    reason: str | None = None

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class OMSJournal(Protocol):
    def append(self, entry: OMSJournalEntry) -> None: ...

    def load(self) -> tuple[OMSJournalEntry, ...]: ...


class InMemoryOMSJournal:
    """Deterministic replay journal implementing the production OMS contract."""

    def __init__(self) -> None:
        self.entries: list[OMSJournalEntry] = []

    def append(self, entry: OMSJournalEntry) -> None:
        expected = len(self.entries) + 1
        if entry.sequence != expected:
            raise ValueError("OMS journal sequence is not contiguous")
        self.entries.append(entry)

    def load(self) -> tuple[OMSJournalEntry, ...]:
        return tuple(self.entries)


class JsonlOMSJournal:
    """Append-only journal; every entry is flushed and fsynced before returning."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: OMSJournalEntry) -> None:
        encoded = entry.model_dump_json() + "\n"
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())

    def load(self) -> tuple[OMSJournalEntry, ...]:
        if not self.path.exists():
            return ()
        entries = tuple(
            OMSJournalEntry.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        expected = tuple(range(1, len(entries) + 1))
        if tuple(entry.sequence for entry in entries) != expected:
            raise ValueError("OMS journal sequence is not contiguous")
        return entries


class DurableOMS:
    def __init__(self, journal: OMSJournal) -> None:
        self.journal = journal
        self.orders: dict[str, OMSOrderSnapshot] = {}
        self._report_fingerprints: set[str] = set()
        self._sequence = 0
        self._recover()

    def create_order(
        self,
        risk_decision: RiskDecision,
        *,
        leg_index: int,
        instrument: InstrumentKey,
        side: Side,
        order_type: OrderType,
        quantity: Decimal,
        limit_price: Decimal | None,
        reduce_only: bool,
        timestamp: datetime,
    ) -> OMSOrderSnapshot:
        if not risk_decision.approved:
            raise ValueError("OMS order requires approved risk decision")
        if quantity <= 0 or quantity > risk_decision.approved_quantity:
            raise ValueError("OMS quantity exceeds risk authorization")
        if order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and limit_price is None:
            raise ValueError("limit order requires limit price")
        client_order_id = _client_order_id(
            risk_decision.signal_id,
            risk_decision.decision_id,
            leg_index,
        )
        existing = self.orders.get(client_order_id)
        if existing is not None:
            candidate_identity = (
                instrument,
                side,
                order_type,
                quantity,
                limit_price,
                reduce_only,
            )
            existing_identity = (
                existing.instrument,
                existing.side,
                existing.order_type,
                existing.requested_quantity,
                existing.limit_price,
                existing.reduce_only,
            )
            if candidate_identity != existing_identity:
                raise ValueError("deterministic client order ID collision")
            return existing
        now = _utc(timestamp)
        snapshot = OMSOrderSnapshot(
            client_order_id=client_order_id,
            signal_id=risk_decision.signal_id,
            risk_decision_id=risk_decision.decision_id,
            leg_index=leg_index,
            instrument=instrument,
            side=side,
            order_type=order_type,
            requested_quantity=quantity,
            limit_price=limit_price,
            reduce_only=reduce_only,
            status=OrderStatus.NEW,
            version=1,
            created_at=now,
            updated_at=now,
        )
        self._persist(OMSEventType.CREATED, snapshot, now)
        return snapshot

    def prepare_submit(self, client_order_id: str, timestamp: datetime) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status is OrderStatus.SUBMITTING:
            return order
        if order.status is not OrderStatus.NEW:
            raise ValueError("only NEW order can be prepared for submission")
        updated = self._update(order, timestamp, status=OrderStatus.SUBMITTING)
        self._persist(OMSEventType.SUBMIT_PREPARED, updated, timestamp)
        return updated

    def apply_report(self, report: ExecutionReport) -> OMSOrderSnapshot:
        order = self._order(report.client_order_id)
        fingerprint = _report_fingerprint(report)
        if fingerprint in self._report_fingerprints:
            return order
        if report.requested_quantity != order.requested_quantity:
            raise ValueError("execution report requested quantity mismatch")
        if report.filled_quantity < order.filled_quantity:
            raise ValueError("execution report cumulative fill moved backwards")
        if not self._report_transition_allowed(order.status, report.status):
            raise ValueError(
                f"invalid OMS report transition {order.status} -> {report.status}"
            )
        status = report.status
        if report.filled_quantity == order.requested_quantity:
            status = OrderStatus.FILLED
        elif report.filled_quantity > 0 and status in {
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.NEW,
            OrderStatus.SUBMITTING,
        }:
            status = OrderStatus.PARTIALLY_FILLED
        updated = self._update(
            order,
            report.receive_timestamp,
            status=status,
            filled_quantity=report.filled_quantity,
            exchange_order_id=report.exchange_order_id or order.exchange_order_id,
            rejection_reason=(
                report.reject_code or report.reject_message
                if status is OrderStatus.REJECTED
                else order.rejection_reason
            ),
        )
        self._persist(
            OMSEventType.EXECUTION_REPORT,
            updated,
            report.receive_timestamp,
            report_fingerprint=fingerprint,
        )
        return updated

    def prepare_cancel(self, client_order_id: str, timestamp: datetime) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status is OrderStatus.CANCEL_PENDING:
            return order
        if order.status not in {
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.UNKNOWN,
            OrderStatus.RECONCILING,
        }:
            raise ValueError("order is not cancellable")
        updated = self._update(
            order,
            timestamp,
            status=OrderStatus.CANCEL_PENDING,
            cancel_requested=True,
        )
        self._persist(OMSEventType.CANCEL_PREPARED, updated, timestamp)
        return updated

    def mark_unknown(
        self,
        client_order_id: str,
        timestamp: datetime,
        reason: str,
    ) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status in {OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}:
            raise ValueError("terminal order cannot become unknown")
        updated = self._update(order, timestamp, status=OrderStatus.UNKNOWN)
        self._persist(OMSEventType.UNKNOWN_MARKED, updated, timestamp, reason=reason)
        return updated

    def start_reconciliation(
        self,
        client_order_id: str,
        timestamp: datetime,
    ) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status not in {OrderStatus.UNKNOWN, OrderStatus.CANCEL_PENDING}:
            raise ValueError("only unknown/cancel-pending order can reconcile")
        updated = self._update(order, timestamp, status=OrderStatus.RECONCILING)
        self._persist(OMSEventType.RECONCILIATION_STARTED, updated, timestamp)
        return updated

    def apply_reconciliation(
        self,
        client_order_id: str,
        *,
        status: OrderStatus,
        filled_quantity: Decimal,
        exchange_order_id: str | None,
        timestamp: datetime,
    ) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status is not OrderStatus.RECONCILING:
            raise ValueError("order is not reconciling")
        if status in {OrderStatus.UNKNOWN, OrderStatus.RECONCILING, OrderStatus.SUBMITTING}:
            raise ValueError("reconciliation must produce an observable venue state")
        if filled_quantity < order.filled_quantity or filled_quantity > order.requested_quantity:
            raise ValueError("reconciled fill quantity is invalid")
        if filled_quantity == order.requested_quantity:
            status = OrderStatus.FILLED
        elif filled_quantity > 0 and status is OrderStatus.ACKNOWLEDGED:
            status = OrderStatus.PARTIALLY_FILLED
        updated = self._update(
            order,
            timestamp,
            status=status,
            filled_quantity=filled_quantity,
            exchange_order_id=exchange_order_id or order.exchange_order_id,
        )
        self._persist(OMSEventType.RECONCILIATION_APPLIED, updated, timestamp)
        return updated

    def expire(self, client_order_id: str, timestamp: datetime) -> OMSOrderSnapshot:
        order = self._order(client_order_id)
        if order.status not in {
            OrderStatus.NEW,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise ValueError("order cannot expire from current state")
        updated = self._update(order, timestamp, status=OrderStatus.EXPIRED)
        self._persist(OMSEventType.EXPIRED, updated, timestamp)
        return updated

    def _recover(self) -> None:
        for entry in self.journal.load():
            if entry.sequence != self._sequence + 1:
                raise ValueError("OMS journal replay sequence gap")
            current = self.orders.get(entry.snapshot.client_order_id)
            if current is not None and entry.snapshot.version != current.version + 1:
                raise ValueError("OMS order version gap during replay")
            if current is None and entry.snapshot.version != 1:
                raise ValueError("OMS first order version must be one")
            self.orders[entry.snapshot.client_order_id] = entry.snapshot
            if entry.report_fingerprint is not None:
                self._report_fingerprints.add(entry.report_fingerprint)
            self._sequence = entry.sequence

    def _persist(
        self,
        event_type: OMSEventType,
        snapshot: OMSOrderSnapshot,
        timestamp: datetime,
        *,
        report_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> None:
        sequence = self._sequence + 1
        event_id = "omsevt_" + hashlib.sha256(
            (
                f"{sequence}|{event_type.value}|{snapshot.client_order_id}|"
                f"{snapshot.version}|{_utc(timestamp).isoformat()}"
            ).encode()
        ).hexdigest()[:32]
        entry = OMSJournalEntry(
            sequence=sequence,
            event_id=event_id,
            event_type=event_type,
            timestamp=timestamp,
            snapshot=snapshot,
            report_fingerprint=report_fingerprint,
            reason=reason,
        )
        self.journal.append(entry)
        self.orders[snapshot.client_order_id] = snapshot
        if report_fingerprint is not None:
            self._report_fingerprints.add(report_fingerprint)
        self._sequence = sequence

    def _order(self, client_order_id: str) -> OMSOrderSnapshot:
        try:
            return self.orders[client_order_id]
        except KeyError as exc:
            raise ValueError("unknown OMS client order ID") from exc

    @staticmethod
    def _update(
        order: OMSOrderSnapshot,
        timestamp: datetime,
        **updates: object,
    ) -> OMSOrderSnapshot:
        return order.model_copy(
            update={
                **updates,
                "version": order.version + 1,
                "updated_at": _utc(timestamp),
            }
        )

    @staticmethod
    def _report_transition_allowed(current: OrderStatus, target: OrderStatus) -> bool:
        allowed = {
            OrderStatus.NEW: {OrderStatus.ACKNOWLEDGED, OrderStatus.REJECTED},
            OrderStatus.SUBMITTING: {
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.REJECTED,
                OrderStatus.UNKNOWN,
            },
            OrderStatus.ACKNOWLEDGED: {
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
            },
            OrderStatus.PARTIALLY_FILLED: {
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
            },
            OrderStatus.CANCEL_PENDING: {
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.UNKNOWN,
            },
            OrderStatus.CANCELLED: {OrderStatus.FILLED},
            OrderStatus.UNKNOWN: {
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
            },
            OrderStatus.RECONCILING: {
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
            },
        }
        return target in allowed.get(current, set())


def _client_order_id(signal_id: str, decision_id: str, leg_index: int) -> str:
    digest = hashlib.sha256(f"{signal_id}|{decision_id}|{leg_index}".encode()).hexdigest()
    return "codexv1_" + digest[:24]


def _report_fingerprint(report: ExecutionReport) -> str:
    payload = json.dumps(
        report.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
