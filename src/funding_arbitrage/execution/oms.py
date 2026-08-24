"""Durable venue-independent OMS with restart replay and cancel-race handling."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Protocol
from weakref import finalize

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
        self._lock = Lock()
        self._descriptor: int | None = None
        self._finalizer: finalize | None = None
        self._created_file = False
        self._closed = False

    def append(self, entry: OMSJournalEntry) -> None:
        encoded = (entry.model_dump_json() + "\n").encode("utf-8")
        with self._lock:
            if self._closed:
                raise RuntimeError("OMS journal is closed")
            descriptor = self._ensure_open()
            try:
                remaining = memoryview(encoded)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("OMS journal write made no progress")
                    remaining = remaining[written:]
                _sync_data(descriptor)
                if self._created_file:
                    _sync_parent_directory(self.path.parent)
                    self._created_file = False
            except OSError:
                self._close_unlocked()
                raise

    def load(self) -> tuple[OMSJournalEntry, ...]:
        if not self.path.exists():
            return ()
        with self._lock:
            payload = _read_and_repair_torn_tail(self.path)
            entries = tuple(
                OMSJournalEntry.model_validate_json(line)
                for line in payload.decode("utf-8").splitlines()
                if line.strip()
            )
        expected = tuple(range(1, len(entries) + 1))
        if tuple(entry.sequence for entry in entries) != expected:
            raise ValueError("OMS journal sequence is not contiguous")
        return entries

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            self._close_unlocked()

    def _close_unlocked(self) -> None:
        self._closed = True
        if self._finalizer is not None and self._finalizer.alive:
            self._finalizer()

    def _ensure_open(self) -> int:
        if self._descriptor is not None:
            return self._descriptor
        self._created_file = not self.path.exists()
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
        descriptor = os.open(self.path, flags, 0o600)
        if os.name == "posix":
            os.chmod(self.path, 0o600)
        self._descriptor = descriptor
        self._finalizer = finalize(self, os.close, descriptor)
        return descriptor


class SqliteOMSJournal:
    """Transactional OMS journal backed by SQLite WAL with FULL durability."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._connection: sqlite3.Connection | None = None
        self._finalizer: finalize | None = None
        self._closed = False
        self._initialize()

    def append(self, entry: OMSJournalEntry) -> None:
        encoded = entry.model_dump_json()
        with self._lock:
            connection = self._open_connection()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM oms_journal"
                ).fetchone()
                expected = int(row[0]) if row is not None else 1
                if entry.sequence != expected:
                    raise ValueError("OMS journal sequence is not contiguous")
                connection.execute(
                    "INSERT INTO oms_journal(sequence, payload) VALUES (?, ?)",
                    (entry.sequence, encoded),
                )
                connection.execute("COMMIT")
                _secure_sqlite_files(self.path)
            except Exception as error:
                try:
                    connection.rollback()
                except sqlite3.Error as rollback_error:
                    error.add_note(
                        "OMS SQLite rollback failed: " + type(rollback_error).__name__
                    )
                self._poison_unlocked(error)
                raise

    def load(self) -> tuple[OMSJournalEntry, ...]:
        with self._lock:
            connection = self._open_connection()
            try:
                rows = connection.execute(
                    "SELECT sequence, payload FROM oms_journal ORDER BY sequence"
                ).fetchall()
                entries: list[OMSJournalEntry] = []
                for stored_sequence, payload in rows:
                    entry = OMSJournalEntry.model_validate_json(payload)
                    if entry.sequence != stored_sequence:
                        raise ValueError(
                            "OMS journal stored sequence does not match payload"
                        )
                    entries.append(entry)
                expected = tuple(range(1, len(entries) + 1))
                if tuple(entry.sequence for entry in entries) != expected:
                    raise ValueError("OMS journal sequence is not contiguous")
                return tuple(entries)
            except Exception as error:
                self._poison_unlocked(error)
                raise

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            self._close_unlocked()

    def _initialize(self) -> None:
        _validate_sqlite_parent(self.path.parent)
        _secure_sqlite_files(self.path)
        expected_identity = _prepare_sqlite_path(self.path)
        _secure_sqlite_files(self.path)
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                self.path,
                timeout=30,
                isolation_level=None,
                check_same_thread=False,
            )
            _verify_sqlite_identity(connection, self.path, expected_identity)
            connection.execute("PRAGMA busy_timeout=30000")
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RuntimeError("OMS SQLite journal requires WAL mode")
            connection.execute("PRAGMA synchronous=FULL")
            synchronous = connection.execute("PRAGMA synchronous").fetchone()
            if synchronous is None or int(synchronous[0]) != 2:
                raise RuntimeError("OMS SQLite journal requires FULL synchronous mode")
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS oms_journal (
                    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
                    payload TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "SELECT sequence, payload FROM oms_journal LIMIT 0"
            ).fetchall()
            _secure_sqlite_files(self.path)
        except Exception as error:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error as close_error:
                    error.add_note(
                        "OMS SQLite initialization close failed: "
                        + type(close_error).__name__
                    )
            raise
        self._connection = connection
        self._finalizer = finalize(self, _close_sqlite_connection, connection)

    def _open_connection(self) -> sqlite3.Connection:
        if self._closed or self._connection is None:
            raise RuntimeError("OMS journal is closed")
        return self._connection

    def _close_unlocked(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._connection = None
        finalizer = self._finalizer
        self._finalizer = None
        if finalizer is not None and finalizer.alive:
            finalizer()


    def _poison_unlocked(self, error: Exception) -> None:
        try:
            self._close_unlocked()
        except Exception as close_error:
            error.add_note(
                "OMS SQLite poison close failed: " + type(close_error).__name__
            )


def _validate_sqlite_parent(path: Path) -> None:
    if os.name != "posix":
        return
    details = path.lstat()
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise RuntimeError("OMS SQLite journal parent must be a real directory")
    if details.st_mode & 0o022:
        raise RuntimeError("OMS SQLite journal parent must not be group/world writable")


def _prepare_sqlite_path(path: Path) -> tuple[int, int]:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as create_error:
        if not path.exists() and not path.is_symlink():
            raise
        try:
            identity = _sqlite_path_identity(path)
        except ValueError as validation_error:
            raise validation_error from create_error
        return identity
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("OMS SQLite journal path must be a regular non-symlink file")
        _set_private_file_mode(descriptor)
        identity = (details.st_dev, details.st_ino)
        _sync_data(descriptor)
    finally:
        os.close(descriptor)
    _sync_parent_directory(path.parent)
    return identity


def _set_private_file_mode(descriptor: int) -> None:
    if os.name != "posix":
        return
    file_chmod = getattr(os, "fchmod", None)
    if file_chmod is None:
        raise RuntimeError("OMS SQLite journal requires descriptor chmod support")
    file_chmod(descriptor, 0o600)


def _sqlite_path_identity(path: Path) -> tuple[int, int]:
    if path.is_symlink():
        raise ValueError("OMS SQLite journal path must be a regular non-symlink file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(
            "OMS SQLite journal path must be a regular non-symlink file"
        ) from error
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("OMS SQLite journal path must be a regular non-symlink file")
        _set_private_file_mode(descriptor)
        return details.st_dev, details.st_ino
    finally:
        os.close(descriptor)


def _verify_sqlite_identity(
    connection: sqlite3.Connection,
    path: Path,
    expected_identity: tuple[int, int],
) -> None:
    if _sqlite_path_identity(path) != expected_identity:
        raise RuntimeError("OMS SQLite journal identity changed while opening")
    row = connection.execute("PRAGMA database_list").fetchone()
    if row is None or not row[2]:
        raise RuntimeError("OMS SQLite journal did not expose its main database path")
    opened_path = Path(str(row[2])).resolve(strict=True)
    if opened_path != path.resolve(strict=True):
        raise RuntimeError("OMS SQLite journal opened an unexpected database path")


def _secure_sqlite_files(path: Path) -> None:
    if os.name != "posix":
        return
    for candidate in (
        path,
        path.with_name(path.name + "-wal"),
        path.with_name(path.name + "-shm"),
    ):
        if not candidate.exists() and not candidate.is_symlink():
            continue
        flags = (
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            descriptor = os.open(candidate, flags)
        except OSError as error:
            if candidate == path:
                raise ValueError(
                    "OMS SQLite journal path must be a regular non-symlink file"
                ) from error
            raise RuntimeError("OMS SQLite runtime file could not be opened safely") from error
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                if candidate == path:
                    raise ValueError(
                        "OMS SQLite journal path must be a regular non-symlink file"
                    )
                raise RuntimeError("OMS SQLite runtime file is not a regular file")
            _set_private_file_mode(descriptor)
            if os.fstat(descriptor).st_mode & 0o077:
                raise RuntimeError("OMS SQLite runtime file permissions are not private")
        finally:
            os.close(descriptor)


def _close_sqlite_connection(connection: sqlite3.Connection) -> None:
    connection.close()


def _sync_data(descriptor: int) -> None:
    data_sync = getattr(os, "fdatasync", None)
    if data_sync is None:
        os.fsync(descriptor)
    else:
        data_sync(descriptor)


def _sync_parent_directory(path: Path) -> None:
    if os.name != "posix":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_and_repair_torn_tail(path: Path) -> bytes:
    payload = path.read_bytes()
    if not payload or payload.endswith(b"\n"):
        return payload
    last_complete = payload.rfind(b"\n") + 1
    flags = os.O_WRONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        os.ftruncate(descriptor, last_complete)
        _sync_data(descriptor)
    finally:
        os.close(descriptor)
    return payload[:last_complete]


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
