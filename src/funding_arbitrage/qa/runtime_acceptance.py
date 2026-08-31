"""Fail-closed runtime collector for elapsed Shadow and Paper acceptance windows."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    CanonicalEventRecord,
    ExecutionFillRecord,
    LedgerTransactionRecord,
    LiveOrderRecord,
    MultiRegimeDecisionRecord,
    OMSOrderStateRecord,
    PaperFillRecord,
    PaperPositionRecord,
    PaperRuntimeIncidentRecord,
    PortfolioSnapshotRecord,
    PositionStateRecord,
    ReconciliationAuditRecord,
    RiskDecisionRecord,
    TelegramDailyReportRecord,
    WithdrawalStateRecord,
)
from funding_arbitrage.domain.events import InstrumentKey, TradingMode
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.qa.acceptance_artifacts import acceptance_replay_runner_sha256
from funding_arbitrage.qa.acceptance_provenance import (
    RuntimeReleaseIdentity,
    load_runtime_release_identity,
)
from funding_arbitrage.qa.acceptance_window import (
    MAX_EVIDENCE_BYTES,
    MAX_OBSERVATIONS,
    REQUIRED_VENUES,
    AcceptanceCosts,
    AcceptanceCounters,
    AcceptanceGate,
    AcceptanceObservationInput,
    AcceptanceWindowSealInput,
    DeterministicReplayEvidence,
    FailureInjectionEvidence,
)
from funding_arbitrage.services.runtime import RuntimeState

RUNTIME_RELEASE_IDENTITY_PATH = Path("/run/funding-arbitrage/release-identity.json")
_MAX_JOURNAL_LINE_BYTES = 64 * 1024
_MAX_FUTURE_SKEW_SECONDS = Decimal("5")
_DECIMAL_RECONCILIATION_TOLERANCE = Decimal("1e-12")
_ZERO = Decimal("0")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def acceptance_config_sha256(settings: Settings) -> str:
    """Hash the complete effective settings without serializing raw secrets to evidence."""

    payload = settings.model_dump(mode="json", by_alias=True)
    payload["TRADING_MODE"] = settings.effective_trading_mode.value
    return _sha256(payload)


class AcceptanceRuntimeJournalHeader(BaseModel):
    """Immutable identity line written before any runtime observation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-runtime-journal"]
    schema_version: Literal[1]
    gate_id: AcceptanceGate
    window_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    created_at: datetime
    process_start_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    mode: TradingMode
    release_identity: RuntimeReleaseIdentity

    @field_validator("created_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("runtime journal timestamps require a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_gate_mode(self) -> AcceptanceRuntimeJournalHeader:
        expected = {
            AcceptanceGate.SHADOW: TradingMode.SHADOW,
            AcceptanceGate.PAPER: TradingMode.PAPER,
        }[self.gate_id]
        if self.mode is not expected:
            raise ValueError("runtime journal gate and mode do not match")
        return self


class AcceptanceRuntimeJournalObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-runtime-observation"]
    schema_version: Literal[1]
    observation: AcceptanceObservationInput


class AcceptanceRuntimeAttachments(BaseModel):
    """Externally produced artifacts attached after the elapsed runtime window."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-runtime-attachments"]
    schema_version: Literal[1]
    failure_injections: tuple[FailureInjectionEvidence, ...] = Field(
        min_length=1, max_length=32
    )
    deterministic_replay: DeterministicReplayEvidence


class AcceptanceJournalSink(Protocol):
    def open(self, header: AcceptanceRuntimeJournalHeader) -> None: ...

    def append(self, observation: AcceptanceObservationInput) -> None: ...

    def close(self) -> None: ...


class SecureAcceptanceJournal:
    """Append-only Linux journal opened without following path components."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    def open(self, header: AcceptanceRuntimeJournalHeader) -> None:
        if self._descriptor is not None:
            raise RuntimeError("acceptance journal is already open")
        if os.name != "posix" or not self.path.is_absolute():
            raise ValueError("runtime acceptance journal requires an absolute Linux path")
        parent_descriptor = _open_secure_directory(self.path.parent)
        try:
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_APPEND
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(
                self.path.name,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
        finally:
            os.close(parent_descriptor)
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != _effective_uid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise ValueError("runtime acceptance journal ownership is unsafe")
        self._descriptor = descriptor
        self._write_record(header.model_dump(mode="json"))

    def append(self, observation: AcceptanceObservationInput) -> None:
        self._write_record(
            AcceptanceRuntimeJournalObservation(
                document_kind="acceptance-runtime-observation",
                schema_version=1,
                observation=observation,
            ).model_dump(mode="json")
        )

    def close(self) -> None:
        if self._descriptor is None:
            return
        descriptor = self._descriptor
        self._descriptor = None
        os.fsync(descriptor)
        os.close(descriptor)

    def _write_record(self, record: dict[str, Any]) -> None:
        if self._descriptor is None:
            raise RuntimeError("acceptance journal is not open")
        payload = _canonical_json(record) + b"\n"
        if len(payload) > _MAX_JOURNAL_LINE_BYTES:
            raise ValueError("runtime acceptance journal record is too large")
        view = memoryview(payload)
        while view:
            written = os.write(self._descriptor, view)
            if written <= 0:
                raise OSError("runtime acceptance journal write failed")
            view = view[written:]
        os.fsync(self._descriptor)


def _open_secure_directory(path: Path) -> int:
    if not path.is_absolute() or os.open not in os.supports_dir_fd:
        raise ValueError("secure runtime acceptance directory walking is unavailable")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    current = os.open(path.anchor, flags)
    try:
        _validate_secure_directory(current)
        for part in path.parts[1:]:
            next_descriptor = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = next_descriptor
            _validate_secure_directory(current)
        return current
    except BaseException:
        os.close(current)
        raise


def _validate_secure_directory(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid not in {0, _effective_uid()}
        or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise ValueError("runtime acceptance directory ownership is unsafe")


@dataclass(frozen=True)
class _SnapshotHealth:
    observed_at: datetime
    healthy_venues: tuple[str, ...]
    ready: bool
    data_quality_valid: bool
    market_age: Decimal
    orderbook_age: Decimal
    funding_age: Decimal
    source_payload: dict[str, object]


@dataclass(frozen=True)
class _DatabaseGuard:
    canonical_events: int
    canonical_row_id: int
    canonical_event_id: str
    sent_reports: int
    risk_rejections: int
    real_order_submissions: int
    withdrawal_requests: int
    runner_errors: int
    unresolved_reconciliation_items: int
    unknown_orders: int
    process_restarts: int
    directional_fills: int
    directional_reconciled_fills: int
    directional_unreconciled_fills: int
    directional_closed_positions: int
    directional_fill_venues: tuple[str, ...]
    directional_fees: Decimal
    directional_spread: Decimal
    directional_slippage: Decimal
    directional_state_sha256: str


class RuntimeAcceptanceCollector:
    """Collect authoritative, monotonic runtime evidence from one clean namespace."""

    def __init__(
        self,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
        release_identity: RuntimeReleaseIdentity,
        journal: AcceptanceJournalSink,
        *,
        now: datetime | None = None,
    ) -> None:
        mode = settings.effective_trading_mode
        if mode not in {TradingMode.SHADOW, TradingMode.PAPER}:
            raise ValueError("runtime acceptance collector requires SHADOW or PAPER mode")
        if tuple(sorted(settings.paper_venue_values)) != REQUIRED_VENUES:
            raise ValueError("runtime acceptance collector requires the exact eight-venue set")
        calculated_config = acceptance_config_sha256(settings)
        if release_identity.config_sha256 != calculated_config:
            raise ValueError("runtime acceptance configuration hash mismatch")
        if release_identity.runner_sha256 != acceptance_replay_runner_sha256():
            raise ValueError("runtime acceptance runner hash mismatch")
        current = _utc(now or datetime.now(UTC))
        if release_identity.observed_at > current:
            raise ValueError("runtime release identity is future dated")

        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self.release_identity = release_identity
        self.journal = journal
        self.gate_id = (
            AcceptanceGate.SHADOW
            if mode is TradingMode.SHADOW
            else AcceptanceGate.PAPER
        )
        self.mode = mode
        self.window_id = settings.acceptance_window_id
        self.process_start_id = f"proc-{uuid4().hex}"
        self._created_at = current
        self._sample_interval = Decimal(
            str(settings.acceptance_sample_interval_seconds)
        )
        self._started = False
        self._window_started = False
        self._failed = False
        self._closed = False
        self._sequence = 0
        self._last_sample_at: datetime | None = None
        self._last_health: _SnapshotHealth | None = None
        self._interval_market_age = _ZERO
        self._interval_orderbook_age = _ZERO
        self._interval_funding_age = _ZERO
        self._canonical_baseline = 0
        self._counters = {field: 0 for field in AcceptanceCounters.model_fields}
        self._costs = AcceptanceCosts(
            fees_usd=0,
            spread_usd=0,
            slippage_usd=0,
            borrow_usd=0,
            gas_and_transfer_usd=0,
        )
        self._seen_fill_ids: set[str] = set()
        self._seen_closed_positions: set[str] = set()
        self._seen_funding_events: set[str] = set()
        self._fill_venues: set[str] = set()
        self._directional_last_fill_id = 0
        self._directional_fills = 0
        self._directional_reconciled_fills = 0
        self._directional_unreconciled_fills = 0
        self._directional_fill_venues: set[str] = set()
        self._directional_fees = _ZERO
        self._directional_spread = _ZERO
        self._directional_slippage = _ZERO
        self._directional_fill_chain = "0" * 64
        self._lock = asyncio.Lock()
        self.runtime.set_acceptance_entries_enabled(False)

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> RuntimeAcceptanceCollector:
        identity = load_runtime_release_identity(RUNTIME_RELEASE_IDENTITY_PATH)
        return cls(
            settings,
            runtime,
            session_factory,
            identity,
            SecureAcceptanceJournal(Path(settings.acceptance_journal_path)),
        )

    @property
    def window_started(self) -> bool:
        return self._window_started

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            if self._closed:
                raise RuntimeError("runtime acceptance collector is closed")
            await self._assert_clean_namespace()
            header = AcceptanceRuntimeJournalHeader(
                document_kind="acceptance-runtime-journal",
                schema_version=1,
                gate_id=self.gate_id,
                window_id=self.window_id,
                created_at=self._created_at,
                process_start_id=self.process_start_id,
                mode=self.mode,
                release_identity=self.release_identity,
            )
            self.journal.open(header)
            self._started = True

    async def observe_market_snapshot(self, snapshot: MarketSnapshot) -> None:
        async with self._lock:
            self._require_started()
            health = self._assess_snapshot(snapshot)
            self._last_health = health
            self._accumulate_ages(health)
            if not self._window_started:
                if not health.ready or not health.data_quality_valid:
                    return
                await self._assert_clean_financial_state()
                guard = await self._database_guard()
                self._canonical_baseline = guard.canonical_events
                await self._emit_observation(health, guard=guard)
                self._window_started = True
                self.runtime.set_acceptance_entries_enabled(True)
                return
            if not health.ready or not health.data_quality_valid:
                self._record_data_quality_failure(health)
                self._fail_window()
                await self._emit_observation(health, guard=await self._database_guard())
                return
            if self._sample_due(health.observed_at):
                await self._emit_observation(health, guard=await self._database_guard())

    async def record_market_gap(self, venues: tuple[str, ...]) -> None:
        async with self._lock:
            self._require_started()
            if not self._window_started:
                return
            self._counters["data_quality_incidents"] += 1
            self._counters["readiness_failures"] += 1
            self._counters["venue_outage_incidents"] += max(1, len(set(venues)))
            self._fail_window()
            await self._emit_failure_observation("market-gap")

    def record_strategy_evaluation(self, opportunities: Sequence[object]) -> None:
        if not self._window_started or self._closed:
            return
        self._counters["strategy_evaluations"] += 1
        confirmed = sum(getattr(item, "status", None) == "confirmed" for item in opportunities)
        self._counters["strategy_decisions"] += confirmed
        if self.mode is TradingMode.SHADOW:
            self._counters["shadow_suppressed_orders"] += confirmed

    def record_risk_rejection(self) -> None:
        if self._window_started and not self._closed:
            self._counters["risk_rejections"] += 1

    def record_successful_cycle(
        self,
        snapshot: MarketSnapshot,
        *,
        daily_report_sent: bool,
    ) -> None:
        if not self._window_started or self._closed:
            return
        self._counters["runner_cycles"] += 1
        if daily_report_sent:
            self._counters["daily_reports"] += 1
        self._observe_portfolio(snapshot)

    async def record_runner_failure(self) -> None:
        async with self._lock:
            self._require_started()
            if not self._window_started:
                return
            self._counters["runner_errors"] += 1
            self._fail_window()
            await self._emit_failure_observation("runner-error")

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self.runtime.permanently_disable_acceptance_entries()
            self.journal.close()
            self._closed = True

    def _observe_portfolio(self, snapshot: MarketSnapshot) -> None:
        positions = tuple(self.runtime.portfolio.positions.values())
        for position in positions:
            for fill in _position_fills(position):
                if fill.fill_id in self._seen_fill_ids:
                    continue
                self._seen_fill_ids.add(fill.fill_id)
                self._counters["simulated_fills"] += 1
                self._fill_venues.add(fill.exchange.lower())
                if _fill_reconciles_to_book(
                    fill, snapshot, self.settings.market_data_stale_seconds
                ):
                    self._counters["fill_book_reconciliations"] += 1
                else:
                    self._counters["unreconciled_fills"] += 1
                    self._fail_window()
            if (
                position.state is PositionState.CLOSED
                and position.id not in self._seen_closed_positions
            ):
                self._seen_closed_positions.add(position.id)
                self._counters["closed_positions"] += 1
            for marker in position.settled_funding_events:
                scoped = f"{position.id}:{marker}"
                if scoped not in self._seen_funding_events:
                    self._seen_funding_events.add(scoped)
                    self._counters["funding_settlements"] += 1

        costs = AcceptanceCosts(
            fees_usd=sum((item.pnl.fees for item in positions), _ZERO),
            spread_usd=sum((item.pnl.spread for item in positions), _ZERO),
            slippage_usd=sum(
                (item.pnl.slippage + item.pnl.legging_cost for item in positions),
                _ZERO,
            ),
            borrow_usd=sum((item.pnl.borrow_cost for item in positions), _ZERO),
            gas_and_transfer_usd=0,
        )
        if any(
            getattr(costs, field) < getattr(self._costs, field)
            for field in AcceptanceCosts.model_fields
        ):
            self._counters["accounting_violations"] += 1
            self._fail_window()
        self._costs = costs

    async def _emit_failure_observation(self, reason: str) -> None:
        health = self._last_health
        if health is None:
            return
        now = datetime.now(UTC)
        failed_health = _SnapshotHealth(
            observed_at=max(now, health.observed_at),
            healthy_venues=health.healthy_venues,
            ready=False,
            data_quality_valid=False,
            market_age=max(
                health.market_age,
                Decimal(str(self.settings.market_data_stale_seconds)) + 1,
            ),
            orderbook_age=max(
                health.orderbook_age,
                Decimal(str(self.settings.orderbook_stream_stale_seconds)) + 1,
            ),
            funding_age=max(
                health.funding_age,
                Decimal(str(self.settings.funding_snapshot_stale_seconds)) + 1,
            ),
            source_payload={"failure": reason, "observed_at": now.isoformat()},
        )
        self._accumulate_ages(failed_health)
        await self._emit_observation(failed_health, guard=await self._database_guard())

    async def _emit_observation(
        self,
        health: _SnapshotHealth,
        *,
        guard: _DatabaseGuard,
    ) -> None:
        counters_payload = dict(self._counters)
        counters_payload["canonical_market_events"] = max(
            0, guard.canonical_events - self._canonical_baseline
        )
        counters_payload["daily_reports"] = max(
            counters_payload["daily_reports"], guard.sent_reports
        )
        counters_payload["risk_rejections"] = max(
            counters_payload["risk_rejections"], guard.risk_rejections
        )
        counters_payload["simulated_fills"] += guard.directional_fills
        counters_payload[
            "fill_book_reconciliations"
        ] += guard.directional_reconciled_fills
        counters_payload["unreconciled_fills"] += (
            guard.directional_unreconciled_fills
        )
        counters_payload["closed_positions"] += guard.directional_closed_positions
        for field in (
            "real_order_submissions",
            "withdrawal_requests",
            "runner_errors",
            "unresolved_reconciliation_items",
            "unknown_orders",
            "process_restarts",
        ):
            counters_payload[field] = max(counters_payload[field], getattr(guard, field))
        counters = AcceptanceCounters.model_validate(counters_payload)
        effective_costs = AcceptanceCosts(
            fees_usd=self._costs.fees_usd + guard.directional_fees,
            spread_usd=self._costs.spread_usd + guard.directional_spread,
            slippage_usd=(
                self._costs.slippage_usd + guard.directional_slippage
            ),
            borrow_usd=self._costs.borrow_usd,
            gas_and_transfer_usd=self._costs.gas_and_transfer_usd,
        )
        if any(
            getattr(counters, field) > 0
            for field in (
                "real_order_submissions",
                "withdrawal_requests",
                "runner_errors",
                "unresolved_reconciliation_items",
                "unknown_orders",
                "process_restarts",
                "unreconciled_fills",
            )
        ):
            self._fail_window()
        accounting_error = _accounting_error(self.runtime)
        if accounting_error > Decimal("0.01"):
            self._counters["accounting_violations"] += 1
            counters = counters.model_copy(
                update={
                    "accounting_violations": self._counters["accounting_violations"]
                }
            )
            self._fail_window()
        source_watermark = _source_watermark(
            self._sequence,
            health.source_payload,
            guard.canonical_row_id,
            guard.canonical_event_id,
        )
        ledger_payload = _ledger_payload(
            self.runtime,
            directional_state_sha256=guard.directional_state_sha256,
        )
        runtime_payload = {
            "counters": counters.model_dump(mode="json"),
            "costs": effective_costs.model_dump(mode="json"),
            "source_watermark": source_watermark,
            "failed": self._failed,
        }
        observed_at = health.observed_at
        if self._last_sample_at is not None and observed_at <= self._last_sample_at:
            observed_at = self._last_sample_at + timedelta(microseconds=1)
        observation = AcceptanceObservationInput(
            sequence=self._sequence,
            sample_id=f"{self.window_id}:{self._sequence}",
            observed_at=observed_at,
            code_revision=self.release_identity.code_revision,
            image_digest=self.release_identity.image_digest,
            config_sha256=self.release_identity.config_sha256,
            process_start_id=self.process_start_id,
            source_watermark=source_watermark,
            ledger_sha256=_sha256(ledger_payload),
            runtime_state_sha256=_sha256(runtime_payload),
            mode=self.mode,
            ready=health.ready and not self._failed,
            exchange_orders_enabled=self.settings.mode_contract.exchange_orders_enabled,
            healthy_venues=health.healthy_venues,
            simulated_fill_venues=tuple(
                sorted(self._fill_venues | set(guard.directional_fill_venues))
            ),
            data_quality_valid=health.data_quality_valid and not self._failed,
            configured_cycle_interval_seconds=Decimal(
                str(self.settings.paper_loop_interval_seconds)
            ),
            configured_market_data_stale_seconds=Decimal(
                str(self.settings.market_data_stale_seconds)
            ),
            configured_orderbook_stream_stale_seconds=Decimal(
                str(self.settings.orderbook_stream_stale_seconds)
            ),
            configured_funding_snapshot_stale_seconds=Decimal(
                str(self.settings.funding_snapshot_stale_seconds)
            ),
            interval_max_market_data_age_seconds=self._interval_market_age,
            interval_max_orderbook_stream_age_seconds=self._interval_orderbook_age,
            interval_max_funding_snapshot_age_seconds=self._interval_funding_age,
            accounting_error_usd=accounting_error,
            counters=counters,
            costs=effective_costs,
        )
        try:
            self.journal.append(observation)
        except BaseException:
            self._fail_window()
            raise
        self._sequence += 1
        self._last_sample_at = observation.observed_at
        self._interval_market_age = _ZERO
        self._interval_orderbook_age = _ZERO
        self._interval_funding_age = _ZERO

    def _assess_snapshot(self, snapshot: MarketSnapshot) -> _SnapshotHealth:
        observed_at = _utc(snapshot.captured_at)
        ticker_ages = _ages_by_venue(snapshot.tickers, observed_at)
        funding_ages = _ages_by_venue(snapshot.funding, observed_at)
        orderbook_ages = _ages_by_venue(snapshot.orderbooks.values(), observed_at)
        incomplete = {item.lower() for item in snapshot.incomplete_venues}
        healthy: list[str] = []
        future_dated = False
        for venue in REQUIRED_VENUES:
            venue_tickers = ticker_ages.get(venue, ())
            venue_funding = funding_ages.get(venue, ())
            venue_books = orderbook_ages.get(venue, ())
            ages = (*venue_tickers, *venue_funding, *venue_books)
            future_dated = future_dated or any(age < -_MAX_FUTURE_SKEW_SECONDS for age in ages)
            if (
                venue not in incomplete
                and venue_tickers
                and venue_funding
                and venue_books
                and max(venue_tickers) <= self.settings.market_data_stale_seconds
                and max(venue_funding) <= self.settings.funding_snapshot_stale_seconds
                and max(venue_books) <= self.settings.orderbook_stream_stale_seconds
                and all(age >= -_MAX_FUTURE_SKEW_SECONDS for age in ages)
            ):
                healthy.append(venue)

        market_age = _maximum_age(
            ticker_ages,
            Decimal(str(self.settings.market_data_stale_seconds)) + 1,
        )
        funding_age = _maximum_age(
            funding_ages,
            Decimal(str(self.settings.funding_snapshot_stale_seconds)) + 1,
        )
        orderbook_age = _maximum_age(
            orderbook_ages,
            Decimal(str(self.settings.orderbook_stream_stale_seconds)) + 1,
        )
        complete = tuple(healthy) == REQUIRED_VENUES
        entry_healthy = True
        if self.runtime.entry_health is not None:
            try:
                entry_healthy = bool(self.runtime.entry_health()[0])
            except Exception:
                entry_healthy = False
        valid = complete and not future_dated
        return _SnapshotHealth(
            observed_at=observed_at,
            healthy_venues=tuple(healthy),
            ready=valid and entry_healthy,
            data_quality_valid=valid,
            market_age=max(_ZERO, market_age),
            orderbook_age=max(_ZERO, orderbook_age),
            funding_age=max(_ZERO, funding_age),
            source_payload={
                "captured_at": observed_at.isoformat(),
                "tickers": len(snapshot.tickers),
                "funding": len(snapshot.funding),
                "orderbooks": len(snapshot.orderbooks),
                "healthy_venues": healthy,
            },
        )

    def _record_data_quality_failure(self, health: _SnapshotHealth) -> None:
        self._counters["data_quality_incidents"] += 1
        if not health.ready:
            self._counters["readiness_failures"] += 1
        missing = set(REQUIRED_VENUES) - set(health.healthy_venues)
        if missing:
            self._counters["venue_outage_incidents"] += len(missing)
        if (
            health.market_age > self.settings.market_data_stale_seconds
            or health.orderbook_age > self.settings.orderbook_stream_stale_seconds
            or health.funding_age > self.settings.funding_snapshot_stale_seconds
        ):
            self._counters["stale_stream_incidents"] += 1

    async def _assert_clean_namespace(self) -> None:
        await self._assert_clean_financial_state()
        version = self.settings.paper_simulation_version
        async with self.session_factory() as session:
            counts = {
                "paper_positions": await _count(
                    session,
                    select(func.count(PaperPositionRecord.id)).where(
                        PaperPositionRecord.simulation_version == version
                    ),
                ),
                "paper_fills": await _count(session, select(func.count(PaperFillRecord.id))),
                "execution_fills": await _count(
                    session,
                    select(func.count(ExecutionFillRecord.id)).where(
                        ExecutionFillRecord.simulation_version == version
                    ),
                ),
                "position_states": await _count(
                    session,
                    select(func.count(PositionStateRecord.id)).where(
                        PositionStateRecord.simulation_version == version
                    ),
                ),
                "oms_orders": await _count(
                    session,
                    select(func.count(OMSOrderStateRecord.id)).where(
                        OMSOrderStateRecord.simulation_version == version
                    ),
                ),
                "portfolio_snapshots": await _count(
                    session,
                    select(func.count(PortfolioSnapshotRecord.id)).where(
                        PortfolioSnapshotRecord.simulation_version == version
                    ),
                ),
                "decisions": await _count(
                    session, select(func.count(MultiRegimeDecisionRecord.id))
                ),
                "risk_decisions": await _count(
                    session, select(func.count(RiskDecisionRecord.id))
                ),
                "ledger": await _count(
                    session, select(func.count(LedgerTransactionRecord.id))
                ),
                "reports": await _count(
                    session, select(func.count(TelegramDailyReportRecord.id))
                ),
                "live_orders": await _count(
                    session, select(func.count(LiveOrderRecord.id))
                ),
                "withdrawals": await _count(
                    session, select(func.count(WithdrawalStateRecord.id))
                ),
                "reconciliations": await _count(
                    session, select(func.count(ReconciliationAuditRecord.id))
                ),
            }
            starts = await _count(
                session,
                select(func.count(PaperRuntimeIncidentRecord.id)).where(
                    PaperRuntimeIncidentRecord.simulation_version == version,
                    PaperRuntimeIncidentRecord.category == "process_start",
                ),
            )
            incidents = await _count(
                session,
                select(func.count(PaperRuntimeIncidentRecord.id)).where(
                    PaperRuntimeIncidentRecord.simulation_version == version
                ),
            )
        if any(counts.values()) or starts != 1 or incidents != 1:
            raise ValueError("runtime acceptance requires a clean dedicated namespace")

    async def _assert_clean_financial_state(self) -> None:
        snapshot = self.runtime.portfolio.snapshot()
        if (
            self.runtime.portfolio.positions
            or snapshot.locked_capital != 0
            or snapshot.total_pnl != 0
            or snapshot.funding_pnl != 0
            or snapshot.fees != 0
            or snapshot.cash != self.settings.paper_initial_balance_usd
        ):
            raise ValueError("runtime acceptance requires zero financial carry-in")

    async def _database_guard(self) -> _DatabaseGuard:
        version = self.settings.paper_simulation_version
        async with self.session_factory() as session:
            canonical_events = await _count(
                session, select(func.count(CanonicalEventRecord.id))
            )
            latest = await session.execute(
                select(CanonicalEventRecord.id, CanonicalEventRecord.event_id)
                .order_by(CanonicalEventRecord.id.desc())
                .limit(1)
            )
            latest_row = latest.one_or_none()
            reports = await _count(
                session,
                select(func.count(TelegramDailyReportRecord.id)).where(
                    TelegramDailyReportRecord.status == "sent"
                ),
            )
            risk_rejections = await _count(
                session,
                select(func.count(RiskDecisionRecord.id)).where(
                    RiskDecisionRecord.approved.is_(False)
                ),
            )
            real_orders = await _count(session, select(func.count(LiveOrderRecord.id)))
            withdrawals = await _count(
                session, select(func.count(WithdrawalStateRecord.id))
            )
            starts = await _count(
                session,
                select(func.count(PaperRuntimeIncidentRecord.id)).where(
                    PaperRuntimeIncidentRecord.simulation_version == version,
                    PaperRuntimeIncidentRecord.category == "process_start",
                ),
            )
            runner_errors = await _count(
                session,
                select(func.count(PaperRuntimeIncidentRecord.id)).where(
                    PaperRuntimeIncidentRecord.simulation_version == version,
                    PaperRuntimeIncidentRecord.category != "process_start",
                ),
            )
            funding_reconciliation_errors = await _count(
                session,
                select(func.count(PaperRuntimeIncidentRecord.id)).where(
                    PaperRuntimeIncidentRecord.simulation_version == version,
                    PaperRuntimeIncidentRecord.category == "funding_reconciliation",
                ),
            )
            failed_reconciliations = await _count(
                session,
                select(func.count(ReconciliationAuditRecord.id)).where(
                    ReconciliationAuditRecord.passed.is_(False)
                ),
            )
            unknown_orders = await _count(
                session,
                select(func.count(LiveOrderRecord.id)).where(
                    LiveOrderRecord.status.in_(("UNKNOWN", "SUBMISSION_UNKNOWN"))
                ),
            )
            new_directional_fills = tuple(
                (
                    await session.scalars(
                        select(ExecutionFillRecord)
                        .where(
                            ExecutionFillRecord.simulation_version == version,
                            ExecutionFillRecord.id > self._directional_last_fill_id,
                        )
                        .order_by(ExecutionFillRecord.id)
                    )
                ).all()
            )
            source_ids = {
                str(record.payload.get("source_event_id"))
                for record in new_directional_fills
                if isinstance(record.payload, dict)
                and isinstance(record.payload.get("source_event_id"), str)
            }
            source_records = (
                tuple(
                    (
                        await session.scalars(
                            select(CanonicalEventRecord).where(
                                CanonicalEventRecord.event_id.in_(source_ids)
                            )
                        )
                    ).all()
                )
                if source_ids
                else ()
            )
            directional_positions = tuple(
                (
                    await session.scalars(
                        select(PositionStateRecord)
                        .where(PositionStateRecord.simulation_version == version)
                        .order_by(PositionStateRecord.position_id)
                    )
                ).all()
            )
        self._observe_directional_fills(
            new_directional_fills,
            {record.event_id: record for record in source_records},
        )
        directional_closed = sum(
            record.status == "CLOSED" for record in directional_positions
        )
        directional_state_sha256 = _sha256(
            {
                "fill_chain": self._directional_fill_chain,
                "positions": [
                    {
                        "position_id": record.position_id,
                        "status": record.status,
                        "realized_pnl": str(record.realized_pnl),
                        "unrealized_pnl": str(record.unrealized_pnl),
                        "collateral": str(record.collateral),
                        "updated_at": _database_utc(record.updated_at).isoformat(),
                        "payload": record.payload,
                    }
                    for record in directional_positions
                ],
            }
        )
        return _DatabaseGuard(
            canonical_events=canonical_events,
            canonical_row_id=int(latest_row[0]) if latest_row is not None else 0,
            canonical_event_id=str(latest_row[1]) if latest_row is not None else "none",
            sent_reports=reports,
            risk_rejections=risk_rejections,
            real_order_submissions=real_orders,
            withdrawal_requests=withdrawals,
            runner_errors=runner_errors,
            unresolved_reconciliation_items=(
                funding_reconciliation_errors + failed_reconciliations
            ),
            unknown_orders=unknown_orders,
            process_restarts=max(0, starts - 1),
            directional_fills=self._directional_fills,
            directional_reconciled_fills=self._directional_reconciled_fills,
            directional_unreconciled_fills=(
                self._directional_unreconciled_fills
            ),
            directional_closed_positions=directional_closed,
            directional_fill_venues=tuple(sorted(self._directional_fill_venues)),
            directional_fees=self._directional_fees,
            directional_spread=self._directional_spread,
            directional_slippage=self._directional_slippage,
            directional_state_sha256=directional_state_sha256,
        )

    def _observe_directional_fills(
        self,
        records: tuple[ExecutionFillRecord, ...],
        source_by_id: dict[str, CanonicalEventRecord],
    ) -> None:
        for record in records:
            self._directional_last_fill_id = max(
                self._directional_last_fill_id, record.id
            )
            self._directional_fills += 1
            self._directional_fill_venues.add(record.venue.lower())
            source_id = (
                record.payload.get("source_event_id")
                if isinstance(record.payload, dict)
                else None
            )
            source = source_by_id.get(source_id) if isinstance(source_id, str) else None
            reconciled, fee, spread, slippage = _directional_fill_economics(
                record,
                source,
                maximum_book_age_seconds=self.settings.orderbook_stream_stale_seconds,
            )
            self._directional_fees += fee
            self._directional_spread += spread
            self._directional_slippage += slippage
            if reconciled:
                self._directional_reconciled_fills += 1
            else:
                self._directional_unreconciled_fills += 1
            self._directional_fill_chain = _sha256(
                {
                    "previous": self._directional_fill_chain,
                    "id": record.id,
                    "fill_id": record.fill_id,
                    "venue": record.venue,
                    "instrument_id": record.instrument_id,
                    "price": str(record.price),
                    "quantity": str(record.quantity),
                    "fee": str(record.fee_amount),
                    "payload": record.payload,
                    "reconciled": reconciled,
                }
            )

    def _accumulate_ages(self, health: _SnapshotHealth) -> None:
        self._interval_market_age = max(self._interval_market_age, health.market_age)
        self._interval_orderbook_age = max(
            self._interval_orderbook_age, health.orderbook_age
        )
        self._interval_funding_age = max(
            self._interval_funding_age, health.funding_age
        )

    def _sample_due(self, observed_at: datetime) -> bool:
        return self._last_sample_at is None or Decimal(
            str((observed_at - self._last_sample_at).total_seconds())
        ) >= self._sample_interval

    def _fail_window(self) -> None:
        self._failed = True
        self.runtime.permanently_disable_acceptance_entries()

    def _require_started(self) -> None:
        if not self._started or self._closed:
            raise RuntimeError("runtime acceptance collector is not active")


def load_runtime_acceptance_journal(
    path: Path,
) -> tuple[AcceptanceRuntimeJournalHeader, tuple[AcceptanceObservationInput, ...]]:
    payload = _read_bounded(path)
    if not payload.endswith(b"\n"):
        raise ValueError("runtime acceptance journal is incomplete")
    lines = payload.splitlines()
    if len(lines) < 3 or len(lines) - 1 > MAX_OBSERVATIONS:
        raise ValueError("runtime acceptance journal observation count is invalid")
    header = AcceptanceRuntimeJournalHeader.model_validate(_decode_json_line(lines[0]))
    observations: list[AcceptanceObservationInput] = []
    for line in lines[1:]:
        record = AcceptanceRuntimeJournalObservation.model_validate(
            _decode_json_line(line)
        )
        observation = record.observation
        if observation.sequence != len(observations):
            raise ValueError("runtime acceptance journal sequence is not contiguous")
        if (
            observation.code_revision != header.release_identity.code_revision
            or observation.image_digest != header.release_identity.image_digest
            or observation.config_sha256 != header.release_identity.config_sha256
            or observation.process_start_id != header.process_start_id
            or observation.mode is not header.mode
        ):
            raise ValueError("runtime acceptance journal identity mismatch")
        observations.append(observation)
    return header, tuple(observations)


def load_runtime_acceptance_attachments(path: Path) -> AcceptanceRuntimeAttachments:
    return AcceptanceRuntimeAttachments.model_validate(
        _decode_json_line(_read_bounded(path))
    )


def build_acceptance_seal_input(
    header: AcceptanceRuntimeJournalHeader,
    observations: tuple[AcceptanceObservationInput, ...],
    attachments: AcceptanceRuntimeAttachments,
    *,
    created_at: datetime | None = None,
) -> AcceptanceWindowSealInput:
    identity = (
        header.release_identity.code_revision,
        header.release_identity.image_digest,
        header.release_identity.config_sha256,
    )
    if any(
        (item.code_revision, item.image_digest, item.config_sha256) != identity
        for item in attachments.failure_injections
    ) or (
        attachments.deterministic_replay.code_revision,
        attachments.deterministic_replay.image_digest,
        attachments.deterministic_replay.config_sha256,
    ) != identity:
        raise ValueError("runtime acceptance attachments use a different release")
    return AcceptanceWindowSealInput(
        document_kind="acceptance-window-seal-input",
        schema_version=1,
        gate_id=header.gate_id,
        window_id=header.window_id,
        created_at=_utc(created_at or datetime.now(UTC)),
        observations=observations,
        failure_injections=attachments.failure_injections,
        deterministic_replay=attachments.deterministic_replay,
    )


def write_acceptance_seal_input(path: Path, payload: AcceptanceWindowSealInput) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(_canonical_json(payload.model_dump(mode="json")) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


async def _count(session: AsyncSession, statement: Any) -> int:
    return int((await session.scalar(statement)) or 0)


def _position_fills(position: PaperPosition) -> tuple[Any, ...]:
    return tuple(
        fill
        for fill in (
            position.leg_a,
            position.leg_b,
            position.close_leg_a,
            position.close_leg_b,
        )
        if fill is not None and fill.filled_quantity > 0
    )


def _fill_reconciles_to_book(fill: Any, snapshot: MarketSnapshot, stale_seconds: int) -> bool:
    if fill.instrument_type is None or fill.price is None or fill.filled_quantity <= 0:
        return False
    try:
        instrument_type = InstrumentType(fill.instrument_type)
    except ValueError:
        return False
    book = snapshot.orderbook(fill.exchange, fill.symbol, instrument_type)
    if book is None:
        return False
    fill_time = _utc(fill.timestamp)
    book_time = _utc(book.timestamp)
    age = Decimal(str((fill_time - book_time).total_seconds()))
    return -_MAX_FUTURE_SKEW_SECONDS <= age <= Decimal(str(stale_seconds))


def _directional_fill_economics(
    record: ExecutionFillRecord,
    source: CanonicalEventRecord | None,
    *,
    maximum_book_age_seconds: int,
) -> tuple[bool, Decimal, Decimal, Decimal]:
    record_price = Decimal(str(record.price))
    record_quantity = Decimal(str(record.quantity))
    raw_fee = Decimal(str(record.fee_amount))
    if record_price <= 0 or record_quantity <= 0 or raw_fee < 0:
        return False, max(_ZERO, raw_fee), _ZERO, _ZERO
    fee = raw_fee
    payload = record.payload
    if not isinstance(payload, dict):
        return False, fee, _ZERO, _ZERO
    fill = payload.get("fill")
    source_instrument = payload.get("source_instrument")
    if not isinstance(fill, dict) or not isinstance(source_instrument, dict):
        return False, fee, _ZERO, _ZERO
    try:
        payload_fee = Decimal(str(fill["fee"]))
        spread = Decimal(str(fill["spread_cost"]))
        slippage = Decimal(str(fill["impact_cost"]))
        payload_price = Decimal(str(fill["price"]))
        payload_quantity = Decimal(str(fill["quantity"]))
        payload_notional = Decimal(str(fill["notional"]))
        fill_timestamp = _utc(datetime.fromisoformat(str(fill["timestamp"])))
        payload_source_exchange_time = _utc(
            datetime.fromisoformat(str(payload["source_exchange_timestamp"]))
        )
        payload_source_receive_time = _utc(
            datetime.fromisoformat(str(payload["source_receive_timestamp"]))
        )
        instrument = InstrumentKey.model_validate(source_instrument)
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False, fee, _ZERO, _ZERO
    if (
        payload_price <= 0
        or payload_quantity <= 0
        or min(payload_fee, spread, slippage, payload_notional) < 0
    ):
        return False, fee, _ZERO, _ZERO
    if source is None or not isinstance(source.payload, dict):
        return False, fee, spread, slippage
    event_instrument = source.payload.get("instrument")
    if not isinstance(event_instrument, dict):
        return False, fee, spread, slippage
    fill_time = _database_utc(record.exchange_timestamp)
    source_time = _database_utc(source.exchange_timestamp)
    source_receive_time = _database_utc(source.receive_timestamp)
    age = Decimal(str((fill_time - source_time).total_seconds()))
    reconciled = (
        payload.get("source_event_id") == source.event_id
        and payload.get("source_event_kind") == source.kind
        and payload.get("source_event_source") == source.source
        and payload.get("source_event_quality") == source.quality == "VALID"
        and source.kind == "BOOK_SNAPSHOT"
        and source_instrument == event_instrument
        and instrument.venue.lower() == record.venue.lower()
        and instrument.canonical_id == record.instrument_id
        and instrument.quote_asset == record.fee_asset.upper()
        and _decimal_reconciles(payload_price, record_price)
        and _decimal_reconciles(payload_quantity, record_quantity)
        and _decimal_reconciles(payload_notional, payload_price * payload_quantity)
        and _decimal_reconciles(payload_fee, fee)
        and fill_timestamp == fill_time
        and payload_source_exchange_time == source_time
        and payload_source_receive_time == source_receive_time
        and fill.get("liquidity_role") == record.liquidity_role
        and -_MAX_FUTURE_SKEW_SECONDS
        <= age
        <= Decimal(str(maximum_book_age_seconds))
    )
    return reconciled, fee, spread, slippage


def _decimal_reconciles(left: Decimal, right: Decimal) -> bool:
    """Allow only sub-picounit drift from SQLite's NUMERIC test adapter."""

    return abs(left - right) <= _DECIMAL_RECONCILIATION_TOLERANCE


def _ages_by_venue(items: Any, observed_at: datetime) -> dict[str, tuple[Decimal, ...]]:
    grouped: dict[str, list[Decimal]] = {}
    for item in items:
        venue = str(item.exchange).lower()
        age = Decimal(str((observed_at - _utc(item.timestamp)).total_seconds()))
        grouped.setdefault(venue, []).append(age)
    return {venue: tuple(values) for venue, values in grouped.items()}


def _maximum_age(
    ages_by_venue: dict[str, tuple[Decimal, ...]], missing_value: Decimal
) -> Decimal:
    values = [age for venue in REQUIRED_VENUES for age in ages_by_venue.get(venue, ())]
    if any(not ages_by_venue.get(venue) for venue in REQUIRED_VENUES):
        values.append(missing_value)
    return max(values, default=missing_value)


def _accounting_error(runtime: RuntimeState) -> Decimal:
    snapshot = runtime.portfolio.snapshot()
    expected = snapshot.cash + snapshot.locked_capital + snapshot.total_pnl
    return abs(snapshot.equity - expected)


def _ledger_payload(
    runtime: RuntimeState,
    *,
    directional_state_sha256: str,
) -> dict[str, object]:
    snapshot = runtime.portfolio.snapshot()
    positions = [
        position.model_dump(mode="json")
        for position in sorted(
            runtime.portfolio.positions.values(), key=lambda item: item.id
        )
    ]
    return {
        "simulation_version": snapshot.simulation_version,
        "equity": str(snapshot.equity),
        "cash": str(snapshot.cash),
        "locked_capital": str(snapshot.locked_capital),
        "total_pnl": str(snapshot.total_pnl),
        "funding_pnl": str(snapshot.funding_pnl),
        "fees": str(snapshot.fees),
        "positions": positions,
        "directional_state_sha256": directional_state_sha256,
    }


def _source_watermark(
    sequence: int,
    source_payload: dict[str, object],
    canonical_row_id: int,
    canonical_event_id: str,
) -> str:
    digest = _sha256(
        {
            "source": source_payload,
            "canonical_row_id": canonical_row_id,
            "canonical_event_id": canonical_event_id,
        }
    )
    return f"src-{sequence}-{canonical_row_id}-{digest[:32]}"


def _read_bounded(path: Path) -> bytes:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_EVIDENCE_BYTES:
            raise ValueError("runtime acceptance evidence size is invalid")
        with os.fdopen(os.dup(descriptor), "rb", closefd=True) as stream:
            payload = stream.read(MAX_EVIDENCE_BYTES + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) != before.st_size
            or len(payload) > MAX_EVIDENCE_BYTES
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("runtime acceptance evidence changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _decode_json_line(payload: bytes) -> dict[str, Any]:
    if not payload or len(payload) > _MAX_JOURNAL_LINE_BYTES:
        raise ValueError("runtime acceptance journal line size is invalid")
    document = json.loads(
        payload.decode("utf-8", errors="strict"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_non_finite,
    )
    if not isinstance(document, dict):
        raise ValueError("runtime acceptance evidence root must be an object")
    return document


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("runtime acceptance evidence contains duplicate keys")
        result[key] = value
    return result


def _reject_non_finite(_: str) -> None:
    raise ValueError("runtime acceptance evidence contains a non-finite number")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("runtime acceptance timestamps require a timezone")
    return value.astimezone(UTC)


def _database_utc(value: datetime) -> datetime:
    return (value if value.tzinfo is not None else value.replace(tzinfo=UTC)).astimezone(
        UTC
    )


def _effective_uid() -> int:
    if os.name != "posix":
        raise ValueError("runtime acceptance ownership checks require Linux")
    return os.geteuid()  # type: ignore[attr-defined]
