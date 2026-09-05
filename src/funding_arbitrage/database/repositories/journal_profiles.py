"""Immutable canonical-journal profile boundaries and replay compatibility."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import BinaryIO, Literal
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    CanonicalEventRecord,
    CanonicalJournalProfileRecord,
)

CanonicalJournalProfileName = Literal["full", "sampled", "disabled"]
_CANONICAL_JOURNAL_WRITER_LOCK_ID = 5_068_047_852_292_196_166
_LOCAL_WRITER_LOCKS: dict[int, asyncio.Lock] = {}


class CanonicalJournalProfileCoverageError(RuntimeError):
    """The requested canonical rows do not share one compatible profile."""


class CanonicalJournalWriterLeaseError(RuntimeError):
    """Another process already owns the canonical journal writer lease."""


@dataclass
class CanonicalJournalWriterLease:
    """One database-scoped writer lease held for the complete producer lifetime."""

    connection: AsyncConnection
    bind_identity: int
    local_lock: asyncio.Lock | None = None
    sqlite_lock_file: BinaryIO | None = None
    _active: bool = True

    @property
    def active(self) -> bool:
        return self._active

    async def release(self) -> None:
        if not self._active:
            return
        self._active = False
        try:
            if self.connection.dialect.name == "postgresql":
                released = await self.connection.scalar(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": _CANONICAL_JOURNAL_WRITER_LOCK_ID},
                )
                if released is not True:
                    raise CanonicalJournalWriterLeaseError(
                        "canonical journal PostgreSQL writer lease was not held"
                    )
                await self.connection.commit()
        finally:
            try:
                await self.connection.close()
            finally:
                try:
                    if self.sqlite_lock_file is not None:
                        _release_sqlite_file_lock(self.sqlite_lock_file)
                finally:
                    if self.local_lock is not None:
                        self.local_lock.release()
                        _LOCAL_WRITER_LOCKS.pop(self.bind_identity, None)


@asynccontextmanager
async def canonical_journal_writer_lease(
    engine: AsyncEngine,
) -> AsyncIterator[CanonicalJournalWriterLease]:
    """Acquire one fail-closed journal writer lease before creating a boundary."""

    connection = await engine.connect()
    bind_identity = id(engine.sync_engine)
    local_lock: asyncio.Lock | None = None
    local_lock_acquired = False
    sqlite_lock_file: BinaryIO | None = None
    try:
        if connection.dialect.name == "postgresql":
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": _CANONICAL_JOURNAL_WRITER_LOCK_ID},
            )
            if acquired is not True:
                raise CanonicalJournalWriterLeaseError(
                    "another process owns the canonical journal writer lease"
                )
            await connection.commit()
        elif connection.dialect.name == "sqlite":
            sqlite_lock_path = _sqlite_writer_lock_path(engine)
            if sqlite_lock_path is not None:
                sqlite_lock_file = _acquire_sqlite_file_lock(sqlite_lock_path)
            else:
                local_lock = _LOCAL_WRITER_LOCKS.setdefault(
                    bind_identity,
                    asyncio.Lock(),
                )
                if local_lock.locked():
                    raise CanonicalJournalWriterLeaseError(
                        "another process owns the canonical journal writer lease"
                    )
                await local_lock.acquire()
                local_lock_acquired = True
        else:
            raise CanonicalJournalWriterLeaseError(
                "canonical journal writer lease supports only PostgreSQL or SQLite"
            )
        lease = CanonicalJournalWriterLease(
            connection=connection,
            bind_identity=bind_identity,
            local_lock=local_lock,
            sqlite_lock_file=sqlite_lock_file,
        )
    except BaseException:
        await connection.close()
        if sqlite_lock_file is not None:
            _release_sqlite_file_lock(sqlite_lock_file)
        if local_lock is not None and local_lock_acquired:
            local_lock.release()
            _LOCAL_WRITER_LOCKS.pop(bind_identity, None)
        raise
    try:
        yield lease
    finally:
        await lease.release()


def _sqlite_writer_lock_path(engine: AsyncEngine) -> Path | None:
    database = engine.url.database
    if database is None or database in {"", ":memory:"}:
        return None
    if database.startswith("file:"):
        raise CanonicalJournalWriterLeaseError(
            "SQLite URI databases are unsupported for canonical journal writers"
        )
    database_path = Path(database).expanduser().resolve(strict=False)
    return database_path.with_name(database_path.name + ".canonical-journal.lock")


def _acquire_sqlite_file_lock(path: Path) -> BinaryIO:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
    except OSError as error:
        if "handle" in locals():
            handle.close()
        raise CanonicalJournalWriterLeaseError(
            "cannot establish the canonical journal SQLite writer lease"
        ) from error
    try:
        if os.name == "nt":
            msvcrt = import_module("msvcrt")
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl = import_module("fcntl")
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return handle
    except OSError as error:
        handle.close()
        raise CanonicalJournalWriterLeaseError(
            "another process owns the canonical journal writer lease"
        ) from error


def _release_sqlite_file_lock(handle: BinaryIO) -> None:
    try:
        handle.seek(0)
        if os.name == "nt":
            msvcrt = import_module("msvcrt")
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl = import_module("fcntl")
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


@dataclass(frozen=True)
class CanonicalJournalProfileSpec:
    """Deterministic, non-secret identity of one journal recording contract."""

    profile: CanonicalJournalProfileName
    high_frequency_events_enabled: bool
    minimum_interval_seconds: str
    simulation_versions: tuple[str, ...]
    config_sha256: str


@dataclass(frozen=True)
class CanonicalJournalProfileBoundary:
    """A profile that applies to canonical rows after one durable row ID."""

    boundary_id: str
    started_at: datetime
    after_event_row_id: int
    spec: CanonicalJournalProfileSpec


@dataclass(frozen=True)
class CompatibleCanonicalJournalWindow:
    """Latest contiguous row window recorded with an exact profile contract."""

    after_event_row_id: int
    first_boundary_id: str
    latest_boundary_id: str
    boundary_ids: tuple[str, ...]
    started_at: datetime


def canonical_journal_profile_spec(settings: Settings) -> CanonicalJournalProfileSpec:
    """Build a stable profile hash without serializing credentials or secrets."""

    interval = _decimal_text(settings.canonical_high_frequency_market_event_min_interval_seconds)
    if not settings.canonical_high_frequency_market_events_enabled:
        profile: CanonicalJournalProfileName = "disabled"
    elif Decimal(interval) > 0:
        profile = "sampled"
    else:
        profile = "full"
    simulation_versions = (
        (settings.paper_simulation_version, settings.paper_baseline_simulation_version)
        if settings.run_mode == "paper_test" and settings.paper_comparison_enabled
        else (
            (settings.paper_simulation_version,)
            if settings.run_mode == "paper_test"
            else (("live",) if settings.run_mode == "live" else ("api",))
        )
    )
    venues = (
        settings.live_venue_values if settings.run_mode == "live" else settings.paper_venue_values
    )
    payload = {
        "schema_version": 1,
        "release_commit_sha": settings.release_commit_sha,
        "profile": profile,
        "high_frequency_events_enabled": (settings.canonical_high_frequency_market_events_enabled),
        "minimum_interval_seconds": interval,
        "run_mode": settings.run_mode,
        "trading_mode": settings.effective_trading_mode.value,
        "market_data_mode": settings.market_data_mode,
        "simulation_versions": simulation_versions,
        "venues": venues,
        "public_event_symbol_limit_per_profile": (settings.public_event_symbol_limit_per_profile),
        "public_event_rest_interval_seconds": _decimal_text(
            settings.public_event_rest_interval_seconds
        ),
        "paper_loop_interval_seconds": _decimal_text(settings.paper_loop_interval_seconds),
        "paper_orderbook_symbol_limit": settings.paper_orderbook_symbol_limit,
        "paper_market_asset_limit": settings.paper_market_asset_limit,
        "paper_history_symbol_limit": settings.paper_history_symbol_limit,
        "paper_history_refresh_seconds": settings.paper_history_refresh_seconds,
        "public_metadata_refresh_seconds": _decimal_text(
            settings.public_metadata_refresh_seconds
        ),
        "options_market_data_enabled": settings.options_market_data_enabled,
        "options_refresh_seconds": _decimal_text(settings.options_refresh_seconds),
        "options_maximum_expiries": settings.options_maximum_expiries,
        "options_strikes_per_expiry": settings.options_strikes_per_expiry,
        "multi_regime_enabled": settings.multi_regime_enabled,
        "multi_regime_assets": tuple(sorted(settings.multi_regime_asset_values)),
        "multi_regime_dynamic_universe_enabled": (
            settings.multi_regime_dynamic_universe_enabled
        ),
        "multi_regime_universe_rebalance_seconds": (
            settings.multi_regime_universe_rebalance_seconds
        ),
        "multi_regime_universe_maximum_assets": (
            settings.multi_regime_universe_maximum_assets
        ),
        "multi_regime_universe_maximum_new_assets": (
            settings.multi_regime_universe_maximum_new_assets
        ),
        "multi_regime_universe_maximum_data_age_seconds": _decimal_text(
            settings.multi_regime_universe_maximum_data_age_seconds
        ),
        "multi_regime_universe_minimum_listing_age_days": _decimal_text(
            settings.multi_regime_universe_minimum_listing_age_days
        ),
        "multi_regime_universe_minimum_statistics_days": _decimal_text(
            settings.multi_regime_universe_minimum_statistics_days
        ),
        "multi_regime_universe_minimum_venue_count": (
            settings.multi_regime_universe_minimum_venue_count
        ),
        "multi_regime_universe_minimum_quote_volume_usd": _decimal_text(
            settings.multi_regime_universe_minimum_quote_volume_usd
        ),
        "multi_regime_universe_minimum_depth_usd": _decimal_text(
            settings.multi_regime_universe_minimum_depth_usd
        ),
        "multi_regime_universe_minimum_open_interest_usd": _decimal_text(
            settings.multi_regime_universe_minimum_open_interest_usd
        ),
        "multi_regime_universe_maximum_spread_bps": _decimal_text(
            settings.multi_regime_universe_maximum_spread_bps
        ),
        "multi_regime_universe_maximum_slippage_bps": _decimal_text(
            settings.multi_regime_universe_maximum_slippage_bps
        ),
        "multi_regime_universe_minimum_funding_samples": (
            settings.multi_regime_universe_minimum_funding_samples
        ),
        "multi_regime_universe_minimum_data_coverage": _decimal_text(
            settings.multi_regime_universe_minimum_data_coverage
        ),
        "multi_regime_universe_minimum_entry_score": _decimal_text(
            settings.multi_regime_universe_minimum_entry_score
        ),
        "multi_regime_universe_minimum_retention_score": _decimal_text(
            settings.multi_regime_universe_minimum_retention_score
        ),
        "multi_regime_universe_target_funding_bps_daily": _decimal_text(
            settings.multi_regime_universe_target_funding_bps_daily
        ),
        "multi_regime_universe_excluded_assets": tuple(
            sorted(settings.multi_regime_universe_excluded_asset_values)
        ),
        "multi_regime_source_interval_seconds": (settings.multi_regime_source_interval_seconds),
        "multi_regime_strategy_interval_seconds": (settings.multi_regime_strategy_interval_seconds),
        "multi_regime_regime_interval_seconds": (settings.multi_regime_regime_interval_seconds),
        "multi_regime_stale_after_seconds": settings.multi_regime_stale_after_seconds,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return CanonicalJournalProfileSpec(
        profile=profile,
        high_frequency_events_enabled=(settings.canonical_high_frequency_market_events_enabled),
        minimum_interval_seconds=interval,
        simulation_versions=simulation_versions,
        config_sha256=hashlib.sha256(encoded).hexdigest(),
    )


async def append_canonical_journal_profile_boundary(
    session: AsyncSession,
    spec: CanonicalJournalProfileSpec,
    *,
    writer_lease: CanonicalJournalWriterLease,
    started_at: datetime | None = None,
) -> CanonicalJournalProfileBoundary:
    """Append a boundary before producers may write rows under ``spec``."""

    if not writer_lease.active or writer_lease.bind_identity != id(session.get_bind()):
        raise CanonicalJournalWriterLeaseError(
            "canonical journal boundary requires the active database writer lease"
        )

    observed_at = started_at or datetime.now(UTC)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("canonical journal profile timestamp requires a timezone")
    observed_at = observed_at.astimezone(UTC)
    after_event_row_id = int(await session.scalar(select(func.max(CanonicalEventRecord.id))) or 0)
    boundary_id = "journal-profile-" + uuid4().hex
    session.add(
        CanonicalJournalProfileRecord(
            boundary_id=boundary_id,
            started_at=observed_at,
            after_event_row_id=after_event_row_id,
            profile=spec.profile,
            high_frequency_events_enabled=spec.high_frequency_events_enabled,
            minimum_interval_seconds=spec.minimum_interval_seconds,
            simulation_versions=list(spec.simulation_versions),
            config_sha256=spec.config_sha256,
        )
    )
    await session.commit()
    return CanonicalJournalProfileBoundary(
        boundary_id=boundary_id,
        started_at=observed_at,
        after_event_row_id=after_event_row_id,
        spec=spec,
    )


async def load_latest_compatible_journal_window(
    session: AsyncSession,
    spec: CanonicalJournalProfileSpec,
    *,
    up_to_event_row_id: int,
) -> CompatibleCanonicalJournalWindow:
    """Return the latest exact-profile chain, rejecting missing current metadata."""

    if up_to_event_row_id < 0:
        raise ValueError("canonical journal row boundary cannot be negative")
    records = await _load_boundaries(
        session,
        maximum_after_event_row_id=up_to_event_row_id,
        inclusive=True,
    )
    return _compatible_window(records, spec)


async def load_canonical_journal_boundary_covering_row(
    session: AsyncSession,
    spec: CanonicalJournalProfileSpec,
    *,
    event_row_id: int,
) -> CanonicalJournalProfileBoundary:
    """Return the latest exact-profile boundary that applies to one event row."""

    if event_row_id <= 0:
        raise ValueError("canonical journal event row must be positive")
    records = await _load_boundaries(
        session,
        maximum_after_event_row_id=event_row_id,
        inclusive=False,
    )
    window = _compatible_window(records, spec)
    record = records[0]
    if record.boundary_id != window.latest_boundary_id:
        raise CanonicalJournalProfileCoverageError(
            "canonical journal event row has no exact-profile boundary"
        )
    return _boundary_from_record(record, spec)


async def assert_canonical_journal_checkpoint_compatible(
    session: AsyncSession,
    spec: CanonicalJournalProfileSpec,
    *,
    boundary_id: str,
    event_row_id: int,
) -> CompatibleCanonicalJournalWindow:
    """Validate both the identity and strict row coverage of a checkpoint."""

    if event_row_id <= 0:
        raise ValueError("canonical journal checkpoint row must be positive")
    record = await session.scalar(
        select(CanonicalJournalProfileRecord).where(
            CanonicalJournalProfileRecord.boundary_id == boundary_id
        )
    )
    if record is None or not _matches(record, spec):
        raise CanonicalJournalProfileCoverageError(
            "paper checkpoint canonical journal profile is incompatible"
        )
    if record.after_event_row_id >= event_row_id:
        raise CanonicalJournalProfileCoverageError(
            "paper checkpoint canonical journal profile does not cover its event row"
        )
    records = await _load_boundaries(
        session,
        maximum_after_event_row_id=event_row_id,
        inclusive=False,
    )
    window = _compatible_window(records, spec)
    if boundary_id != window.latest_boundary_id:
        raise CanonicalJournalProfileCoverageError(
            "paper checkpoint does not reference the latest covering journal boundary"
        )
    return window


async def assert_canonical_journal_rows_compatible(
    session: AsyncSession,
    spec: CanonicalJournalProfileSpec,
    *,
    first_event_row_id: int,
    last_event_row_id: int,
) -> CompatibleCanonicalJournalWindow:
    """Reject replay rows that are sampled, incompatible, or unlabeled."""

    if first_event_row_id <= 0 or last_event_row_id < first_event_row_id:
        raise ValueError("canonical journal replay row range is invalid")
    # A boundary at row N applies to rows strictly after N, so a boundary at the
    # requested last row must not change that row's profile.
    records = await _load_boundaries(
        session,
        maximum_after_event_row_id=last_event_row_id,
        inclusive=False,
    )
    window = _compatible_window(records, spec)
    if first_event_row_id <= window.after_event_row_id:
        raise CanonicalJournalProfileCoverageError(
            "canonical journal replay crosses an incompatible or unlabeled profile"
        )
    return window


async def _load_boundaries(
    session: AsyncSession,
    *,
    maximum_after_event_row_id: int,
    inclusive: bool,
) -> tuple[CanonicalJournalProfileRecord, ...]:
    boundary = CanonicalJournalProfileRecord.after_event_row_id
    statement = select(CanonicalJournalProfileRecord).where(
        boundary <= maximum_after_event_row_id
        if inclusive
        else boundary < maximum_after_event_row_id
    )
    records = list(
        (
            await session.scalars(
                statement.order_by(boundary.desc(), CanonicalJournalProfileRecord.id.desc())
            )
        ).all()
    )
    # More than one process may restart at the same canonical tip.  The newest
    # committed boundary is the contract that applies to subsequent rows.
    collapsed: list[CanonicalJournalProfileRecord] = []
    seen: set[int] = set()
    for record in records:
        if record.after_event_row_id in seen:
            continue
        seen.add(record.after_event_row_id)
        collapsed.append(record)
    return tuple(collapsed)


def _compatible_window(
    records: tuple[CanonicalJournalProfileRecord, ...],
    spec: CanonicalJournalProfileSpec,
) -> CompatibleCanonicalJournalWindow:
    if not records:
        raise CanonicalJournalProfileCoverageError("canonical journal profile metadata is missing")
    latest = records[0]
    if not _matches(latest, spec):
        raise CanonicalJournalProfileCoverageError(
            "latest canonical journal profile is incompatible"
        )
    compatible = [latest]
    for record in records[1:]:
        if not _matches(record, spec):
            break
        compatible.append(record)
    earliest = compatible[-1]
    return CompatibleCanonicalJournalWindow(
        after_event_row_id=earliest.after_event_row_id,
        first_boundary_id=earliest.boundary_id,
        latest_boundary_id=latest.boundary_id,
        boundary_ids=tuple(record.boundary_id for record in reversed(compatible)),
        started_at=_utc(earliest.started_at),
    )


def _matches(
    record: CanonicalJournalProfileRecord,
    spec: CanonicalJournalProfileSpec,
) -> bool:
    return (
        record.profile == spec.profile
        and record.high_frequency_events_enabled is spec.high_frequency_events_enabled
        and record.minimum_interval_seconds == spec.minimum_interval_seconds
        and tuple(record.simulation_versions) == spec.simulation_versions
        and record.config_sha256 == spec.config_sha256
    )


def _boundary_from_record(
    record: CanonicalJournalProfileRecord,
    spec: CanonicalJournalProfileSpec,
) -> CanonicalJournalProfileBoundary:
    return CanonicalJournalProfileBoundary(
        boundary_id=record.boundary_id,
        started_at=_utc(record.started_at),
        after_event_row_id=record.after_event_row_id,
        spec=spec,
    )


def _decimal_text(value: int | float | Decimal) -> str:
    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("canonical journal profile value must be finite")
    return format(decimal.normalize(), "f")


def _utc(value: datetime) -> datetime:
    # SQLite drops tzinfo from timezone-aware columns. All boundaries enter this
    # repository as UTC, so restoring that marker is deterministic and mirrors
    # the PostgreSQL result.
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
