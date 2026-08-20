"""Deterministic canonical stream quality state machine."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from funding_arbitrage.domain.events import (
    BookDelta,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
)


@dataclass(frozen=True, order=True)
class StreamIdentity:
    venue: str
    stream: str
    instrument_id: str = "*"


@dataclass(frozen=True)
class StreamQualitySnapshot:
    identity: StreamIdentity
    quality: DataQuality
    reason: str | None
    last_exchange_timestamp: datetime | None
    last_receive_timestamp: datetime | None
    last_sequence: int | None

    @property
    def usable(self) -> bool:
        return self.quality is DataQuality.VALID


@dataclass
class _StreamState:
    quality: DataQuality = DataQuality.RECOVERING
    reason: str | None = "awaiting_first_event"
    last_exchange_timestamp: datetime | None = None
    last_receive_timestamp: datetime | None = None
    last_sequence: int | None = None


class DataQualityMonitor:
    """Track every canonical quality state without repairing or inventing data."""

    def __init__(
        self,
        *,
        stale_after: timedelta,
        unavailable_after: timedelta,
        max_future_skew: timedelta = timedelta(seconds=2),
    ) -> None:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if unavailable_after <= stale_after:
            raise ValueError("unavailable_after must exceed stale_after")
        if max_future_skew < timedelta(0):
            raise ValueError("max_future_skew cannot be negative")
        self.stale_after = stale_after
        self.unavailable_after = unavailable_after
        self.max_future_skew = max_future_skew
        self._states: dict[StreamIdentity, _StreamState] = {}

    def preview(
        self,
        event: EventEnvelope[Any],
        *,
        identity: StreamIdentity | None = None,
    ) -> StreamQualitySnapshot:
        """Evaluate an event against a copy without advancing authoritative state."""

        key = identity or identity_for_event(event)
        current = self._states.get(key)
        prospective = replace(current) if current is not None else _StreamState()
        return self._observe_state(key, prospective, event)

    def observe(
        self,
        event: EventEnvelope[Any],
        *,
        identity: StreamIdentity | None = None,
    ) -> StreamQualitySnapshot:
        """Commit a successfully persisted event to authoritative stream state."""

        key = identity or identity_for_event(event)
        state = self._states.setdefault(key, _StreamState())
        return self._observe_state(key, state, event)

    def _observe_state(
        self,
        key: StreamIdentity,
        state: _StreamState,
        event: EventEnvelope[Any],
    ) -> StreamQualitySnapshot:
        metadata = event.metadata
        payload = event.payload
        quality = metadata.quality
        reason: str | None = None

        if metadata.exchange_timestamp > metadata.receive_timestamp + self.max_future_skew:
            quality = DataQuality.INVALID
            reason = "exchange_timestamp_in_future"
        elif (
            state.last_exchange_timestamp is not None
            and metadata.exchange_timestamp < state.last_exchange_timestamp
        ):
            return StreamQualitySnapshot(
                identity=key,
                quality=DataQuality.INVALID,
                reason="exchange_timestamp_regressed",
                last_exchange_timestamp=state.last_exchange_timestamp,
                last_receive_timestamp=state.last_receive_timestamp,
                last_sequence=state.last_sequence,
            )
        elif quality is not DataQuality.VALID:
            reason = f"source_quality_{quality.value.lower()}"
        elif isinstance(payload, BookSnapshot):
            quality, reason = _snapshot_quality(payload)
            state.last_sequence = payload.sequence
        elif isinstance(payload, BookDelta):
            quality, reason = self._delta_quality(state, payload)
            if quality is DataQuality.VALID:
                state.last_sequence = payload.last_sequence

        state.last_exchange_timestamp = metadata.exchange_timestamp
        state.last_receive_timestamp = metadata.receive_timestamp
        state.quality = quality
        state.reason = reason
        return self._snapshot(key, state, metadata.receive_timestamp)
    def mark_unavailable(
        self,
        identity: StreamIdentity,
        *,
        reason: str,
        observed_at: datetime | None = None,
    ) -> StreamQualitySnapshot:
        if not reason.strip():
            raise ValueError("unavailable stream requires a reason")
        state = self._states.setdefault(identity, _StreamState())
        state.quality = DataQuality.UNAVAILABLE
        state.reason = reason
        state.last_receive_timestamp = _utc(observed_at or datetime.now(UTC))
        return self._snapshot(identity, state, state.last_receive_timestamp)

    def status(
        self, identity: StreamIdentity, *, now: datetime | None = None
    ) -> StreamQualitySnapshot:
        state = self._states.get(identity)
        current = _utc(now or datetime.now(UTC))
        if state is None:
            return StreamQualitySnapshot(
                identity=identity,
                quality=DataQuality.UNAVAILABLE,
                reason="stream_never_observed",
                last_exchange_timestamp=None,
                last_receive_timestamp=None,
                last_sequence=None,
            )
        return self._snapshot(identity, state, current)

    def statuses(self, *, now: datetime | None = None) -> tuple[StreamQualitySnapshot, ...]:
        current = _utc(now or datetime.now(UTC))
        return tuple(
            self._snapshot(identity, self._states[identity], current)
            for identity in sorted(self._states)
        )

    def required_streams_usable(
        self,
        identities: tuple[StreamIdentity, ...],
        *,
        now: datetime | None = None,
    ) -> tuple[bool, tuple[str, ...]]:
        snapshots = tuple(self.status(identity, now=now) for identity in identities)
        reasons = tuple(
            f"{item.identity.venue}:{item.identity.stream}:"
            f"{item.identity.instrument_id}:{item.quality.value}"
            for item in snapshots
            if not item.usable
        )
        return not reasons, reasons

    def _delta_quality(
        self, state: _StreamState, delta: BookDelta
    ) -> tuple[DataQuality, str | None]:
        if state.quality in {DataQuality.GAP, DataQuality.CROSSED, DataQuality.INVALID}:
            return DataQuality.RECOVERING, "snapshot_required"
        if state.last_sequence is None:
            return DataQuality.RECOVERING, "snapshot_required"
        if delta.last_sequence <= state.last_sequence:
            return DataQuality.VALID, "duplicate_delta"
        if delta.previous_sequence is not None:
            if delta.previous_sequence != state.last_sequence:
                return DataQuality.GAP, "previous_sequence_mismatch"
        elif delta.first_sequence > state.last_sequence + 1:
            return DataQuality.GAP, "sequence_gap"
        return DataQuality.VALID, None

    def _snapshot(
        self, identity: StreamIdentity, state: _StreamState, now: datetime
    ) -> StreamQualitySnapshot:
        quality = state.quality
        reason = state.reason
        if state.last_receive_timestamp is not None and quality is not DataQuality.UNAVAILABLE:
            age = now - state.last_receive_timestamp
            if age > self.unavailable_after:
                quality = DataQuality.UNAVAILABLE
                reason = "stream_timeout"
            elif age > self.stale_after:
                quality = DataQuality.STALE
                reason = "stream_stale"
        return StreamQualitySnapshot(
            identity=identity,
            quality=quality,
            reason=reason,
            last_exchange_timestamp=state.last_exchange_timestamp,
            last_receive_timestamp=state.last_receive_timestamp,
            last_sequence=state.last_sequence,
        )


def identity_for_event(event: EventEnvelope[Any]) -> StreamIdentity:
    payload = event.payload
    instrument = getattr(payload, "instrument", None)
    venue = getattr(instrument, "venue", None) or getattr(payload, "venue", None)
    canonical_id = getattr(instrument, "canonical_id", "*")
    if not venue:
        venue = event.metadata.source.split(":", 1)[0]
    stream = (
        "BOOK"
        if event.kind.value in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        else event.kind.value
    )
    return StreamIdentity(str(venue).upper(), stream, str(canonical_id))


def _snapshot_quality(snapshot: BookSnapshot) -> tuple[DataQuality, str | None]:
    if not snapshot.bids or not snapshot.asks:
        return DataQuality.INVALID, "empty_book_side"
    if snapshot.bids[0].price >= snapshot.asks[0].price:
        return DataQuality.CROSSED, "crossed_book"
    return DataQuality.VALID, None


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
