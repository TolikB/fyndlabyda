"""Canonical quality observation followed by durable event publication."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from funding_arbitrage.domain.events import DataQuality, EventEnvelope
from funding_arbitrage.market_data.quality import (
    DataQualityMonitor,
    StreamIdentity,
    StreamQualitySnapshot,
    identity_for_event,
)
from funding_arbitrage.monitoring.metrics import canonical_stream_quality
from funding_arbitrage.services.event_writer import CanonicalEventWriter


class CanonicalEventRouter:
    """Expose one typed boundary for all public and private canonical events."""

    def __init__(
        self, writer: CanonicalEventWriter, quality_monitor: DataQualityMonitor
    ) -> None:
        self.writer = writer
        self.quality_monitor = quality_monitor
        self._stream_locks: dict[StreamIdentity, asyncio.Lock] = {}
        self._consumers: list[
            Callable[[EventEnvelope[Any]], Awaitable[None]]
        ] = []

    def subscribe(
        self, consumer: Callable[[EventEnvelope[Any]], Awaitable[None]]
    ) -> None:
        """Register a post-durability consumer before event producers start."""

        if consumer in self._consumers:
            raise ValueError("canonical event consumer already subscribed")
        self._consumers.append(consumer)

    async def publish(self, event: EventEnvelope[Any]) -> None:
        identity = identity_for_event(event)
        lock = self._stream_locks.setdefault(identity, asyncio.Lock())
        async with lock:
            quality = self.quality_monitor.preview(event, identity=identity)
            routed = event.model_copy(
                update={
                    "metadata": event.metadata.model_copy(
                        update={"quality": quality.quality}
                    )
                }
            )
            await self.writer.publish(routed)
            committed = self.quality_monitor.observe(event, identity=identity)
            if committed != quality:
                raise RuntimeError(
                    "canonical stream quality changed during durable publication"
                )
            self._set_metric(committed)
            for consumer in tuple(self._consumers):
                await consumer(routed)

    def required_streams_usable(
        self,
        identities: tuple[StreamIdentity, ...],
        *,
        now: datetime | None = None,
    ) -> tuple[bool, tuple[str, ...]]:
        result = self.quality_monitor.required_streams_usable(identities, now=now)
        for identity in identities:
            self._set_metric(self.quality_monitor.status(identity, now=now))
        return result

    def venue_streams_usable(
        self,
        identities: tuple[StreamIdentity, ...],
        venues: Iterable[str],
        streams: Iterable[str],
        *,
        now: datetime | None = None,
    ) -> tuple[bool, tuple[str, ...]]:
        """Require a usable stream of every observed kind on each venue.

        Instrument-level quality remains fail-closed for strategy consumers, while
        service readiness is not taken down by one optional discovery instrument.
        """

        current = now or datetime.now(UTC)
        snapshots = tuple(
            self.quality_monitor.status(identity, now=current)
            for identity in identities
        )
        for snapshot in snapshots:
            self._set_metric(snapshot)
        normalized_venues = tuple(sorted({venue.strip().upper() for venue in venues}))
        normalized_streams = tuple(
            sorted({stream.strip().upper() for stream in streams})
        )
        reasons: list[str] = []
        for venue in normalized_venues:
            venue_snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if snapshot.identity.venue.upper() == venue
            )
            for stream in normalized_streams:
                stream_snapshots = tuple(
                    snapshot
                    for snapshot in venue_snapshots
                    if snapshot.identity.stream.upper() == stream
                )
                stream_usable = any(
                    snapshot.usable for snapshot in stream_snapshots
                )
                aggregate = StreamQualitySnapshot(
                    identity=StreamIdentity(venue, stream),
                    quality=(
                        DataQuality.VALID
                        if stream_usable
                        else DataQuality.UNAVAILABLE
                    ),
                    reason=None if stream_usable else "required_stream_unusable",
                    last_exchange_timestamp=None,
                    last_receive_timestamp=None,
                    last_sequence=None,
                )
                self._set_metric(aggregate)
                if not stream_usable:
                    if stream_snapshots:
                        qualities = ",".join(
                            sorted(
                                {
                                    snapshot.quality.value
                                    for snapshot in stream_snapshots
                                }
                            )
                        )
                    else:
                        qualities = DataQuality.UNAVAILABLE.value
                    reasons.append(f"{venue}:{stream}:*:{qualities}")
        return not reasons, tuple(reasons)

    @staticmethod
    def _set_metric(snapshot: StreamQualitySnapshot) -> None:
        for quality in DataQuality:
            canonical_stream_quality.labels(
                snapshot.identity.venue,
                snapshot.identity.stream,
                snapshot.identity.instrument_id,
                quality.value,
            ).set(1 if snapshot.quality is quality else 0)
