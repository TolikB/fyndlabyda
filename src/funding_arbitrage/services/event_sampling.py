"""Bounded sampling for repeated canonical market observations."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from time import monotonic
from typing import Any

from funding_arbitrage.domain.events import EventEnvelope, EventKind, InstrumentKey
from funding_arbitrage.monitoring.metrics import (
    canonical_high_frequency_events_sampled_out_total,
)

CanonicalEventSink = Callable[[EventEnvelope[Any]], Awaitable[None]]

_SAMPLED_MARKET_KINDS = frozenset(
    {
        EventKind.TRADE_TICK,
        EventKind.BOOK_SNAPSHOT,
        EventKind.BOOK_DELTA,
        EventKind.FUNDING_SNAPSHOT,
        EventKind.OPEN_INTEREST_SNAPSHOT,
        EventKind.OPTION_QUOTE_SNAPSHOT,
    }
)


class CanonicalHighFrequencyEventSampler:
    """Forward at most one event per kind and instrument in each interval.

    Events outside the explicit repeated-market-data allowlist always pass through.
    The bounded stream map prevents a rotating discovery universe from causing
    unbounded process-memory growth.
    """

    def __init__(
        self,
        sink: CanonicalEventSink,
        *,
        minimum_interval_seconds: float,
        maximum_streams: int = 4096,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if not math.isfinite(minimum_interval_seconds) or minimum_interval_seconds <= 0:
            raise ValueError("canonical event sample interval must be finite and positive")
        if maximum_streams <= 0:
            raise ValueError("canonical event sampler stream bound must be positive")
        self.sink = sink
        self.minimum_interval_seconds = minimum_interval_seconds
        self.maximum_streams = maximum_streams
        self._clock = clock
        self._last_forwarded: dict[tuple[EventKind, str], float] = {}
        self._lock = asyncio.Lock()

    async def __call__(self, event: EventEnvelope[Any]) -> None:
        if event.kind not in _SAMPLED_MARKET_KINDS:
            await self.sink(event)
            return
        instrument = getattr(event.payload, "instrument", None)
        if not isinstance(instrument, InstrumentKey):
            raise TypeError("sampled canonical market event has no instrument identity")
        key = (event.kind, instrument.canonical_id)
        sampled_at = self._clock()
        async with self._lock:
            previous = self._last_forwarded.get(key)
            if previous is not None and sampled_at < previous:
                self._last_forwarded.clear()
                previous = None
            if previous is not None and sampled_at - previous < self.minimum_interval_seconds:
                canonical_high_frequency_events_sampled_out_total.labels(event.kind.value).inc()
                return
            self._prune(sampled_at)
            self._last_forwarded[key] = sampled_at
        # Keep the reservation even when the sink raises.  The canonical router
        # durably commits before notifying downstream consumers, so a downstream
        # failure may mean the row already exists.  Releasing the slot here would
        # let every subsequent tick bypass the bound while the consumer remains
        # unhealthy.  The original error still propagates and the stream becomes
        # eligible again after the configured interval.
        await self.sink(event)

    def _prune(self, now: float) -> None:
        if len(self._last_forwarded) < self.maximum_streams:
            return
        cutoff = now - self.minimum_interval_seconds * 2
        self._last_forwarded = {
            key: observed_at
            for key, observed_at in self._last_forwarded.items()
            if observed_at >= cutoff
        }
        if len(self._last_forwarded) < self.maximum_streams:
            return
        oldest = min(self._last_forwarded, key=self._last_forwarded.__getitem__)
        self._last_forwarded.pop(oldest)
