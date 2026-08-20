"""Leakage-safe incremental aggregation of closed canonical candles."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.events import Candle, InstrumentKey


class CandleAggregator:
    """Aggregate one fixed source interval without emitting the active bucket."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        source_interval_seconds: int,
        target_interval_seconds: int,
    ) -> None:
        if source_interval_seconds <= 0 or target_interval_seconds <= 0:
            raise ValueError("candle intervals must be positive")
        if target_interval_seconds % source_interval_seconds != 0:
            raise ValueError("target interval must be a multiple of source interval")
        self.instrument = instrument
        self.source_interval_seconds = source_interval_seconds
        self.target_interval_seconds = target_interval_seconds
        self._bucket: list[Candle] = []
        self._bucket_open: datetime | None = None
        self._last_source_open: datetime | None = None

    def reset(self) -> None:
        """Discard partial state after an explicitly invalid source event."""

        self._bucket = []
        self._bucket_open = None
        self._last_source_open = None

    def on_candle(self, candle: Candle) -> Candle | None:
        if candle.instrument != self.instrument:
            raise ValueError("candle aggregation instrument mismatch")
        if candle.interval_seconds != self.source_interval_seconds:
            raise ValueError("candle aggregation source interval mismatch")
        if not candle.closed:
            raise ValueError("candle aggregation requires closed source candles")
        if self._last_source_open is not None and candle.open_time <= self._last_source_open:
            raise ValueError("out-of-order or duplicate source candle")

        expected_open = (
            self._last_source_open + timedelta(seconds=self.source_interval_seconds)
            if self._last_source_open is not None
            else None
        )
        bucket_open = self._floor_time(candle.open_time)
        completed: Candle | None = None
        if expected_open is not None and candle.open_time != expected_open:
            self._bucket = []
            self._bucket_open = None
        elif self._bucket_open is not None and bucket_open != self._bucket_open:
            completed = self._complete_bucket()
            self._bucket = []
            self._bucket_open = None

        if self._bucket_open is None:
            self._bucket_open = bucket_open
        self._bucket.append(candle)
        self._last_source_open = candle.open_time
        return completed

    def _complete_bucket(self) -> Candle | None:
        bucket_open = self._bucket_open
        if bucket_open is None:
            return None
        required = self.target_interval_seconds // self.source_interval_seconds
        if len(self._bucket) != required:
            return None
        expected = tuple(
            bucket_open + timedelta(seconds=index * self.source_interval_seconds)
            for index in range(required)
        )
        if tuple(candle.open_time for candle in self._bucket) != expected:
            return None
        first = self._bucket[0]
        last = self._bucket[-1]
        close_time = bucket_open + timedelta(seconds=self.target_interval_seconds)
        if last.close_time != close_time:
            return None
        quote_volumes = tuple(candle.quote_volume for candle in self._bucket)
        return Candle(
            instrument=self.instrument,
            interval_seconds=self.target_interval_seconds,
            open_time=bucket_open,
            close_time=close_time,
            open=first.open,
            high=max(candle.high for candle in self._bucket),
            low=min(candle.low for candle in self._bucket),
            close=last.close,
            volume=sum((candle.volume for candle in self._bucket), Decimal("0")),
            quote_volume=(
                sum((value for value in quote_volumes if value is not None), Decimal("0"))
                if all(value is not None for value in quote_volumes)
                else None
            ),
            closed=True,
            exchange_timestamp=close_time,
        )

    def _floor_time(self, value: datetime) -> datetime:
        current = (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
        seconds = int(current.timestamp())
        floored = seconds - seconds % self.target_interval_seconds
        return datetime.fromtimestamp(floored, tz=UTC)
