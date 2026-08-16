"""Leakage-safe swing, structure-break, liquidity-zone, and fair-value-gap features."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.events import Candle, DataQuality, InstrumentKey

BPS = Decimal("10000")


class StructureDirection(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class StructureEventType(StrEnum):
    SWING_HIGH = "SWING_HIGH"
    SWING_LOW = "SWING_LOW"
    BOS_BULLISH = "BOS_BULLISH"
    BOS_BEARISH = "BOS_BEARISH"
    CHOCH_BULLISH = "CHOCH_BULLISH"
    CHOCH_BEARISH = "CHOCH_BEARISH"
    LIQUIDITY_SWEPT = "LIQUIDITY_SWEPT"
    FVG_CREATED = "FVG_CREATED"
    FVG_FILLED = "FVG_FILLED"


class StructureEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_type: StructureEventType
    direction: StructureDirection
    price: Decimal = Field(gt=0)
    source_time: datetime
    confirmed_time: datetime

    @field_validator("source_time", "confirmed_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class SwingPoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    direction: StructureDirection
    price: Decimal = Field(gt=0)
    timestamp: datetime
    bar_index: int = Field(ge=0)


class LiquidityZone(BaseModel):
    model_config = ConfigDict(frozen=True)

    zone_id: str
    direction: StructureDirection
    lower_price: Decimal = Field(gt=0)
    upper_price: Decimal = Field(gt=0)
    created_at: datetime
    swept_at: datetime | None = None


class FairValueGap(BaseModel):
    model_config = ConfigDict(frozen=True)

    gap_id: str
    direction: StructureDirection
    lower_price: Decimal = Field(gt=0)
    upper_price: Decimal = Field(gt=0)
    created_at: datetime
    filled_at: datetime | None = None


class MarketStructureSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    trend: StructureDirection
    last_swing_high: SwingPoint | None = None
    last_swing_low: SwingPoint | None = None
    active_liquidity_zones: tuple[LiquidityZone, ...] = ()
    active_fair_value_gaps: tuple[FairValueGap, ...] = ()
    events: tuple[StructureEvent, ...] = ()
    recovery_reason: str | None = None


class MarketStructureEngine:
    """Confirm pivots only after right-hand bars have closed."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        interval_seconds: int,
        swing_lookback: int = 2,
        liquidity_zone_width_bps: Decimal = Decimal("5"),
        retained_zones: int = 50,
        retained_gaps: int = 50,
    ) -> None:
        if interval_seconds <= 0 or swing_lookback <= 0:
            raise ValueError("structure interval and lookback must be positive")
        if liquidity_zone_width_bps <= 0:
            raise ValueError("liquidity-zone width must be positive")
        if retained_zones <= 0 or retained_gaps <= 0:
            raise ValueError("retention limits must be positive")
        self.instrument = instrument
        self.interval_seconds = interval_seconds
        self.swing_lookback = swing_lookback
        self.liquidity_zone_width_bps = liquidity_zone_width_bps
        self.retained_zones = retained_zones
        self.retained_gaps = retained_gaps
        self._candles: deque[tuple[int, Candle]]
        self._zones: deque[LiquidityZone]
        self._gaps: deque[FairValueGap]
        self._last_open_time: datetime | None
        self._last_close_time: datetime | None
        self._last_swing_high: SwingPoint | None
        self._last_swing_low: SwingPoint | None
        self._broken_high_index: int | None
        self._broken_low_index: int | None
        self._trend: StructureDirection
        self._bar_index: int
        self._reset()

    def on_candle(self, candle: Candle) -> MarketStructureSnapshot:
        self._validate_candle(candle)
        gap = self._last_close_time is not None and candle.open_time != self._last_close_time
        if gap:
            self._reset()
        self._bar_index += 1
        events: list[StructureEvent] = []
        events.extend(self._update_existing_areas(candle))
        self._candles.append((self._bar_index, candle))
        events.extend(self._detect_fair_value_gap(candle))
        events.extend(self._confirm_swing(candle.exchange_timestamp))
        events.extend(self._detect_structure_break(candle))
        self._last_open_time = candle.open_time
        self._last_close_time = candle.close_time
        quality = (
            DataQuality.GAP
            if gap
            else DataQuality.VALID
            if self._last_swing_high is not None and self._last_swing_low is not None
            else DataQuality.RECOVERING
        )
        reason = "candle_gap" if gap else None if quality is DataQuality.VALID else "warmup"
        return MarketStructureSnapshot(
            instrument=self.instrument,
            timestamp=candle.exchange_timestamp,
            data_quality=quality,
            trend=self._trend,
            last_swing_high=self._last_swing_high,
            last_swing_low=self._last_swing_low,
            active_liquidity_zones=tuple(zone for zone in self._zones if zone.swept_at is None),
            active_fair_value_gaps=tuple(gap for gap in self._gaps if gap.filled_at is None),
            events=tuple(events),
            recovery_reason=reason,
        )

    def _reset(self) -> None:
        window = self.swing_lookback * 2 + 1
        self._candles = deque(maxlen=max(window, 3))
        self._zones = deque(maxlen=self.retained_zones)
        self._gaps = deque(maxlen=self.retained_gaps)
        self._last_open_time = None
        self._last_close_time = None
        self._last_swing_high = None
        self._last_swing_low = None
        self._broken_high_index = None
        self._broken_low_index = None
        self._trend = StructureDirection.NEUTRAL
        self._bar_index = -1

    def _validate_candle(self, candle: Candle) -> None:
        if candle.instrument != self.instrument:
            raise ValueError("market structure instrument mismatch")
        if candle.interval_seconds != self.interval_seconds:
            raise ValueError("market structure candle interval mismatch")
        if not candle.closed:
            raise ValueError("market structure requires closed candles")
        if self._last_open_time is not None and candle.open_time <= self._last_open_time:
            raise ValueError("out-of-order or duplicate candle")

    def _confirm_swing(self, confirmed_time: datetime) -> list[StructureEvent]:
        window = tuple(self._candles)
        required = self.swing_lookback * 2 + 1
        if len(window) < required:
            return []
        candidate_index = self.swing_lookback
        bar_index, candidate = window[candidate_index]
        others = [item[1] for index, item in enumerate(window) if index != candidate_index]
        events: list[StructureEvent] = []
        if all(candidate.high > item.high for item in others):
            point = SwingPoint(
                direction=StructureDirection.BEARISH,
                price=candidate.high,
                timestamp=candidate.close_time,
                bar_index=bar_index,
            )
            self._last_swing_high = point
            self._broken_high_index = None
            self._zones.append(self._zone(point))
            events.append(
                self._event(
                    StructureEventType.SWING_HIGH,
                    StructureDirection.BEARISH,
                    candidate.high,
                    candidate.close_time,
                    confirmed_time,
                )
            )
        if all(candidate.low < item.low for item in others):
            point = SwingPoint(
                direction=StructureDirection.BULLISH,
                price=candidate.low,
                timestamp=candidate.close_time,
                bar_index=bar_index,
            )
            self._last_swing_low = point
            self._broken_low_index = None
            self._zones.append(self._zone(point))
            events.append(
                self._event(
                    StructureEventType.SWING_LOW,
                    StructureDirection.BULLISH,
                    candidate.low,
                    candidate.close_time,
                    confirmed_time,
                )
            )
        return events

    def _detect_structure_break(self, candle: Candle) -> list[StructureEvent]:
        if (
            self._last_swing_high is not None
            and candle.close > self._last_swing_high.price
            and self._broken_high_index != self._last_swing_high.bar_index
        ):
            event_type = (
                StructureEventType.CHOCH_BULLISH
                if self._trend is StructureDirection.BEARISH
                else StructureEventType.BOS_BULLISH
            )
            self._trend = StructureDirection.BULLISH
            self._broken_high_index = self._last_swing_high.bar_index
            return [
                self._event(
                    event_type,
                    StructureDirection.BULLISH,
                    self._last_swing_high.price,
                    self._last_swing_high.timestamp,
                    candle.exchange_timestamp,
                )
            ]
        if (
            self._last_swing_low is not None
            and candle.close < self._last_swing_low.price
            and self._broken_low_index != self._last_swing_low.bar_index
        ):
            event_type = (
                StructureEventType.CHOCH_BEARISH
                if self._trend is StructureDirection.BULLISH
                else StructureEventType.BOS_BEARISH
            )
            self._trend = StructureDirection.BEARISH
            self._broken_low_index = self._last_swing_low.bar_index
            return [
                self._event(
                    event_type,
                    StructureDirection.BEARISH,
                    self._last_swing_low.price,
                    self._last_swing_low.timestamp,
                    candle.exchange_timestamp,
                )
            ]
        return []

    def _zone(self, point: SwingPoint) -> LiquidityZone:
        half_width = point.price * self.liquidity_zone_width_bps / BPS
        return LiquidityZone(
            zone_id=f"zone:{point.direction}:{point.bar_index}",
            direction=point.direction,
            lower_price=point.price - half_width,
            upper_price=point.price + half_width,
            created_at=point.timestamp,
        )

    def _update_existing_areas(self, candle: Candle) -> list[StructureEvent]:
        events: list[StructureEvent] = []
        updated_zones: deque[LiquidityZone] = deque(maxlen=self.retained_zones)
        for zone in self._zones:
            swept = zone.swept_at is None and (
                candle.high > zone.upper_price
                if zone.direction is StructureDirection.BEARISH
                else candle.low < zone.lower_price
            )
            current = (
                zone.model_copy(update={"swept_at": candle.exchange_timestamp})
                if swept
                else zone
            )
            updated_zones.append(current)
            if swept:
                events.append(
                    self._event(
                        StructureEventType.LIQUIDITY_SWEPT,
                        zone.direction,
                        (zone.lower_price + zone.upper_price) / Decimal("2"),
                        zone.created_at,
                        candle.exchange_timestamp,
                    )
                )
        self._zones = updated_zones
        updated_gaps: deque[FairValueGap] = deque(maxlen=self.retained_gaps)
        for gap in self._gaps:
            filled = gap.filled_at is None and (
                candle.low <= gap.lower_price
                if gap.direction is StructureDirection.BULLISH
                else candle.high >= gap.upper_price
            )
            current_gap = (
                gap.model_copy(update={"filled_at": candle.exchange_timestamp})
                if filled
                else gap
            )
            updated_gaps.append(current_gap)
            if filled:
                events.append(
                    self._event(
                        StructureEventType.FVG_FILLED,
                        gap.direction,
                        (gap.lower_price + gap.upper_price) / Decimal("2"),
                        gap.created_at,
                        candle.exchange_timestamp,
                    )
                )
        self._gaps = updated_gaps
        return events

    def _detect_fair_value_gap(self, candle: Candle) -> list[StructureEvent]:
        if len(self._candles) < 3:
            return []
        _, first = tuple(self._candles)[-3]
        direction: StructureDirection | None = None
        lower = Decimal("0")
        upper = Decimal("0")
        if candle.low > first.high:
            direction = StructureDirection.BULLISH
            lower, upper = first.high, candle.low
        elif candle.high < first.low:
            direction = StructureDirection.BEARISH
            lower, upper = candle.high, first.low
        if direction is None:
            return []
        gap = FairValueGap(
            gap_id=f"fvg:{direction}:{self._bar_index}",
            direction=direction,
            lower_price=lower,
            upper_price=upper,
            created_at=candle.close_time,
        )
        self._gaps.append(gap)
        return [
            self._event(
                StructureEventType.FVG_CREATED,
                direction,
                (lower + upper) / Decimal("2"),
                candle.close_time,
                candle.exchange_timestamp,
            )
        ]

    @staticmethod
    def _event(
        event_type: StructureEventType,
        direction: StructureDirection,
        price: Decimal,
        source_time: datetime,
        confirmed_time: datetime,
    ) -> StructureEvent:
        return StructureEvent(
            event_type=event_type,
            direction=direction,
            price=price,
            source_time=source_time,
            confirmed_time=confirmed_time,
        )
