"""Deterministic incremental trend, volatility, VWAP, and volume-profile features."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.events import Candle, DataQuality, InstrumentKey

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


class VolumeProfileLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)


class TechnicalFeatureSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    sample_count: int = Field(ge=0)
    close: Decimal = Field(gt=0)
    ema_fast: Decimal = Field(gt=0)
    ema_slow: Decimal = Field(gt=0)
    atr: Decimal | None = Field(default=None, ge=0)
    plus_di: Decimal | None = Field(default=None, ge=0)
    minus_di: Decimal | None = Field(default=None, ge=0)
    adx: Decimal | None = Field(default=None, ge=0, le=100)
    efficiency_ratio: Decimal | None = Field(default=None, ge=0, le=1)
    rolling_vwap: Decimal | None = Field(default=None, gt=0)
    point_of_control: Decimal | None = Field(default=None, gt=0)
    value_area_low: Decimal | None = Field(default=None, gt=0)
    value_area_high: Decimal | None = Field(default=None, gt=0)
    volume_profile: tuple[VolumeProfileLevel, ...] = ()
    recovery_reason: str | None = None

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class TechnicalFeatureEngine:
    """Single-instrument candle engine with no future-data dependency."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        interval_seconds: int,
        ema_fast_period: int = 9,
        ema_slow_period: int = 21,
        atr_period: int = 14,
        adx_period: int = 14,
        efficiency_period: int = 10,
        vwap_window: int = 100,
        volume_profile_window: int = 100,
        profile_bin_width: Decimal = Decimal("1"),
    ) -> None:
        periods = (
            ema_fast_period,
            ema_slow_period,
            atr_period,
            adx_period,
            efficiency_period,
            vwap_window,
            volume_profile_window,
        )
        if interval_seconds <= 0 or any(period <= 0 for period in periods):
            raise ValueError("feature intervals and periods must be positive")
        if ema_fast_period >= ema_slow_period:
            raise ValueError("fast EMA period must be below slow EMA period")
        if profile_bin_width <= 0:
            raise ValueError("volume-profile bin width must be positive")
        self.instrument = instrument
        self.interval_seconds = interval_seconds
        self.ema_fast_period = ema_fast_period
        self.ema_slow_period = ema_slow_period
        self.atr_period = atr_period
        self.adx_period = adx_period
        self.efficiency_period = efficiency_period
        self.vwap_window = vwap_window
        self.volume_profile_window = volume_profile_window
        self.profile_bin_width = profile_bin_width
        self._sample_count: int
        self._last_open_time: datetime | None
        self._last_close_time: datetime | None
        self._previous_high: Decimal | None
        self._previous_low: Decimal | None
        self._previous_close: Decimal | None
        self._ema_fast: Decimal | None
        self._ema_slow: Decimal | None
        self._atr: Decimal | None
        self._smoothed_plus_dm: Decimal | None
        self._smoothed_minus_dm: Decimal | None
        self._adx: Decimal | None
        self._tr_seed: deque[Decimal]
        self._plus_dm_seed: deque[Decimal]
        self._minus_dm_seed: deque[Decimal]
        self._dx_seed: deque[Decimal]
        self._closes: deque[Decimal]
        self._vwap_samples: deque[tuple[Decimal, Decimal]]
        self._profile_samples: deque[tuple[Decimal, Decimal]]
        self._reset()

    def on_candle(self, candle: Candle) -> TechnicalFeatureSnapshot:
        self._validate_candle(candle)
        gap = self._last_close_time is not None and candle.open_time != self._last_close_time
        if gap:
            self._reset()
        self._sample_count += 1
        self._update_ema(candle.close)
        self._update_directional_movement(candle)
        self._closes.append(candle.close)
        typical_price = (candle.high + candle.low + candle.close) / Decimal("3")
        self._vwap_samples.append((typical_price, candle.volume))
        self._profile_samples.append((self._profile_bin(typical_price), candle.volume))
        self._previous_high = candle.high
        self._previous_low = candle.low
        self._previous_close = candle.close
        self._last_open_time = candle.open_time
        self._last_close_time = candle.close_time
        quality = (
            DataQuality.GAP
            if gap
            else DataQuality.VALID
            if self._is_warm()
            else DataQuality.RECOVERING
        )
        reason = "candle_gap" if gap else None if quality is DataQuality.VALID else "warmup"
        return self._snapshot(candle.exchange_timestamp, candle.close, quality, reason)

    def _reset(self) -> None:
        self._sample_count = 0
        self._last_open_time = None
        self._last_close_time = None
        self._previous_high = None
        self._previous_low = None
        self._previous_close = None
        self._ema_fast = None
        self._ema_slow = None
        self._atr = None
        self._smoothed_plus_dm = None
        self._smoothed_minus_dm = None
        self._adx = None
        self._tr_seed = deque(maxlen=self.atr_period)
        self._plus_dm_seed = deque(maxlen=self.atr_period)
        self._minus_dm_seed = deque(maxlen=self.atr_period)
        self._dx_seed = deque(maxlen=self.adx_period)
        self._closes = deque(maxlen=self.efficiency_period + 1)
        self._vwap_samples = deque(maxlen=self.vwap_window)
        self._profile_samples = deque(
            maxlen=self.volume_profile_window
        )

    def _validate_candle(self, candle: Candle) -> None:
        if candle.instrument != self.instrument:
            raise ValueError("technical feature instrument mismatch")
        if candle.interval_seconds != self.interval_seconds:
            raise ValueError("technical feature candle interval mismatch")
        if not candle.closed:
            raise ValueError("technical features require closed candles")
        if self._last_open_time is not None and candle.open_time <= self._last_open_time:
            raise ValueError("out-of-order or duplicate candle")

    def _update_ema(self, close: Decimal) -> None:
        self._ema_fast = self._ema(self._ema_fast, close, self.ema_fast_period)
        self._ema_slow = self._ema(self._ema_slow, close, self.ema_slow_period)

    @staticmethod
    def _ema(previous: Decimal | None, value: Decimal, period: int) -> Decimal:
        if previous is None:
            return value
        alpha = Decimal("2") / Decimal(period + 1)
        return previous + alpha * (value - previous)

    def _update_directional_movement(self, candle: Candle) -> None:
        if self._previous_close is None:
            true_range = candle.high - candle.low
            plus_dm = ZERO
            minus_dm = ZERO
        else:
            true_range = max(
                candle.high - candle.low,
                abs(candle.high - self._previous_close),
                abs(candle.low - self._previous_close),
            )
            up_move = candle.high - (self._previous_high or candle.high)
            down_move = (self._previous_low or candle.low) - candle.low
            plus_dm = up_move if up_move > down_move and up_move > 0 else ZERO
            minus_dm = down_move if down_move > up_move and down_move > 0 else ZERO
        if self._atr is None:
            self._tr_seed.append(true_range)
            self._plus_dm_seed.append(plus_dm)
            self._minus_dm_seed.append(minus_dm)
            if len(self._tr_seed) < self.atr_period:
                return
            divisor = Decimal(self.atr_period)
            self._atr = sum(self._tr_seed, ZERO) / divisor
            self._smoothed_plus_dm = sum(self._plus_dm_seed, ZERO) / divisor
            self._smoothed_minus_dm = sum(self._minus_dm_seed, ZERO) / divisor
        else:
            divisor = Decimal(self.atr_period)
            self._atr = (self._atr * (divisor - ONE) + true_range) / divisor
            assert self._smoothed_plus_dm is not None
            assert self._smoothed_minus_dm is not None
            self._smoothed_plus_dm = (
                self._smoothed_plus_dm * (divisor - ONE) + plus_dm
            ) / divisor
            self._smoothed_minus_dm = (
                self._smoothed_minus_dm * (divisor - ONE) + minus_dm
            ) / divisor
        plus_di, minus_di = self._directional_indexes()
        total = plus_di + minus_di
        dx = abs(plus_di - minus_di) / total * HUNDRED if total > 0 else ZERO
        if self._adx is None:
            self._dx_seed.append(dx)
            if len(self._dx_seed) == self.adx_period:
                self._adx = sum(self._dx_seed, ZERO) / Decimal(self.adx_period)
        else:
            divisor = Decimal(self.adx_period)
            self._adx = (self._adx * (divisor - ONE) + dx) / divisor

    def _directional_indexes(self) -> tuple[Decimal, Decimal]:
        if (
            self._atr is None
            or self._atr <= 0
            or self._smoothed_plus_dm is None
            or self._smoothed_minus_dm is None
        ):
            return ZERO, ZERO
        return (
            HUNDRED * self._smoothed_plus_dm / self._atr,
            HUNDRED * self._smoothed_minus_dm / self._atr,
        )

    def _snapshot(
        self,
        timestamp: datetime,
        close: Decimal,
        quality: DataQuality,
        reason: str | None,
    ) -> TechnicalFeatureSnapshot:
        assert self._ema_fast is not None
        assert self._ema_slow is not None
        plus_di, minus_di = self._directional_indexes()
        profile, point_of_control, value_area_low, value_area_high = self._volume_profile()
        return TechnicalFeatureSnapshot(
            instrument=self.instrument,
            timestamp=timestamp,
            data_quality=quality,
            sample_count=self._sample_count,
            close=close,
            ema_fast=self._ema_fast,
            ema_slow=self._ema_slow,
            atr=self._atr,
            plus_di=plus_di if self._atr is not None else None,
            minus_di=minus_di if self._atr is not None else None,
            adx=self._adx,
            efficiency_ratio=self._efficiency_ratio(),
            rolling_vwap=self._rolling_vwap(),
            point_of_control=point_of_control,
            value_area_low=value_area_low,
            value_area_high=value_area_high,
            volume_profile=profile,
            recovery_reason=reason,
        )

    def _efficiency_ratio(self) -> Decimal | None:
        if len(self._closes) < self.efficiency_period + 1:
            return None
        direction = abs(self._closes[-1] - self._closes[0])
        closes = tuple(self._closes)
        volatility = sum(
            (
                abs(current - previous)
                for previous, current in zip(closes, closes[1:], strict=False)
            ),
            ZERO,
        )
        return direction / volatility if volatility > 0 else ZERO

    def _rolling_vwap(self) -> Decimal | None:
        volume = sum((item[1] for item in self._vwap_samples), ZERO)
        if volume <= 0:
            return None
        notional = sum((price * quantity for price, quantity in self._vwap_samples), ZERO)
        return notional / volume

    def _profile_bin(self, price: Decimal) -> Decimal:
        units = (price / self.profile_bin_width).to_integral_value(rounding=ROUND_FLOOR)
        return units * self.profile_bin_width

    def _volume_profile(
        self,
    ) -> tuple[
        tuple[VolumeProfileLevel, ...], Decimal | None, Decimal | None, Decimal | None
    ]:
        bins: dict[Decimal, Decimal] = {}
        for price, volume in self._profile_samples:
            bins[price] = bins.get(price, ZERO) + volume
        if not bins:
            return (), None, None, None
        profile = tuple(
            VolumeProfileLevel(price=price, volume=volume)
            for price, volume in sorted(bins.items())
        )
        point_of_control = max(bins, key=lambda price: (bins[price], -price))
        total_volume = sum(bins.values(), ZERO)
        if total_volume <= 0:
            return profile, point_of_control, None, None
        selected: list[Decimal] = []
        accumulated = ZERO
        target = total_volume * Decimal("0.70")
        for price, volume in sorted(
            bins.items(), key=lambda item: (-item[1], item[0])
        ):
            selected.append(price)
            accumulated += volume
            if accumulated >= target:
                break
        return profile, point_of_control, min(selected), max(selected)

    def _is_warm(self) -> bool:
        return (
            self._sample_count >= self.ema_slow_period
            and self._adx is not None
            and self._efficiency_ratio() is not None
            and self._rolling_vwap() is not None
        )
