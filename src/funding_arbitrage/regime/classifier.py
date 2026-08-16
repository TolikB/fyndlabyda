"""Deterministic multi-regime classification with hysteresis and dwell control."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.events import DataQuality, InstrumentKey
from funding_arbitrage.features.derivatives import DerivativesFeatureSnapshot
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class MarketRegime(StrEnum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    TRANSITION = "TRANSITION"
    VOLATILITY_EXPANSION = "VOLATILITY_EXPANSION"
    STRESS = "STRESS"
    UNKNOWN = "UNKNOWN"


class RegimeThresholds(BaseModel):
    model_config = ConfigDict(frozen=True)

    trend_adx_min: Decimal = Field(default=Decimal("25"), ge=0, le=100)
    trend_efficiency_min: Decimal = Field(default=Decimal("0.35"), ge=0, le=1)
    trend_ema_spread_bps_min: Decimal = Field(default=Decimal("5"), ge=0)
    range_adx_max: Decimal = Field(default=Decimal("20"), ge=0, le=100)
    range_efficiency_max: Decimal = Field(default=Decimal("0.30"), ge=0, le=1)
    volatility_atr_percent_min: Decimal = Field(default=Decimal("2"), gt=0)
    stress_spread_bps: Decimal = Field(default=Decimal("30"), gt=0)
    stress_ofi_zscore: Decimal = Field(default=Decimal("4"), gt=0)
    transition_confidence_min: Decimal = Field(
        default=Decimal("0.55"), ge=0, le=1
    )
    minimum_dwell_seconds: int = Field(default=300, ge=0)
    candidate_confirmations: int = Field(default=2, gt=0)


class RegimeObservation(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    adx: Decimal | None = Field(default=None, ge=0, le=100)
    efficiency_ratio: Decimal | None = Field(default=None, ge=0, le=1)
    ema_spread_bps: Decimal | None = None
    atr_percent: Decimal | None = Field(default=None, ge=0)
    spread_bps: Decimal | None = Field(default=None, ge=0)
    ofi_zscore: Decimal | None = None
    funding_outlier: bool = False
    open_interest_change_percent: Decimal | None = None
    structure_bias: int = Field(default=0, ge=-1, le=1)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @classmethod
    def from_features(
        cls,
        technical: TechnicalFeatureSnapshot,
        orderflow: OrderFlowFeatureSnapshot,
        *,
        structure: MarketStructureSnapshot | None = None,
        derivatives: DerivativesFeatureSnapshot | None = None,
    ) -> RegimeObservation:
        instrument = technical.instrument
        supplied = [orderflow, structure, derivatives]
        if any(item is not None and item.instrument != instrument for item in supplied):
            raise ValueError("regime feature instrument mismatch")
        qualities = [technical.data_quality, orderflow.data_quality]
        timestamps = [technical.timestamp, orderflow.timestamp]
        if structure is not None:
            qualities.append(structure.data_quality)
            timestamps.append(structure.timestamp)
        if derivatives is not None:
            qualities.append(derivatives.data_quality)
            timestamps.append(derivatives.timestamp)
        quality = _worst_quality(qualities)
        if (
            technical.adx is None
            or technical.efficiency_ratio is None
            or technical.atr is None
        ):
            quality = _worst_quality([quality, DataQuality.RECOVERING])
        structure_bias = 0
        if structure is not None:
            if structure.trend is StructureDirection.BULLISH:
                structure_bias = 1
            elif structure.trend is StructureDirection.BEARISH:
                structure_bias = -1
        return cls(
            instrument=instrument,
            timestamp=max(timestamps),
            data_quality=quality,
            adx=technical.adx,
            efficiency_ratio=technical.efficiency_ratio,
            ema_spread_bps=(
                (technical.ema_fast - technical.ema_slow) / technical.close * BPS
            ),
            atr_percent=(
                technical.atr / technical.close * Decimal("100")
                if technical.atr is not None
                else None
            ),
            spread_bps=orderflow.spread_bps,
            ofi_zscore=orderflow.ofi_zscore_5s,
            funding_outlier=(derivatives.funding_outlier if derivatives else False),
            open_interest_change_percent=(
                derivatives.open_interest_change_percent if derivatives else None
            ),
            structure_bias=structure_bias,
        )


class RegimeTransition(BaseModel):
    model_config = ConfigDict(frozen=True)

    previous: MarketRegime
    current: MarketRegime
    timestamp: datetime
    reason: str


class RegimeSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    regime: MarketRegime
    candidate: MarketRegime
    confidence: Decimal = Field(ge=0, le=1)
    regime_since: datetime
    dwell_seconds: Decimal = Field(ge=0)
    pending_confirmations: int = Field(ge=0)
    data_quality: DataQuality
    transition: RegimeTransition | None = None


class RegimeClassifier:
    """Classify observations while suppressing non-safety regime flapping."""

    def __init__(
        self,
        instrument: InstrumentKey,
        thresholds: RegimeThresholds | None = None,
    ) -> None:
        self.instrument = instrument
        self.thresholds = thresholds or RegimeThresholds()
        self._regime = MarketRegime.UNKNOWN
        self._regime_since: datetime | None = None
        self._last_timestamp: datetime | None = None
        self._pending_candidate: MarketRegime | None = None
        self._pending_count = 0
        self._confidence = ONE

    def update(self, observation: RegimeObservation) -> RegimeSnapshot:
        if observation.instrument != self.instrument:
            raise ValueError("regime observation instrument mismatch")
        if self._last_timestamp is not None and observation.timestamp <= self._last_timestamp:
            raise ValueError("out-of-order or duplicate regime observation")
        self._last_timestamp = observation.timestamp
        candidate, candidate_confidence, reason = self._classify(observation)
        transition: RegimeTransition | None = None
        if self._regime_since is None:
            transition = self._switch(candidate, observation.timestamp, "initial_classification")
            self._confidence = candidate_confidence
        elif candidate is self._regime:
            self._pending_candidate = None
            self._pending_count = 0
            self._confidence = candidate_confidence
        elif candidate in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            transition = self._switch(candidate, observation.timestamp, reason)
            self._confidence = candidate_confidence
        else:
            if candidate is self._pending_candidate:
                self._pending_count += 1
            else:
                self._pending_candidate = candidate
                self._pending_count = 1
            dwell = observation.timestamp - self._regime_since
            if (
                dwell >= timedelta(seconds=self.thresholds.minimum_dwell_seconds)
                and self._pending_count >= self.thresholds.candidate_confirmations
                and candidate_confidence >= self.thresholds.transition_confidence_min
            ):
                transition = self._switch(candidate, observation.timestamp, reason)
                self._confidence = candidate_confidence
            else:
                self._confidence = max(ZERO, ONE - candidate_confidence)
        assert self._regime_since is not None
        return RegimeSnapshot(
            instrument=self.instrument,
            timestamp=observation.timestamp,
            regime=self._regime,
            candidate=candidate,
            confidence=self._confidence,
            regime_since=self._regime_since,
            dwell_seconds=Decimal(
                str((observation.timestamp - self._regime_since).total_seconds())
            ),
            pending_confirmations=self._pending_count,
            data_quality=observation.data_quality,
            transition=transition,
        )

    def _classify(
        self, observation: RegimeObservation
    ) -> tuple[MarketRegime, Decimal, str]:
        thresholds = self.thresholds
        if observation.data_quality is not DataQuality.VALID:
            return MarketRegime.UNKNOWN, ONE, f"data_quality:{observation.data_quality}"
        required = (
            observation.adx,
            observation.efficiency_ratio,
            observation.ema_spread_bps,
            observation.atr_percent,
            observation.spread_bps,
        )
        if any(value is None for value in required):
            return MarketRegime.UNKNOWN, ONE, "required_feature_missing"
        assert observation.adx is not None
        assert observation.efficiency_ratio is not None
        assert observation.ema_spread_bps is not None
        assert observation.atr_percent is not None
        assert observation.spread_bps is not None
        ofi_zscore = abs(observation.ofi_zscore or ZERO)
        if (
            observation.funding_outlier
            or observation.spread_bps >= thresholds.stress_spread_bps
            or (
                ofi_zscore >= thresholds.stress_ofi_zscore
                and observation.atr_percent
                >= thresholds.volatility_atr_percent_min
            )
        ):
            confidence = max(
                ONE if observation.funding_outlier else ZERO,
                observation.spread_bps / thresholds.stress_spread_bps,
                ofi_zscore / thresholds.stress_ofi_zscore,
            )
            return MarketRegime.STRESS, _clamp(confidence), "stress_threshold"
        if observation.atr_percent >= thresholds.volatility_atr_percent_min:
            confidence = observation.atr_percent / thresholds.volatility_atr_percent_min
            return (
                MarketRegime.VOLATILITY_EXPANSION,
                _clamp(confidence),
                "atr_expansion",
            )
        trend_ready = (
            observation.adx >= thresholds.trend_adx_min
            and observation.efficiency_ratio >= thresholds.trend_efficiency_min
            and abs(observation.ema_spread_bps)
            >= thresholds.trend_ema_spread_bps_min
        )
        if trend_ready:
            direction = (
                MarketRegime.TREND_UP
                if observation.ema_spread_bps > 0
                else MarketRegime.TREND_DOWN
            )
            confidence = (
                observation.adx / thresholds.trend_adx_min
                + observation.efficiency_ratio / thresholds.trend_efficiency_min
                + abs(observation.ema_spread_bps)
                / thresholds.trend_ema_spread_bps_min
            ) / Decimal("3")
            if observation.structure_bias != 0:
                aligned = (
                    observation.structure_bias > 0
                    and direction is MarketRegime.TREND_UP
                ) or (
                    observation.structure_bias < 0
                    and direction is MarketRegime.TREND_DOWN
                )
                confidence *= Decimal("1.10") if aligned else Decimal("0.75")
            return direction, _clamp(confidence), "trend_thresholds"
        if (
            observation.adx <= thresholds.range_adx_max
            and observation.efficiency_ratio <= thresholds.range_efficiency_max
        ):
            adx_confidence = ONE - observation.adx / (thresholds.range_adx_max * 2)
            efficiency_confidence = ONE - observation.efficiency_ratio / (
                thresholds.range_efficiency_max * 2
            )
            return (
                MarketRegime.RANGE,
                _clamp((adx_confidence + efficiency_confidence) / Decimal("2")),
                "range_thresholds",
            )
        return MarketRegime.TRANSITION, Decimal("0.60"), "mixed_features"

    def _switch(
        self, regime: MarketRegime, timestamp: datetime, reason: str
    ) -> RegimeTransition | None:
        previous = self._regime
        self._regime = regime
        self._regime_since = timestamp
        self._pending_candidate = None
        self._pending_count = 0
        if previous is regime:
            return None
        return RegimeTransition(
            previous=previous,
            current=regime,
            timestamp=timestamp,
            reason=reason,
        )


_QUALITY_RANK = {
    DataQuality.VALID: 0,
    DataQuality.RECOVERING: 1,
    DataQuality.STALE: 2,
    DataQuality.CROSSED: 3,
    DataQuality.GAP: 4,
    DataQuality.INVALID: 5,
    DataQuality.UNAVAILABLE: 6,
}


def _worst_quality(qualities: list[DataQuality]) -> DataQuality:
    return max(qualities, key=_QUALITY_RANK.__getitem__)


def _clamp(value: Decimal) -> Decimal:
    return max(ZERO, min(ONE, value))


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
