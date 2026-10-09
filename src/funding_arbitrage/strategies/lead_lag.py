"""Cross-venue lead-lag fair value for filtering and executable stat-arbitrage."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import DataQuality, InstrumentKey, Side, TradingMode

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class VenueFairValueInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    mid_price: Decimal = Field(gt=0)
    microprice: Decimal = Field(gt=0)
    liquidity_score: Decimal = Field(gt=0, le=1)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class LeadLagFairValue(BaseModel):
    model_config = ConfigDict(frozen=True)

    primary: InstrumentKey
    fair_price: Decimal = Field(gt=0)
    primary_microprice: Decimal = Field(gt=0)
    deviation_bps: Decimal
    reference_dispersion_bps: Decimal = Field(ge=0)
    reference_venues: tuple[str, ...]
    hedge_reference: InstrumentKey
    aggregate_reference_weight: Decimal = Field(gt=0)
    timestamp: datetime


class LeadLagAssessment(BaseModel):
    model_config = ConfigDict(frozen=True)

    usable: bool
    reason: str | None = None
    trade_bias: Side | None = None
    confidence: Decimal = Field(ge=0, le=1)
    fair_value: LeadLagFairValue | None = None


class LeadLagCostModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    fees_bps: Decimal = Field(ge=0)
    spread_bps: Decimal = Field(ge=0)
    slippage_bps: Decimal = Field(ge=0)
    adverse_selection_bps: Decimal = Field(ge=0)
    funding_bps: Decimal = Field(default=ZERO, ge=0)
    borrow_bps: Decimal = Field(default=ZERO, ge=0)
    transfer_bps: Decimal = Field(default=ZERO, ge=0)
    legging_risk_bps: Decimal = Field(default=ZERO, ge=0)

    @property
    def total_bps(self) -> Decimal:
        return sum(
            (
                self.fees_bps,
                self.spread_bps,
                self.slippage_bps,
                self.adverse_selection_bps,
                self.funding_bps,
                self.borrow_bps,
                self.transfer_bps,
                self.legging_risk_bps,
            ),
            ZERO,
        )


class LeadLagConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "cross-exchange-lead-lag-v1"
    maximum_age_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    maximum_reference_dispersion_bps: Decimal = Field(default=Decimal("20"), gt=0)
    minimum_deviation_bps: Decimal = Field(default=Decimal("8"), gt=0)
    minimum_confidence: Decimal = Field(default=Decimal("0.50"), ge=0, le=1)
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    ttl_seconds: int = Field(default=2, gt=0)
    expected_holding_seconds: int = Field(default=30, gt=0)


class CrossExchangeLeadLagEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    assessment: LeadLagAssessment

    @model_validator(mode="after")
    def require_exact_outcome(self) -> CrossExchangeLeadLagEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("lead-lag evaluation requires exactly one outcome")
        return self


class LeadLagFairValueEngine:
    def __init__(self, config: LeadLagConfig | None = None) -> None:
        self.config = config or LeadLagConfig()

    def assess(
        self,
        primary: VenueFairValueInput,
        references: tuple[VenueFairValueInput, ...],
        timestamp: datetime,
    ) -> LeadLagAssessment:
        now = _utc(timestamp)
        if primary.data_quality is not DataQuality.VALID:
            return LeadLagAssessment(
                usable=False, reason="primary_quality_not_valid", confidence=ZERO
            )
        if self._age_seconds(primary, now) > self.config.maximum_age_seconds:
            return LeadLagAssessment(
                usable=False, reason="primary_stale", confidence=ZERO
            )
        valid: list[tuple[VenueFairValueInput, Decimal]] = []
        for reference in references:
            if (
                reference.instrument.venue == primary.instrument.venue
                or reference.instrument.base_asset != primary.instrument.base_asset
                or reference.instrument.quote_asset != primary.instrument.quote_asset
                or reference.data_quality is not DataQuality.VALID
            ):
                continue
            age = self._age_seconds(reference, now)
            if age > self.config.maximum_age_seconds:
                continue
            freshness = ONE - age / self.config.maximum_age_seconds
            weight = reference.liquidity_score * max(ZERO, freshness)
            if weight > 0:
                valid.append((reference, weight))
        venues = {item.instrument.venue for item, _ in valid}
        if len(venues) < 2:
            return LeadLagAssessment(
                usable=False,
                reason="two_independent_references_required",
                confidence=ZERO,
            )
        points = [
            (price, weight / Decimal("2"))
            for item, weight in valid
            for price in (item.mid_price, item.microprice)
        ]
        fair_price = _weighted_median(points)
        reference_prices = [item.microprice for item, _ in valid]
        dispersion = (max(reference_prices) - min(reference_prices)) / fair_price * BPS
        if dispersion > self.config.maximum_reference_dispersion_bps:
            return LeadLagAssessment(
                usable=False,
                reason="reference_dispersion_too_high",
                confidence=ZERO,
            )
        deviation = (primary.microprice - fair_price) / fair_price * BPS
        bias = Side.SELL if deviation > 0 else Side.BUY
        aggregate_weight = sum((weight for _, weight in valid), ZERO)
        hedge_reference = max(
            valid,
            key=lambda item: (item[1], item[0].instrument.canonical_id),
        )[0].instrument
        confidence = _clamp(
            abs(deviation) / self.config.minimum_deviation_bps
            * (ONE - dispersion / self.config.maximum_reference_dispersion_bps)
        )
        fair_value = LeadLagFairValue(
            primary=primary.instrument,
            fair_price=fair_price,
            primary_microprice=primary.microprice,
            deviation_bps=deviation,
            reference_dispersion_bps=dispersion,
            reference_venues=tuple(sorted(venues)),
            hedge_reference=hedge_reference,
            aggregate_reference_weight=aggregate_weight,
            timestamp=now,
        )
        return LeadLagAssessment(
            usable=True,
            trade_bias=bias,
            confidence=confidence,
            fair_value=fair_value,
        )

    @staticmethod
    def _age_seconds(item: VenueFairValueInput, now: datetime) -> Decimal:
        age = Decimal(str((now - item.timestamp).total_seconds()))
        return max(ZERO, age)


class CrossExchangeLeadLagStrategy:
    def __init__(self, config: LeadLagConfig | None = None) -> None:
        self.config = config or LeadLagConfig()
        self.fair_value_engine = LeadLagFairValueEngine(self.config)

    def evaluate(
        self,
        *,
        primary: VenueFairValueInput,
        references: tuple[VenueFairValueInput, ...],
        timestamp: datetime,
        mode: TradingMode,
        regime: MarketRegime,
        costs: LeadLagCostModel,
        inventory_available: bool,
        transfer_ready: bool,
    ) -> CrossExchangeLeadLagEvaluation:
        assessment = self.fair_value_engine.assess(primary, references, timestamp)
        if not assessment.usable or assessment.fair_value is None:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason=assessment.reason,
                assessment=assessment,
            )
        if regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="unsafe_regime", assessment=assessment
            )
        if not inventory_available:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="hedge_inventory_unavailable", assessment=assessment
            )
        if not transfer_ready:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="transfer_path_unavailable", assessment=assessment
            )
        edge = abs(assessment.fair_value.deviation_bps)
        if edge < self.config.minimum_deviation_bps:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="deviation_below_threshold", assessment=assessment
            )
        if assessment.confidence < self.config.minimum_confidence:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="fair_value_confidence_low", assessment=assessment
            )
        if edge < costs.total_bps * self.config.minimum_edge_to_cost_ratio:
            return CrossExchangeLeadLagEvaluation(
                rejection_reason="insufficient_edge_to_cost", assessment=assessment
            )
        assert assessment.trade_bias is not None
        primary_side = assessment.trade_bias
        hedge_side = Side.BUY if primary_side is Side.SELL else Side.SELL
        hedge_input = next(
            item
            for item in references
            if item.instrument == assessment.fair_value.hedge_reference
        )
        now = _utc(timestamp)
        signal_id = _signal_id(
            self.config.strategy_id,
            primary.instrument.canonical_id,
            hedge_input.instrument.canonical_id,
            now.isoformat(),
            primary_side,
        )
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=self.config.strategy_id,
            mode=mode,
            signal_type=SignalType.CROSS_EXCHANGE_STAT_ARB,
            primary_instrument=primary.instrument,
            side=primary_side,
            legs=(
                SignalLeg(
                    instrument=primary.instrument,
                    side=primary_side,
                    execution_priority=0,
                ),
                SignalLeg(
                    instrument=hedge_input.instrument,
                    side=hedge_side,
                    execution_priority=1,
                ),
            ),
            regime=regime,
            quality_score=assessment.confidence * Decimal("100"),
            confidence=assessment.confidence,
            expected_holding_seconds=self.config.expected_holding_seconds,
            expected_move_bps=edge,
            estimated_cost_bps=costs.total_bps,
            created_at=now,
            expires_at=now + timedelta(seconds=self.config.ttl_seconds),
            evidence={
                "fair_price": str(assessment.fair_value.fair_price),
                "deviation_bps": str(assessment.fair_value.deviation_bps),
                "reference_dispersion_bps": str(
                    assessment.fair_value.reference_dispersion_bps
                ),
                "reference_venues": assessment.fair_value.reference_venues,
                "all_in_cost_bps": str(costs.total_bps),
                "inventory_verified": True,
                "transfer_path_verified": True,
            },
        )
        return CrossExchangeLeadLagEvaluation(intent=intent, assessment=assessment)


def _weighted_median(points: list[tuple[Decimal, Decimal]]) -> Decimal:
    if not points or any(weight <= 0 for _, weight in points):
        raise ValueError("weighted median requires positive weights")
    ordered = sorted(points, key=lambda item: item[0])
    threshold = sum((weight for _, weight in ordered), ZERO) / Decimal("2")
    cumulative = ZERO
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= threshold:
            return value
    return ordered[-1][0]


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _clamp(value: Decimal) -> Decimal:
    return max(ZERO, min(ONE, value))


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
