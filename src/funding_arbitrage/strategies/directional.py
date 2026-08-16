"""Regime-gated order-flow breakout and liquidity-sweep reversion strategies."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import DataQuality, InstrumentKey, Side, TradingMode
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
    StructureEventType,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import RegimeSnapshot

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class DirectionalStrategyContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    mode: TradingMode
    technical: TechnicalFeatureSnapshot
    orderflow: OrderFlowFeatureSnapshot
    structure: MarketStructureSnapshot
    regime: RegimeSnapshot
    estimated_cost_bps: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_instruments(self) -> DirectionalStrategyContext:
        snapshots = (self.technical, self.orderflow, self.structure, self.regime)
        if any(snapshot.instrument != self.instrument for snapshot in snapshots):
            raise ValueError("directional strategy feature instrument mismatch")
        return self


class DirectionalStrategyEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    score: Decimal = Field(ge=0, le=1)

    @model_validator(mode="after")
    def require_exact_outcome(self) -> DirectionalStrategyEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("strategy evaluation requires exactly one outcome")
        return self


class OrderFlowBreakoutConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "orderflow-breakout-v1"
    minimum_ofi_zscore: Decimal = Field(default=Decimal("1.5"), gt=0)
    minimum_book_imbalance: Decimal = Field(default=Decimal("0.10"), ge=0, le=1)
    minimum_trade_imbalance: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    minimum_score: Decimal = Field(default=Decimal("0.60"), ge=0, le=1)
    atr_stop_multiplier: Decimal = Field(default=Decimal("1.5"), gt=0)
    reward_to_risk: Decimal = Field(default=Decimal("2.5"), gt=0)
    ttl_seconds: int = Field(default=15, gt=0)
    time_stop_seconds: int = Field(default=1800, gt=0)


class LiquiditySweepReversionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "liquidity-sweep-reversion-v1"
    minimum_ofi_zscore: Decimal = Field(default=Decimal("1"), gt=0)
    minimum_book_imbalance: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    minimum_score: Decimal = Field(default=Decimal("0.55"), ge=0, le=1)
    atr_stop_multiplier: Decimal = Field(default=Decimal("0.75"), gt=0)
    reward_to_risk: Decimal = Field(default=Decimal("2"), gt=0)
    ttl_seconds: int = Field(default=15, gt=0)
    time_stop_seconds: int = Field(default=900, gt=0)


class OrderFlowBreakoutStrategy:
    def __init__(self, config: OrderFlowBreakoutConfig | None = None) -> None:
        self.config = config or OrderFlowBreakoutConfig()

    def evaluate(self, context: DirectionalStrategyContext) -> DirectionalStrategyEvaluation:
        rejection = _quality_rejection(context)
        if rejection is not None:
            return self._reject(rejection)
        regime = context.regime.regime
        if regime not in {MarketRegime.TREND_UP, MarketRegime.TREND_DOWN}:
            return self._reject("regime_not_trending")
        side = Side.BUY if regime is MarketRegime.TREND_UP else Side.SELL
        values = (
            context.technical.atr,
            context.technical.adx,
            context.orderflow.ofi_zscore_5s,
            context.orderflow.book_imbalance_l5,
            context.orderflow.trade_imbalance_5s,
        )
        if any(value is None for value in values):
            return self._reject("required_feature_missing")
        assert context.technical.atr is not None
        assert context.technical.adx is not None
        assert context.orderflow.ofi_zscore_5s is not None
        assert context.orderflow.book_imbalance_l5 is not None
        assert context.orderflow.trade_imbalance_5s is not None
        swing = (
            context.structure.last_swing_high
            if side is Side.BUY
            else context.structure.last_swing_low
        )
        protective_swing = (
            context.structure.last_swing_low
            if side is Side.BUY
            else context.structure.last_swing_high
        )
        if swing is None or protective_swing is None:
            return self._reject("confirmed_swings_required")
        price = context.technical.close
        breakout = price > swing.price if side is Side.BUY else price < swing.price
        if not breakout:
            return self._reject("structure_not_broken")
        direction = Decimal("1") if side is Side.BUY else Decimal("-1")
        signed_ofi = context.orderflow.ofi_zscore_5s * direction
        signed_book = context.orderflow.book_imbalance_l5 * direction
        signed_trade = context.orderflow.trade_imbalance_5s * direction
        if signed_ofi < self.config.minimum_ofi_zscore:
            return self._reject("ofi_not_confirmed")
        if signed_book < self.config.minimum_book_imbalance:
            return self._reject("book_imbalance_not_confirmed")
        if signed_trade < self.config.minimum_trade_imbalance:
            return self._reject("trade_flow_not_confirmed")
        score = _mean(
            (
                _clamp(signed_ofi / (self.config.minimum_ofi_zscore * 2)),
                _clamp(signed_book / (self.config.minimum_book_imbalance * 2)),
                _clamp(signed_trade / (self.config.minimum_trade_imbalance * 2)),
                _clamp(context.technical.adx / Decimal("50")),
                context.regime.confidence,
            )
        )
        if score < self.config.minimum_score:
            return self._reject("score_below_threshold", score)
        try:
            stop, target, expected_move_bps = _stop_and_target(
                side=side,
                entry=price,
                structural_price=protective_swing.price,
                atr=context.technical.atr,
                atr_multiplier=self.config.atr_stop_multiplier,
                reward_to_risk=self.config.reward_to_risk,
            )
        except ValueError:
            return self._reject("invalid_stop_or_target")
        intent = _intent(
            strategy_id=self.config.strategy_id,
            signal_type=SignalType.ORDERFLOW_BREAKOUT,
            side=side,
            context=context,
            score=score,
            entry=price,
            stop=stop,
            target=target,
            expected_move_bps=expected_move_bps,
            reward_to_risk=self.config.reward_to_risk,
            ttl_seconds=self.config.ttl_seconds,
            time_stop_seconds=self.config.time_stop_seconds,
            evidence={
                "adx": str(context.technical.adx),
                "ofi_zscore_5s": str(context.orderflow.ofi_zscore_5s),
                "book_imbalance_l5": str(context.orderflow.book_imbalance_l5),
                "trade_imbalance_5s": str(context.orderflow.trade_imbalance_5s),
                "breakout_swing": str(swing.price),
                "time_stop_seconds": self.config.time_stop_seconds,
            },
        )
        return DirectionalStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            intent=intent,
            score=score,
        )

    def _reject(
        self, reason: str, score: Decimal = ZERO
    ) -> DirectionalStrategyEvaluation:
        return DirectionalStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            rejection_reason=reason,
            score=score,
        )


class LiquiditySweepReversionStrategy:
    def __init__(self, config: LiquiditySweepReversionConfig | None = None) -> None:
        self.config = config or LiquiditySweepReversionConfig()

    def evaluate(self, context: DirectionalStrategyContext) -> DirectionalStrategyEvaluation:
        rejection = _quality_rejection(context)
        if rejection is not None:
            return self._reject(rejection)
        if context.regime.regime not in {MarketRegime.RANGE, MarketRegime.TRANSITION}:
            return self._reject("regime_not_mean_reverting")
        sweep = next(
            (
                event
                for event in reversed(context.structure.events)
                if event.event_type is StructureEventType.LIQUIDITY_SWEPT
            ),
            None,
        )
        if sweep is None:
            return self._reject("liquidity_sweep_required")
        side = (
            Side.SELL
            if sweep.direction is StructureDirection.BEARISH
            else Side.BUY
        )
        if context.technical.atr is None:
            return self._reject("atr_required")
        if (
            context.orderflow.ofi_zscore_5s is None
            or context.orderflow.book_imbalance_l5 is None
        ):
            return self._reject("required_feature_missing")
        price = context.technical.close
        failed_breakout = price < sweep.price if side is Side.SELL else price > sweep.price
        if not failed_breakout:
            return self._reject("sweep_not_rejected")
        direction = Decimal("1") if side is Side.BUY else Decimal("-1")
        signed_ofi = context.orderflow.ofi_zscore_5s * direction
        signed_book = context.orderflow.book_imbalance_l5 * direction
        if signed_ofi < self.config.minimum_ofi_zscore:
            return self._reject("reversal_ofi_not_confirmed")
        if signed_book < self.config.minimum_book_imbalance:
            return self._reject("reversal_book_not_confirmed")
        score = _mean(
            (
                _clamp(signed_ofi / (self.config.minimum_ofi_zscore * 2)),
                _clamp(signed_book / (self.config.minimum_book_imbalance * 2)),
                context.regime.confidence,
            )
        )
        if score < self.config.minimum_score:
            return self._reject("score_below_threshold", score)
        try:
            stop, target, expected_move_bps = _stop_and_target(
                side=side,
                entry=price,
                structural_price=sweep.price,
                atr=context.technical.atr,
                atr_multiplier=self.config.atr_stop_multiplier,
                reward_to_risk=self.config.reward_to_risk,
            )
        except ValueError:
            return self._reject("invalid_stop_or_target")
        intent = _intent(
            strategy_id=self.config.strategy_id,
            signal_type=SignalType.LIQUIDITY_SWEEP_REVERSION,
            side=side,
            context=context,
            score=score,
            entry=price,
            stop=stop,
            target=target,
            expected_move_bps=expected_move_bps,
            reward_to_risk=self.config.reward_to_risk,
            ttl_seconds=self.config.ttl_seconds,
            time_stop_seconds=self.config.time_stop_seconds,
            evidence={
                "sweep_price": str(sweep.price),
                "sweep_direction": sweep.direction,
                "ofi_zscore_5s": str(context.orderflow.ofi_zscore_5s),
                "book_imbalance_l5": str(context.orderflow.book_imbalance_l5),
                "rolling_vwap": (
                    str(context.technical.rolling_vwap)
                    if context.technical.rolling_vwap is not None
                    else None
                ),
                "time_stop_seconds": self.config.time_stop_seconds,
            },
        )
        return DirectionalStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            intent=intent,
            score=score,
        )

    def _reject(
        self, reason: str, score: Decimal = ZERO
    ) -> DirectionalStrategyEvaluation:
        return DirectionalStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            rejection_reason=reason,
            score=score,
        )


def _quality_rejection(context: DirectionalStrategyContext) -> str | None:
    qualities = (
        context.technical.data_quality,
        context.orderflow.data_quality,
        context.structure.data_quality,
        context.regime.data_quality,
    )
    return "feature_quality_not_valid" if any(
        quality is not DataQuality.VALID for quality in qualities
    ) else None


def _stop_and_target(
    *,
    side: Side,
    entry: Decimal,
    structural_price: Decimal,
    atr: Decimal,
    atr_multiplier: Decimal,
    reward_to_risk: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    structural_distance = (
        entry - structural_price if side is Side.BUY else structural_price - entry
    )
    stop_distance = max(structural_distance, atr * atr_multiplier)
    if stop_distance <= 0:
        raise ValueError("directional stop distance must be positive")
    stop = entry - stop_distance if side is Side.BUY else entry + stop_distance
    if stop <= 0:
        raise ValueError("directional stop price must be positive")
    target_distance = stop_distance * reward_to_risk
    target = entry + target_distance if side is Side.BUY else entry - target_distance
    if target <= 0:
        raise ValueError("directional target price must be positive")
    return stop, target, target_distance / entry * BPS


def _intent(
    *,
    strategy_id: str,
    signal_type: SignalType,
    side: Side,
    context: DirectionalStrategyContext,
    score: Decimal,
    entry: Decimal,
    stop: Decimal,
    target: Decimal,
    expected_move_bps: Decimal,
    reward_to_risk: Decimal,
    ttl_seconds: int,
    time_stop_seconds: int,
    evidence: dict[str, object],
) -> SignalIntent:
    spread_bps = context.orderflow.spread_bps or ZERO
    half_spread = entry * spread_bps / BPS / Decimal("2")
    created_at = max(
        context.technical.timestamp,
        context.orderflow.timestamp,
        context.structure.timestamp,
        context.regime.timestamp,
    )
    signal_id = _signal_id(
        strategy_id,
        context.instrument.canonical_id,
        created_at.isoformat(),
        side,
        str(entry),
    )
    return SignalIntent(
        signal_id=signal_id,
        strategy_id=strategy_id,
        mode=context.mode,
        signal_type=signal_type,
        primary_instrument=context.instrument,
        side=side,
        legs=(SignalLeg(instrument=context.instrument, side=side),),
        regime=context.regime.regime,
        quality_score=score * Decimal("100"),
        confidence=min(score, context.regime.confidence),
        entry_zone_low=max(Decimal("0.00000001"), entry - half_spread),
        entry_zone_high=entry + half_spread,
        structural_stop=stop,
        targets=(target,),
        expected_holding_seconds=time_stop_seconds,
        expected_move_bps=expected_move_bps,
        estimated_cost_bps=context.estimated_cost_bps,
        expected_rr=reward_to_risk,
        created_at=created_at,
        expires_at=created_at + timedelta(seconds=ttl_seconds),
        evidence=evidence,
    )


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, ZERO) / Decimal(len(values))


def _clamp(value: Decimal) -> Decimal:
    return max(ZERO, min(ONE, value))
