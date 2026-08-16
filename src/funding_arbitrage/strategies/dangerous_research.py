"""Fail-closed research implementations of grid, Martingale, and loss averaging."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

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
LIVE_MODES = frozenset({TradingMode.LIMITED_LIVE, TradingMode.LIVE})


class DangerousResearchContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    price: Decimal = Field(gt=0)
    market_timestamp: datetime
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    data_quality: DataQuality
    margin_available: bool
    portfolio_drawdown_fraction: Decimal = Field(ge=0, le=1)
    estimated_cost_bps: Decimal = Field(ge=0)
    operator_authorized: bool = False
    reference_side: Side | None = None
    latest_closed_trade_pnl_bps: Decimal | None = None
    consecutive_losses: int = Field(default=0, ge=0)
    anchor_price: Decimal | None = Field(default=None, gt=0)
    current_signed_quantity: Decimal = ZERO
    average_entry_price: Decimal | None = Field(default=None, gt=0)
    prior_additions: int = Field(default=0, ge=0)

    @field_validator("market_timestamp", "timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class DangerousStrategyEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    requested_size_multiplier: Decimal = Field(default=ZERO, ge=0)

    @model_validator(mode="after")
    def require_exact_outcome(self) -> DangerousStrategyEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("dangerous strategy evaluation requires exactly one outcome")
        return self


class DangerousControls(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    live_enabled: bool = False
    maximum_age_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    maximum_portfolio_drawdown_fraction: Decimal = Field(
        default=Decimal("0.05"), gt=0, le=1
    )
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("3"), gt=0)
    ttl_seconds: int = Field(default=5, gt=0)


class MartingaleConfig(DangerousControls):
    strategy_id: str = "martingale-research-v1"
    loss_trigger_bps: Decimal = Field(default=Decimal("20"), gt=0)
    base_multiplier: Decimal = Field(default=Decimal("1.5"), gt=1)
    maximum_multiplier: Decimal = Field(default=Decimal("3"), gt=1)
    maximum_consecutive_losses: int = Field(default=2, gt=0)
    holding_seconds: int = Field(default=900, gt=0)


class GridConfig(DangerousControls):
    strategy_id: str = "grid-research-v1"
    levels_per_side: int = Field(default=3, gt=0, le=20)
    spacing_bps: Decimal = Field(default=Decimal("25"), gt=0)
    maximum_total_range_bps: Decimal = Field(default=Decimal("300"), gt=0)
    price_tick: Decimal = Field(default=Decimal("0.0001"), gt=0)
    holding_seconds: int = Field(default=3600, gt=0)

    @model_validator(mode="after")
    def validate_range(self) -> GridConfig:
        if self.spacing_bps * Decimal(self.levels_per_side) > self.maximum_total_range_bps:
            raise ValueError("configured grid exceeds maximum range")
        return self


class LossAveragingConfig(DangerousControls):
    strategy_id: str = "loss-averaging-research-v1"
    minimum_adverse_move_bps: Decimal = Field(default=Decimal("50"), gt=0)
    maximum_adverse_move_bps: Decimal = Field(default=Decimal("300"), gt=0)
    addition_multiplier: Decimal = Field(default=Decimal("0.5"), gt=0)
    maximum_total_multiplier: Decimal = Field(default=Decimal("2"), gt=0)
    maximum_additions: int = Field(default=2, gt=0)
    holding_seconds: int = Field(default=900, gt=0)

    @model_validator(mode="after")
    def validate_adverse_move(self) -> LossAveragingConfig:
        if self.maximum_adverse_move_bps <= self.minimum_adverse_move_bps:
            raise ValueError("maximum adverse move must exceed minimum")
        return self


class MartingaleResearchStrategy:
    def __init__(self, config: MartingaleConfig | None = None) -> None:
        self.config = config or MartingaleConfig()

    def evaluate(self, context: DangerousResearchContext) -> DangerousStrategyEvaluation:
        rejection = _common_rejection(context, self.config, self.config.strategy_id)
        if rejection is not None:
            return self._reject(rejection)
        if context.reference_side is None or context.latest_closed_trade_pnl_bps is None:
            return self._reject("martingale_loss_context_missing")
        if context.latest_closed_trade_pnl_bps > -self.config.loss_trigger_bps:
            return self._reject("martingale_loss_trigger_not_met")
        if context.consecutive_losses <= 0:
            return self._reject("martingale_loss_streak_required")
        if context.consecutive_losses > self.config.maximum_consecutive_losses:
            return self._reject("martingale_loss_streak_limit")
        multiplier = min(
            self.config.maximum_multiplier,
            self.config.base_multiplier ** context.consecutive_losses,
        )
        expected_move = abs(context.latest_closed_trade_pnl_bps)
        if expected_move < context.estimated_cost_bps * self.config.minimum_edge_to_cost_ratio:
            return self._reject("martingale_edge_below_cost")
        intent = _intent(
            strategy_id=self.config.strategy_id,
            signal_type=SignalType.MARTINGALE,
            side=context.reference_side,
            legs=(
                SignalLeg(
                    instrument=context.instrument,
                    side=context.reference_side,
                    hedge_ratio=multiplier,
                ),
            ),
            context=context,
            expected_move_bps=expected_move,
            holding_seconds=self.config.holding_seconds,
            ttl_seconds=self.config.ttl_seconds,
            evidence={
                "dangerous_research": True,
                "consecutive_losses": context.consecutive_losses,
                "latest_closed_trade_pnl_bps": str(
                    context.latest_closed_trade_pnl_bps
                ),
                "requested_size_multiplier": str(multiplier),
                "risk_size_authority": "external",
            },
        )
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            intent=intent,
            requested_size_multiplier=multiplier,
        )

    def _reject(self, reason: str) -> DangerousStrategyEvaluation:
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            rejection_reason=reason,
        )


class GridResearchStrategy:
    def __init__(self, config: GridConfig | None = None) -> None:
        self.config = config or GridConfig()

    def evaluate(self, context: DangerousResearchContext) -> DangerousStrategyEvaluation:
        rejection = _common_rejection(context, self.config, self.config.strategy_id)
        if rejection is not None:
            return self._reject(rejection)
        if context.regime is not MarketRegime.RANGE:
            return self._reject("grid_requires_range_regime")
        anchor = context.anchor_price or context.price
        buy_levels = tuple(
            _floor_tick(
                anchor * (ONE - self.config.spacing_bps * level / BPS),
                self.config.price_tick,
            )
            for level in map(Decimal, range(1, self.config.levels_per_side + 1))
        )
        sell_levels = tuple(
            _ceil_tick(
                anchor * (ONE + self.config.spacing_bps * level / BPS),
                self.config.price_tick,
            )
            for level in map(Decimal, range(1, self.config.levels_per_side + 1))
        )
        expected_move = self.config.spacing_bps * Decimal("2")
        if expected_move < context.estimated_cost_bps * self.config.minimum_edge_to_cost_ratio:
            return self._reject("grid_edge_below_cost")
        legs = tuple(
            SignalLeg(instrument=context.instrument, side=side)
            for side in (
                (Side.BUY,) * len(buy_levels) + (Side.SELL,) * len(sell_levels)
            )
        )
        primary_side = Side.SELL if context.price > anchor else Side.BUY
        intent = _intent(
            strategy_id=self.config.strategy_id,
            signal_type=SignalType.GRID,
            side=primary_side,
            legs=legs,
            context=context,
            expected_move_bps=expected_move,
            holding_seconds=self.config.holding_seconds,
            ttl_seconds=self.config.ttl_seconds,
            evidence={
                "dangerous_research": True,
                "anchor_price": str(anchor),
                "buy_levels": tuple(
                    _format_tick(level, self.config.price_tick) for level in buy_levels
                ),
                "sell_levels": tuple(
                    _format_tick(level, self.config.price_tick) for level in sell_levels
                ),
                "post_only_required": True,
                "risk_size_authority": "external",
            },
        )
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            intent=intent,
            requested_size_multiplier=ONE,
        )

    def _reject(self, reason: str) -> DangerousStrategyEvaluation:
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            rejection_reason=reason,
        )


class LossAveragingResearchStrategy:
    def __init__(self, config: LossAveragingConfig | None = None) -> None:
        self.config = config or LossAveragingConfig()

    def evaluate(self, context: DangerousResearchContext) -> DangerousStrategyEvaluation:
        rejection = _common_rejection(context, self.config, self.config.strategy_id)
        if rejection is not None:
            return self._reject(rejection)
        if context.current_signed_quantity == 0 or context.average_entry_price is None:
            return self._reject("loss_averaging_position_required")
        if context.prior_additions >= self.config.maximum_additions:
            return self._reject("loss_averaging_addition_limit")
        side = Side.BUY if context.current_signed_quantity > 0 else Side.SELL
        adverse_move = (
            (context.average_entry_price - context.price)
            / context.average_entry_price
            * BPS
            if side is Side.BUY
            else (context.price - context.average_entry_price)
            / context.average_entry_price
            * BPS
        )
        if adverse_move < self.config.minimum_adverse_move_bps:
            return self._reject("loss_averaging_trigger_not_met")
        if adverse_move > self.config.maximum_adverse_move_bps:
            return self._reject("loss_averaging_adverse_move_limit")
        multiplier = min(
            self.config.maximum_total_multiplier,
            self.config.addition_multiplier * Decimal(context.prior_additions + 1),
        )
        if adverse_move < context.estimated_cost_bps * self.config.minimum_edge_to_cost_ratio:
            return self._reject("loss_averaging_edge_below_cost")
        intent = _intent(
            strategy_id=self.config.strategy_id,
            signal_type=SignalType.LOSS_AVERAGING,
            side=side,
            legs=(
                SignalLeg(
                    instrument=context.instrument,
                    side=side,
                    hedge_ratio=multiplier,
                ),
            ),
            context=context,
            expected_move_bps=adverse_move,
            holding_seconds=self.config.holding_seconds,
            ttl_seconds=self.config.ttl_seconds,
            evidence={
                "dangerous_research": True,
                "average_entry_price": str(context.average_entry_price),
                "adverse_move_bps": str(adverse_move),
                "prior_additions": context.prior_additions,
                "requested_size_multiplier": str(multiplier),
                "risk_size_authority": "external",
            },
        )
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            intent=intent,
            requested_size_multiplier=multiplier,
        )

    def _reject(self, reason: str) -> DangerousStrategyEvaluation:
        return DangerousStrategyEvaluation(
            strategy_id=self.config.strategy_id,
            rejection_reason=reason,
        )


def _common_rejection(
    context: DangerousResearchContext,
    controls: DangerousControls,
    strategy_id: str,
) -> str | None:
    if not controls.enabled:
        return f"{strategy_id}_disabled"
    if context.mode in LIVE_MODES and not (
        controls.live_enabled and context.operator_authorized
    ):
        return "dangerous_live_not_authorized"
    if context.data_quality is not DataQuality.VALID:
        return "dangerous_strategy_data_quality_not_valid"
    age = Decimal(str((context.timestamp - context.market_timestamp).total_seconds()))
    if age < 0:
        return "dangerous_strategy_timestamp_in_future"
    if age > controls.maximum_age_seconds:
        return "dangerous_strategy_data_stale"
    if context.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
        return "dangerous_strategy_unsafe_regime"
    if not context.margin_available:
        return "dangerous_strategy_margin_unavailable"
    if (
        context.portfolio_drawdown_fraction
        > controls.maximum_portfolio_drawdown_fraction
    ):
        return "dangerous_strategy_drawdown_limit"
    return None


def _intent(
    *,
    strategy_id: str,
    signal_type: SignalType,
    side: Side,
    legs: tuple[SignalLeg, ...],
    context: DangerousResearchContext,
    expected_move_bps: Decimal,
    holding_seconds: int,
    ttl_seconds: int,
    evidence: dict[str, object],
) -> SignalIntent:
    signal_id = _signal_id(
        strategy_id,
        context.instrument.canonical_id,
        context.timestamp.isoformat(),
        side,
        str(context.price),
    )
    return SignalIntent(
        signal_id=signal_id,
        strategy_id=strategy_id,
        mode=context.mode,
        signal_type=signal_type,
        primary_instrument=context.instrument,
        side=side,
        legs=legs,
        regime=context.regime,
        quality_score=Decimal("50"),
        confidence=Decimal("0.5"),
        expected_holding_seconds=holding_seconds,
        expected_move_bps=expected_move_bps,
        estimated_cost_bps=context.estimated_cost_bps,
        created_at=context.timestamp,
        expires_at=context.timestamp + timedelta(seconds=ttl_seconds),
        evidence=evidence,
    )


def _floor_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _ceil_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def _format_tick(price: Decimal, tick: Decimal) -> str:
    exponent = tick.as_tuple().exponent
    decimal_places = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    return f"{price:.{decimal_places}f}"


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
