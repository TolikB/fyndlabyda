"""Inventory-, adverse-selection-, and fee-aware passive market making."""

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
from funding_arbitrage.domain.events import (
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    Side,
    TradingMode,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class MarketMakingInventory(BaseModel):
    model_config = ConfigDict(frozen=True)

    signed_quantity: Decimal
    target_quantity: Decimal = ZERO
    maximum_abs_quantity: Decimal = Field(gt=0)

    @property
    def normalized_deviation(self) -> Decimal:
        return _clamp_signed(
            (self.signed_quantity - self.target_quantity) / self.maximum_abs_quantity
        )


class MarketMakingCosts(BaseModel):
    model_config = ConfigDict(frozen=True)

    maker_fee_bps_per_fill: Decimal
    expected_adverse_selection_bps: Decimal = Field(ge=0)
    expected_hedging_bps: Decimal = Field(default=ZERO, ge=0)
    cancellation_bps: Decimal = Field(default=ZERO, ge=0)
    operational_buffer_bps: Decimal = Field(default=ZERO, ge=0)

    @property
    def round_trip_bps(self) -> Decimal:
        return max(
            ZERO,
            self.maker_fee_bps_per_fill * Decimal("2")
            + self.expected_adverse_selection_bps
            + self.expected_hedging_bps
            + self.cancellation_bps
            + self.operational_buffer_bps,
        )


class PassiveQuoteProposal(BaseModel):
    model_config = ConfigDict(frozen=True)

    fair_price: Decimal = Field(gt=0)
    reservation_price: Decimal = Field(gt=0)
    bid_price: Decimal | None = Field(default=None, gt=0)
    ask_price: Decimal | None = Field(default=None, gt=0)
    bid_size_multiplier: Decimal = Field(ge=0, le=2)
    ask_size_multiplier: Decimal = Field(ge=0, le=2)
    quote_half_spread_bps: Decimal = Field(gt=0)
    gross_capture_bps: Decimal = Field(gt=0)
    estimated_cost_bps: Decimal = Field(ge=0)
    expected_net_edge_bps: Decimal
    adverse_selection_bps: Decimal = Field(ge=0)
    inventory_deviation: Decimal = Field(ge=-1, le=1)

    @model_validator(mode="after")
    def validate_sides(self) -> PassiveQuoteProposal:
        if self.bid_price is None and self.ask_price is None:
            raise ValueError("passive quote must retain at least one side")
        if (
            self.bid_price is not None
            and self.ask_price is not None
            and self.bid_price >= self.ask_price
        ):
            raise ValueError("passive bid must be below passive ask")
        return self


class MarketMakingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "passive-market-making-v1"
    maximum_age_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    minimum_top_depth_quote: Decimal = Field(default=Decimal("1000"), gt=0)
    minimum_half_spread_bps: Decimal = Field(default=Decimal("2"), gt=0)
    maximum_half_spread_bps: Decimal = Field(default=Decimal("50"), gt=0)
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    maximum_adverse_selection_bps: Decimal = Field(default=Decimal("15"), gt=0)
    inventory_price_skew_bps: Decimal = Field(default=Decimal("10"), ge=0)
    inventory_size_skew: Decimal = Field(default=Decimal("0.75"), ge=0, le=1)
    ofi_adverse_weight_bps: Decimal = Field(default=Decimal("1"), ge=0)
    trade_adverse_weight_bps: Decimal = Field(default=Decimal("2"), ge=0)
    volatility_adverse_weight: Decimal = Field(default=Decimal("0.25"), ge=0)
    price_tick: Decimal = Field(default=Decimal("0.01"), gt=0)
    quote_ttl_seconds: int = Field(default=2, gt=0)
    inventory_rebalance_seconds: int = Field(default=60, gt=0)
    live_market_making_enabled: bool = False


class MarketMakingContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    book: BookSnapshot
    book_quality: DataQuality
    orderflow: OrderFlowFeatureSnapshot
    inventory: MarketMakingInventory
    costs: MarketMakingCosts
    short_horizon_volatility_bps: Decimal = Field(ge=0)
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    live_operator_authorized: bool = False

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_instruments(self) -> MarketMakingContext:
        if self.book.instrument != self.instrument or self.orderflow.instrument != self.instrument:
            raise ValueError("market-making inputs must share one instrument")
        return self


class MarketMakingEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    proposal: PassiveQuoteProposal | None = None

    @model_validator(mode="after")
    def require_exact_outcome(self) -> MarketMakingEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("market-making evaluation requires exactly one outcome")
        if self.intent is not None and self.proposal is None:
            raise ValueError("accepted market-making intent requires a quote proposal")
        return self


class PassiveMarketMakingStrategy:
    def __init__(self, config: MarketMakingConfig | None = None) -> None:
        self.config = config or MarketMakingConfig()

    def evaluate(self, context: MarketMakingContext) -> MarketMakingEvaluation:
        rejection = self._input_rejection(context)
        if rejection is not None:
            return MarketMakingEvaluation(rejection_reason=rejection)
        assert context.orderflow.mid_price is not None
        assert context.orderflow.microprice is not None
        assert context.orderflow.spread_bps is not None
        fair_price = context.orderflow.microprice
        inventory_deviation = context.inventory.normalized_deviation
        reservation_price = fair_price * (
            ONE - inventory_deviation * self.config.inventory_price_skew_bps / BPS
        )
        adverse_selection = self._adverse_selection(context)
        costs = context.costs.round_trip_bps - context.costs.expected_adverse_selection_bps
        total_costs = max(ZERO, costs + adverse_selection)
        required_half_spread = max(
            self.config.minimum_half_spread_bps,
            context.orderflow.spread_bps / Decimal("2"),
            total_costs * self.config.minimum_edge_to_cost_ratio / Decimal("2"),
        )
        if required_half_spread > self.config.maximum_half_spread_bps:
            return MarketMakingEvaluation(
                rejection_reason="required_quote_width_exceeds_limit"
            )
        best_bid = context.book.bids[0].price
        best_ask = context.book.asks[0].price
        proposed_bid = min(
            best_bid,
            reservation_price * (ONE - required_half_spread / BPS),
        )
        proposed_ask = max(
            best_ask,
            reservation_price * (ONE + required_half_spread / BPS),
        )
        bid_price: Decimal | None = _floor_tick(proposed_bid, self.config.price_tick)
        ask_price: Decimal | None = _ceil_tick(proposed_ask, self.config.price_tick)
        bid_multiplier, ask_multiplier = self._size_multipliers(inventory_deviation)
        if context.inventory.signed_quantity >= context.inventory.maximum_abs_quantity:
            bid_price = None
            bid_multiplier = ZERO
        if context.inventory.signed_quantity <= -context.inventory.maximum_abs_quantity:
            ask_price = None
            ask_multiplier = ZERO
        if bid_price is None and ask_price is None:
            return MarketMakingEvaluation(rejection_reason="inventory_limit_breached")
        gross_capture = self._gross_capture(fair_price, bid_price, ask_price)
        net_edge = gross_capture - total_costs
        if gross_capture < total_costs * self.config.minimum_edge_to_cost_ratio:
            return MarketMakingEvaluation(rejection_reason="insufficient_quote_edge")
        if net_edge <= 0:
            return MarketMakingEvaluation(rejection_reason="nonpositive_quote_edge")
        proposal = PassiveQuoteProposal(
            fair_price=fair_price,
            reservation_price=reservation_price,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size_multiplier=bid_multiplier,
            ask_size_multiplier=ask_multiplier,
            quote_half_spread_bps=required_half_spread,
            gross_capture_bps=gross_capture,
            estimated_cost_bps=total_costs,
            expected_net_edge_bps=net_edge,
            adverse_selection_bps=adverse_selection,
            inventory_deviation=inventory_deviation,
        )
        legs: list[SignalLeg] = []
        if bid_price is not None:
            legs.append(
                SignalLeg(
                    instrument=context.instrument,
                    side=Side.BUY,
                    hedge_ratio=bid_multiplier,
                    preferred_limit_price=bid_price,
                    post_only=True,
                )
            )
        if ask_price is not None:
            legs.append(
                SignalLeg(
                    instrument=context.instrument,
                    side=Side.SELL,
                    hedge_ratio=ask_multiplier,
                    preferred_limit_price=ask_price,
                    post_only=True,
                )
            )
        primary_side = Side.SELL if inventory_deviation > 0 else Side.BUY
        confidence = min(
            ONE,
            net_edge / max(total_costs, self.config.minimum_half_spread_bps),
        )
        signal_id = _signal_id(
            self.config.strategy_id,
            context.instrument.canonical_id,
            context.timestamp.isoformat(),
            str(bid_price),
            str(ask_price),
        )
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=self.config.strategy_id,
            mode=context.mode,
            signal_type=SignalType.PASSIVE_MARKET_MAKING,
            primary_instrument=context.instrument,
            side=primary_side,
            legs=tuple(legs),
            regime=context.regime,
            quality_score=confidence * Decimal("100"),
            confidence=confidence,
            expected_holding_seconds=self.config.inventory_rebalance_seconds,
            expected_move_bps=gross_capture,
            estimated_cost_bps=total_costs,
            created_at=context.timestamp,
            expires_at=context.timestamp
            + timedelta(seconds=self.config.quote_ttl_seconds),
            evidence={
                "quote": proposal.model_dump(mode="json"),
                "cancel_replace_seconds": self.config.quote_ttl_seconds,
                "post_only_required": True,
                "inventory_rebalance_seconds": self.config.inventory_rebalance_seconds,
            },
        )
        return MarketMakingEvaluation(intent=intent, proposal=proposal)

    def _input_rejection(self, context: MarketMakingContext) -> str | None:
        if context.regime not in {MarketRegime.RANGE, MarketRegime.TRANSITION}:
            return "regime_not_market_making"
        if context.mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE} and not (
            self.config.live_market_making_enabled
            and context.live_operator_authorized
        ):
            return "live_market_making_disabled"
        if (
            context.book_quality is not DataQuality.VALID
            or context.orderflow.data_quality is not DataQuality.VALID
        ):
            return "market_making_data_quality_not_valid"
        ages = (
            Decimal(str((context.timestamp - context.book.exchange_timestamp).total_seconds())),
            Decimal(str((context.timestamp - context.orderflow.timestamp).total_seconds())),
        )
        if any(age < 0 for age in ages):
            return "market_making_timestamp_in_future"
        if any(age > self.config.maximum_age_seconds for age in ages):
            return "market_making_data_stale"
        if not context.book.bids or not context.book.asks:
            return "market_making_book_empty"
        if context.book.bids[0].price >= context.book.asks[0].price:
            return "market_making_book_crossed"
        if any(
            value is None
            for value in (
                context.orderflow.mid_price,
                context.orderflow.microprice,
                context.orderflow.spread_bps,
            )
        ):
            return "market_making_feature_missing"
        top_depth_quote = (
            context.book.bids[0].price * context.book.bids[0].quantity
            + context.book.asks[0].price * context.book.asks[0].quantity
        )
        if top_depth_quote < self.config.minimum_top_depth_quote:
            return "market_making_depth_below_threshold"
        if abs(context.inventory.signed_quantity) > context.inventory.maximum_abs_quantity:
            return "inventory_limit_breached"
        if self._adverse_selection(context) > self.config.maximum_adverse_selection_bps:
            return "adverse_selection_too_high"
        return None

    def _adverse_selection(self, context: MarketMakingContext) -> Decimal:
        ofi = abs(context.orderflow.ofi_zscore_5s or ZERO)
        trade = abs(context.orderflow.trade_imbalance_5s or ZERO)
        return (
            context.costs.expected_adverse_selection_bps
            + ofi * self.config.ofi_adverse_weight_bps
            + trade * self.config.trade_adverse_weight_bps
            + context.short_horizon_volatility_bps
            * self.config.volatility_adverse_weight
        )

    def _size_multipliers(self, inventory_deviation: Decimal) -> tuple[Decimal, Decimal]:
        skew = inventory_deviation * self.config.inventory_size_skew
        return (
            min(Decimal("2"), max(Decimal("0.01"), ONE - skew)),
            min(Decimal("2"), max(Decimal("0.01"), ONE + skew)),
        )

    @staticmethod
    def _gross_capture(
        fair_price: Decimal,
        bid_price: Decimal | None,
        ask_price: Decimal | None,
    ) -> Decimal:
        if bid_price is not None and ask_price is not None:
            return (ask_price - bid_price) / fair_price * BPS
        if bid_price is not None:
            return (fair_price - bid_price) / fair_price * BPS
        assert ask_price is not None
        return (ask_price - fair_price) / fair_price * BPS


def _floor_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _ceil_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def _clamp_signed(value: Decimal) -> Decimal:
    return max(-ONE, min(ONE, value))


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
