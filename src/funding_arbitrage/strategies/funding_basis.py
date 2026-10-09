"""Exact-settlement funding/basis harvesting and auditable two-leg state machine."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")
LIVE_MODES = frozenset({TradingMode.LIMITED_LIVE, TradingMode.LIVE})


class FundingMarketLeg(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    price: Decimal = Field(gt=0)
    timestamp: datetime
    data_quality: DataQuality
    liquidity_score: Decimal = Field(gt=0, le=1)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class FundingForecastEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    settlement_time: datetime
    predicted_rate: Decimal
    source: str = Field(min_length=1)

    @field_validator("settlement_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class FundingLegForecast(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    generated_at: datetime
    events: tuple[FundingForecastEvent, ...]
    median_rate: Decimal
    ewma_rate: Decimal
    persistence_score: Decimal = Field(ge=0, le=1)
    sign_change_count: int = Field(ge=0)
    two_sided_outlier_score: Decimal = Field(ge=0)
    sample_count: int = Field(ge=0)
    data_quality: DataQuality

    @field_validator("generated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_forecast(self) -> FundingLegForecast:
        if self.instrument.instrument_type is not InstrumentType.PERPETUAL:
            raise ValueError("funding forecast requires a perpetual instrument")
        timestamps = [event.settlement_time for event in self.events]
        if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
            raise ValueError("funding forecast events must be unique and ordered")
        return self


class SpotBorrowQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    asset: str
    available: bool
    daily_rate: Decimal = Field(ge=0)
    maximum_notional_usd: Decimal = Field(ge=0)
    quoted_at: datetime
    valid_until: datetime

    @field_validator("venue", "asset")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("quoted_at", "valid_until")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_window(self) -> SpotBorrowQuote:
        if self.valid_until <= self.quoted_at:
            raise ValueError("borrow quote validity must follow quote time")
        return self


class FundingHarvestCosts(BaseModel):
    model_config = ConfigDict(frozen=True)

    leg_a_entry_fee_bps: Decimal = Field(ge=0)
    leg_a_exit_fee_bps: Decimal = Field(ge=0)
    leg_b_entry_fee_bps: Decimal = Field(ge=0)
    leg_b_exit_fee_bps: Decimal = Field(ge=0)
    spread_bps: Decimal = Field(default=ZERO, ge=0)
    slippage_bps: Decimal = Field(default=ZERO, ge=0)
    legging_risk_bps: Decimal = Field(default=ZERO, ge=0)
    transfer_cost_bps: Decimal = Field(default=ZERO, ge=0)
    operational_buffer_bps: Decimal = Field(default=ZERO, ge=0)

    @property
    def total_without_borrow_bps(self) -> Decimal:
        return sum(
            (
                self.leg_a_entry_fee_bps,
                self.leg_a_exit_fee_bps,
                self.leg_b_entry_fee_bps,
                self.leg_b_exit_fee_bps,
                self.spread_bps,
                self.slippage_bps,
                self.legging_risk_bps,
                self.transfer_cost_bps,
                self.operational_buffer_bps,
            ),
            ZERO,
        )


class FundingBasisContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    leg_a: FundingMarketLeg
    leg_b: FundingMarketLeg
    forecasts: tuple[FundingLegForecast, ...]
    costs: FundingHarvestCosts
    requested_notional_usd: Decimal = Field(gt=0)
    expected_basis_convergence_bps: Decimal = Field(default=ZERO, ge=0)
    holding_horizon_seconds: int = Field(gt=0)
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    margin_available: bool
    borrow_quote: SpotBorrowQuote | None = None
    live_operator_authorized: bool = False

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_pair(self) -> FundingBasisContext:
        types = {self.leg_a.instrument.instrument_type, self.leg_b.instrument.instrument_type}
        if types not in (
            {InstrumentType.SPOT, InstrumentType.PERPETUAL},
            {InstrumentType.PERPETUAL},
        ):
            raise ValueError("funding harvest requires spot/perpetual or perpetual/perpetual")
        if (
            self.leg_a.instrument.base_asset != self.leg_b.instrument.base_asset
            or self.leg_a.instrument.quote_asset != self.leg_b.instrument.quote_asset
        ):
            raise ValueError("funding harvest legs must share base and quote assets")
        forecast_ids = {forecast.instrument.canonical_id for forecast in self.forecasts}
        required = {
            leg.instrument.canonical_id
            for leg in (self.leg_a, self.leg_b)
            if leg.instrument.instrument_type is InstrumentType.PERPETUAL
        }
        if forecast_ids != required:
            raise ValueError("funding forecasts must exactly match perpetual legs")
        return self


class FundingBasisConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "funding-basis-harvest-v1"
    maximum_market_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    maximum_forecast_age_seconds: Decimal = Field(default=Decimal("300"), gt=0)
    minimum_liquidity_score: Decimal = Field(default=Decimal("0.70"), gt=0, le=1)
    minimum_forecast_samples: int = Field(default=20, gt=0)
    minimum_persistence_score: Decimal = Field(default=Decimal("0.65"), ge=0, le=1)
    maximum_sign_changes: int = Field(default=8, ge=0)
    maximum_outlier_score: Decimal = Field(default=Decimal("4"), gt=0)
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    ttl_seconds: int = Field(default=15, gt=0)
    live_funding_harvest_enabled: bool = False


class FundingBasisEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    expected_funding_bps: Decimal
    expected_basis_bps: Decimal
    expected_gross_bps: Decimal
    expected_cost_bps: Decimal = Field(ge=0)
    expected_borrow_bps: Decimal = Field(ge=0)
    expected_net_bps: Decimal
    target_settlements: tuple[datetime, ...]

    @model_validator(mode="after")
    def require_exact_outcome(self) -> FundingBasisEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("funding-basis evaluation requires exactly one outcome")
        return self


class FundingBasisHarvestStrategy:
    def __init__(self, config: FundingBasisConfig | None = None) -> None:
        self.config = config or FundingBasisConfig()

    def evaluate(self, context: FundingBasisContext) -> FundingBasisEvaluation:
        sides = self._sides(context)
        horizon_end = context.timestamp + timedelta(
            seconds=context.holding_horizon_seconds
        )
        due_by_instrument = {
            forecast.instrument.canonical_id: tuple(
                event
                for event in forecast.events
                if context.timestamp < event.settlement_time <= horizon_end
            )
            for forecast in context.forecasts
        }
        target_settlements = tuple(
            sorted(
                {
                    event.settlement_time
                    for events in due_by_instrument.values()
                    for event in events
                }
            )
        )
        funding_bps = ZERO
        for leg, side in zip((context.leg_a, context.leg_b), sides, strict=True):
            if leg.instrument.instrument_type is not InstrumentType.PERPETUAL:
                continue
            sign = ONE if side is Side.BUY else -ONE
            funding_bps += sum(
                (-sign * event.predicted_rate * BPS)
                for event in due_by_instrument[leg.instrument.canonical_id]
            )
        borrow_bps, borrow_rejection = self._borrow_cost(context, sides)
        total_costs = context.costs.total_without_borrow_bps + borrow_bps
        gross = funding_bps + context.expected_basis_convergence_bps
        net = gross - total_costs
        metrics = {
            "expected_funding_bps": funding_bps,
            "expected_basis_bps": context.expected_basis_convergence_bps,
            "expected_gross_bps": gross,
            "expected_cost_bps": total_costs,
            "expected_borrow_bps": borrow_bps,
            "expected_net_bps": net,
            "target_settlements": target_settlements,
        }
        rejection = borrow_rejection or self._rejection(
            context,
            target_settlements,
            gross,
            total_costs,
        )
        if rejection is not None:
            return FundingBasisEvaluation(rejection_reason=rejection, **metrics)
        confidence = min(
            ONE,
            net / max(total_costs, Decimal("1")),
        )
        signal_id = _signal_id(
            self.config.strategy_id,
            context.leg_a.instrument.canonical_id,
            context.leg_b.instrument.canonical_id,
            context.timestamp.isoformat(),
            *(side.value for side in sides),
        )
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=self.config.strategy_id,
            mode=context.mode,
            signal_type=SignalType.FUNDING_BASIS,
            primary_instrument=context.leg_a.instrument,
            side=sides[0],
            legs=(
                SignalLeg(instrument=context.leg_a.instrument, side=sides[0]),
                SignalLeg(instrument=context.leg_b.instrument, side=sides[1]),
            ),
            regime=context.regime,
            quality_score=confidence * Decimal("100"),
            confidence=confidence,
            expected_holding_seconds=context.holding_horizon_seconds,
            expected_move_bps=gross,
            estimated_cost_bps=total_costs,
            created_at=context.timestamp,
            expires_at=context.timestamp + timedelta(seconds=self.config.ttl_seconds),
            evidence={
                "target_settlements": tuple(
                    settlement.isoformat() for settlement in target_settlements
                ),
                "funding_events": {
                    instrument: tuple(
                        {
                            "timestamp": event.settlement_time.isoformat(),
                            "rate": str(event.predicted_rate),
                            "source": event.source,
                        }
                        for event in events
                    )
                    for instrument, events in due_by_instrument.items()
                },
                "venue_specific_fees_bps": {
                    "leg_a_entry": str(context.costs.leg_a_entry_fee_bps),
                    "leg_a_exit": str(context.costs.leg_a_exit_fee_bps),
                    "leg_b_entry": str(context.costs.leg_b_entry_fee_bps),
                    "leg_b_exit": str(context.costs.leg_b_exit_fee_bps),
                },
                "expected_borrow_bps": str(borrow_bps),
                "expected_net_bps": str(net),
            },
        )
        return FundingBasisEvaluation(intent=intent, **metrics)

    def _sides(self, context: FundingBasisContext) -> tuple[Side, Side]:
        legs = (context.leg_a, context.leg_b)
        if {leg.instrument.instrument_type for leg in legs} == {
            InstrumentType.SPOT,
            InstrumentType.PERPETUAL,
        }:
            perpetual_index = next(
                index
                for index, leg in enumerate(legs)
                if leg.instrument.instrument_type is InstrumentType.PERPETUAL
            )
            forecast = next(
                item
                for item in context.forecasts
                if item.instrument == legs[perpetual_index].instrument
            )
            horizon_end = context.timestamp + timedelta(
                seconds=context.holding_horizon_seconds
            )
            total_rate = sum(
                (
                    event.predicted_rate
                    for event in forecast.events
                    if context.timestamp < event.settlement_time <= horizon_end
                ),
                ZERO,
            )
            perpetual_side = Side.SELL if total_rate >= 0 else Side.BUY
            spot_side = Side.BUY if perpetual_side is Side.SELL else Side.SELL
            return (
                (spot_side, perpetual_side)
                if perpetual_index == 1
                else (perpetual_side, spot_side)
            )
        totals = []
        horizon_end = context.timestamp + timedelta(
            seconds=context.holding_horizon_seconds
        )
        for leg in legs:
            forecast = next(item for item in context.forecasts if item.instrument == leg.instrument)
            totals.append(
                sum(
                    (
                        event.predicted_rate
                        for event in forecast.events
                        if context.timestamp < event.settlement_time <= horizon_end
                    ),
                    ZERO,
                )
            )
        return (Side.SELL, Side.BUY) if totals[0] >= totals[1] else (Side.BUY, Side.SELL)

    def _borrow_cost(
        self,
        context: FundingBasisContext,
        sides: tuple[Side, Side],
    ) -> tuple[Decimal, str | None]:
        short_spot = next(
            (
                leg
                for leg, side in zip((context.leg_a, context.leg_b), sides, strict=True)
                if leg.instrument.instrument_type is InstrumentType.SPOT
                and side is Side.SELL
            ),
            None,
        )
        if short_spot is None:
            return ZERO, None
        quote = context.borrow_quote
        if quote is None or not quote.available:
            return ZERO, "spot_borrow_unavailable"
        if (
            quote.venue != short_spot.instrument.venue
            or quote.asset != short_spot.instrument.base_asset
        ):
            return ZERO, "spot_borrow_quote_mismatch"
        if quote.quoted_at > context.timestamp or quote.valid_until <= context.timestamp:
            return ZERO, "spot_borrow_quote_stale"
        if quote.maximum_notional_usd < context.requested_notional_usd:
            return ZERO, "spot_borrow_capacity_insufficient"
        holding_days = Decimal(context.holding_horizon_seconds) / Decimal("86400")
        return quote.daily_rate * holding_days * BPS, None

    def _rejection(
        self,
        context: FundingBasisContext,
        target_settlements: tuple[datetime, ...],
        gross: Decimal,
        total_costs: Decimal,
    ) -> str | None:
        if context.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            return "funding_basis_unsafe_regime"
        if context.mode in LIVE_MODES and not (
            self.config.live_funding_harvest_enabled
            and context.live_operator_authorized
        ):
            return "live_funding_harvest_disabled"
        if not context.margin_available:
            return "funding_basis_margin_unavailable"
        if any(
            leg.data_quality is not DataQuality.VALID
            for leg in (context.leg_a, context.leg_b)
        ):
            return "funding_basis_market_quality_not_valid"
        ages = tuple(
            Decimal(str((context.timestamp - leg.timestamp).total_seconds()))
            for leg in (context.leg_a, context.leg_b)
        )
        if any(age < 0 for age in ages):
            return "funding_basis_market_timestamp_in_future"
        if any(age > self.config.maximum_market_age_seconds for age in ages):
            return "funding_basis_market_stale"
        if min(context.leg_a.liquidity_score, context.leg_b.liquidity_score) < (
            self.config.minimum_liquidity_score
        ):
            return "funding_basis_liquidity_below_threshold"
        for forecast in context.forecasts:
            age = Decimal(str((context.timestamp - forecast.generated_at).total_seconds()))
            if age < 0:
                return "funding_forecast_timestamp_in_future"
            if age > self.config.maximum_forecast_age_seconds:
                return "funding_forecast_stale"
            if forecast.data_quality is not DataQuality.VALID:
                return "funding_forecast_quality_not_valid"
            if forecast.sample_count < self.config.minimum_forecast_samples:
                return "funding_forecast_samples_insufficient"
            if forecast.persistence_score < self.config.minimum_persistence_score:
                return "funding_forecast_persistence_low"
            if forecast.sign_change_count > self.config.maximum_sign_changes:
                return "funding_forecast_sign_unstable"
            if forecast.two_sided_outlier_score > self.config.maximum_outlier_score:
                return "funding_forecast_outlier"
        if not target_settlements:
            return "no_funding_settlement_in_horizon"
        if gross <= 0:
            return "funding_basis_nonpositive_gross"
        if gross < total_costs * self.config.minimum_edge_to_cost_ratio:
            return "funding_basis_edge_below_cost"
        return None


class FundingLegExecutionState(StrEnum):
    DETECTED = "DETECTED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    HEDGED = "HEDGED"
    EXITING = "EXITING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class FundingHarvestPositionState(StrEnum):
    DETECTED = "DETECTED"
    OPENING = "OPENING"
    HEDGED = "HEDGED"
    OPEN = "OPEN"
    UNWIND_REQUIRED = "UNWIND_REQUIRED"
    EXITING = "EXITING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class FundingLegExecution(BaseModel):
    instrument: InstrumentKey
    side: Side
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(default=ZERO, ge=0)
    closed_quantity: Decimal = Field(default=ZERO, ge=0)
    state: FundingLegExecutionState = FundingLegExecutionState.DETECTED
    exchange_order_id: str | None = None


class FundingLegTransition(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    timestamp: datetime
    leg_index: int | None = Field(default=None, ge=0, le=1)
    from_state: str
    to_state: str
    reason: str
    fill_id: str | None = None
    quantity: Decimal = Field(default=ZERO, ge=0)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class FundingHarvestStateMachine:
    """Two-leg lifecycle that requires exact fills and explicit compensation."""

    def __init__(
        self,
        legs: tuple[FundingLegExecution, FundingLegExecution],
        *,
        legging_timeout_seconds: int = 3,
    ) -> None:
        if legging_timeout_seconds <= 0:
            raise ValueError("legging timeout must be positive")
        if legs[0].instrument == legs[1].instrument and legs[0].side is legs[1].side:
            raise ValueError("funding hedge legs cannot be identical exposures")
        self.legs = [leg.model_copy(deep=True) for leg in legs]
        self.state = FundingHarvestPositionState.DETECTED
        self.legging_timeout_seconds = legging_timeout_seconds
        self.opening_started_at: datetime | None = None
        self.transitions: list[FundingLegTransition] = []
        self._fill_ids: set[str] = set()

    def start_opening(self, timestamp: datetime) -> None:
        self._require_position(FundingHarvestPositionState.DETECTED)
        now = _utc(timestamp)
        self._position_transition(FundingHarvestPositionState.OPENING, now, "opening_started")
        self.opening_started_at = now
        for index, _leg in enumerate(self.legs):
            self._leg_transition(index, FundingLegExecutionState.SUBMITTING, now, "submit")

    def acknowledge(
        self,
        leg_index: int,
        exchange_order_id: str,
        timestamp: datetime,
    ) -> None:
        leg = self._leg(leg_index)
        if leg.state is not FundingLegExecutionState.SUBMITTING:
            raise ValueError("only submitting leg can be acknowledged")
        if not exchange_order_id:
            raise ValueError("exchange order id is required")
        leg.exchange_order_id = exchange_order_id
        self._leg_transition(
            leg_index,
            FundingLegExecutionState.ACKNOWLEDGED,
            _utc(timestamp),
            "acknowledged",
        )

    def apply_open_fill(
        self,
        leg_index: int,
        fill_id: str,
        quantity: Decimal,
        timestamp: datetime,
    ) -> bool:
        self._require_position(FundingHarvestPositionState.OPENING)
        if fill_id in self._fill_ids:
            return False
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        leg = self._leg(leg_index)
        if leg.state not in {
            FundingLegExecutionState.ACKNOWLEDGED,
            FundingLegExecutionState.PARTIALLY_FILLED,
        }:
            raise ValueError("leg is not fillable")
        if leg.filled_quantity + quantity > leg.requested_quantity:
            raise ValueError("open fill exceeds requested quantity")
        self._fill_ids.add(fill_id)
        leg.filled_quantity += quantity
        target = (
            FundingLegExecutionState.FILLED
            if leg.filled_quantity == leg.requested_quantity
            else FundingLegExecutionState.PARTIALLY_FILLED
        )
        self._leg_transition(
            leg_index,
            target,
            _utc(timestamp),
            "open_fill",
            fill_id,
            quantity,
        )
        if all(item.state is FundingLegExecutionState.FILLED for item in self.legs):
            now = _utc(timestamp)
            self._position_transition(FundingHarvestPositionState.HEDGED, now, "both_legs_filled")
            for index in range(2):
                self._leg_transition(
                    index,
                    FundingLegExecutionState.HEDGED,
                    now,
                    "hedge_complete",
                )
        return True

    def mark_open(self, timestamp: datetime) -> None:
        self._require_position(FundingHarvestPositionState.HEDGED)
        self._position_transition(
            FundingHarvestPositionState.OPEN,
            _utc(timestamp),
            "position_open",
        )

    def check_legging_timeout(self, timestamp: datetime) -> bool:
        if self.state is not FundingHarvestPositionState.OPENING:
            return False
        assert self.opening_started_at is not None
        now = _utc(timestamp)
        if now < self.opening_started_at:
            raise ValueError("state-machine time cannot move backwards")
        exposed = any(leg.filled_quantity > 0 for leg in self.legs)
        incomplete = any(
            leg.filled_quantity < leg.requested_quantity for leg in self.legs
        )
        if exposed and incomplete and (
            now - self.opening_started_at
        ).total_seconds() >= self.legging_timeout_seconds:
            self._position_transition(
                FundingHarvestPositionState.UNWIND_REQUIRED,
                now,
                "legging_timeout",
            )
            return True
        return False

    def complete_compensating_unwind(
        self,
        closed_quantities: tuple[Decimal, Decimal],
        timestamp: datetime,
    ) -> None:
        self._require_position(FundingHarvestPositionState.UNWIND_REQUIRED)
        for index, (leg, quantity) in enumerate(
            zip(self.legs, closed_quantities, strict=True)
        ):
            if quantity != leg.filled_quantity:
                raise ValueError("compensation must close every filled unit exactly")
            leg.closed_quantity = quantity
            self._leg_transition(
                index,
                FundingLegExecutionState.FAILED,
                _utc(timestamp),
                "compensated",
                quantity=quantity,
            )
        self._position_transition(
            FundingHarvestPositionState.FAILED,
            _utc(timestamp),
            "compensating_unwind_complete",
        )

    def start_exit(self, timestamp: datetime) -> None:
        self._require_position(FundingHarvestPositionState.OPEN)
        now = _utc(timestamp)
        self._position_transition(FundingHarvestPositionState.EXITING, now, "exit_started")
        for index in range(2):
            self._leg_transition(
                index,
                FundingLegExecutionState.EXITING,
                now,
                "exit_submit",
            )

    def apply_close_fill(
        self,
        leg_index: int,
        fill_id: str,
        quantity: Decimal,
        timestamp: datetime,
    ) -> bool:
        self._require_position(FundingHarvestPositionState.EXITING)
        if fill_id in self._fill_ids:
            return False
        if quantity <= 0:
            raise ValueError("close fill quantity must be positive")
        leg = self._leg(leg_index)
        if leg.state is not FundingLegExecutionState.EXITING:
            raise ValueError("leg is not exiting")
        if leg.closed_quantity + quantity > leg.filled_quantity:
            raise ValueError("close fill exceeds exact open quantity")
        self._fill_ids.add(fill_id)
        leg.closed_quantity += quantity
        if leg.closed_quantity == leg.filled_quantity:
            self._leg_transition(
                leg_index,
                FundingLegExecutionState.CLOSED,
                _utc(timestamp),
                "close_fill",
                fill_id,
                quantity,
            )
        else:
            self._record(
                timestamp=_utc(timestamp),
                leg_index=leg_index,
                from_state=leg.state.value,
                to_state=leg.state.value,
                reason="partial_close_fill",
                fill_id=fill_id,
                quantity=quantity,
            )
        if all(item.state is FundingLegExecutionState.CLOSED for item in self.legs):
            self._position_transition(
                FundingHarvestPositionState.CLOSED,
                _utc(timestamp),
                "both_legs_closed",
            )
        return True

    def _leg(self, index: int) -> FundingLegExecution:
        if index not in (0, 1):
            raise ValueError("leg index must be 0 or 1")
        return self.legs[index]

    def _require_position(self, state: FundingHarvestPositionState) -> None:
        if self.state is not state:
            raise ValueError(f"position must be {state}, got {self.state}")

    def _position_transition(
        self,
        target: FundingHarvestPositionState,
        timestamp: datetime,
        reason: str,
    ) -> None:
        previous = self.state
        self.state = target
        self._record(
            timestamp=timestamp,
            leg_index=None,
            from_state=previous.value,
            to_state=target.value,
            reason=reason,
        )

    def _leg_transition(
        self,
        index: int,
        target: FundingLegExecutionState,
        timestamp: datetime,
        reason: str,
        fill_id: str | None = None,
        quantity: Decimal = ZERO,
    ) -> None:
        leg = self._leg(index)
        previous = leg.state
        leg.state = target
        self._record(
            timestamp=timestamp,
            leg_index=index,
            from_state=previous.value,
            to_state=target.value,
            reason=reason,
            fill_id=fill_id,
            quantity=quantity,
        )

    def _record(
        self,
        *,
        timestamp: datetime,
        leg_index: int | None,
        from_state: str,
        to_state: str,
        reason: str,
        fill_id: str | None = None,
        quantity: Decimal = ZERO,
    ) -> None:
        self.transitions.append(
            FundingLegTransition(
                sequence=len(self.transitions) + 1,
                timestamp=timestamp,
                leg_index=leg_index,
                from_state=from_state,
                to_state=to_state,
                reason=reason,
                fill_id=fill_id,
                quantity=quantity,
            )
        )


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
