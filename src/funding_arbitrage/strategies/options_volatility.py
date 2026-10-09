"""Deterministic option valuation, portfolio Greeks, stress risk, and vol signals."""

from __future__ import annotations

import hashlib
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

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
    OptionRight,
    Side,
    TradingMode,
)

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")
YEAR_SECONDS = Decimal("31536000")
MIN_VOLATILITY = Decimal("0.0001")
USD_STABLE_QUOTE_ASSETS = frozenset({"USD", "USDC", "USDT"})


def option_quote_assets_compatible(option_quote: str, hedge_quote: str) -> bool:
    """Allow only identical quotes or the explicit USD-stable valuation group."""

    normalized_option = option_quote.strip().upper()
    normalized_hedge = hedge_quote.strip().upper()
    return normalized_option == normalized_hedge or {
        normalized_option,
        normalized_hedge,
    }.issubset(USD_STABLE_QUOTE_ASSETS)


class OptionValuation(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal = Field(ge=0)
    delta: Decimal
    gamma: Decimal = Field(ge=0)
    vega: Decimal = Field(ge=0)
    theta_annual: Decimal
    rho: Decimal


class BlackScholesModel:
    """European option model with deterministic bisection for implied volatility."""

    @staticmethod
    def value(
        *,
        spot: Decimal,
        strike: Decimal,
        time_years: Decimal,
        volatility: Decimal,
        right: OptionRight,
        risk_free_rate: Decimal = ZERO,
        carry_yield: Decimal = ZERO,
    ) -> OptionValuation:
        if spot <= 0 or strike <= 0 or time_years <= 0 or volatility <= 0:
            raise ValueError("spot, strike, time, and volatility must be positive")
        s = float(spot)
        k = float(strike)
        t = float(time_years)
        sigma = float(volatility)
        rate = float(risk_free_rate)
        carry = float(carry_yield)
        root_t = math.sqrt(t)
        d1 = (
            math.log(s / k) + (rate - carry + 0.5 * sigma * sigma) * t
        ) / (sigma * root_t)
        d2 = d1 - sigma * root_t
        discount_rate = math.exp(-rate * t)
        discount_carry = math.exp(-carry * t)
        density = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
        if right is OptionRight.CALL:
            price = s * discount_carry * _cdf(d1) - k * discount_rate * _cdf(d2)
            delta = discount_carry * _cdf(d1)
            theta = (
                -s * discount_carry * density * sigma / (2 * root_t)
                - rate * k * discount_rate * _cdf(d2)
                + carry * s * discount_carry * _cdf(d1)
            )
            rho = k * t * discount_rate * _cdf(d2)
        else:
            price = k * discount_rate * _cdf(-d2) - s * discount_carry * _cdf(-d1)
            delta = discount_carry * (_cdf(d1) - 1)
            theta = (
                -s * discount_carry * density * sigma / (2 * root_t)
                + rate * k * discount_rate * _cdf(-d2)
                - carry * s * discount_carry * _cdf(-d1)
            )
            rho = -k * t * discount_rate * _cdf(-d2)
        gamma = discount_carry * density / (s * sigma * root_t)
        vega = s * discount_carry * density * root_t
        return OptionValuation(
            price=_decimal(price),
            delta=_decimal(delta),
            gamma=_decimal(gamma),
            vega=_decimal(vega),
            theta_annual=_decimal(theta),
            rho=_decimal(rho),
        )

    @classmethod
    def implied_volatility(
        cls,
        *,
        option_price: Decimal,
        spot: Decimal,
        strike: Decimal,
        time_years: Decimal,
        right: OptionRight,
        risk_free_rate: Decimal = ZERO,
        carry_yield: Decimal = ZERO,
        tolerance: Decimal = Decimal("0.00000001"),
        maximum_iterations: int = 128,
    ) -> Decimal:
        if option_price < 0 or maximum_iterations <= 0 or tolerance <= 0:
            raise ValueError("invalid implied-volatility solver inputs")
        lower_price, upper_price = _arbitrage_bounds(
            spot,
            strike,
            time_years,
            right,
            risk_free_rate,
            carry_yield,
        )
        if option_price < lower_price - tolerance or option_price > upper_price + tolerance:
            raise ValueError("option price violates no-arbitrage bounds")
        low = MIN_VOLATILITY
        high = Decimal("5")
        for _ in range(maximum_iterations):
            midpoint = (low + high) / Decimal("2")
            modeled = cls.value(
                spot=spot,
                strike=strike,
                time_years=time_years,
                volatility=midpoint,
                right=right,
                risk_free_rate=risk_free_rate,
                carry_yield=carry_yield,
            ).price
            error = modeled - option_price
            if abs(error) <= tolerance:
                return midpoint
            if error < 0:
                low = midpoint
            else:
                high = midpoint
        return (low + high) / Decimal("2")


class OptionQuote(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    underlying_price: Decimal = Field(gt=0)
    bid_price: Decimal = Field(ge=0)
    ask_price: Decimal = Field(gt=0)
    market_implied_volatility: Decimal = Field(gt=0, le=5)
    contract_multiplier: Decimal = Field(default=ONE, gt=0)
    open_interest_contracts: Decimal = Field(default=ZERO, ge=0)
    volume_contracts: Decimal = Field(default=ZERO, ge=0)
    liquidity_score: Decimal = Field(gt=0, le=1)
    timestamp: datetime
    data_quality: DataQuality

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_quote(self) -> OptionQuote:
        if self.instrument.instrument_type is not InstrumentType.OPTION:
            raise ValueError("option quote requires an option instrument")
        if self.bid_price > self.ask_price:
            raise ValueError("option quote bid cannot exceed ask")
        return self

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal("2")


class OptionPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    quote: OptionQuote
    signed_contracts: Decimal

    @field_validator("signed_contracts")
    @classmethod
    def reject_zero_contracts(cls, value: Decimal) -> Decimal:
        if value == 0:
            raise ValueError("option position cannot have zero contracts")
        return value


class OptionsRiskLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_abs_delta_notional_usd: Decimal = Field(default=Decimal("100000"), gt=0)
    maximum_abs_gamma_cash_usd: Decimal = Field(default=Decimal("100000"), gt=0)
    maximum_abs_vega_usd: Decimal = Field(default=Decimal("100000"), gt=0)
    maximum_daily_theta_decay_usd: Decimal = Field(default=Decimal("10000"), gt=0)
    maximum_stress_loss_usd: Decimal = Field(default=Decimal("10000"), gt=0)
    spot_shocks: tuple[Decimal, ...] = (
        Decimal("-0.30"),
        Decimal("-0.15"),
        Decimal("0.15"),
        Decimal("0.30"),
    )
    volatility_shocks: tuple[Decimal, ...] = (Decimal("-0.20"), Decimal("0.20"))

    @model_validator(mode="after")
    def validate_scenarios(self) -> OptionsRiskLimits:
        if not self.spot_shocks or not self.volatility_shocks:
            raise ValueError("option risk requires spot and volatility stress scenarios")
        if any(shock <= -1 for shock in self.spot_shocks):
            raise ValueError("spot shocks must preserve a positive underlying price")
        return self


class OptionsRiskSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_value_usd: Decimal
    delta_underlying: Decimal
    delta_hedge_underlying: Decimal
    delta_notional_usd: Decimal
    gamma_cash_usd: Decimal
    vega_usd: Decimal
    theta_annual_usd: Decimal
    daily_theta_decay_usd: Decimal = Field(ge=0)
    worst_stress_loss_usd: Decimal = Field(ge=0)
    worst_spot_shock: Decimal
    worst_volatility_shock: Decimal


class OptionsRiskAssessment(BaseModel):
    model_config = ConfigDict(frozen=True)

    approved: bool
    rejection_reason: str | None = None
    snapshot: OptionsRiskSnapshot

    @model_validator(mode="after")
    def require_exact_outcome(self) -> OptionsRiskAssessment:
        if self.approved is (self.rejection_reason is not None):
            raise ValueError("approved option risk must have no reason; rejected risk needs one")
        return self


class OptionsRiskEngine:
    def __init__(self, model: BlackScholesModel | None = None) -> None:
        self.model = model or BlackScholesModel()

    def assess(
        self,
        positions: tuple[OptionPosition, ...],
        timestamp: datetime,
        limits: OptionsRiskLimits,
        *,
        margin_available: bool,
        risk_free_rate: Decimal = ZERO,
        carry_yield: Decimal = ZERO,
        delta_hedge_underlying: Decimal = ZERO,
    ) -> OptionsRiskAssessment:
        if not positions:
            raise ValueError("option risk assessment requires positions")
        now = _utc(timestamp)
        base_asset = positions[0].quote.instrument.base_asset
        if any(position.quote.instrument.base_asset != base_asset for position in positions):
            raise ValueError("option risk positions must share one underlying")

        current_value = ZERO
        delta = delta_hedge_underlying
        gamma_cash = ZERO
        vega = ZERO
        theta = ZERO
        spot_reference = positions[0].quote.underlying_price
        valuations: list[tuple[OptionPosition, OptionValuation, Decimal]] = []
        for position in positions:
            quote = position.quote
            expiry = quote.instrument.expiry
            assert expiry is not None
            years = Decimal(str((expiry - now).total_seconds())) / YEAR_SECONDS
            if years <= 0:
                raise ValueError("cannot risk an expired option")
            valuation = self.model.value(
                spot=quote.underlying_price,
                strike=_strike(quote),
                time_years=years,
                volatility=quote.market_implied_volatility,
                right=_right(quote),
                risk_free_rate=risk_free_rate,
                carry_yield=carry_yield,
            )
            scale = position.signed_contracts * quote.contract_multiplier
            current_value += valuation.price * scale
            delta += valuation.delta * scale
            gamma_cash += valuation.gamma * scale * quote.underlying_price**2
            vega += valuation.vega * scale
            theta += valuation.theta_annual * scale
            valuations.append((position, valuation, years))

        worst_loss = ZERO
        worst_spot_shock = ZERO
        worst_volatility_shock = ZERO
        for spot_shock in limits.spot_shocks:
            for volatility_shock in limits.volatility_shocks:
                scenario_value = ZERO
                for position, _valuation, years in valuations:
                    quote = position.quote
                    shocked_spot = quote.underlying_price * (ONE + spot_shock)
                    shocked_volatility = max(
                        MIN_VOLATILITY,
                        quote.market_implied_volatility + volatility_shock,
                    )
                    shocked = self.model.value(
                        spot=shocked_spot,
                        strike=_strike(quote),
                        time_years=years,
                        volatility=shocked_volatility,
                        right=_right(quote),
                        risk_free_rate=risk_free_rate,
                        carry_yield=carry_yield,
                    )
                    scenario_value += (
                        shocked.price
                        * position.signed_contracts
                        * quote.contract_multiplier
                    )
                hedge_pnl = delta_hedge_underlying * spot_reference * spot_shock
                loss = max(ZERO, current_value - scenario_value - hedge_pnl)
                if loss > worst_loss:
                    worst_loss = loss
                    worst_spot_shock = spot_shock
                    worst_volatility_shock = volatility_shock

        snapshot = OptionsRiskSnapshot(
            model_value_usd=current_value,
            delta_underlying=delta,
            delta_hedge_underlying=delta_hedge_underlying,
            delta_notional_usd=delta * spot_reference,
            gamma_cash_usd=gamma_cash,
            vega_usd=vega,
            theta_annual_usd=theta,
            daily_theta_decay_usd=max(ZERO, -theta / Decimal("365")),
            worst_stress_loss_usd=worst_loss,
            worst_spot_shock=worst_spot_shock,
            worst_volatility_shock=worst_volatility_shock,
        )
        rejection = self._rejection(snapshot, limits, margin_available)
        return OptionsRiskAssessment(
            approved=rejection is None,
            rejection_reason=rejection,
            snapshot=snapshot,
        )

    @staticmethod
    def _rejection(
        snapshot: OptionsRiskSnapshot,
        limits: OptionsRiskLimits,
        margin_available: bool,
    ) -> str | None:
        if not margin_available:
            return "options_margin_unavailable"
        if abs(snapshot.delta_notional_usd) > limits.maximum_abs_delta_notional_usd:
            return "options_delta_limit"
        if abs(snapshot.gamma_cash_usd) > limits.maximum_abs_gamma_cash_usd:
            return "options_gamma_limit"
        if abs(snapshot.vega_usd) > limits.maximum_abs_vega_usd:
            return "options_vega_limit"
        if snapshot.daily_theta_decay_usd > limits.maximum_daily_theta_decay_usd:
            return "options_theta_limit"
        if snapshot.worst_stress_loss_usd > limits.maximum_stress_loss_usd:
            return "options_stress_loss_limit"
        return None


class OptionsVolatilityConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "options-volatility-v1"
    maximum_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    maximum_days_to_expiry: Decimal = Field(default=Decimal("90"), gt=0)
    maximum_underlying_divergence_bps: Decimal = Field(default=Decimal("20"), gt=0)
    minimum_liquidity_score: Decimal = Field(default=Decimal("0.70"), gt=0, le=1)
    minimum_volatility_edge: Decimal = Field(default=Decimal("0.05"), gt=0)
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    delta_hedge_threshold: Decimal = Field(default=Decimal("0.01"), ge=0)
    maximum_holding_seconds: int = Field(default=86400, gt=0)
    expiry_exit_buffer_seconds: int = Field(default=900, gt=0)
    ttl_seconds: int = Field(default=120, gt=0)
    short_volatility_enabled: bool = False
    live_options_enabled: bool = False


class OptionsVolatilityContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    call: OptionQuote
    put: OptionQuote
    hedge_instrument: InstrumentKey
    forecast_realized_volatility: Decimal = Field(gt=0, le=5)
    estimated_cost_bps: Decimal = Field(ge=0)
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    margin_available: bool
    short_volatility_operator_authorized: bool = False
    live_operator_authorized: bool = False
    risk_free_rate: Decimal = ZERO
    carry_yield: Decimal = ZERO
    hedge_quote_conversion_rate: Decimal | None = Field(default=None, gt=0)
    risk_limits: OptionsRiskLimits = OptionsRiskLimits()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_structure(self) -> OptionsVolatilityContext:
        if _right(self.call) is not OptionRight.CALL or _right(self.put) is not OptionRight.PUT:
            raise ValueError("options volatility context requires call then put")
        call_identity = (
            self.call.instrument.venue,
            self.call.instrument.base_asset,
            self.call.instrument.quote_asset,
            self.call.instrument.settlement_asset,
            self.call.instrument.expiry,
            self.call.instrument.strike_price,
        )
        put_identity = (
            self.put.instrument.venue,
            self.put.instrument.base_asset,
            self.put.instrument.quote_asset,
            self.put.instrument.settlement_asset,
            self.put.instrument.expiry,
            self.put.instrument.strike_price,
        )
        if call_identity != put_identity:
            raise ValueError(
                "call and put must share venue, underlying, settlement, strike, and expiry"
            )
        if self.hedge_instrument.instrument_type not in {
            InstrumentType.SPOT,
            InstrumentType.PERPETUAL,
            InstrumentType.FUTURE,
        }:
            raise ValueError("option delta hedge requires spot or futures exposure")
        if self.hedge_instrument.base_asset != self.call.instrument.base_asset:
            raise ValueError("option hedge must share the base asset")
        option_quote = self.call.instrument.quote_asset
        hedge_quote = self.hedge_instrument.quote_asset
        if not option_quote_assets_compatible(option_quote, hedge_quote):
            raise ValueError("option hedge quote assets are incompatible")
        if option_quote == hedge_quote:
            if self.hedge_quote_conversion_rate not in (None, ONE):
                raise ValueError("same-quote option hedge conversion must be one")
        elif self.hedge_quote_conversion_rate is None:
            raise ValueError("cross-quote option hedge requires an explicit conversion rate")
        elif self.hedge_quote_conversion_rate != ONE:
            raise ValueError("cross-quote option hedge currently requires parity conversion")
        return self


class OptionsVolatilityEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    market_implied_volatility: Decimal
    volatility_edge: Decimal
    risk: OptionsRiskAssessment | None = None

    @model_validator(mode="after")
    def require_exact_outcome(self) -> OptionsVolatilityEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("options-volatility evaluation requires exactly one outcome")
        return self


class OptionsVolatilityStrategy:
    def __init__(
        self,
        config: OptionsVolatilityConfig | None = None,
        risk_engine: OptionsRiskEngine | None = None,
    ) -> None:
        self.config = config or OptionsVolatilityConfig()
        self.risk_engine = risk_engine or OptionsRiskEngine()

    def evaluate(self, context: OptionsVolatilityContext) -> OptionsVolatilityEvaluation:
        market_volatility = (
            context.call.market_implied_volatility
            + context.put.market_implied_volatility
        ) / Decimal("2")
        volatility_edge = context.forecast_realized_volatility - market_volatility
        rejection = self._market_rejection(context, volatility_edge)
        if rejection is not None:
            return OptionsVolatilityEvaluation(
                rejection_reason=rejection,
                market_implied_volatility=market_volatility,
                volatility_edge=volatility_edge,
            )

        option_side = Side.BUY if volatility_edge > 0 else Side.SELL
        signed_contracts = ONE if option_side is Side.BUY else -ONE
        positions = (
            OptionPosition(quote=context.call, signed_contracts=signed_contracts),
            OptionPosition(quote=context.put, signed_contracts=signed_contracts),
        )
        unhedged = self.risk_engine.assess(
            positions,
            context.timestamp,
            context.risk_limits,
            margin_available=context.margin_available,
            risk_free_rate=context.risk_free_rate,
            carry_yield=context.carry_yield,
        )
        hedge_quantity = (
            -unhedged.snapshot.delta_underlying
            if abs(unhedged.snapshot.delta_underlying)
            >= self.config.delta_hedge_threshold
            else ZERO
        )
        risk = self.risk_engine.assess(
            positions,
            context.timestamp,
            context.risk_limits,
            margin_available=context.margin_available,
            risk_free_rate=context.risk_free_rate,
            carry_yield=context.carry_yield,
            delta_hedge_underlying=hedge_quantity,
        )
        if not risk.approved:
            return OptionsVolatilityEvaluation(
                rejection_reason=risk.rejection_reason,
                market_implied_volatility=market_volatility,
                volatility_edge=volatility_edge,
                risk=risk,
            )

        expected_move_bps = abs(volatility_edge) * BPS
        if (
            expected_move_bps
            < context.estimated_cost_bps * self.config.minimum_edge_to_cost_ratio
        ):
            return OptionsVolatilityEvaluation(
                rejection_reason="insufficient_volatility_edge_to_cost",
                market_implied_volatility=market_volatility,
                volatility_edge=volatility_edge,
                risk=risk,
            )
        legs = [
            SignalLeg(instrument=context.call.instrument, side=option_side),
            SignalLeg(instrument=context.put.instrument, side=option_side),
        ]
        if hedge_quantity != 0:
            legs.append(
                SignalLeg(
                    instrument=context.hedge_instrument,
                    side=Side.BUY if hedge_quantity > 0 else Side.SELL,
                    hedge_ratio=abs(hedge_quantity),
                    execution_priority=1,
                )
            )
        expiry = context.call.instrument.expiry
        assert expiry is not None
        seconds_to_expiry = Decimal(
            str((expiry - context.timestamp).total_seconds())
        )
        holding_budget_seconds = int(
            seconds_to_expiry - Decimal(self.config.expiry_exit_buffer_seconds)
        )
        assert holding_budget_seconds > 0
        confidence = min(ONE, abs(volatility_edge) / self.config.minimum_volatility_edge)
        signal_id = _signal_id(
            self.config.strategy_id,
            context.call.instrument.canonical_id,
            context.put.instrument.canonical_id,
            context.timestamp.isoformat(),
            option_side,
        )
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=self.config.strategy_id,
            mode=context.mode,
            signal_type=SignalType.OPTIONS_VOLATILITY,
            primary_instrument=context.call.instrument,
            side=option_side,
            legs=tuple(legs),
            regime=context.regime,
            quality_score=confidence * Decimal("100"),
            confidence=confidence,
            expected_holding_seconds=max(
                1,
                min(
                    self.config.maximum_holding_seconds,
                    holding_budget_seconds,
                ),
            ),
            expected_move_bps=expected_move_bps,
            estimated_cost_bps=context.estimated_cost_bps,
            created_at=context.timestamp,
            expires_at=context.timestamp + timedelta(seconds=self.config.ttl_seconds),
            evidence={
                "market_implied_volatility": str(market_volatility),
                "forecast_realized_volatility": str(
                    context.forecast_realized_volatility
                ),
                "volatility_edge": str(volatility_edge),
                "delta_hedge_underlying": str(hedge_quantity),
                "hedge_quote_conversion_rate": str(
                    context.hedge_quote_conversion_rate or ONE
                ),
                "expiry_exit_buffer_seconds": str(
                    self.config.expiry_exit_buffer_seconds
                ),
                "risk": risk.snapshot.model_dump(mode="json"),
                "risk_limits": context.risk_limits.model_dump(mode="json"),
            },
        )
        return OptionsVolatilityEvaluation(
            intent=intent,
            market_implied_volatility=market_volatility,
            volatility_edge=volatility_edge,
            risk=risk,
        )

    def _market_rejection(
        self,
        context: OptionsVolatilityContext,
        volatility_edge: Decimal,
    ) -> str | None:
        if context.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            return "unsafe_regime"
        if context.mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE} and not (
            self.config.live_options_enabled and context.live_operator_authorized
        ):
            return "live_options_disabled"
        if volatility_edge < 0 and not (
            self.config.short_volatility_enabled
            and context.short_volatility_operator_authorized
        ):
            return "short_volatility_disabled"
        quotes = (context.call, context.put)
        if any(quote.data_quality is not DataQuality.VALID for quote in quotes):
            return "option_market_quality_not_valid"
        ages = tuple(
            Decimal(str((context.timestamp - quote.timestamp).total_seconds()))
            for quote in quotes
        )
        if any(age < 0 for age in ages):
            return "option_timestamp_in_future"
        if any(age > self.config.maximum_age_seconds for age in ages):
            return "option_market_data_stale"
        expiry = context.call.instrument.expiry
        assert expiry is not None
        seconds_to_expiry = Decimal(str((expiry - context.timestamp).total_seconds()))
        if seconds_to_expiry <= 0:
            return "option_expired"
        holding_budget_seconds = int(
            seconds_to_expiry - Decimal(self.config.expiry_exit_buffer_seconds)
        )
        if holding_budget_seconds < 1:
            return "option_expiry_exit_window"
        if seconds_to_expiry > self.config.maximum_days_to_expiry * Decimal("86400"):
            return "option_expiry_too_distant"
        if min(quote.liquidity_score for quote in quotes) < self.config.minimum_liquidity_score:
            return "option_liquidity_below_threshold"
        price_divergence_bps = (
            abs(context.call.underlying_price - context.put.underlying_price)
            / min(context.call.underlying_price, context.put.underlying_price)
            * BPS
        )
        if price_divergence_bps > self.config.maximum_underlying_divergence_bps:
            return "option_underlying_price_divergence"
        if abs(volatility_edge) < self.config.minimum_volatility_edge:
            return "volatility_edge_below_threshold"
        return None


def _arbitrage_bounds(
    spot: Decimal,
    strike: Decimal,
    time_years: Decimal,
    right: OptionRight,
    risk_free_rate: Decimal,
    carry_yield: Decimal,
) -> tuple[Decimal, Decimal]:
    discounted_spot = spot * _decimal(math.exp(-float(carry_yield * time_years)))
    discounted_strike = strike * _decimal(math.exp(-float(risk_free_rate * time_years)))
    if right is OptionRight.CALL:
        return max(ZERO, discounted_spot - discounted_strike), discounted_spot
    return max(ZERO, discounted_strike - discounted_spot), discounted_strike


def _strike(quote: OptionQuote) -> Decimal:
    strike = quote.instrument.strike_price
    assert strike is not None
    return strike


def _right(quote: OptionQuote) -> OptionRight:
    right = quote.instrument.option_right
    assert right is not None
    return right


def _cdf(value: float) -> float:
    return 0.5 * (1 + math.erf(value / math.sqrt(2)))


def _decimal(value: float) -> Decimal:
    if not math.isfinite(value):
        raise ValueError("option model produced a non-finite value")
    return Decimal(str(value))


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
