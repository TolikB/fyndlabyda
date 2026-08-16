"""Perpetual-versus-dated-future basis convergence strategy."""

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
from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)

ZERO = Decimal("0")
BPS = Decimal("10000")
YEAR_SECONDS = Decimal("31536000")


class BasisMarketLeg(BaseModel):
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


class ForecastFundingEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    settlement_time: datetime
    funding_rate: Decimal

    @field_validator("settlement_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class DatedBasisCosts(BaseModel):
    model_config = ConfigDict(frozen=True)

    entry_exit_fees_bps: Decimal = Field(ge=0)
    spread_bps: Decimal = Field(ge=0)
    slippage_bps: Decimal = Field(ge=0)
    capital_cost_bps: Decimal = Field(default=ZERO, ge=0)
    transfer_cost_bps: Decimal = Field(default=ZERO, ge=0)
    operational_buffer_bps: Decimal = Field(default=ZERO, ge=0)

    @property
    def total_bps(self) -> Decimal:
        return sum(
            (
                self.entry_exit_fees_bps,
                self.spread_bps,
                self.slippage_bps,
                self.capital_cost_bps,
                self.transfer_cost_bps,
                self.operational_buffer_bps,
            ),
            ZERO,
        )


class DatedBasisContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    perpetual: BasisMarketLeg
    future: BasisMarketLeg
    funding_events: tuple[ForecastFundingEvent, ...]
    costs: DatedBasisCosts
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    margin_available: bool

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_pair(self) -> DatedBasisContext:
        if self.perpetual.instrument.instrument_type is not InstrumentType.PERPETUAL:
            raise ValueError("perpetual leg must be a perpetual instrument")
        if (
            self.future.instrument.instrument_type is not InstrumentType.FUTURE
            or self.future.instrument.expiry is None
        ):
            raise ValueError("future leg must have a dated expiry")
        if (
            self.perpetual.instrument.base_asset != self.future.instrument.base_asset
            or self.perpetual.instrument.quote_asset != self.future.instrument.quote_asset
        ):
            raise ValueError("basis legs must share base and quote assets")
        return self


class DatedBasisConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str = "dated-futures-basis-v1"
    maximum_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    maximum_days_to_expiry: Decimal = Field(default=Decimal("120"), gt=0)
    minimum_absolute_basis_bps: Decimal = Field(default=Decimal("25"), gt=0)
    minimum_annualized_basis_percent: Decimal = Field(default=Decimal("5"), gt=0)
    minimum_carry_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    ttl_seconds: int = Field(default=15, gt=0)


class DatedBasisEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    basis_bps: Decimal
    annualized_basis_percent: Decimal
    expected_funding_bps: Decimal
    expected_gross_carry_bps: Decimal
    expected_net_carry_bps: Decimal
    included_settlements: tuple[datetime, ...] = ()

    @model_validator(mode="after")
    def require_exact_outcome(self) -> DatedBasisEvaluation:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("dated-basis evaluation requires exactly one outcome")
        return self


class DatedFuturesBasisStrategy:
    def __init__(self, config: DatedBasisConfig | None = None) -> None:
        self.config = config or DatedBasisConfig()

    def evaluate(self, context: DatedBasisContext) -> DatedBasisEvaluation:
        expiry = context.future.instrument.expiry
        assert expiry is not None
        now = context.timestamp
        seconds_to_expiry = Decimal(str((expiry - now).total_seconds()))
        basis_bps = (
            (context.future.price - context.perpetual.price)
            / context.perpetual.price
            * BPS
        )
        annualized_percent = (
            basis_bps / BPS * YEAR_SECONDS / seconds_to_expiry * Decimal("100")
            if seconds_to_expiry > 0
            else ZERO
        )
        future_side = Side.SELL if basis_bps > 0 else Side.BUY
        perpetual_side = Side.BUY if future_side is Side.SELL else Side.SELL
        due_events = tuple(
            event
            for event in sorted(
                context.funding_events, key=lambda item: item.settlement_time
            )
            if now < event.settlement_time <= expiry
        )
        perpetual_sign = Decimal("1") if perpetual_side is Side.BUY else Decimal("-1")
        funding_bps = sum(
            (-perpetual_sign * event.funding_rate * BPS for event in due_events),
            ZERO,
        )
        gross = abs(basis_bps) + funding_bps
        net = gross - context.costs.total_bps
        metrics = {
            "basis_bps": basis_bps,
            "annualized_basis_percent": annualized_percent,
            "expected_funding_bps": funding_bps,
            "expected_gross_carry_bps": gross,
            "expected_net_carry_bps": net,
            "included_settlements": tuple(event.settlement_time for event in due_events),
        }
        rejection = self._rejection(
            context,
            seconds_to_expiry,
            basis_bps,
            annualized_percent,
            gross,
        )
        if rejection is not None:
            return DatedBasisEvaluation(rejection_reason=rejection, **metrics)
        signal_id = _signal_id(
            self.config.strategy_id,
            context.perpetual.instrument.canonical_id,
            context.future.instrument.canonical_id,
            now.isoformat(),
            future_side,
        )
        holding_seconds = max(1, int(seconds_to_expiry))
        confidence = min(
            Decimal("1"),
            net / max(self.config.minimum_absolute_basis_bps, Decimal("1")),
        )
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=self.config.strategy_id,
            mode=context.mode,
            signal_type=SignalType.DATED_FUTURES_BASIS,
            primary_instrument=context.future.instrument,
            side=future_side,
            legs=(
                SignalLeg(instrument=context.perpetual.instrument, side=perpetual_side),
                SignalLeg(instrument=context.future.instrument, side=future_side),
            ),
            regime=context.regime,
            quality_score=confidence * Decimal("100"),
            confidence=confidence,
            expected_holding_seconds=holding_seconds,
            expected_move_bps=gross,
            estimated_cost_bps=context.costs.total_bps,
            created_at=now,
            expires_at=now + timedelta(seconds=self.config.ttl_seconds),
            evidence={
                "basis_bps": str(basis_bps),
                "annualized_basis_percent": str(annualized_percent),
                "expected_funding_bps": str(funding_bps),
                "expected_net_carry_bps": str(net),
                "expiry": expiry.isoformat(),
                "settlements": tuple(
                    {
                        "timestamp": event.settlement_time.isoformat(),
                        "rate": str(event.funding_rate),
                    }
                    for event in due_events
                ),
            },
        )
        return DatedBasisEvaluation(intent=intent, **metrics)

    def _rejection(
        self,
        context: DatedBasisContext,
        seconds_to_expiry: Decimal,
        basis_bps: Decimal,
        annualized_percent: Decimal,
        gross: Decimal,
    ) -> str | None:
        if context.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            return "unsafe_regime"
        if not context.margin_available:
            return "margin_unavailable"
        if any(
            leg.data_quality is not DataQuality.VALID
            for leg in (context.perpetual, context.future)
        ):
            return "market_quality_not_valid"
        max_age = self.config.maximum_age_seconds
        market_ages = tuple(
            Decimal(str((context.timestamp - leg.timestamp).total_seconds()))
            for leg in (context.perpetual, context.future)
        )
        if any(age < 0 for age in market_ages):
            return "market_timestamp_in_future"
        if any(age > max_age for age in market_ages):
            return "market_data_stale"
        if seconds_to_expiry <= 0:
            return "future_expired"
        if seconds_to_expiry > self.config.maximum_days_to_expiry * Decimal("86400"):
            return "expiry_too_distant"
        if abs(basis_bps) < self.config.minimum_absolute_basis_bps:
            return "basis_below_threshold"
        if abs(annualized_percent) < self.config.minimum_annualized_basis_percent:
            return "annualized_basis_below_threshold"
        if gross <= 0:
            return "nonpositive_expected_carry"
        if gross < context.costs.total_bps * self.config.minimum_carry_to_cost_ratio:
            return "insufficient_carry_to_cost"
        return None


def _signal_id(*parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"sig_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
