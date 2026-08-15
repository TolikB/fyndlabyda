"""Declarative strategy, risk, and execution contracts with no venue side effects."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import (
    InstrumentKey,
    LiquidityRole,
    OrderStatus,
    Side,
    TradingMode,
)


class MarketRegime(StrEnum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    TRANSITION = "TRANSITION"
    VOLATILITY_EXPANSION = "VOLATILITY_EXPANSION"
    STRESS = "STRESS"
    UNKNOWN = "UNKNOWN"


class SignalType(StrEnum):
    ORDERFLOW_BREAKOUT = "ORDERFLOW_BREAKOUT"
    LIQUIDITY_SWEEP_REVERSION = "LIQUIDITY_SWEEP_REVERSION"
    FUNDING_BASIS = "FUNDING_BASIS"
    LEAD_LAG_FILTER = "LEAD_LAG_FILTER"
    CROSS_EXCHANGE_STAT_ARB = "CROSS_EXCHANGE_STAT_ARB"
    DATED_FUTURES_BASIS = "DATED_FUTURES_BASIS"
    OPTIONS_VOLATILITY = "OPTIONS_VOLATILITY"
    PASSIVE_MARKET_MAKING = "PASSIVE_MARKET_MAKING"
    GRID = "GRID"
    MARTINGALE = "MARTINGALE"
    LOSS_AVERAGING = "LOSS_AVERAGING"
    ML_META_LABEL = "ML_META_LABEL"
    RL_POLICY = "RL_POLICY"
    LLM_DECISION = "LLM_DECISION"
    DEX = "DEX"
    MEV = "MEV"


class SignalLeg(BaseModel):
    """Desired economic exposure, not an exchange-order instruction."""

    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    side: Side
    hedge_ratio: Decimal = Field(default=Decimal("1"), gt=0)
    execution_priority: int = Field(default=0, ge=0)


class SignalIntent(BaseModel):
    """A strategy's expiring thesis; only risk may turn it into executable size."""

    model_config = ConfigDict(frozen=True)

    signal_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    mode: TradingMode
    signal_type: SignalType
    primary_instrument: InstrumentKey
    side: Side
    legs: tuple[SignalLeg, ...] = Field(min_length=1)
    regime: MarketRegime
    quality_score: Decimal = Field(ge=0, le=100)
    confidence: Decimal = Field(ge=0, le=1)
    entry_zone_low: Decimal | None = Field(default=None, gt=0)
    entry_zone_high: Decimal | None = Field(default=None, gt=0)
    structural_stop: Decimal | None = Field(default=None, gt=0)
    targets: tuple[Decimal, ...] = ()
    expected_holding_seconds: int = Field(gt=0)
    expected_move_bps: Decimal
    estimated_cost_bps: Decimal = Field(ge=0)
    expected_rr: Decimal | None = Field(default=None, gt=0)
    created_at: datetime
    expires_at: datetime
    evidence: dict[str, Any] = Field(default_factory=dict)

    @field_validator("created_at", "expires_at")
    @classmethod
    def normalize_timestamps(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @field_validator("targets")
    @classmethod
    def validate_targets(cls, value: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
        if any(target <= 0 for target in value):
            raise ValueError("targets must be positive")
        return value

    @model_validator(mode="after")
    def validate_intent(self) -> SignalIntent:
        if self.expires_at <= self.created_at:
            raise ValueError("signal must expire after it is created")
        if (self.entry_zone_low is None) is not (self.entry_zone_high is None):
            raise ValueError("entry zone bounds must be supplied together")
        if (
            self.entry_zone_low is not None
            and self.entry_zone_high is not None
            and self.entry_zone_low > self.entry_zone_high
        ):
            raise ValueError("entry_zone_low cannot exceed entry_zone_high")
        directional = self.signal_type in {
            SignalType.ORDERFLOW_BREAKOUT,
            SignalType.LIQUIDITY_SWEEP_REVERSION,
        }
        if directional and (
            self.structural_stop is None
            or not self.targets
            or self.expected_rr is None
            or self.entry_zone_low is None
        ):
            raise ValueError("directional signals require entry, stop, targets, and expected R:R")
        if self.signal_type is SignalType.LEAD_LAG_FILTER and self.mode in {
            TradingMode.LIMITED_LIVE,
            TradingMode.LIVE,
        }:
            raise ValueError("lead-lag filter cannot directly request live execution")
        return self


class RiskDecision(BaseModel):
    """The sole contract allowed to authorize executable size."""

    model_config = ConfigDict(frozen=True)

    signal_id: str = Field(min_length=1)
    decision_id: str = Field(min_length=1)
    decided_at: datetime
    approved: bool
    rejection_reason: str | None = None
    approved_risk_usdt: Decimal = Field(ge=0)
    approved_quantity: Decimal = Field(ge=0)
    approved_notional: Decimal = Field(ge=0)
    max_slippage_bps: Decimal = Field(ge=0)
    max_execution_seconds: int = Field(gt=0)
    correlation_multiplier: Decimal = Field(ge=0, le=1)
    drawdown_multiplier: Decimal = Field(ge=0, le=1)
    regime_multiplier: Decimal = Field(ge=0, le=1)

    @field_validator("decided_at")
    @classmethod
    def normalize_decision_time(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @model_validator(mode="after")
    def validate_authorization(self) -> RiskDecision:
        sizing = (self.approved_risk_usdt, self.approved_quantity, self.approved_notional)
        if self.approved and (
            self.rejection_reason is not None or any(value <= 0 for value in sizing)
        ):
            raise ValueError("approved risk decision requires positive size and no rejection")
        if not self.approved and (
            not self.rejection_reason or any(value != 0 for value in sizing)
        ):
            raise ValueError("rejected risk decision requires a reason and zero size")
        return self


class ExecutionReport(BaseModel):
    """Venue-independent acknowledgement/fill result returned to OMS and ledger."""

    model_config = ConfigDict(frozen=True)

    client_order_id: str = Field(min_length=1)
    exchange_order_id: str | None = None
    status: OrderStatus
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    average_fill_price: Decimal | None = Field(default=None, gt=0)
    fee: Decimal = Field(ge=0)
    fee_asset: str | None = None
    liquidity_role: LiquidityRole = LiquidityRole.UNKNOWN
    exchange_timestamp: datetime
    receive_timestamp: datetime
    reject_code: str | None = None
    reject_message: str | None = None

    @field_validator("exchange_timestamp", "receive_timestamp")
    @classmethod
    def normalize_report_timestamps(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @model_validator(mode="after")
    def validate_report(self) -> ExecutionReport:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("filled quantity cannot exceed requested quantity")
        if self.status is OrderStatus.REJECTED and not (self.reject_code or self.reject_message):
            raise ValueError("rejected execution requires a code or message")
        return self
