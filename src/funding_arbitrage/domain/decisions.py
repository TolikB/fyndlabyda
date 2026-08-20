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
    OrderType,
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

class ExecutionInstruction(BaseModel):
    """One venue-independent OMS instruction produced only after risk approval."""

    model_config = ConfigDict(frozen=True)

    leg_index: int = Field(ge=0)
    instrument: InstrumentKey
    side: Side
    order_type: OrderType
    quantity: Decimal = Field(gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    reduce_only: bool = False

    @model_validator(mode="after")
    def validate_order_price(self) -> ExecutionInstruction:
        if self.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT}:
            if self.limit_price is None:
                raise ValueError("limit execution instruction requires limit price")
        return self


class ExecutionPlan(BaseModel):
    """Expiring plan linking one strategy thesis to one risk authorization."""

    model_config = ConfigDict(frozen=True)

    plan_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    risk_decision_id: str = Field(min_length=1)
    mode: TradingMode
    created_at: datetime
    expires_at: datetime
    instructions: tuple[ExecutionInstruction, ...] = Field(min_length=1)

    @field_validator("created_at", "expires_at")
    @classmethod
    def normalize_plan_timestamps(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @model_validator(mode="after")
    def validate_plan(self) -> ExecutionPlan:
        if self.expires_at <= self.created_at:
            raise ValueError("execution plan must expire after creation")
        indexes = tuple(item.leg_index for item in self.instructions)
        if len(indexes) != len(set(indexes)):
            raise ValueError("execution plan leg indexes must be unique")
        return self

class LiveExecutionApproval(BaseModel):
    """Immutable live-execution authority produced only after validation and risk."""

    model_config = ConfigDict(frozen=True)

    intent: SignalIntent
    risk_decision: RiskDecision
    plan: ExecutionPlan
    opportunity_id: str = Field(min_length=1)
    opportunity_key: str = Field(min_length=1)
    strategy: str = Field(min_length=1)
    asset: str = Field(min_length=1)
    capital_per_leg: Decimal = Field(gt=0)
    expected_net_profit: Decimal = Field(gt=0)
    reference_prices: tuple[Decimal, Decimal]
    market_snapshot_at: datetime
    target_settlements: tuple[datetime, ...] = ()

    @field_validator("market_snapshot_at")
    @classmethod
    def normalize_market_snapshot_at(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("approval asset cannot be blank")
        return normalized

    @field_validator("target_settlements")
    @classmethod
    def normalize_settlements(
        cls, value: tuple[datetime, ...]
    ) -> tuple[datetime, ...]:
        return tuple(
            sorted(
                {
                    (item if item.tzinfo else item.replace(tzinfo=UTC)).astimezone(UTC)
                    for item in value
                }
            )
        )

    @model_validator(mode="after")
    def validate_authority_chain(self) -> LiveExecutionApproval:
        if not self.risk_decision.approved:
            raise ValueError("live execution requires approved risk")
        if self.intent.signal_id != self.risk_decision.signal_id:
            raise ValueError("approval signal/risk identity mismatch")
        if self.plan.signal_id != self.intent.signal_id:
            raise ValueError("approval signal/plan identity mismatch")
        if self.plan.risk_decision_id != self.risk_decision.decision_id:
            raise ValueError("approval risk/plan identity mismatch")
        if self.plan.mode is not self.intent.mode:
            raise ValueError("approval mode identity mismatch")
        if self.intent.mode not in {TradingMode.LIMITED_LIVE, TradingMode.LIVE}:
            raise ValueError("live approval requires an exchange-order mode")
        if len(self.plan.instructions) != 2 or len(self.intent.legs) != 2:
            raise ValueError("live funding execution requires exactly two legs")
        if self.plan.created_at < self.risk_decision.decided_at:
            raise ValueError("execution plan predates its risk authorization")
        if self.plan.expires_at > self.intent.expires_at:
            raise ValueError("execution plan outlives signal intent")
        instructions = {
            instruction.leg_index: instruction
            for instruction in self.plan.instructions
        }
        if set(instructions) != {0, 1}:
            raise ValueError("live execution plan leg indexes must be zero and one")
        for index, leg in enumerate(self.intent.legs):
            instruction = instructions[index]
            if (
                instruction.instrument != leg.instrument
                or instruction.side is not leg.side
            ):
                raise ValueError("live execution plan changed approved exposure")
            if instruction.quantity > (
                self.risk_decision.approved_quantity * leg.hedge_ratio
            ):
                raise ValueError("live execution plan exceeds approved quantity")
            if instruction.order_type is not OrderType.LIMIT:
                raise ValueError("live execution plan requires bounded limit orders")
        if any(price <= 0 for price in self.reference_prices):
            raise ValueError("approval reference prices must be positive")
        return self