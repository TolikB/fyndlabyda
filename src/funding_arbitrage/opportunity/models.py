"""Normalized opportunity and cost contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class StrategyName(StrEnum):
    SPOT_PERP = "spot_perp"
    CROSS_EXCHANGE_FUNDING = "cross_exchange_funding"
    PERP_PERP = "perp_perp"
    FUTURES_BASIS = "futures_basis"


class OpportunityStatus(StrEnum):
    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"
    EXPIRED = "expired"


class FeeSchedule(BaseModel):
    maker_fee: Decimal = Field(ge=0)
    taker_fee: Decimal = Field(ge=0)


class CostBreakdown(BaseModel):
    entry_fees: Decimal = Field(ge=0)
    exit_fees: Decimal = Field(ge=0)
    entry_spread: Decimal = Field(ge=0)
    exit_spread: Decimal = Field(ge=0)
    entry_slippage: Decimal = Field(ge=0)
    exit_slippage: Decimal = Field(ge=0)
    borrowing_cost: Decimal = Field(ge=0)
    network_cost: Decimal = Field(ge=0)
    legging_cost: Decimal = Field(default=Decimal("0"), ge=0)

    @property
    def total(self) -> Decimal:
        return sum(self.model_dump().values(), Decimal("0"))


class SizeQuote(BaseModel):
    capital: Decimal = Field(gt=0)
    gross_profit: Decimal
    net_profit: Decimal
    net_return_percent: Decimal
    net_apr: Decimal
    costs: CostBreakdown
    fully_filled: bool = True


class Opportunity(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    id: str = Field(default_factory=lambda: str(uuid4()))
    strategy: StrategyName
    asset: str
    venue_a: str
    venue_b: str | None = None
    symbol_a: str | None = None
    symbol_b: str | None = None
    leg_a_type: str
    leg_b_type: str
    leg_a_side: str
    leg_b_side: str
    price_a: Decimal = Field(gt=0)
    price_b: Decimal = Field(gt=0)
    funding_a: Decimal = Decimal("0")
    funding_b: Decimal = Decimal("0")
    unstable_funding: bool = False
    gross_edge: Decimal
    trading_fees: Decimal = Field(default=Decimal("0"), ge=0)
    estimated_slippage: Decimal = Field(default=Decimal("0"), ge=0)
    borrow_cost: Decimal = Field(default=Decimal("0"), ge=0)
    other_costs: Decimal = Field(default=Decimal("0"), ge=0)
    spread_percent: Decimal = Field(default=Decimal("0"), ge=0)
    net_edge: Decimal
    expected_holding_hours: Decimal = Field(gt=0)
    net_apr: Decimal
    available_liquidity: Decimal = Field(ge=0)
    risk_score: Decimal = Field(ge=0, le=100)
    liquidity_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    funding_stability_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    persistence_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    funding_sample_count: int = Field(default=0, ge=0)
    opportunity_score: Decimal = Decimal("0")
    basis_percent: Decimal = Decimal("0")
    status: OpportunityStatus = OpportunityStatus.CANDIDATE
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    size_quotes: list[SizeQuote] = Field(default_factory=list)

    def with_expiry(self, seconds: int) -> Opportunity:
        self.expires_at = self.created_at + timedelta(seconds=seconds)
        return self
