"""Two-leg paper position with per-leg entry, exit, and funding accounting."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import InstrumentType


class PositionState(StrEnum):
    DETECTED = "DETECTED"
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class PositionLeg(BaseModel):
    exchange: str
    symbol: str
    instrument_type: InstrumentType
    side: str
    quantity: Decimal = Field(gt=0)
    entry_price: Decimal = Field(gt=0)
    entry_notional: Decimal = Field(gt=0)
    # Cash set aside for this leg: full notional for spot, notional / leverage for derivatives.
    collateral: Decimal = Field(gt=0)
    entry_fee: Decimal = Field(ge=0)
    entry_slippage: Decimal = Field(ge=0)
    exit_price: Decimal | None = None
    exit_fee: Decimal = Decimal("0")
    exit_slippage: Decimal = Decimal("0")
    funding_pnl: Decimal = Decimal("0")
    funding_events: int = 0
    # Funding bookkeeping (perpetual legs only).
    last_funding_time: datetime | None = None
    next_funding_time: datetime | None = None
    funding_interval_hours: Decimal | None = None
    last_history_check: datetime | None = None
    pre_funding_for: datetime | None = None
    pre_funding_mark: Decimal | None = None
    pre_funding_rate: Decimal | None = None
    mark_price: Decimal | None = None
    mark_time: datetime | None = None

    @property
    def direction(self) -> Decimal:
        return Decimal("1") if self.side.upper() == "BUY" else Decimal("-1")

    @property
    def is_perpetual(self) -> bool:
        return self.instrument_type is InstrumentType.PERPETUAL

    def price_pnl(self, price: Decimal) -> Decimal:
        return self.direction * (price - self.entry_price) * self.quantity

    @property
    def realized_price_pnl(self) -> Decimal:
        return self.price_pnl(self.exit_price) if self.exit_price is not None else Decimal("0")

    @property
    def unrealized_price_pnl(self) -> Decimal:
        if self.exit_price is not None:
            return Decimal("0")
        return self.price_pnl(self.mark_price) if self.mark_price is not None else Decimal("0")

    def funding_due(self, now: datetime) -> bool:
        return (
            self.is_perpetual
            and self.next_funding_time is not None
            and now >= self.next_funding_time
            and (self.last_funding_time is None or self.last_funding_time < self.next_funding_time)
        )


class PaperPosition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    series_id: str
    opportunity_id: str
    opportunity_key: str
    strategy: str
    asset: str
    state: PositionState = PositionState.OPEN
    legs: list[PositionLeg] = Field(min_length=2, max_length=2)
    opened_at: datetime
    closed_at: datetime | None = None
    close_reason: str | None = None
    entry_funding_rate_8h: Decimal = Decimal("0")
    entry_net_apr: Decimal = Decimal("0")
    low_edge_streak: int = 0
    simulator_version: str

    @property
    def capital(self) -> Decimal:
        return sum((leg.collateral for leg in self.legs), Decimal("0"))

    @property
    def exposure(self) -> Decimal:
        """Hedged size: the larger of the two leg notionals at entry."""

        return max(leg.entry_notional for leg in self.legs)

    @property
    def fees(self) -> Decimal:
        return sum((leg.entry_fee + leg.exit_fee for leg in self.legs), Decimal("0"))

    @property
    def slippage(self) -> Decimal:
        return sum((leg.entry_slippage + leg.exit_slippage for leg in self.legs), Decimal("0"))

    @property
    def funding_pnl(self) -> Decimal:
        return sum((leg.funding_pnl for leg in self.legs), Decimal("0"))

    @property
    def realized_price_pnl(self) -> Decimal:
        return sum((leg.realized_price_pnl for leg in self.legs), Decimal("0"))

    @property
    def unrealized_pnl(self) -> Decimal:
        return sum((leg.unrealized_price_pnl for leg in self.legs), Decimal("0"))

    @property
    def booked_pnl(self) -> Decimal:
        """Cash already moved by this position: funding, fees, and realized price PnL."""

        return self.funding_pnl + self.realized_price_pnl - self.fees

    @property
    def total_pnl(self) -> Decimal:
        return self.booked_pnl + self.unrealized_pnl

    @property
    def perpetual_legs(self) -> list[tuple[int, PositionLeg]]:
        return [(index, leg) for index, leg in enumerate(self.legs) if leg.is_perpetual]

    def funding_due(self, now: datetime) -> bool:
        return any(leg.funding_due(now) for leg in self.legs)
