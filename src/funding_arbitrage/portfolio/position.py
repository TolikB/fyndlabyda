"""Two-leg position state and PnL breakdown."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from funding_arbitrage.execution.base import PaperFill


class PositionState(StrEnum):
    DETECTED = "DETECTED"
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class PnLBreakdown(BaseModel):
    funding_pnl: Decimal = Decimal("0")
    basis_pnl: Decimal = Decimal("0")
    price_pnl_leg_a: Decimal = Decimal("0")
    price_pnl_leg_b: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    slippage: Decimal = Decimal("0")
    borrow_cost: Decimal = Decimal("0")

    @property
    def total_pnl(self) -> Decimal:
        return sum(self.model_dump().values(), Decimal("0"))


class PaperPosition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    opportunity_id: str
    asset: str
    capital: Decimal = Field(gt=0)
    state: PositionState = PositionState.DETECTED
    leg_a: PaperFill | None = None
    leg_b: PaperFill | None = None
    close_leg_a: PaperFill | None = None
    close_leg_b: PaperFill | None = None
    pnl: PnLBreakdown = Field(default_factory=PnLBreakdown)
    legging_risk: Decimal = Decimal("0")
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    allocated_venues: tuple[str, ...] = ()
    opportunity_key: str | None = None

    def transition(self, target: PositionState) -> None:
        allowed = {
            PositionState.DETECTED: {PositionState.OPENING, PositionState.FAILED},
            PositionState.OPENING: {PositionState.OPEN, PositionState.FAILED},
            PositionState.OPEN: {PositionState.CLOSING, PositionState.FAILED},
            PositionState.CLOSING: {PositionState.CLOSED, PositionState.FAILED},
            PositionState.CLOSED: set(),
            PositionState.FAILED: set(),
        }
        if target not in allowed[self.state]:
            raise ValueError(f"invalid position transition {self.state} -> {target}")
        self.state = target
        if target is PositionState.OPEN:
            self.opened_at = datetime.now(UTC)
        elif target is PositionState.CLOSED:
            self.closed_at = datetime.now(UTC)
