"""Two-leg position state and PnL breakdown."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import InstrumentType
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
    unrealized_pnl_leg_a: Decimal = Decimal("0")
    unrealized_pnl_leg_b: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    spread: Decimal = Decimal("0")
    slippage: Decimal = Decimal("0")
    borrow_cost: Decimal = Decimal("0")
    legging_cost: Decimal = Decimal("0")

    @property
    def total_pnl(self) -> Decimal:
        return (
            self.funding_pnl
            + self.basis_pnl
            + self.price_pnl_leg_a
            + self.price_pnl_leg_b
            + self.unrealized_pnl_leg_a
            + self.unrealized_pnl_leg_b
            - self.fees
            - self.spread
            - self.slippage
            - self.borrow_cost
            - self.legging_cost
        )


class PaperPosition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    opportunity_id: str
    asset: str
    capital: Decimal = Field(gt=0)
    strategy: str | None = None
    simulation_version: str = "v1-legacy"
    entry_net_edge: Decimal = Decimal("0")
    entry_basis_percent: Decimal = Decimal("0")
    leg_a_type: InstrumentType | None = None
    leg_b_type: InstrumentType | None = None
    state: PositionState = PositionState.DETECTED
    leg_a: PaperFill | None = None
    leg_b: PaperFill | None = None
    close_leg_a: PaperFill | None = None
    close_leg_b: PaperFill | None = None
    pnl: PnLBreakdown = Field(default_factory=PnLBreakdown)
    legging_risk: Decimal = Decimal("0")
    target_settlements: tuple[datetime, ...] = ()
    target_funding_events: dict[str, datetime] = Field(default_factory=dict)
    settled_funding_at: dict[str, datetime] = Field(default_factory=dict)
    settled_funding_events: set[str] = Field(default_factory=set)
    funding_reconciliation_until: datetime | None = None
    funding_reconciliation_next_poll_at: datetime | None = None
    funding_reconciliation_completed_at: datetime | None = None
    funding_reconciliation_post_deadline_attempts: int = Field(default=0, ge=0)
    funding_reconciliation_failed_at: datetime | None = None
    funding_reconciliation_failure_reason: str | None = None
    funding_events: int = Field(default=0, ge=0)
    edge_miss_count: int = Field(default=0, ge=0)
    exit_requested_at: datetime | None = None
    exit_requested_reason: str | None = None
    borrow_rate_daily: Decimal = Field(default=Decimal("0"), ge=0)
    borrow_accrued_until: datetime | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    allocated_venues: tuple[str, ...] = ()
    opportunity_key: str | None = None
    exposure_key: str | None = None

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
