"""Venue-separated virtual balances and portfolio accounting."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import FundingSnapshot
from funding_arbitrage.portfolio.position import PaperPosition, PositionState


class PortfolioSnapshot(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    equity: Decimal
    cash: Decimal
    locked_capital: Decimal
    total_pnl: Decimal
    funding_pnl: Decimal
    fees: Decimal
    balances: dict[str, Decimal]


class PaperPortfolio:
    def __init__(
        self,
        initial_balance: Decimal,
        venues: tuple[str, ...],
        reserve_percent: Decimal = Decimal("20"),
    ) -> None:
        if initial_balance <= 0:
            raise ValueError("initial balance must be positive")
        self.initial_balance = initial_balance
        reserve = initial_balance * reserve_percent / Decimal("100")
        tradable = initial_balance - reserve
        self.balances: dict[str, Decimal] = {
            venue: tradable / Decimal(len(venues)) for venue in venues
        }
        self.balances["Reserve"] = reserve
        self.positions: dict[str, PaperPosition] = {}
        self.total_realized_pnl = Decimal("0")

    @property
    def cash(self) -> Decimal:
        total = Decimal("0")
        for value in self.balances.values():
            total += value
        return total

    @property
    def locked_capital(self) -> Decimal:
        total = Decimal("0")
        for position in self.positions.values():
            if position.state is PositionState.OPEN:
                total += position.capital
        return total

    def can_allocate(self, venue: str, amount: Decimal) -> bool:
        return amount > 0 and self.balances.get(venue, Decimal("0")) >= amount

    def allocate(self, venue: str, amount: Decimal) -> None:
        if not self.can_allocate(venue, amount):
            raise ValueError(f"insufficient virtual balance on {venue}")
        self.balances[venue] -= amount

    def release(self, venue: str, amount: Decimal) -> None:
        self.balances[venue] = self.balances.get(venue, Decimal("0")) + amount

    def add_position(self, position: PaperPosition) -> None:
        self.positions[position.id] = position

    def restore_balances(self, balances: dict[str, Decimal]) -> None:
        """Restore durable virtual balances after a process restart."""

        self.balances = dict(balances)

    def allocate_position(
        self, position: PaperPosition, venues: tuple[str, ...], amount: Decimal
    ) -> None:
        unique_venues = tuple(dict.fromkeys(venues))
        if any(not self.can_allocate(venue, amount) for venue in unique_venues):
            raise ValueError("insufficient virtual venue balance for position")
        for venue in unique_venues:
            self.allocate(venue, amount)
        position.allocated_venues = unique_venues
        self.add_position(position)

    def settle_funding(
        self, position_id: str, funding: FundingSnapshot, notional: Decimal
    ) -> Decimal:
        position = self.positions[position_id]
        if position.state is not PositionState.OPEN:
            raise ValueError("funding can only settle on open positions")
        same_exchange = [
            leg
            for leg in (position.leg_a, position.leg_b)
            if leg is not None and leg.exchange == funding.exchange
        ]
        leg = same_exchange[-1] if same_exchange else None
        if leg is None:
            raise ValueError("funding venue is not a position leg")
        direction = Decimal("1") if leg.side.upper() == "BUY" else Decimal("-1")
        pnl = -direction * notional * funding.funding_rate
        position.pnl.funding_pnl += pnl
        return pnl

    def close_position(self, position_id: str) -> Decimal:
        position = self.positions[position_id]
        if position.state is not PositionState.CLOSED:
            raise ValueError("position must be closed before realization")
        total = position.pnl.total_pnl
        self.total_realized_pnl += total
        for venue in position.allocated_venues:
            self.release(venue, position.capital)
        return total

    def snapshot(self) -> PortfolioSnapshot:
        pnl = sum(position.pnl.total_pnl for position in self.positions.values())
        funding = sum(position.pnl.funding_pnl for position in self.positions.values())
        fees = sum(position.pnl.fees for position in self.positions.values())
        return PortfolioSnapshot(
            equity=self.cash + self.locked_capital + pnl,
            cash=self.cash,
            locked_capital=self.locked_capital,
            total_pnl=pnl,
            funding_pnl=funding,
            fees=fees,
            balances=dict(self.balances),
        )
