"""Ledger-backed virtual account of one paper series.

Cash model (all amounts in the quote stablecoin):

* opening a leg moves its collateral from ``cash`` to ``locked_capital`` and pays
  the taker fee from ``cash``;
* funding settlements credit or debit ``cash``;
* closing a leg releases the collateral, books the realized price PnL computed
  from the entry and exit VWAPs, and pays the exit fee.

Hence ``equity = cash + locked_capital + unrealized_pnl`` and
``equity - initial_balance = realized_pnl + unrealized_pnl``. The invariant
check compares the ledger aggregates with independently computed position
figures, which is what makes a drift visible instead of self-consistent.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from funding_arbitrage.execution.base import PaperFill
from funding_arbitrage.portfolio.funding import FundingPayment
from funding_arbitrage.portfolio.ledger import LedgerEntry, LedgerEntryType, LedgerTotals
from funding_arbitrage.portfolio.position import PaperPosition, PositionState

INVARIANT_TOLERANCE = Decimal("0.01")


class AccountSnapshot(BaseModel):
    series_id: str
    timestamp: datetime
    initial_balance: Decimal
    equity: Decimal
    cash: Decimal
    locked_capital: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    total_pnl: Decimal
    funding_pnl: Decimal
    fees: Decimal
    slippage: Decimal
    exposure: Decimal
    open_positions: int
    invariant_diff: Decimal

    @property
    def invariant_ok(self) -> bool:
        return self.invariant_diff <= INVARIANT_TOLERANCE


class PaperAccount:
    def __init__(
        self,
        series_id: str,
        initial_balance: Decimal,
        *,
        totals: LedgerTotals | None = None,
        open_positions: Iterable[PaperPosition] = (),
        closed_positions_pnl: Decimal = Decimal("0"),
        closed_slippage: Decimal = Decimal("0"),
    ) -> None:
        if initial_balance <= 0:
            raise ValueError("initial balance must be positive")
        self.series_id = series_id
        self.initial_balance = initial_balance
        self.totals = totals.model_copy() if totals is not None else LedgerTotals()
        self.positions: dict[str, PaperPosition] = {}
        for position in open_positions:
            if position.state is not PositionState.OPEN:
                raise ValueError("only open positions are restored into an account")
            self.positions[position.id] = position
        # Position-side aggregate of every closed position's booked PnL.
        self.closed_positions_pnl = closed_positions_pnl
        self.closed_slippage = closed_slippage
        self.pending_ledger: list[LedgerEntry] = []
        self.pending_fills: list[PaperFill] = []
        self.pending_funding: list[FundingPayment] = []
        self.dirty_positions: dict[str, PaperPosition] = {}
        self.halted_reason: str | None = None

    # ------------------------------------------------------------------ figures
    @property
    def cash(self) -> Decimal:
        return self.initial_balance + self.totals.realized + self.totals.collateral

    @property
    def locked_capital(self) -> Decimal:
        return sum((position.capital for position in self.positions.values()), Decimal("0"))

    @property
    def unrealized_pnl(self) -> Decimal:
        return sum((position.unrealized_pnl for position in self.positions.values()), Decimal("0"))

    @property
    def exposure(self) -> Decimal:
        return sum((position.exposure for position in self.positions.values()), Decimal("0"))

    @property
    def equity(self) -> Decimal:
        return self.cash + self.locked_capital + self.unrealized_pnl

    def invariant_diff(self) -> Decimal:
        open_booked = sum(
            (position.booked_pnl for position in self.positions.values()), Decimal("0")
        )
        collateral_gap = abs(self.totals.locked - self.locked_capital)
        pnl_gap = abs(self.totals.realized - (self.closed_positions_pnl + open_booked))
        equity_gap = abs(
            self.equity - (self.initial_balance + self.totals.realized + self.unrealized_pnl)
        )
        return max(collateral_gap, pnl_gap, equity_gap)

    def snapshot(self, now: datetime) -> AccountSnapshot:
        slippage = self.closed_slippage + sum(
            (position.slippage for position in self.positions.values()), Decimal("0")
        )
        unrealized = self.unrealized_pnl
        return AccountSnapshot(
            series_id=self.series_id,
            timestamp=now,
            initial_balance=self.initial_balance,
            equity=self.cash + self.locked_capital + unrealized,
            cash=self.cash,
            locked_capital=self.locked_capital,
            unrealized_pnl=unrealized,
            realized_pnl=self.totals.realized,
            total_pnl=self.totals.realized + unrealized,
            funding_pnl=self.totals.funding,
            fees=-self.totals.fees,
            slippage=slippage,
            exposure=self.exposure,
            open_positions=len(self.positions),
            invariant_diff=self.invariant_diff(),
        )

    # ---------------------------------------------------------------- mutations
    def _record(
        self,
        entry_type: LedgerEntryType,
        amount: Decimal,
        reference: str,
        timestamp: datetime,
        position_id: str | None,
    ) -> None:
        entry = LedgerEntry(
            series_id=self.series_id,
            position_id=position_id,
            entry_type=entry_type,
            amount=amount,
            reference=reference,
            timestamp=timestamp,
        )
        self.totals.apply(entry)
        self.pending_ledger.append(entry)

    def open_position(self, position: PaperPosition, fills: list[PaperFill]) -> None:
        if position.state is not PositionState.OPEN:
            raise ValueError("only open positions can be added")
        if position.id in self.positions:
            raise ValueError("position already open")
        if len(fills) != len(position.legs):
            raise ValueError("one fill per leg is required")
        required = position.capital + sum((fill.fee for fill in fills), Decimal("0"))
        if required > self.cash:
            raise ValueError("insufficient virtual cash")
        for index, (leg, fill) in enumerate(zip(position.legs, fills, strict=True)):
            self._record(
                LedgerEntryType.COLLATERAL_LOCK,
                -leg.collateral,
                f"{position.id}:lock:{index}",
                fill.timestamp,
                position.id,
            )
            self._record(LedgerEntryType.FEE, -fill.fee, fill.fill_id, fill.timestamp, position.id)
        self.positions[position.id] = position
        self.dirty_positions[position.id] = position
        self.pending_fills.extend(fills)

    def settle_funding(self, position: PaperPosition, payment: FundingPayment) -> None:
        if self.positions.get(position.id) is not position:
            raise ValueError("funding can only settle on open positions of this account")
        leg = position.legs[payment.leg_index]
        if not leg.is_perpetual:
            raise ValueError("funding applies to perpetual legs only")
        if leg.exchange != payment.exchange or leg.symbol != payment.symbol:
            raise ValueError("funding payment does not match the leg's market")
        if leg.last_funding_time is not None and payment.funding_timestamp <= leg.last_funding_time:
            raise ValueError("funding event already settled")
        self._record(
            LedgerEntryType.FUNDING,
            payment.amount,
            payment.reference,
            payment.funding_timestamp,
            position.id,
        )
        leg.funding_pnl += payment.amount
        leg.funding_events += 1
        leg.last_funding_time = payment.funding_timestamp
        self.pending_funding.append(payment)
        self.dirty_positions[position.id] = position

    def close_position(
        self, position: PaperPosition, fills: list[PaperFill], reason: str, now: datetime
    ) -> Decimal:
        if self.positions.get(position.id) is not position:
            raise ValueError("only open positions of this account can be closed")
        if len(fills) != len(position.legs):
            raise ValueError("one fill per leg is required")
        for index, (leg, fill) in enumerate(zip(position.legs, fills, strict=True)):
            if fill.quantity != leg.quantity:
                raise ValueError("close fill must match the open quantity")
            leg.exit_price = fill.price
            leg.exit_fee = fill.fee
            leg.exit_slippage = fill.slippage
            self._record(
                LedgerEntryType.COLLATERAL_RELEASE,
                leg.collateral,
                f"{position.id}:release:{index}",
                fill.timestamp,
                position.id,
            )
            self._record(
                LedgerEntryType.REALIZED_PNL,
                leg.realized_price_pnl,
                f"{position.id}:pnl:{index}",
                fill.timestamp,
                position.id,
            )
            self._record(LedgerEntryType.FEE, -fill.fee, fill.fill_id, fill.timestamp, position.id)
        position.state = PositionState.CLOSED
        position.closed_at = now
        position.close_reason = reason
        del self.positions[position.id]
        self.closed_positions_pnl += position.booked_pnl
        self.closed_slippage += position.slippage
        self.dirty_positions[position.id] = position
        self.pending_fills.extend(fills)
        return position.booked_pnl

    def clear_pending(self) -> None:
        """Forget changes once they are durably committed."""

        self.pending_ledger.clear()
        self.pending_fills.clear()
        self.pending_funding.clear()
        self.dirty_positions.clear()

    @property
    def has_pending(self) -> bool:
        return bool(
            self.pending_ledger
            or self.pending_fills
            or self.pending_funding
            or self.dirty_positions
        )
