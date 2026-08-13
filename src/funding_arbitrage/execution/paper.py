"""Paper-only two-leg executor with orderbook-aware fills."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.exchanges.base.models import InstrumentType, OrderBook, Ticker
from funding_arbitrage.execution.base import ExecutionIntent, FillStatus, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.position import PaperPosition, PositionState


class PaperTradingExecutor:
    """The only execution path exposed in v1; it never sends exchange orders."""

    def __init__(
        self,
        fee_rate: Decimal = Decimal("0"),
        fees: dict[str, Decimal] | None = None,
        stale_seconds: int = 30,
        simulation_version: str = "v20-oos-candidate",
        legging_move_percent: Decimal = Decimal("0"),
    ) -> None:
        self.fee_rate = fee_rate
        self.fees = fees or {}
        self.stale_seconds = stale_seconds
        self.simulation_version = simulation_version
        self.legging_move_percent = legging_move_percent

    async def open(
        self,
        opportunity: Opportunity,
        capital: Decimal,
        snapshot: MarketSnapshot,
        leg_b_price_move: Decimal | None = None,
    ) -> PaperPosition:
        position = PaperPosition(
            opportunity_id=opportunity.id,
            asset=opportunity.asset,
            capital=capital,
            strategy=str(opportunity.strategy),
            simulation_version=self.simulation_version,
            entry_net_edge=opportunity.net_edge,
            entry_basis_percent=opportunity.basis_percent,
            leg_a_type=InstrumentType(opportunity.leg_a_type),
            leg_b_type=InstrumentType(opportunity.leg_b_type),
        )
        position.transition(PositionState.OPENING)
        symbol_a = opportunity.symbol_a or opportunity.asset
        symbol_b = opportunity.symbol_b or opportunity.asset
        try:
            leg_a = self._fill_notional(
                opportunity.venue_a,
                symbol_a,
                InstrumentType(opportunity.leg_a_type),
                opportunity.leg_a_side,
                capital,
                snapshot,
            )
            if leg_b_price_move is None:
                direction = (
                    Decimal("1")
                    if opportunity.leg_b_side.upper() == "BUY"
                    else Decimal("-1")
                )
                leg_b_price_move = self.legging_move_percent * direction
            leg_b = self._fill_notional(
                opportunity.venue_b or opportunity.venue_a,
                symbol_b,
                InstrumentType(opportunity.leg_b_type),
                opportunity.leg_b_side,
                capital,
                snapshot,
                leg_b_price_move,
            )
            # Paper opens are fill-or-kill. This prevents a rejected second leg
            # from leaving an untracked first-leg exposure in the virtual ledger.
            if leg_a.status is not FillStatus.FILLED or leg_b.status is not FillStatus.FILLED:
                position.transition(PositionState.FAILED)
                return position
            position.leg_a = leg_a
            position.leg_b = leg_b
            if leg_b_price_move:
                position.legging_risk = abs(leg_b_price_move)
                position.pnl.legging_cost = capital * position.legging_risk
            position.pnl.fees += position.leg_a.fee + position.leg_b.fee
            position.pnl.spread += position.leg_a.spread + position.leg_b.spread
            position.pnl.slippage += position.leg_a.slippage + position.leg_b.slippage
            position.transition(PositionState.OPEN)
            borrows_spot = any(
                leg_type is InstrumentType.SPOT and leg.side.upper() == "SELL"
                for leg, leg_type in (
                    (position.leg_a, position.leg_a_type),
                    (position.leg_b, position.leg_b_type),
                )
            )
            if borrows_spot:
                if opportunity.borrow_cost <= 0:
                    raise ValueError("paper spot short requires a positive borrow rate")
                position.borrow_rate_daily = (
                    opportunity.borrow_cost
                    * Decimal("24")
                    / opportunity.expected_holding_hours
                )
                position.borrow_accrued_until = position.opened_at
        except (KeyError, ValueError):
            position.transition(PositionState.FAILED)
        return position

    async def close(self, position: PaperPosition, snapshot: MarketSnapshot) -> PaperPosition:
        if (
            position.state is not PositionState.OPEN
            or position.leg_a is None
            or position.leg_b is None
        ):
            raise ValueError("only open positions can be closed")
        leg_a_type = position.leg_a_type or position.leg_a.instrument_type
        leg_b_type = position.leg_b_type or position.leg_b.instrument_type
        if leg_a_type is None or leg_b_type is None:
            raise ValueError("position leg instrument types are required for close")
        close_a = self._fill_quantity(
            position.leg_a.exchange,
            position.leg_a.symbol,
            leg_a_type,
            self._opposite(position.leg_a.side),
            position.leg_a.filled_quantity,
            snapshot,
        )
        close_b = self._fill_quantity(
            position.leg_b.exchange,
            position.leg_b.symbol,
            leg_b_type,
            self._opposite(position.leg_b.side),
            position.leg_b.filled_quantity,
            snapshot,
        )
        if close_a.status is not FillStatus.FILLED or close_b.status is not FillStatus.FILLED:
            return position
        position.transition(PositionState.CLOSING)
        position.close_leg_a = close_a
        position.close_leg_b = close_b
        position.pnl.fees += position.close_leg_a.fee + position.close_leg_b.fee
        position.pnl.spread += position.close_leg_a.spread + position.close_leg_b.spread
        position.pnl.slippage += position.close_leg_a.slippage + position.close_leg_b.slippage
        leg_a_pnl = self._leg_pnl(position.leg_a, position.close_leg_a)
        leg_b_pnl = self._leg_pnl(position.leg_b, position.close_leg_b)
        if position.strategy in {"spot_perp", "futures_basis"}:
            position.pnl.basis_pnl += leg_a_pnl + leg_b_pnl
        else:
            position.pnl.price_pnl_leg_a += leg_a_pnl
            position.pnl.price_pnl_leg_b += leg_b_pnl
        position.transition(PositionState.CLOSED)
        return position

    def _fill_notional(
        self,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType,
        side: str,
        notional: Decimal,
        snapshot: MarketSnapshot,
        price_move: Decimal = Decimal("0"),
    ) -> PaperFill:
        ticker, _book = self._market(
            exchange, symbol, instrument_type, snapshot
        )
        return self._fill_quantity(
            exchange,
            symbol,
            instrument_type,
            side,
            notional / ticker.last_price,
            snapshot,
            price_move,
        )

    def _fill_quantity(
        self,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType,
        side: str,
        quantity: Decimal,
        snapshot: MarketSnapshot,
        price_move: Decimal = Decimal("0"),
    ) -> PaperFill:
        ticker, book = self._market(exchange, symbol, instrument_type, snapshot)
        order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
        estimate = calculate_execution_price(book, order_side, quantity)
        status = FillStatus.FILLED if estimate.is_fully_filled else FillStatus.PARTIAL
        reference_price = ticker.last_price
        base_execution_price = estimate.average_price
        execution_price = (
            base_execution_price * (Decimal("1") + price_move)
            if base_execution_price is not None
            else None
        )
        top_price = book.asks[0].price if order_side is OrderSide.BUY else book.bids[0].price
        if base_execution_price is None:
            spread_cost = Decimal("0")
            depth_slippage = Decimal("0")
        elif order_side is OrderSide.BUY:
            spread_cost = max(top_price - reference_price, Decimal("0"))
            depth_slippage = max(base_execution_price - top_price, Decimal("0"))
        else:
            spread_cost = max(reference_price - top_price, Decimal("0"))
            depth_slippage = max(top_price - base_execution_price, Decimal("0"))
        spread_cost *= estimate.filled_quantity
        depth_slippage *= estimate.filled_quantity
        return PaperFill(
            client_order_id=ExecutionIntent(
                exchange=exchange,
                symbol=symbol,
                instrument_type=instrument_type,
                side=side,
                quantity=quantity,
            ).client_order_id,
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            side=side,
            requested_quantity=quantity,
            filled_quantity=estimate.filled_quantity,
            price=execution_price,
            reference_price=reference_price,
            fee=(execution_price or Decimal("0"))
            * estimate.filled_quantity
            * self._fee_for(exchange),
            spread=spread_cost,
            slippage=depth_slippage,
            status=status,
        )

    def _market(
        self,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType,
        snapshot: MarketSnapshot,
    ) -> tuple[Ticker, OrderBook]:
        ticker = snapshot.ticker(exchange, symbol, instrument_type)
        book = snapshot.orderbook(exchange, symbol, instrument_type)
        if ticker is None or book is None:
            raise ValueError("fresh typed ticker and orderbook are required for paper fills")
        ticker_age = (snapshot.captured_at - ticker.timestamp).total_seconds()
        book_age = (snapshot.captured_at - book.timestamp).total_seconds()
        if ticker_age > self.stale_seconds or book_age > self.stale_seconds:
            raise ValueError("stale market data cannot be used for paper fills")
        return ticker, book

    def _fee_for(self, exchange: str) -> Decimal:
        return self.fees.get(exchange, self.fee_rate)

    @staticmethod
    def _opposite(side: str) -> str:
        return "SELL" if side.upper() == "BUY" else "BUY"

    @staticmethod
    def _leg_pnl(open_fill: PaperFill, close_fill: PaperFill) -> Decimal:
        open_price = open_fill.reference_price or open_fill.price
        close_price = close_fill.reference_price or close_fill.price
        if open_price is None or close_price is None:
            return Decimal("0")
        direction = Decimal("1") if open_fill.side.upper() == "BUY" else Decimal("-1")
        return (close_price - open_price) * open_fill.filled_quantity * direction
