"""Paper-only two-leg executor with orderbook-aware fills."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.execution.base import ExecutionIntent, FillStatus, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.position import PaperPosition, PositionState


class PaperTradingExecutor:
    """The only execution path exposed in v1; it never sends exchange orders."""

    def __init__(self, fee_rate: Decimal = Decimal("0")) -> None:
        self.fee_rate = fee_rate

    async def open(
        self,
        opportunity: Opportunity,
        capital: Decimal,
        snapshot: MarketSnapshot,
        leg_b_price_move: Decimal = Decimal("0"),
    ) -> PaperPosition:
        position = PaperPosition(
            opportunity_id=opportunity.id, asset=opportunity.asset, capital=capital
        )
        position.transition(PositionState.OPENING)
        symbol_a = opportunity.symbol_a or opportunity.asset
        symbol_b = opportunity.symbol_b or opportunity.asset
        try:
            position.leg_a = self._fill_leg(
                opportunity.venue_a, symbol_a, opportunity.leg_a_side, capital, snapshot
            )
            if leg_b_price_move:
                position.legging_risk = abs(leg_b_price_move)
            position.leg_b = self._fill_leg(
                opportunity.venue_b or opportunity.venue_a,
                symbol_b,
                opportunity.leg_b_side,
                capital,
                snapshot,
                leg_b_price_move,
            )
            position.pnl.fees += position.leg_a.fee + position.leg_b.fee
            position.pnl.slippage += position.leg_a.slippage + position.leg_b.slippage
            if (
                position.leg_a.status is not FillStatus.FILLED
                or position.leg_b.status is not FillStatus.FILLED
            ):
                position.transition(PositionState.FAILED)
            else:
                position.transition(PositionState.OPEN)
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
        position.transition(PositionState.CLOSING)
        position.close_leg_a = self._fill_leg(
            position.leg_a.exchange,
            position.leg_a.symbol,
            self._opposite(position.leg_a.side),
            position.leg_a.filled_quantity,
            snapshot,
        )
        position.close_leg_b = self._fill_leg(
            position.leg_b.exchange,
            position.leg_b.symbol,
            self._opposite(position.leg_b.side),
            position.leg_b.filled_quantity,
            snapshot,
        )
        position.pnl.fees += position.close_leg_a.fee + position.close_leg_b.fee
        position.pnl.slippage += position.close_leg_a.slippage + position.close_leg_b.slippage
        position.pnl.price_pnl_leg_a += self._leg_pnl(position.leg_a, position.close_leg_a)
        position.pnl.price_pnl_leg_b += self._leg_pnl(position.leg_b, position.close_leg_b)
        position.transition(PositionState.CLOSED)
        return position

    def _fill_leg(
        self,
        exchange: str,
        symbol: str,
        side: str,
        capital: Decimal,
        snapshot: MarketSnapshot,
        price_move: Decimal = Decimal("0"),
    ) -> PaperFill:
        ticker = next(
            item for item in snapshot.tickers if item.exchange == exchange and item.symbol == symbol
        )
        book = snapshot.orderbooks.get((exchange, symbol))
        quantity = capital / ticker.last_price
        if book is None:
            return PaperFill(
                client_order_id=ExecutionIntent(
                    exchange=exchange, symbol=symbol, side=side, quantity=quantity
                ).client_order_id,
                exchange=exchange,
                symbol=symbol,
                side=side,
                requested_quantity=quantity,
                filled_quantity=quantity,
                price=ticker.last_price * (Decimal("1") + price_move),
                fee=capital * self.fee_rate,
                slippage=Decimal("0"),
                status=FillStatus.FILLED,
            )
        order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
        estimate = calculate_execution_price(book, order_side, quantity)
        status = FillStatus.FILLED if estimate.is_fully_filled else FillStatus.PARTIAL
        return PaperFill(
            client_order_id=ExecutionIntent(
                exchange=exchange, symbol=symbol, side=side, quantity=quantity
            ).client_order_id,
            exchange=exchange,
            symbol=symbol,
            side=side,
            requested_quantity=quantity,
            filled_quantity=estimate.filled_quantity,
            price=estimate.average_price,
            fee=estimate.consumed_notional * self.fee_rate,
            slippage=estimate.consumed_notional * estimate.slippage_percent,
            status=status,
        )

    @staticmethod
    def _opposite(side: str) -> str:
        return "SELL" if side.upper() == "BUY" else "BUY"

    @staticmethod
    def _leg_pnl(open_fill: PaperFill, close_fill: PaperFill) -> Decimal:
        if open_fill.price is None or close_fill.price is None:
            return Decimal("0")
        direction = Decimal("1") if open_fill.side.upper() == "BUY" else Decimal("-1")
        return (close_fill.price - open_fill.price) * open_fill.filled_quantity * direction
