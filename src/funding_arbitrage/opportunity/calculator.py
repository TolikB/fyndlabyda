"""Cost engine kept separate from strategy decisions."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.exchanges.base.models import InstrumentType, OrderBook, Ticker
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.models import CostBreakdown, FeeSchedule


class CostEngine:
    def __init__(
        self,
        fees: dict[str, FeeSchedule] | None = None,
        default_fee: FeeSchedule | None = None,
        borrowing_cost_daily: Decimal = Decimal("0"),
        network_cost: Decimal = Decimal("0"),
    ) -> None:
        self.fees = fees or {}
        self.default_fee = default_fee or FeeSchedule(
            maker_fee=Decimal("0"), taker_fee=Decimal("0")
        )
        self.borrowing_cost_daily = borrowing_cost_daily
        self.network_cost = network_cost

    def fee_for(self, exchange: str) -> FeeSchedule:
        return self.fees.get(exchange, self.default_fee)

    def taker_fee(self, exchange: str, instrument_type: InstrumentType) -> Decimal:
        return self.fee_for(exchange).taker(instrument_type)

    def estimate(
        self,
        notional: Decimal,
        venue_a: str,
        venue_b: str,
        holding_hours: Decimal,
        ticker_a: Ticker | None = None,
        ticker_b: Ticker | None = None,
        orderbook_a: OrderBook | None = None,
        orderbook_b: OrderBook | None = None,
        side_a: OrderSide = OrderSide.BUY,
        side_b: OrderSide = OrderSide.SELL,
        type_a: InstrumentType = InstrumentType.PERPETUAL,
        type_b: InstrumentType = InstrumentType.PERPETUAL,
    ) -> CostBreakdown:
        """Round-trip cost of opening and closing both legs of ``notional`` each.

        Spread is charged as half the quoted spread per crossing (price vs mid);
        slippage is the book-walk impact beyond the touch.
        """

        if notional <= 0 or holding_hours <= 0:
            raise ValueError("notional and holding_hours must be positive")
        entry_fees = notional * (self.taker_fee(venue_a, type_a) + self.taker_fee(venue_b, type_b))
        exit_fees = entry_fees
        entry_spread = self._half_spread_cost(notional, ticker_a, orderbook_a)
        entry_spread += self._half_spread_cost(notional, ticker_b, orderbook_b)
        exit_spread = entry_spread
        entry_slippage = self._slippage_cost(notional, ticker_a, orderbook_a, side_a)
        entry_slippage += self._slippage_cost(notional, ticker_b, orderbook_b, side_b)
        exit_slippage = self._slippage_cost(notional, ticker_a, orderbook_a, _opposite(side_a))
        exit_slippage += self._slippage_cost(notional, ticker_b, orderbook_b, _opposite(side_b))
        borrow = notional * self.borrowing_cost_daily * holding_hours / Decimal("24")
        return CostBreakdown(
            entry_fees=entry_fees,
            exit_fees=exit_fees,
            entry_spread=entry_spread,
            exit_spread=exit_spread,
            entry_slippage=entry_slippage,
            exit_slippage=exit_slippage,
            borrowing_cost=borrow,
            network_cost=self.network_cost,
        )

    @staticmethod
    def _half_spread_cost(
        notional: Decimal, ticker: Ticker | None, orderbook: OrderBook | None
    ) -> Decimal:
        bid: Decimal | None
        ask: Decimal | None
        if orderbook is not None and orderbook.bids and orderbook.asks:
            bid, ask = orderbook.bids[0].price, orderbook.asks[0].price
        elif ticker is not None:
            bid, ask = ticker.best_bid, ticker.best_ask
        else:
            return Decimal("0")
        if bid is None or ask is None or bid <= 0 or ask < bid:
            return Decimal("0")
        midpoint = (bid + ask) / Decimal("2")
        return notional * (ask - bid) / midpoint / Decimal("2")

    @staticmethod
    def _slippage_cost(
        notional: Decimal,
        ticker: Ticker | None,
        orderbook: OrderBook | None,
        side: OrderSide,
    ) -> Decimal:
        if ticker is None or orderbook is None or ticker.last_price <= 0:
            return Decimal("0")
        estimate = calculate_execution_price(orderbook, side, notional / ticker.last_price)
        return notional * estimate.slippage_percent


def _opposite(side: OrderSide) -> OrderSide:
    return OrderSide.SELL if side is OrderSide.BUY else OrderSide.BUY
