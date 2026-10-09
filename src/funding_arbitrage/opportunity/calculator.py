"""Cost engine kept separate from strategy decisions."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.exchanges.base.models import OrderBook, Ticker
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.models import CostBreakdown, FeeSchedule


class CostEngine:
    def __init__(
        self,
        fees: dict[str, FeeSchedule] | None = None,
        default_fee: FeeSchedule | None = None,
        borrowing_cost_daily: Decimal = Decimal("0"),
        network_cost: Decimal = Decimal("0"),
        legging_cost_percent: Decimal = Decimal("0"),
    ) -> None:
        self.fees = fees or {}
        self.default_fee = default_fee or FeeSchedule(
            maker_fee=Decimal("0"), taker_fee=Decimal("0")
        )
        self.borrowing_cost_daily = borrowing_cost_daily
        self.network_cost = network_cost
        self.legging_cost_percent = legging_cost_percent

    def fee_for(self, exchange: str) -> FeeSchedule:
        return self.fees.get(exchange, self.default_fee)

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
        borrowing_required: bool = False,
    ) -> CostBreakdown:
        if notional <= 0 or holding_hours <= 0:
            raise ValueError("notional and holding_hours must be positive")
        fees_a = self.fee_for(venue_a).taker_fee
        fees_b = self.fee_for(venue_b).taker_fee
        entry_fees = notional * (fees_a + fees_b)
        exit_fees = entry_fees
        entry_spread = self._spread_cost(
            notional, ticker_a, orderbook_a
        ) + self._spread_cost(notional, ticker_b, orderbook_b)
        exit_spread = entry_spread
        entry_slippage = self._slippage_cost(notional, ticker_a, orderbook_a, side_a)
        entry_slippage += self._slippage_cost(notional, ticker_b, orderbook_b, side_b)
        exit_slippage = entry_slippage
        borrow = (
            notional * self.borrowing_cost_daily * holding_hours / Decimal("24")
            if borrowing_required
            else Decimal("0")
        )
        return CostBreakdown(
            entry_fees=entry_fees,
            exit_fees=exit_fees,
            entry_spread=entry_spread,
            exit_spread=exit_spread,
            entry_slippage=entry_slippage,
            exit_slippage=exit_slippage,
            borrowing_cost=borrow,
            network_cost=self.network_cost,
            legging_cost=notional * self.legging_cost_percent,
        )

    @staticmethod
    def _spread_cost(
        notional: Decimal,
        ticker: Ticker | None,
        orderbook: OrderBook | None = None,
    ) -> Decimal:
        if ticker is None:
            return notional
        best_bid = ticker.best_bid
        best_ask = ticker.best_ask
        if (best_bid is None or best_ask is None) and orderbook is not None:
            best_bid = orderbook.bids[0].price if orderbook.bids else None
            best_ask = orderbook.asks[0].price if orderbook.asks else None
        if (
            best_bid is None
            or best_ask is None
            or best_bid <= 0
            or best_ask <= 0
            or best_bid > best_ask
        ):
            return notional
        midpoint = (best_bid + best_ask) / Decimal("2")
        if midpoint <= 0:
            return notional
        return notional * (best_ask - best_bid) / midpoint / Decimal("2")

    @staticmethod
    def _slippage_cost(
        notional: Decimal,
        ticker: Ticker | None,
        orderbook: OrderBook | None,
        side: OrderSide,
    ) -> Decimal:
        if ticker is None or orderbook is None or ticker.last_price <= 0:
            return notional
        estimate = calculate_execution_price(orderbook, side, notional / ticker.last_price)
        return notional * estimate.slippage_percent
