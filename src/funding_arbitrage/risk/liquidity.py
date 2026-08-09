"""Liquidity score from volume, depth, spread, and slippage."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.exchanges.base.models import OrderBook, Ticker
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price


def liquidity_score(ticker: Ticker, orderbook: OrderBook | None, notional: Decimal) -> Decimal:
    if notional <= 0:
        raise ValueError("notional must be positive")
    volume_score = min(Decimal("100"), ticker.volume_24h / notional * Decimal("10"))
    if orderbook is None or not orderbook.bids or not orderbook.asks:
        return volume_score * Decimal("0.5")
    best_bid, best_ask = orderbook.bids[0].price, orderbook.asks[0].price
    spread = (best_ask - best_bid) / ((best_ask + best_bid) / Decimal("2"))
    spread_score = max(Decimal("0"), Decimal("100") - spread * Decimal("10000"))
    bid_depth = sum(level.price * level.quantity for level in orderbook.bids)
    ask_depth = sum(level.price * level.quantity for level in orderbook.asks)
    depth_score = min(Decimal("100"), min(bid_depth, ask_depth) / notional * Decimal("100"))
    buy = calculate_execution_price(orderbook, OrderSide.BUY, notional / ticker.last_price)
    sell = calculate_execution_price(orderbook, OrderSide.SELL, notional / ticker.last_price)
    slippage_score = max(
        Decimal("0"),
        Decimal("100") - max(buy.slippage_percent, sell.slippage_percent) * Decimal("10000"),
    )
    return min(
        Decimal("100"),
        volume_score * Decimal("0.25")
        + spread_score * Decimal("0.25")
        + depth_score * Decimal("0.30")
        + slippage_score * Decimal("0.20"),
    )
