"""Small builders for deterministic market snapshots in tests."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    MarketKey,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot

D = Decimal


def instrument(
    exchange: str,
    symbol: str,
    instrument_type: InstrumentType,
    base: str = "BTC",
    quote: str = "USDT",
    step: str = "0.001",
    min_size: str = "0.001",
    min_notional: str | None = None,
) -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange=exchange,
        exchange_symbol=symbol,
        base_asset=base,
        quote_asset=quote,
        instrument_type=instrument_type,
        tick_size=D("0.01"),
        step_size=D(step),
        min_order_size=D(min_size),
        min_notional=D(min_notional) if min_notional is not None else None,
        funding_interval=8 if instrument_type is InstrumentType.PERPETUAL else None,
    )


def ticker(
    exchange: str,
    symbol: str,
    instrument_type: InstrumentType,
    price: str,
    now: datetime,
    spread: str = "0.02",
    volume: str = "100000000",
) -> Ticker:
    value = D(price)
    return Ticker(
        exchange=exchange,
        symbol=symbol,
        instrument_type=instrument_type,
        last_price=value,
        mark_price=value if instrument_type is not InstrumentType.SPOT else None,
        best_bid=value - D(spread),
        best_ask=value + D(spread),
        volume_24h=D(volume),
        timestamp=now,
    )


def book(
    exchange: str,
    symbol: str,
    instrument_type: InstrumentType,
    mid: str,
    now: datetime,
    half_spread: str = "0.02",
    depth: str = "20",
    levels: int = 10,
    tick: str = "0.01",
) -> OrderBook:
    center = D(mid)
    return OrderBook(
        exchange=exchange,
        symbol=symbol,
        instrument_type=instrument_type,
        bids=tuple(
            OrderBookLevel(price=center - D(half_spread) - D(tick) * index, quantity=D(depth))
            for index in range(levels)
        ),
        asks=tuple(
            OrderBookLevel(price=center + D(half_spread) + D(tick) * index, quantity=D(depth))
            for index in range(levels)
        ),
        timestamp=now,
        received_at=now,
    )


def funding(
    exchange: str,
    symbol: str,
    rate: str,
    now: datetime,
    next_time: datetime | None,
    interval_hours: str = "8",
    mark: str | None = None,
) -> FundingSnapshot:
    return FundingSnapshot(
        exchange=exchange,
        symbol=symbol,
        funding_rate=D(rate),
        funding_interval_hours=D(interval_hours),
        next_funding_time=next_time,
        mark_price=D(mark) if mark else None,
        timestamp=now,
    )


def history_point(
    exchange: str, symbol: str, rate: str, at: datetime, mark: str | None = None
) -> FundingHistoryPoint:
    return FundingHistoryPoint(
        exchange=exchange,
        symbol=symbol,
        funding_rate=D(rate),
        funding_timestamp=at,
        mark_price=D(mark) if mark else None,
    )


def snapshot(
    now: datetime,
    instruments: list[NormalizedInstrument],
    tickers: list[Ticker],
    fundings: list[FundingSnapshot],
    books: list[OrderBook] | None = None,
    history: dict[tuple[str, str], list[FundingHistoryPoint]] | None = None,
) -> MarketSnapshot:
    orderbooks: dict[MarketKey, OrderBook] = {item.key: item for item in books or []}
    return MarketSnapshot(
        instruments=instruments,
        tickers=tickers,
        funding=fundings,
        orderbooks=orderbooks,
        captured_at=now,
        funding_history=history or {},
    )


def spot_perp_market(
    now: datetime,
    *,
    exchange: str = "bybit",
    symbol: str = "BTCUSDT",
    spot_price: str = "100",
    perp_price: str = "100.1",
    rate: str = "0.0005",
    next_time: datetime | None = None,
    with_books: bool = True,
    history: list[FundingHistoryPoint] | None = None,
) -> MarketSnapshot:
    """Same-symbol spot and perpetual on one venue (the Bybit/Binance layout)."""

    books = (
        [
            book(exchange, symbol, InstrumentType.SPOT, spot_price, now),
            book(exchange, symbol, InstrumentType.PERPETUAL, perp_price, now),
        ]
        if with_books
        else []
    )
    return snapshot(
        now,
        [
            instrument(exchange, symbol, InstrumentType.SPOT),
            instrument(exchange, symbol, InstrumentType.PERPETUAL),
        ],
        [
            ticker(exchange, symbol, InstrumentType.SPOT, spot_price, now),
            ticker(exchange, symbol, InstrumentType.PERPETUAL, perp_price, now),
        ],
        [funding(exchange, symbol, rate, now, next_time)],
        books,
        {(exchange, symbol): history} if history is not None else None,
    )
