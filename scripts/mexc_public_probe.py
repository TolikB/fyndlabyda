"""Read-only MEXC REST contract probe; never loads private credentials."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter


async def _first[T](stream: AsyncIterator[T]) -> T:
    try:
        async with asyncio.timeout(30):
            return await anext(stream)
    finally:
        close = getattr(stream, "aclose", None)
        if close is not None:
            await close()


async def probe() -> None:
    adapter = MexcPublicAdapter(timeout_seconds=20, max_reconnects=0)
    try:
        instruments = await adapter.get_instruments()
        print(
            "instruments",
            len(instruments),
            sum(item.instrument_type is InstrumentType.SPOT for item in instruments),
            sum(
                item.instrument_type is InstrumentType.PERPETUAL
                for item in instruments
            ),
        )
        tickers = await adapter.get_tickers()
        print("tickers", len(tickers))
        funding = await adapter.get_funding_rates()
        btc = next(item for item in funding if item.symbol == "BTC_USDT")
        print(
            "funding",
            len(funding),
            btc.symbol,
            btc.funding_interval_hours,
            btc.next_funding_time,
        )
        future_book = await adapter.get_orderbook("BTC_USDT", 5)
        print(
            "future_book",
            future_book.bids[0].price,
            future_book.bids[0].quantity,
            future_book.asks[0].price,
        )
        spot_book = await adapter.get_orderbook(
            "BTCUSDT", 5, InstrumentType.SPOT
        )
        print(
            "spot_book",
            spot_book.bids[0].price,
            spot_book.bids[0].quantity,
            spot_book.asks[0].price,
        )
        now = datetime.now(UTC)
        history = await adapter.get_funding_history(
            "BTC_USDT", now - timedelta(days=2), now
        )
        print(
            "history",
            len(history),
            history[-1].funding_timestamp if history else None,
        )
        candles = await adapter.get_candles(
            "BTC_USDT",
            InstrumentType.PERPETUAL,
            now - timedelta(hours=4),
            now,
        )
        print("candles", len(candles))
        future_ticker, spot_ticker, future_ws_book, spot_ws_book = await asyncio.gather(
            _first(
                adapter.stream_tickers(
                    [("BTC_USDT", InstrumentType.PERPETUAL)]
                )
            ),
            _first(adapter.stream_tickers([("BTCUSDT", InstrumentType.SPOT)])),
            _first(
                adapter.stream_orderbooks(
                    [("BTC_USDT", InstrumentType.PERPETUAL)], 5
                )
            ),
            _first(
                adapter.stream_orderbooks([("BTCUSDT", InstrumentType.SPOT)], 5)
            ),
        )
        print(
            "websocket",
            future_ticker.symbol,
            spot_ticker.symbol,
            future_ws_book.symbol,
            spot_ws_book.symbol,
        )
    finally:
        await adapter.close()


if __name__ == "__main__":
    asyncio.run(probe())
