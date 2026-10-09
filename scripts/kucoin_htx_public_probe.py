"""Read-only KuCoin/HTX REST and WebSocket probe; never loads private credentials."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter


async def _first[T](stream: AsyncIterator[T]) -> T:
    try:
        async with asyncio.timeout(30):
            return await anext(stream)
    finally:
        close = getattr(stream, "aclose", None)
        if close is not None:
            await close()


async def _probe_venue(
    adapter: ExchangeAdapter,
    perpetual_symbol: str,
    spot_symbol: str,
) -> None:
    instruments = await adapter.get_instruments()
    tickers = await adapter.get_tickers()
    funding = await adapter.get_funding_rates()
    sample = next(row for row in funding if row.symbol == perpetual_symbol)
    now = datetime.now(UTC)
    history = await adapter.get_funding_history(perpetual_symbol, now - timedelta(days=3), now)
    perpetual_book, spot_book, perpetual_candles, spot_candles = await asyncio.gather(
        adapter.get_orderbook(perpetual_symbol, 5, InstrumentType.PERPETUAL),
        adapter.get_orderbook(spot_symbol, 5, InstrumentType.SPOT),
        adapter.get_candles(
            perpetual_symbol,
            InstrumentType.PERPETUAL,
            now - timedelta(hours=4),
            now,
        ),
        adapter.get_candles(
            spot_symbol,
            InstrumentType.SPOT,
            now - timedelta(hours=4),
            now,
        ),
    )
    perpetual_ticker, spot_ticker, perpetual_ws_book, spot_ws_book = await asyncio.gather(
        _first(adapter.stream_tickers([(perpetual_symbol, InstrumentType.PERPETUAL)])),
        _first(adapter.stream_tickers([(spot_symbol, InstrumentType.SPOT)])),
        _first(adapter.stream_orderbooks([(perpetual_symbol, InstrumentType.PERPETUAL)], 5)),
        _first(adapter.stream_orderbooks([(spot_symbol, InstrumentType.SPOT)], 5)),
    )
    assert sample.next_funding_time is not None
    assert all(
        instrument.contract_size > 0 and instrument.settlement_asset in {"USD", "USDT", "USDC"}
        for instrument in instruments
        if instrument.instrument_type is not InstrumentType.SPOT
    )
    assert history == sorted(history, key=lambda row: row.funding_timestamp)
    assert perpetual_ticker.symbol == perpetual_symbol
    assert spot_ticker.symbol == spot_symbol
    assert perpetual_ws_book.symbol == perpetual_symbol
    assert spot_ws_book.symbol == spot_symbol
    print(
        adapter.name,
        {
            "instruments": len(instruments),
            "tickers": len(tickers),
            "funding_symbols": len(funding),
            "funding_interval_hours": str(sample.funding_interval_hours),
            "next_funding_time": sample.next_funding_time.isoformat(),
            "history_points": len(history),
            "perpetual_book_quantity": str(perpetual_book.bids[0].quantity),
            "spot_book_quantity": str(spot_book.bids[0].quantity),
            "perpetual_candles": len(perpetual_candles),
            "spot_candles": len(spot_candles),
            "websocket_symbols": (
                perpetual_ticker.symbol,
                spot_ticker.symbol,
                perpetual_ws_book.symbol,
                spot_ws_book.symbol,
            ),
        },
    )


async def probe() -> None:
    adapters: tuple[tuple[ExchangeAdapter, str, str], ...] = (
        (KucoinPublicAdapter(timeout_seconds=20, max_reconnects=0), "XBTUSDTM", "BTC-USDT"),
        (HtxPublicAdapter(timeout_seconds=20, max_reconnects=0), "BTC-USDT", "btcusdt"),
    )
    try:
        for adapter, perpetual_symbol, spot_symbol in adapters:
            await _probe_venue(adapter, perpetual_symbol, spot_symbol)
    finally:
        for adapter, _, _ in adapters:
            await adapter.close()


if __name__ == "__main__":
    asyncio.run(probe())
