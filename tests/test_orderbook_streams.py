import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    InstrumentType,
    OrderBook,
)
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.collector import MarketDataCollector


def test_bybit_orderbook_snapshot_and_delta_are_merged() -> None:
    adapter = BybitPublicAdapter()
    state: dict[str, dict[str, dict[Decimal, Decimal]]] = {}
    snapshot = adapter._apply_ws_orderbook(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1735689600000,
            "data": {
                "s": "BTCUSDT",
                "u": 10,
                "b": [["99", "2"], ["98", "3"]],
                "a": [["101", "4"], ["102", "5"]],
            },
        },
        state,
        InstrumentType.PERPETUAL,
        20,
    )
    delta = adapter._apply_ws_orderbook(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1735689601000,
            "data": {
                "s": "BTCUSDT",
                "u": 11,
                "b": [["99", "0"], ["100", "1"]],
                "a": [["101", "6"]],
            },
        },
        state,
        InstrumentType.PERPETUAL,
        20,
    )

    assert snapshot is not None and delta is not None
    assert delta.sequence == 11
    assert delta.bids[0].price == Decimal("100")
    assert delta.asks[0].quantity == Decimal("6")


def test_snapshot_orderbook_parsers_are_typed() -> None:
    binance = BinancePublicAdapter()._parse_ws_orderbook(
        {
            "e": "depthUpdate",
            "E": 1735689600000,
            "s": "BTCUSDT",
            "u": 7,
            "b": [["99", "2"]],
            "a": [["101", "3"]],
        },
        InstrumentType.PERPETUAL,
        20,
    )
    binance_spot = BinancePublicAdapter()._parse_ws_orderbook(
        {
            "s": "BTCUSDT",
            "lastUpdateId": 8,
            "bids": [["99", "2"]],
            "asks": [["101", "3"]],
        },
        InstrumentType.SPOT,
        20,
    )
    okx = OkxPublicAdapter()._parse_ws_orderbook(
        "BTC-USDT-SWAP",
        {
            "ts": "1735689600000",
            "seqId": "8",
            "bids": [["99", "2", "0", "1"]],
            "asks": [["101", "3", "0", "1"]],
        },
        InstrumentType.PERPETUAL,
        20,
    )
    hyperliquid = HyperliquidPublicAdapter()._parse_ws_orderbook(
        {
            "coin": "BTC",
            "time": 1735689600000,
            "levels": [
                [{"px": "99", "sz": "2", "n": 1}],
                [{"px": "101", "sz": "3", "n": 1}],
            ],
        },
        InstrumentType.PERPETUAL,
        20,
    )
    gate_spot = GatePublicAdapter()._parse_ws_orderbook(
        {
            "s": "BTC_USDT",
            "t": 1735689600000,
            "lastUpdateId": 9,
            "bids": [["99", "2"]],
            "asks": [["101", "3"]],
        },
        InstrumentType.SPOT,
        20,
    )
    gate_perp = GatePublicAdapter()._parse_ws_orderbook(
        {
            "contract": "BTC_USDT",
            "t": 1735689600000,
            "id": 10,
            "bids": [{"p": "99", "s": "2"}],
            "asks": [{"p": "101", "s": "3"}],
        },
        InstrumentType.PERPETUAL,
        20,
    )

    for book in (binance, binance_spot, okx, hyperliquid, gate_spot, gate_perp):
        assert book.timestamp.tzinfo is UTC
        assert book.bids[0].price == Decimal("99")
        assert book.asks[0].price == Decimal("101")


@pytest.mark.asyncio
async def test_collector_uses_websocket_book_before_rest_revalidation() -> None:
    class CountingMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.rest_books = 0

        async def get_orderbook(
            self,
            symbol: str,
            depth: int,
            instrument_type: InstrumentType = InstrumentType.PERPETUAL,
        ) -> OrderBook:
            self.rest_books += 1
            return await super().get_orderbook(symbol, depth, instrument_type)

        def stream_orderbooks(
            self,
            symbols: list[tuple[str, InstrumentType]],
            depth: int = 20,
        ) -> AsyncIterator[OrderBook]:
            return self._one_book_stream(symbols, depth)

        async def _one_book_stream(
            self,
            symbols: list[tuple[str, InstrumentType]],
            depth: int,
        ) -> AsyncIterator[OrderBook]:
            for symbol, instrument_type in symbols:
                yield await MockExchangeAdapter.get_orderbook(
                    self, symbol, depth, instrument_type
                )
            await asyncio.Event().wait()

    adapter = CountingMock()
    collector = MarketDataCollector(
        [adapter], enable_streams=True, rest_validation_seconds=60
    )
    request = {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
    await collector.collect_once(request)
    initial_rest_books = adapter.rest_books
    for _ in range(20):
        if collector._stream_orderbook_cache:
            break
        await asyncio.sleep(0)
    await collector.collect_once(request)
    await collector.close()

    assert collector._stream_orderbook_cache
    assert initial_rest_books > 0
    assert adapter.rest_books == initial_rest_books


@pytest.mark.asyncio
async def test_collector_only_refetches_cached_history_when_forced() -> None:
    class CountingHistoryMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.history_calls = 0

        async def get_funding_history(
            self, symbol: str, start: datetime, end: datetime
        ) -> list[FundingHistoryPoint]:
            self.history_calls += 1
            return await super().get_funding_history(symbol, start, end)

    adapter = CountingHistoryMock()
    collector = MarketDataCollector([adapter], enable_streams=False)

    await collector.collect_once(include_history=True)
    await collector.collect_once(include_history=True, force_history_refresh=False)
    assert adapter.history_calls == 1

    await collector.collect_once(include_history=True, force_history_refresh=True)
    assert adapter.history_calls == 2
