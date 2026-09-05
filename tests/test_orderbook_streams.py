import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import BookEvent, EventKind
from funding_arbitrage.exchanges.base.exceptions import NetworkError
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.monitoring.metrics import (
    exchange_stream_last_message_timestamp,
    market_data_dropped_total,
)


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
                "seq": 100,
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
                "seq": 101,
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
async def test_collector_publishes_fresh_rest_book_once_without_replaying_cache() -> None:
    class OneBookStreamMock(MockExchangeAdapter):
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

    events: list[BookEvent] = []

    async def capture(event: BookEvent) -> None:
        events.append(event)

    adapter = OneBookStreamMock("bybit", sleep=0)
    collector = MarketDataCollector(
        [adapter],
        orderbook_symbol_limit=1,
        enable_streams=True,
        rest_validation_seconds=60,
        canonical_book_event_sink=capture,
    )
    request = {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}

    await collector.collect_once(request)
    first_event_count = len(events)
    for _ in range(100):
        if collector._stream_orderbook_cache:
            break
        await asyncio.sleep(0)
    await collector.collect_once(request)
    await collector.close()

    assert first_event_count == 1
    assert len(events) == first_event_count
    assert events[0].kind is EventKind.BOOK_SNAPSHOT
    assert events[0].metadata.source == "BYBIT.PUBLIC.ORDERBOOK.REST_VALIDATION"
    assert events[0].payload.sequence >= 0


@pytest.mark.asyncio
async def test_collector_batches_distinct_rest_book_publications_concurrently() -> None:
    class TwoBookMock(MockExchangeAdapter):
        async def get_instruments(self) -> list[NormalizedInstrument]:
            instruments = await super().get_instruments()
            perpetual = next(
                item
                for item in instruments
                if item.instrument_type is InstrumentType.PERPETUAL
            )
            return [
                perpetual,
                perpetual.model_copy(
                    update={"exchange_symbol": "ETHUSDT", "base_asset": "ETH"}
                ),
            ]

        async def get_tickers(self) -> list[Ticker]:
            return [
                item
                for item in await super().get_tickers()
                if item.instrument_type is InstrumentType.PERPETUAL
            ]

    started: list[str] = []
    release = asyncio.Event()

    async def blocking_capture(event: BookEvent) -> None:
        started.append(event.payload.instrument.exchange_symbol)
        if len(started) >= 2:
            release.set()
        await asyncio.wait_for(release.wait(), timeout=1)

    adapter = TwoBookMock("bybit", sleep=0)
    collector = MarketDataCollector(
        [adapter],
        orderbook_symbol_limit=1,
        enable_streams=False,
        canonical_book_event_sink=blocking_capture,
    )

    snapshot = await asyncio.wait_for(
        collector.collect_once(
            orderbook_symbols={
                "bybit": [
                    ("BTCUSDT", InstrumentType.PERPETUAL),
                    ("ETHUSDT", InstrumentType.PERPETUAL),
                ]
            }
        ),
        timeout=2,
    )
    await collector.close()

    assert sorted(started) == ["BTCUSDT", "ETHUSDT"]
    assert len(snapshot.orderbooks) == 2


@pytest.mark.asyncio
async def test_collector_does_not_swallow_canonical_rest_publication_failure() -> None:
    async def fail_publication(event: BookEvent) -> None:
        del event
        raise RuntimeError("synthetic canonical sink failure")

    adapter = MockExchangeAdapter("bybit", sleep=0)
    collector = MarketDataCollector(
        [adapter],
        orderbook_symbol_limit=1,
        enable_streams=False,
        canonical_book_event_sink=fail_publication,
    )

    with pytest.raises(
        RuntimeError, match="fresh REST order-book canonical publication failed"
    ):
        await collector.collect_once(
            {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
        )

    await collector.close()


@pytest.mark.asyncio
async def test_collector_rejects_rest_book_overtaken_by_websocket() -> None:
    class OvertakenRestMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.stream_emitted = asyncio.Event()
            now = datetime.now(UTC)
            self.older = OrderBook(
                exchange=self.name,
                symbol="BTCUSDT",
                instrument_type=InstrumentType.PERPETUAL,
                bids=(
                    OrderBookLevel(price=Decimal("99"), quantity=Decimal("2")),
                ),
                asks=(
                    OrderBookLevel(price=Decimal("101"), quantity=Decimal("3")),
                ),
                timestamp=now,
                sequence=1,
            )
            self.newer = self.older.model_copy(
                update={"timestamp": now + timedelta(seconds=1), "sequence": 2}
            )

        async def get_tickers(self) -> list[Ticker]:
            return [
                item
                for item in await super().get_tickers()
                if item.instrument_type is InstrumentType.PERPETUAL
            ]

        async def get_orderbook(
            self,
            symbol: str,
            depth: int,
            instrument_type: InstrumentType = InstrumentType.PERPETUAL,
        ) -> OrderBook:
            del symbol, depth, instrument_type
            await self.stream_emitted.wait()
            await asyncio.sleep(0.01)
            return self.older

        def stream_orderbooks(
            self,
            symbols: list[tuple[str, InstrumentType]],
            depth: int = 20,
        ) -> AsyncIterator[OrderBook]:
            del symbols, depth
            return self._newer_stream()

        async def _newer_stream(self) -> AsyncIterator[OrderBook]:
            self.stream_emitted.set()
            yield self.newer
            await asyncio.Event().wait()

    published: list[BookEvent] = []

    async def capture(event: BookEvent) -> None:
        published.append(event)

    adapter = OvertakenRestMock()
    collector = MarketDataCollector(
        [adapter],
        orderbook_symbol_limit=1,
        enable_streams=True,
        canonical_book_event_sink=capture,
    )

    snapshot = await asyncio.wait_for(
        collector.collect_once(
            {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
        ),
        timeout=2,
    )
    await collector.close()

    selected = snapshot.orderbook(
        "bybit", "BTCUSDT", InstrumentType.PERPETUAL
    )
    assert selected is not None
    assert selected.sequence == 2
    assert published == []

    sampled_adapter = OvertakenRestMock()
    sampled_collector = MarketDataCollector(
        [sampled_adapter],
        orderbook_symbol_limit=1,
        enable_streams=True,
        canonical_book_event_sink=capture,
        canonical_book_snapshot_from_selected=True,
    )
    sampled_snapshot = await asyncio.wait_for(
        sampled_collector.collect_once(
            {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
        ),
        timeout=2,
    )
    await sampled_collector.close()

    sampled_selected = sampled_snapshot.orderbook(
        "bybit", "BTCUSDT", InstrumentType.PERPETUAL
    )
    assert sampled_selected is not None
    assert sampled_selected.sequence == 2
    assert len(published) == 1
    assert published[0].payload.sequence == 2
    assert (
        published[0].metadata.source
        == "BYBIT.PUBLIC.ORDERBOOK.COLLECTOR_SNAPSHOT"
    )


@pytest.mark.asyncio
async def test_collector_records_ticker_and_orderbook_stream_heartbeats() -> None:
    adapter = MockExchangeAdapter("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=True)

    await collector.collect_once(
        {"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
    )
    ticker_value = 0.0
    orderbook_value = 0.0
    for _ in range(100):
        ticker_value = exchange_stream_last_message_timestamp.labels(
            "bybit", "ticker"
        )._value.get()
        orderbook_value = exchange_stream_last_message_timestamp.labels(
            "bybit", "orderbook"
        )._value.get()
        if ticker_value > 0 and orderbook_value > 0:
            break
        await asyncio.sleep(0)

    assert ticker_value > 0
    assert orderbook_value > 0
    await collector.close()
    assert exchange_stream_last_message_timestamp.labels(
        "bybit", "ticker"
    )._value.get() == 0
    assert exchange_stream_last_message_timestamp.labels(
        "bybit", "orderbook"
    )._value.get() == 0


@pytest.mark.asyncio
async def test_collector_prunes_retired_stream_markets() -> None:
    adapter = MockExchangeAdapter("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=True)
    now = datetime.now(UTC)
    old_market = ("OLDUSDT", InstrumentType.PERPETUAL)
    new_market = ("BTCUSDT", InstrumentType.PERPETUAL)
    old_key = (adapter.name, *old_market)
    old_ticker = next(
        ticker
        for ticker in await adapter.get_tickers()
        if ticker.instrument_type is InstrumentType.PERPETUAL
    ).model_copy(update={"symbol": old_market[0], "timestamp": now})
    collector._stream_ticker_cache[old_key] = old_ticker
    collector._stream_orderbook_cache[old_key] = await adapter.get_orderbook(
        old_market[0], 20, old_market[1]
    )
    collector._last_rest_book_fetch[old_key] = now

    collector._ensure_ticker_stream(
        adapter,
        [old_ticker.model_copy(update={"symbol": new_market[0]})],
    )
    collector._ensure_orderbook_stream(adapter, [new_market])

    assert old_key not in collector._stream_ticker_cache
    assert old_key not in collector._stream_orderbook_cache
    assert old_key not in collector._last_rest_book_fetch
    await collector.close()


@pytest.mark.asyncio
async def test_retired_stream_consumer_cannot_reinsert_old_market() -> None:
    class RetiredStreamMock(MockExchangeAdapter):
        def stream_tickers(
            self, symbols: list[tuple[str, InstrumentType]]
        ) -> AsyncIterator[Ticker]:
            del symbols
            return self._old_ticker_stream()

        async def _old_ticker_stream(self) -> AsyncIterator[Ticker]:
            ticker = next(
                row
                for row in await MockExchangeAdapter.get_tickers(self)
                if row.instrument_type is InstrumentType.PERPETUAL
            )
            yield ticker.model_copy(update={"symbol": "OLDUSDT"})

        def stream_orderbooks(
            self,
            symbols: list[tuple[str, InstrumentType]],
            depth: int = 20,
        ) -> AsyncIterator[OrderBook]:
            del symbols
            return self._old_book_stream(depth)

        async def _old_book_stream(self, depth: int) -> AsyncIterator[OrderBook]:
            yield await MockExchangeAdapter.get_orderbook(
                self, "OLDUSDT", depth, InstrumentType.PERPETUAL
            )

    adapter = RetiredStreamMock("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=True)
    active = frozenset({("BTCUSDT", InstrumentType.PERPETUAL)})
    collector._stream_ticker_requests[adapter.name] = active
    collector._orderbook_stream_requests[adapter.name] = active

    await collector._consume_ticker_stream(
        adapter, [("OLDUSDT", InstrumentType.PERPETUAL)]
    )
    await collector._consume_orderbook_stream(
        adapter, [("OLDUSDT", InstrumentType.PERPETUAL)]
    )

    assert not collector._stream_ticker_cache
    assert not collector._stream_orderbook_cache


@pytest.mark.asyncio
async def test_collector_bounds_history_cache_to_current_funding_universe() -> None:
    adapter = MockExchangeAdapter("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=False)
    old_key = (adapter.name, "OLDUSDT")
    collector.seed_funding_history(
        {
            old_key: [
                FundingHistoryPoint(
                    exchange=adapter.name,
                    symbol=old_key[1],
                    funding_rate=Decimal("0.0001"),
                    funding_timestamp=datetime.now(UTC) - timedelta(hours=8),
                )
            ]
        }
    )

    snapshot = await collector.collect_once(include_history=True)

    assert old_key not in collector._funding_history_cache
    assert set(snapshot.funding_history) == {
        (item.exchange, item.symbol) for item in snapshot.funding
    }
    assert set(snapshot.funding_history_refreshed) == {
        (item.exchange, item.symbol) for item in snapshot.funding
    }


@pytest.mark.asyncio
async def test_collector_keeps_fresh_websocket_tickers_when_rest_validation_fails() -> None:
    class RestFailingMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.fail_rest = False

        async def get_tickers(self) -> list[Ticker]:
            if self.fail_rest:
                raise NetworkError("synthetic REST outage")
            return await super().get_tickers()

        def stream_tickers(
            self, symbols: list[tuple[str, InstrumentType]]
        ) -> AsyncIterator[Ticker]:
            return self._reliable_stream(symbols)

        async def _reliable_stream(
            self, symbols: list[tuple[str, InstrumentType]]
        ) -> AsyncIterator[Ticker]:
            requested = set(symbols)
            for ticker in await MockExchangeAdapter.get_tickers(self):
                if (ticker.symbol, ticker.instrument_type) in requested:
                    yield ticker
            await asyncio.Event().wait()

    adapter = RestFailingMock()
    collector = MarketDataCollector(
        [adapter],
        enable_streams=True,
        stale_after_seconds=300,
        rest_validation_seconds=60,
    )
    first = await collector.collect_once()
    for _ in range(100):
        if collector._stream_ticker_cache:
            break
        await asyncio.sleep(0)
    assert collector._stream_ticker_cache

    adapter.fail_rest = True
    collector._last_rest_ticker_fetch[adapter.name] = datetime.now(UTC) - timedelta(
        seconds=61
    )
    second = await collector.collect_once()
    await collector.close()

    assert first.tickers
    assert second.tickers
    assert {ticker.exchange for ticker in second.tickers} == {"bybit"}
    assert second.incomplete_venues == ()


@pytest.mark.asyncio
async def test_collector_does_not_mask_rest_failure_without_fresh_stream() -> None:
    class InitialFailureMock(MockExchangeAdapter):
        async def get_tickers(self) -> list[Ticker]:
            raise NetworkError("synthetic initial outage")

    adapter = InitialFailureMock("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=True)

    snapshot = await collector.collect_once()
    await collector.close()

    assert snapshot.tickers == []
    assert snapshot.funding == []
    assert snapshot.incomplete_venues == ("bybit",)


@pytest.mark.asyncio
async def test_missing_discovery_book_does_not_block_shared_snapshot(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class MissingDiscoveryBookMock(MockExchangeAdapter):
        async def get_orderbook(
            self,
            symbol: str,
            depth: int,
            instrument_type: InstrumentType = InstrumentType.PERPETUAL,
        ) -> OrderBook:
            if symbol == "MISSINGUSDT":
                raise NetworkError("synthetic discovery book outage")
            return await super().get_orderbook(symbol, depth, instrument_type)

    adapter = MissingDiscoveryBookMock("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=False)
    dropped_before = market_data_dropped_total.labels(
        "bybit", "orderbook_fetch_error"
    )._value.get()

    caplog.set_level("WARNING")
    snapshot = await collector.collect_once(
        orderbook_symbols={"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]},
        discovery_orderbook_symbols={
            "bybit": [("MISSINGUSDT", InstrumentType.PERPETUAL)]
        },
    )
    await collector.close()

    assert snapshot.incomplete_venues == ()
    assert snapshot.orderbook(
        "bybit", "BTCUSDT", InstrumentType.PERPETUAL
    ) is not None
    assert snapshot.orderbook(
        "bybit", "MISSINGUSDT", InstrumentType.PERPETUAL
    ) is None
    assert (
        market_data_dropped_total.labels(
            "bybit", "orderbook_fetch_error"
        )._value.get()
        == dropped_before + 1
    )
    record = next(
        item for item in caplog.records if item.message == "orderbook_fetch_failed"
    )
    assert record.exchange == "bybit"
    assert record.symbol == "MISSINGUSDT"
    assert record.instrument_type == "PERPETUAL"
    assert record.error_type == "NetworkError"
    assert "synthetic discovery book outage" not in caplog.text


@pytest.mark.asyncio
async def test_missing_open_position_book_blocks_shared_snapshot() -> None:
    class MissingRequiredBookMock(MockExchangeAdapter):
        async def get_orderbook(
            self,
            symbol: str,
            depth: int,
            instrument_type: InstrumentType = InstrumentType.PERPETUAL,
        ) -> OrderBook:
            if symbol == "MISSINGUSDT":
                raise NetworkError("synthetic required book outage")
            return await super().get_orderbook(symbol, depth, instrument_type)

    adapter = MissingRequiredBookMock("bybit", sleep=0)
    collector = MarketDataCollector([adapter], enable_streams=False)

    snapshot = await collector.collect_once(
        orderbook_symbols={
            "bybit": [("MISSINGUSDT", InstrumentType.PERPETUAL)]
        }
    )
    await collector.close()

    assert snapshot.incomplete_venues == ("bybit",)


@pytest.mark.asyncio
async def test_collector_refreshes_required_ticker_aged_before_snapshot() -> None:
    class AgingRequiredTickerMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.ticker_calls = 0

        async def get_tickers(self) -> list[Ticker]:
            self.ticker_calls += 1
            rows = await super().get_tickers()
            if self.ticker_calls == 1:
                stale_at = datetime.now(UTC) - timedelta(seconds=31)
                return [row.model_copy(update={"timestamp": stale_at}) for row in rows]
            return rows

    adapter = AgingRequiredTickerMock()
    collector = MarketDataCollector(
        [adapter], enable_streams=False, stale_after_seconds=30
    )

    snapshot = await collector.collect_once(
        orderbook_symbols={"bybit": [("BTCUSDT", InstrumentType.PERPETUAL)]}
    )
    await collector.close()

    assert adapter.ticker_calls == 2
    assert snapshot.incomplete_venues == ()
    ticker = snapshot.ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL)
    assert ticker is not None
    assert (snapshot.captured_at - ticker.timestamp).total_seconds() <= 1


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

    first = await collector.collect_once(include_history=True)
    cached = await collector.collect_once(
        include_history=True, force_history_refresh=False
    )
    assert adapter.history_calls == 1
    assert first.funding_history_refreshed
    assert cached.funding_history_refreshed == {}

    forced_symbol = await collector.collect_once(
        include_history=True,
        history_symbols={"bybit": ["DELISTEDUSDT"]},
        force_history_refresh=False,
        force_history_symbols={"bybit": ["DELISTEDUSDT"]},
    )
    assert adapter.history_calls == 2

    delisted_key = ("bybit", "DELISTEDUSDT")
    assert delisted_key in forced_symbol.funding_history_refreshed
    assert delisted_key in (forced_symbol.funding_history or {})
    assert delisted_key not in collector._funding_history_cache
    await collector.collect_once(include_history=True, force_history_refresh=True)
    assert adapter.history_calls == 3


@pytest.mark.asyncio
async def test_collector_refreshes_funding_before_it_becomes_stale() -> None:
    class CountingFundingMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.ticker_calls = 0
            self.funding_calls = 0

        async def get_tickers(self) -> list[Ticker]:
            self.ticker_calls += 1
            return await super().get_tickers()

        async def get_funding_rates(self) -> list[FundingSnapshot]:
            self.funding_calls += 1
            return await super().get_funding_rates()

    adapter = CountingFundingMock()
    collector = MarketDataCollector(
        [adapter],
        enable_streams=True,
        stale_after_seconds=30,
        rest_validation_seconds=60,
    )
    await collector.collect_once()
    first_rest_ticker_fetch = collector._last_rest_ticker_fetch[adapter.name]
    collector._last_funding_fetch[adapter.name] = datetime.now(UTC) - timedelta(
        seconds=31
    )
    await collector.collect_once()
    await collector.close()

    assert collector._last_rest_ticker_fetch[adapter.name] == first_rest_ticker_fetch
    assert adapter.funding_calls == 2


@pytest.mark.asyncio
async def test_collector_refetches_funding_that_aged_during_collection() -> None:
    class AgingFundingMock(MockExchangeAdapter):
        def __init__(self) -> None:
            super().__init__("bybit", sleep=0)
            self.funding_calls = 0

        async def get_funding_rates(self) -> list[FundingSnapshot]:
            self.funding_calls += 1
            rows = await super().get_funding_rates()
            timestamp = datetime.now(UTC)
            if self.funding_calls == 1:
                timestamp -= timedelta(seconds=31)
            return [row.model_copy(update={"timestamp": timestamp}) for row in rows]

    adapter = AgingFundingMock()
    collector = MarketDataCollector(
        [adapter], enable_streams=False, stale_after_seconds=30
    )

    snapshot = await collector.collect_once()
    await collector.close()

    assert adapter.funding_calls == 2
    assert len(snapshot.funding) == 1
    assert all(
        snapshot.captured_at >= row.timestamp
        and (snapshot.captured_at - row.timestamp).total_seconds() <= 1
        for row in snapshot.funding
    )
