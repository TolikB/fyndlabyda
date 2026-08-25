from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError, NetworkError
from funding_arbitrage.exchanges.base.models import InstrumentType, Ticker
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter


def response(result: dict[str, object], time: str = "1735689600000") -> httpx.Response:
    return httpx.Response(200, json={"retCode": 0, "retMsg": "OK", "time": time, "result": result})


@pytest.mark.asyncio
async def test_bybit_rest_payloads_are_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("instruments-info"):
            category = request.url.params["category"]
            if category == "spot":
                return response(
                    {
                        "list": [
                            {
                                "symbol": "BTCUSDT",
                                "baseCoin": "BTC",
                                "quoteCoin": "USDT",
                                "status": "Trading",
                                "priceFilter": {"tickSize": "0.01"},
                                "lotSizeFilter": {
                                    "basePrecision": "0.000001",
                                    "minOrderQty": "0.00001",
                                },
                            }
                        ]
                    }
                )
            return response(
                {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "baseCoin": "BTC",
                            "quoteCoin": "USDT",
                            "settleCoin": "USDT",
                            "contractType": "LinearPerpetual",
                            "deliveryTime": "0",
                            "status": "Trading",
                            "priceFilter": {"tickSize": "0.1"},
                            "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                            "fundingInterval": 480,
                        }
                    ]
                }
            )
        if path.endswith("tickers"):
            category = request.url.params["category"]
            row = {
                "symbol": "BTCUSDT",
                "lastPrice": "100",
                "bid1Price": "99.9",
                "ask1Price": "100.1",
                "volume24h": "12",
                "ts": "1735689600000",
            }
            if category == "linear":
                row.update(
                    {
                        "markPrice": "100.0",
                        "indexPrice": "100.0",
                        "openInterest": "300",
                        "fundingRate": "0.001",
                        "fundingIntervalHour": "8",
                        "nextFundingTime": "1735718400000",
                    }
                )
            return response({"list": [row]})
        if path.endswith("funding/history"):
            return response(
                {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "fundingRate": "0.001",
                            "fundingRateTimestamp": "1735689600000",
                            "markPrice": "100",
                        }
                    ]
                }
            )
        if path.endswith("orderbook"):
            return response(
                {"ts": "1735689600000", "u": 42, "b": [["99.9", "2"]], "a": [["100.1", "3"]]}
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="https://test.invalid")
    adapter = BybitPublicAdapter(base_url="https://test.invalid", http_client=client)
    instruments = await adapter.get_instruments()
    tickers = await adapter.get_tickers()
    funding = await adapter.get_funding_rates()
    history = await adapter.get_funding_history(
        "BTCUSDT", datetime(2024, 12, 31, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)
    )
    orderbook = await adapter.get_orderbook("BTCUSDT", 25)
    await client.aclose()

    assert {item.instrument_type for item in instruments} == {
        InstrumentType.SPOT,
        InstrumentType.PERPETUAL,
    }
    assert (
        next(
            item for item in instruments if item.instrument_type is InstrumentType.PERPETUAL
        ).expiry
        is None
    )
    assert tickers[0].last_price == Decimal("100")
    assert tickers[0].timestamp == datetime(2025, 1, 1, tzinfo=UTC)
    assert funding[0].funding_rate_daily == Decimal("0.003")
    assert funding[0].timestamp == datetime(2025, 1, 1, tzinfo=UTC)
    assert history[0].funding_timestamp.year == 2025
    assert orderbook.bids[0].price < orderbook.asks[0].price


def test_bybit_dated_future_requires_positive_expiry() -> None:
    adapter = BybitPublicAdapter()
    row: dict[str, object] = {
        "symbol": "BTC-01JAN26",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "contractType": "LinearFutures",
        "deliveryTime": 1767225600000,
        "status": "Trading",
        "priceFilter": {"tickSize": "0.1"},
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
    }
    dated = adapter._parse_instrument(row, "linear")

    assert dated.instrument_type is InstrumentType.FUTURE
    assert dated.expiry is not None
    assert dated.expiry.year == 2026

    for invalid in (None, "", 0, "0", -1, 10**30):
        with pytest.raises(InvalidResponseError):
            adapter._parse_instrument({**row, "deliveryTime": invalid}, "linear")


@pytest.mark.asyncio
async def test_bybit_rejects_crossed_book() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return response({"ts": "1735689600000", "u": 1, "b": [["101", "2"]], "a": [["100", "3"]]})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = BybitPublicAdapter(http_client=client)
    with pytest.raises(InvalidResponseError):
        await adapter.get_orderbook("BTCUSDT", 25)
    await client.aclose()


@pytest.mark.asyncio
async def test_websocket_reconnects_after_disconnect() -> None:
    adapter = BybitPublicAdapter(max_reconnects=2, sleep=lambda _: _noop())
    attempts = 0

    async def fake_connection(
        _url: str, _args: list[str], _kind: InstrumentType
    ) -> AsyncIterator[Ticker]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disconnected")
        yield adapter._parse_ws_ticker(
            {
                "topic": "tickers.BTCUSDT",
                "ts": "1735689600000",
                "data": {
                    "symbol": "BTCUSDT",
                    "lastPrice": "100",
                    "bid1Price": "99",
                    "ask1Price": "101",
                    "volume24h": "1",
                },
            }
        )

    async def patched_sleep(_: float) -> None:
        return None

    adapter._sleep = patched_sleep
    adapter._stream_ticker_connection = fake_connection  # type: ignore[assignment]
    stream = adapter.stream_tickers([("BTCUSDT", InstrumentType.PERPETUAL)])
    ticker = await anext(stream)
    await stream.aclose()
    assert attempts >= 2
    assert ticker.last_price == Decimal("100")


@pytest.mark.asyncio
async def test_websocket_tickers_batch_large_universe_under_topic_cap() -> None:
    adapter = BybitPublicAdapter()
    subscriptions: list[list[str]] = []

    async def fake_group(
        _url: str, requested: list[str], instrument_type: InstrumentType
    ) -> AsyncIterator[Ticker]:
        subscriptions.append(requested)
        for symbol in requested:
            yield Ticker(
                exchange="bybit",
                symbol=symbol,
                instrument_type=instrument_type,
                last_price=Decimal("100"),
                volume_24h=Decimal("1"),
                timestamp=datetime.now(UTC),
            )

    adapter._stream_ticker_group = fake_group  # type: ignore[assignment]
    symbols = [(f"ASSET{index}USDT", InstrumentType.PERPETUAL) for index in range(21)]
    stream = adapter.stream_tickers(symbols)
    received = [await anext(stream) for _ in symbols]
    await stream.aclose()

    assert len(received) == len(symbols)
    assert sorted(len(batch) for batch in subscriptions) == [1, 10, 10]
    assert all(len(batch) <= 10 for batch in subscriptions)


def test_websocket_ticker_merges_partial_delta() -> None:
    adapter = BybitPublicAdapter()
    state: dict[str, dict[str, object]] = {}
    snapshot = adapter._merge_ws_ticker(
        {
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": "1735689600000",
            "data": {
                "symbol": "BTCUSDT",
                "lastPrice": "100",
                "bid1Price": "99",
                "ask1Price": "101",
                "volume24h": "1",
            },
        },
        state,
    )
    delta = adapter._merge_ws_ticker(
        {
            "topic": "tickers.BTCUSDT",
            "type": "delta",
            "ts": "1735689601000",
            "data": {"bid1Price": "99.5"},
        },
        state,
    )

    assert snapshot is not None
    assert delta is not None
    assert delta.last_price == Decimal("100")
    assert delta.best_bid == Decimal("99.5")
    assert delta.best_ask == Decimal("101")


async def _noop() -> None:
    return None


@pytest.mark.asyncio
async def test_http_network_errors_are_typed() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = BybitPublicAdapter(http_client=client)
    with pytest.raises(NetworkError):
        await adapter.get_tickers()
    await client.aclose()
