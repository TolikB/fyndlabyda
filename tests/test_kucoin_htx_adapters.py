from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter


def _kucoin_perpetual() -> dict[str, object]:
    return {
        "symbol": "XBTUSDTM",
        "baseCurrency": "XBT",
        "displayBaseCurrency": "XBT",
        "quoteCurrency": "USDT",
        "settleCurrency": "USDT",
        "multiplier": "0.001",
        "tickSize": "0.1",
        "lotSize": "1",
        "status": "Open",
        "marketStage": "NORMAL",
        "currentFundingRateGranularity": 14_400_000,
        "fundingFeeRate": "0.00025",
        "nextFundingRateDateTime": 1_735_704_000_000,
        "markPrice": "100000",
        "indexPrice": "99990",
        "expireDate": None,
    }


def _kucoin_dated_future() -> dict[str, object]:
    row = _kucoin_perpetual()
    row.update(
        {
            "symbol": "XBTUSDTU26",
            "fundingFeeRate": None,
            "nextFundingRateDateTime": None,
            "expireDate": 1_790_323_200_000,
        }
    )
    return row


def _kucoin_inverse_perpetual() -> dict[str, object]:
    row = _kucoin_perpetual()
    row.update(
        {
            "symbol": "XBTUSDM",
            "quoteCurrency": "USD",
            "settleCurrency": "XBT",
            "multiplier": "-1",
        }
    )
    return row


def _kucoin_spot() -> dict[str, object]:
    return {
        "symbol": "BTC-USDT",
        "baseCurrency": "BTC",
        "quoteCurrency": "USDT",
        "priceIncrement": "0.1",
        "baseIncrement": "0.00001",
        "baseMinSize": "0.0001",
        "enableTrading": True,
        "st": False,
    }


@pytest.mark.asyncio
async def test_kucoin_instruments_distinguish_perpetual_dated_future_and_spot() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/contracts/active":
            return httpx.Response(
                200,
                json={
                    "code": "200000",
                    "data": [
                        _kucoin_perpetual(),
                        _kucoin_dated_future(),
                        _kucoin_inverse_perpetual(),
                    ],
                },
            )
        assert request.url.path == "/api/v2/symbols"
        return httpx.Response(200, json={"code": "200000", "data": [_kucoin_spot()]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = KucoinPublicAdapter(
        spot_base_url="https://spot.invalid",
        futures_base_url="https://futures.invalid",
        http_client=client,
    )
    instruments = await adapter.get_instruments()
    funding = await adapter.get_funding_rates()
    await client.aclose()

    perpetual = next(row for row in instruments if row.exchange_symbol == "XBTUSDTM")
    dated = next(row for row in instruments if row.exchange_symbol == "XBTUSDTU26")
    assert all(row.exchange_symbol != "XBTUSDM" for row in instruments)
    spot = next(row for row in instruments if row.instrument_type is InstrumentType.SPOT)
    assert perpetual.base_asset == "BTC"
    assert perpetual.instrument_type is InstrumentType.PERPETUAL
    assert perpetual.contract_size == Decimal("0.001")
    assert perpetual.step_size == Decimal("0.001")
    assert perpetual.funding_interval == 4
    assert dated.instrument_type is InstrumentType.FUTURE
    assert dated.expiry == datetime(2026, 9, 25, 8, tzinfo=UTC)
    assert dated.funding_interval is None
    assert spot.exchange_symbol == "BTC-USDT"
    assert [row.symbol for row in funding] == ["XBTUSDTM"]
    assert funding[0].funding_interval_hours == Decimal("4")
    assert funding[0].next_funding_time == datetime(2025, 1, 1, 4, tzinfo=UTC)


@pytest.mark.asyncio
async def test_kucoin_tickers_skip_null_rows_without_dropping_venue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/contracts/active":
            return httpx.Response(
                200,
                json={"code": "200000", "data": [_kucoin_perpetual()]},
            )
        if request.url.path == "/api/v1/allTickers":
            return httpx.Response(
                200,
                json={
                    "code": "200000",
                    "data": [
                        {
                            "symbol": "XBTUSDTM",
                            "price": "100000",
                            "bestBidPrice": "99999",
                            "bestAskPrice": "100001",
                            "turnoverOf24h": "12",
                            "ts": 1_735_689_600_000,
                        },
                        {"symbol": "XBTUSDTM", "price": None},
                    ],
                },
            )
        assert request.url.path == "/api/v1/market/allTickers"
        return httpx.Response(
            200,
            json={
                "code": "200000",
                "data": {
                    "time": 1_735_689_600_000,
                    "ticker": [
                        {
                            "symbol": "BTC-USDT",
                            "last": "100000",
                            "buy": "99999",
                            "sell": "100001",
                            "volValue": "12",
                        },
                        {"symbol": "PREOPEN-USDT", "last": None},
                    ],
                },
            },
        )

    caplog.set_level("WARNING")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = KucoinPublicAdapter(
        spot_base_url="https://spot.invalid",
        futures_base_url="https://futures.invalid",
        http_client=client,
    )
    tickers = await adapter.get_tickers()
    await client.aclose()

    assert {(ticker.symbol, ticker.instrument_type) for ticker in tickers} == {
        ("XBTUSDTM", InstrumentType.PERPETUAL),
        ("BTC-USDT", InstrumentType.SPOT),
    }
    assert caplog.messages.count("kucoin_ticker_skipped") == 2


def test_kucoin_websocket_parsers_keep_typed_units() -> None:
    adapter = KucoinPublicAdapter()
    adapter._contract_sizes["XBTUSDTM"] = Decimal("0.001")
    ticker = adapter._parse_ws_ticker(
        {
            "type": "message",
            "topic": "/contractMarket/tickerV2:XBTUSDTM",
            "data": {
                "symbol": "XBTUSDTM",
                "price": "100000",
                "bestBidPrice": "99999",
                "bestAskPrice": "100001",
                "ts": 1_735_689_600_000_000_000,
            },
        },
        InstrumentType.PERPETUAL,
    )
    spot_ticker = adapter._parse_ws_ticker(
        {
            "type": "message",
            "topic": "/market/ticker:BTC-USDT",
            "subject": "trade.ticker",
            "data": {
                "price": "100000",
                "bestBid": "99999",
                "bestAsk": "100001",
                "time": 1_735_689_600_000,
            },
        },
        InstrumentType.SPOT,
    )
    book = adapter._parse_ws_orderbook(
        {
            "topic": "/contractMarket/level2Depth50:XBTUSDTM",
            "data": {
                "bids": [["99999", "2"]],
                "asks": [["100001", "3"]],
                "timestamp": 1_735_689_600_000,
                "sequence": 7,
            },
        },
        InstrumentType.PERPETUAL,
        20,
    )
    assert ticker.timestamp == datetime(2025, 1, 1, tzinfo=UTC)
    assert ticker.instrument_type is InstrumentType.PERPETUAL
    assert spot_ticker.symbol == "BTC-USDT"
    assert book.bids[0].quantity == Decimal("0.002")
    assert book.asks[0].quantity == Decimal("0.003")
    assert book.sequence == 7


@pytest.mark.asyncio
async def test_kucoin_funding_history_pages_backward_and_sorts() -> None:
    adapter = KucoinPublicAdapter()
    calls: list[dict[str, str | int]] = []
    newest = datetime(2026, 1, 10, tzinfo=UTC)

    async def request(
        _base: str,
        _path: str,
        params: dict[str, str | int] | None = None,
        *,
        method: str = "GET",
    ) -> Any:
        assert method == "GET"
        assert params is not None
        calls.append(params)
        count = 100 if len(calls) == 1 else 2
        first = 200 if len(calls) == 1 else 100
        return [
            {
                "symbol": "XBTUSDTM",
                "fundingRate": "0.0001",
                "timepoint": int((newest - timedelta(hours=offset)).timestamp() * 1000),
            }
            for offset in range(first, first + count)
        ]

    adapter._request = request  # type: ignore[method-assign]
    history = await adapter.get_funding_history("XBTUSDTM", newest - timedelta(days=20), newest)

    assert len(calls) == 2
    assert len(history) == 102
    assert history == sorted(history, key=lambda row: row.funding_timestamp)
    assert int(calls[1]["to"]) < int(calls[0]["to"])


@pytest.mark.asyncio
async def test_kucoin_candles_page_long_ranges_without_duplicate_boundaries() -> None:
    adapter = KucoinPublicAdapter()
    calls: list[dict[str, str | int]] = []
    start = datetime(2025, 1, 1, tzinfo=UTC)

    async def request(
        _base: str,
        _path: str,
        params: dict[str, str | int] | None = None,
        *,
        method: str = "GET",
    ) -> Any:
        assert method == "GET"
        assert params is not None
        calls.append(params)
        opened = int(params["startAt"])
        return [[opened, "100", "101", "102", "99", "10", "1000"]]

    adapter._request = request  # type: ignore[method-assign]
    candles = await adapter.get_candles(
        "BTC-USDT",
        InstrumentType.SPOT,
        start,
        start + timedelta(hours=1600),
        60,
    )

    assert len(calls) == 2
    assert len(candles) == 2
    assert candles[0].open_time == start
    assert candles[1].open_time == start + timedelta(hours=1500)


def _htx_contract() -> dict[str, object]:
    return {
        "symbol": "BTC",
        "contract_code": "BTC-USDT",
        "contract_size": "0.001",
        "price_tick": "0.1",
        "contract_status": 1,
        "settlement_period": "4",
        "business_type": "swap",
    }


def _htx_spot() -> dict[str, object]:
    return {
        "symbol": "btcusdt",
        "base-currency": "btc",
        "quote-currency": "usdt",
        "price-precision": 1,
        "amount-precision": 5,
        "min-order-amt": "0.0001",
        "state": "online",
        "api-trading": "enabled",
    }


@pytest.mark.asyncio
async def test_htx_instruments_and_funding_use_dynamic_exact_settlement() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/linear-swap-api/v1/swap_contract_info":
            return httpx.Response(200, json={"status": "ok", "data": [_htx_contract()]})
        if request.url.path == "/v1/common/symbols":
            return httpx.Response(200, json={"status": "ok", "data": [_htx_spot()]})
        if request.url.path == "/linear-swap-api/v1/swap_index":
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "data": [
                        {"contract_code": "BTC-USDT", "index_price": "99990"}
                    ],
                },
            )
        if request.url.path == "/index/market/history/linear_swap_mark_price_kline":
            assert request.url.params["contract_code"] == "BTC-USDT"
            assert request.url.params["period"] == "1min"
            assert request.url.params["size"] == "1"
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "data": [{"id": 1735689600, "close": "100010"}],
                },
            )
        assert request.url.path == "/linear-swap-api/v1/swap_batch_funding_rate"
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "data": [
                    {
                        "contract_code": "BTC-USDT",
                        "funding_rate": "0.0003",
                        "funding_time": "1735704000000",
                        "next_funding_time": "1735718400000",
                    },
                    {
                        "contract_code": "BTC-USDT-250103",
                        "funding_rate": None,
                        "funding_time": None,
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = HtxPublicAdapter(
        spot_base_url="https://spot.invalid",
        futures_base_url="https://futures.invalid",
        http_client=client,
    )
    instruments = await adapter.get_instruments()
    funding = await adapter.get_funding_rates()
    await client.aclose()

    perpetual = next(row for row in instruments if row.instrument_type is InstrumentType.PERPETUAL)
    spot = next(row for row in instruments if row.instrument_type is InstrumentType.SPOT)
    assert perpetual.contract_size == Decimal("0.001")
    assert perpetual.step_size == Decimal("0.001")
    assert perpetual.funding_interval == 4
    assert spot.step_size == Decimal("0.00001")
    assert len(funding) == 1
    assert funding[0].funding_interval_hours == Decimal("4")
    assert funding[0].next_funding_time == datetime(2025, 1, 1, 8, tzinfo=UTC)
    assert funding[0].mark_price == Decimal("100010")
    assert funding[0].index_price == Decimal("99990")


def test_htx_gzip_websocket_parsers_keep_contract_quantities_in_base_units() -> None:
    adapter = HtxPublicAdapter()
    adapter._contract_sizes["BTC-USDT"] = Decimal("0.001")
    payload = {
        "ch": "market.BTC-USDT.depth.step0",
        "ts": 1_735_689_600_000,
        "tick": {
            "bids": [["99999", "2"]],
            "asks": [["100001", "3"]],
            "id": 9,
        },
    }
    decoded = adapter._decode_ws(gzip.compress(json.dumps(payload).encode()))
    book = adapter._parse_ws_orderbook(decoded, InstrumentType.PERPETUAL, 20)
    ticker = adapter._parse_ws_ticker(
        {
            "ch": "market.btcusdt.ticker",
            "ts": 1_735_689_600_000,
            "tick": {
                "close": "100000",
                "bid": "99999",
                "ask": "100001",
                "vol": "12",
            },
        },
        InstrumentType.SPOT,
    )
    assert book.bids[0].quantity == Decimal("0.002")
    assert book.asks[0].quantity == Decimal("0.003")
    assert book.sequence == 9
    assert ticker.symbol == "btcusdt"
    assert ticker.instrument_type is InstrumentType.SPOT


def test_htx_spot_depth_version_is_the_native_sequence() -> None:
    adapter = HtxPublicAdapter()
    payload = {
        "ch": "market.btcusdt.depth.step0",
        "ts": 1_787_604_357_952,
        "tick": {
            "bids": [["99999", "2"]],
            "asks": [["100001", "3"]],
            "ts": 1_787_604_357_008,
            "version": 192_939_594_849,
        },
    }

    book = adapter._parse_ws_orderbook(payload, InstrumentType.SPOT, 20)

    assert book.sequence == 192_939_594_849


@pytest.mark.asyncio
async def test_htx_funding_history_pages_backward_and_sorts() -> None:
    adapter = HtxPublicAdapter()
    calls: list[dict[str, str | int]] = []
    newest = datetime(2026, 1, 10, tzinfo=UTC)

    async def request(
        _base: str,
        _path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        assert params is not None
        calls.append(params)
        count = 100 if len(calls) == 1 else 2
        first = 200 if len(calls) == 1 else 100
        return [
            {
                "id": str(offset),
                "contract_code": "BTC-USDT",
                "funding_rate": "0.0001",
                "funding_time": str(int((newest - timedelta(hours=offset)).timestamp() * 1000)),
            }
            for offset in range(first, first + count)
        ]

    adapter._request = request  # type: ignore[method-assign]
    history = await adapter.get_funding_history("BTC-USDT", newest - timedelta(days=20), newest)

    assert len(calls) == 2
    assert len(history) == 102
    assert history == sorted(history, key=lambda row: row.funding_timestamp)
    assert int(calls[1]["end_time"]) < int(calls[0]["end_time"])
