from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter


@pytest.mark.asyncio
async def test_okx_funding_rates_are_symbol_scoped_without_btc_hardcode(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requested_symbols: list[str] = []
    returned_symbol = "ETH-USDT-SWAP"
    returned_timestamp: str | None = str(
        int(datetime.now(UTC).timestamp() * 1000)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/public/instruments"):
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {"instId": "ETH-USDT-SWAP", "state": "live"},
                        {
                            "instId": "JP225-USDT-SWAP",
                            "state": "preopen",
                            "ctVal": "",
                            "tickSz": "",
                        },
                    ],
                },
            )
        if request.url.path.endswith("/public/funding-rate"):
            requested_symbols.append(request.url.params["instId"])
            assert request.url.params["instId"] == "ETH-USDT-SWAP"
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": returned_symbol,
                            "fundingRate": "0.001",
                            "fundingTime": "1735704000000",
                            "nextFundingTime": "1735718400000",
                            "ts": returned_timestamp,
                        }
                    ],
                },
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = OkxPublicAdapter(base_url="https://test.invalid", http_client=client)
    funding = await adapter.get_funding_rates()
    returned_symbol = "BTC-USDT-SWAP"
    mismatched = await adapter.get_funding_rates()
    returned_symbol = "ETH-USDT-SWAP"
    returned_timestamp = None
    missing_timestamp = await adapter.get_funding_rates()
    await client.aclose()

    assert funding[0].symbol == "ETH-USDT-SWAP"
    assert funding[0].funding_interval_hours == Decimal("4.0")
    assert funding[0].funding_rate_daily == Decimal("0.006")
    assert funding[0].next_funding_time is not None
    assert funding[0].next_funding_time.hour == 4
    assert funding[0].mark_price is None
    assert funding[0].index_price is None
    assert mismatched == []
    assert missing_timestamp == []
    assert requested_symbols == [
        "ETH-USDT-SWAP",
        "ETH-USDT-SWAP",
        "ETH-USDT-SWAP",
    ]
    assert "okx_reference_price_fetch_failed" in caplog.messages
    assert "okx_funding_symbol_mismatch" in caplog.messages
    assert "okx_funding_timestamp_invalid" in caplog.messages


@pytest.mark.asyncio
async def test_okx_funding_rates_use_public_mark_and_index_prices() -> None:
    requested_quotes: list[str] = []
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    fresh_reference_timestamp_ms = now_ms - 20_000
    reference_timestamp_ms = fresh_reference_timestamp_ms
    funding_timestamp_ms = now_ms

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/public/instruments"):
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "uly": "ETH-USDT",
                            "state": "live",
                        }
                    ],
                },
            )
        if request.url.path.endswith("/public/mark-price"):
            assert request.url.params["instType"] == "SWAP"
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "markPx": "4512.25",
                            "ts": str(reference_timestamp_ms),
                        }
                    ],
                },
            )
        if request.url.path.endswith("/market/index-tickers"):
            requested_quotes.append(request.url.params["quoteCcy"])
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "ETH-USDT",
                            "idxPx": "4510.75",
                            "ts": str(reference_timestamp_ms),
                        }
                    ],
                },
            )
        if request.url.path.endswith("/public/funding-rate"):
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "fundingRate": "0.0001",
                            "fundingTime": "1735704000000",
                            "nextFundingTime": "1735718400000",
                            "markPx": "0",
                            "idxPx": "0",
                            "ts": str(funding_timestamp_ms),
                        }
                    ],
                },
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = OkxPublicAdapter(base_url="https://test.invalid", http_client=client)
    funding = await adapter.get_funding_rates()
    reference_timestamp_ms = now_ms - 60_000
    stale_reference_funding = await adapter.get_funding_rates()
    reference_timestamp_ms = now_ms
    funding_timestamp_ms = now_ms + 60_000
    future_funding = await adapter.get_funding_rates()
    await client.aclose()

    assert len(funding) == 1
    assert funding[0].mark_price == Decimal("4512.25")
    assert funding[0].index_price == Decimal("4510.75")
    assert funding[0].timestamp == datetime.fromtimestamp(
        fresh_reference_timestamp_ms / 1000, tz=UTC
    )
    assert stale_reference_funding[0].mark_price is None
    assert stale_reference_funding[0].index_price is None
    assert future_funding == []
    assert requested_quotes == ["USDT", "USDT", "USDT"]


def test_binance_perpetual_delivery_sentinel_is_not_an_expiry() -> None:
    adapter = BinancePublicAdapter()
    row: dict[str, object] = {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "deliveryDate": 4133404800000,
        "status": "TRADING",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
        ],
    }
    perpetual = adapter._parse_instrument(row, spot=False)
    dated = adapter._parse_instrument(
        {
            **row,
            "symbol": "BTCUSDT_260101",
            "contractType": "CURRENT_QUARTER",
            "deliveryDate": 1767225600000,
        },
        spot=False,
    )

    assert perpetual.instrument_type is InstrumentType.PERPETUAL
    assert perpetual.expiry is None
    assert dated.instrument_type is InstrumentType.FUTURE
    assert dated.expiry is not None
    assert dated.expiry.year == 2026

    for invalid in (None, "", 0, "0", -1, 10**30):
        with pytest.raises(InvalidResponseError):
            adapter._parse_instrument(
                {
                    **row,
                    "symbol": "BTCUSDT_INVALID",
                    "contractType": "CURRENT_QUARTER",
                    "deliveryDate": invalid,
                },
                spot=False,
            )


@pytest.mark.asyncio
async def test_binance_uses_adjusted_symbol_funding_interval() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/premiumIndex"):
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.001",
                        "nextFundingTime": 1735718400000,
                        "time": 1735689600000,
                    },
                    {
                        "symbol": "BTCUSDT_261225",
                        "lastFundingRate": "0",
                        "nextFundingTime": 0,
                        "time": 1735689600000,
                    },
                ],
            )
        if request.url.path.endswith("/fundingInfo"):
            return httpx.Response(
                200,
                json=[{"symbol": "BTCUSDT", "fundingIntervalHours": 4}],
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = BinancePublicAdapter(
        futures_base_url="https://test.invalid", http_client=client
    )
    funding = await adapter.get_funding_rates()
    await client.aclose()

    assert len(funding) == 1
    assert funding[0].funding_interval_hours == Decimal("4")
    assert funding[0].funding_rate_daily == Decimal("0.006")


@pytest.mark.asyncio
async def test_binance_refresh_removes_expired_interval_override() -> None:
    funding_info_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal funding_info_requests
        if request.url.path.endswith("/premiumIndex"):
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.001",
                        "nextFundingTime": 1735718400000,
                        "time": 1735689600000,
                    }
                ],
            )
        if request.url.path.endswith("/fundingInfo"):
            funding_info_requests += 1
            return httpx.Response(
                200,
                json=(
                    [{"symbol": "BTCUSDT", "fundingIntervalHours": 4}]
                    if funding_info_requests == 1
                    else []
                ),
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = BinancePublicAdapter(
        futures_base_url="https://test.invalid",
        http_client=client,
        funding_metadata_ttl_seconds=0,
    )
    first = await adapter.get_funding_rates()
    second = await adapter.get_funding_rates()
    await client.aclose()

    assert first[0].funding_interval_hours == Decimal("4")
    assert second[0].funding_interval_hours == Decimal("8")


@pytest.mark.asyncio
async def test_okx_empty_spot_contract_value_is_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/public/instruments")
        if request.url.params["instType"] == "SWAP":
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [{"instId": "BTC-USDT-SWAP", "ctVal": "0.01", "tickSz": "0.1"}],
                },
            )
        return httpx.Response(
            200,
            json={
                "code": "0",
                "data": [
                    {
                        "instId": "BTC-USDT",
                        "ctVal": "",
                        "tickSz": "0.01",
                        "settleCcy": "",
                    },
                    {
                        "instId": "XALAB-USDT",
                        "state": "preopen",
                        "ctVal": "",
                        "tickSz": "",
                    },
                ],
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = OkxPublicAdapter(base_url="https://test.invalid", http_client=client)
    instruments = await adapter.get_instruments()
    await client.aclose()

    spot = next(item for item in instruments if item.instrument_type is InstrumentType.SPOT)
    assert spot.contract_size == Decimal("1")
    assert adapter._canonical_instruments[
        ("BTC-USDT", InstrumentType.SPOT)
    ].settlement_asset == "USDT"
    assert {item.exchange_symbol for item in instruments} == {
        "BTC-USDT-SWAP",
        "BTC-USDT",
    }


@pytest.mark.asyncio
async def test_okx_tickers_skip_blank_last_without_dropping_venue() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/market/tickers")
        inst_type = request.url.params["instType"]
        suffix = "-SWAP" if inst_type == "SWAP" else ""
        return httpx.Response(
            200,
            json={
                "code": "0",
                "data": [
                    {
                        "instId": f"BTC-USDT{suffix}",
                        "last": "100",
                        "bidPx": "99",
                        "askPx": "101",
                        "volCcy24h": "12",
                        "ts": "1735689600000",
                    },
                    {
                        "instId": f"PREOPEN-USDT{suffix}",
                        "last": "",
                        "bidPx": "",
                        "askPx": "",
                        "volCcy24h": "0",
                        "ts": "1735689600000",
                    },
                ],
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = OkxPublicAdapter(base_url="https://test.invalid", http_client=client)
    tickers = await adapter.get_tickers()
    await client.aclose()

    assert {(ticker.symbol, ticker.instrument_type) for ticker in tickers} == {
        ("BTC-USDT-SWAP", InstrumentType.PERPETUAL),
        ("BTC-USDT", InstrumentType.SPOT),
    }


def test_binance_websocket_ticker_aliases_are_normalized() -> None:
    adapter = BinancePublicAdapter()
    ticker = adapter._parse_futures_ticker(
        {"s": "BTCUSDT", "c": "100", "b": "99", "a": "101", "q": "12", "E": 1735689600000},
        None,
    )
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("100")
    assert ticker.best_bid == Decimal("99")
    assert ticker.instrument_type is InstrumentType.PERPETUAL

    spot = adapter._parse_spot_ticker(
        {"s": "BTCUSDT", "c": "100", "b": "99", "a": "101", "q": "12", "E": 1735689600000}
    )
    assert spot.symbol == "BTCUSDT"
    assert spot.instrument_type is InstrumentType.SPOT
    assert spot.best_bid == Decimal("99")

    futures_book = adapter._parse_futures_book_ticker(
        {"e": "bookTicker", "s": "BTCUSDT", "b": "99", "a": "101", "E": 1735689600000}
    )
    assert futures_book.instrument_type is InstrumentType.PERPETUAL
    assert futures_book.last_price == Decimal("100")
    assert futures_book.best_bid == Decimal("99")
    assert futures_book.best_ask == Decimal("101")


@pytest.mark.asyncio
async def test_hyperliquid_public_info_orderbook_is_normalized() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "levels": [
                    [{"px": "99", "sz": "2"}],
                    [{"px": "101", "sz": "3"}],
                ]
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = HyperliquidPublicAdapter(base_url="https://test.invalid", http_client=client)
    book = await adapter.get_orderbook("ETH", 10)
    await client.aclose()

    assert book.timestamp.tzinfo is UTC
    assert book.bids[0].price == Decimal("99")
    assert book.asks[0].price == Decimal("101")


@pytest.mark.asyncio
async def test_hyperliquid_funding_is_hourly_with_next_utc_boundary() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"universe": [{"name": "BTC", "szDecimals": 5}]},
                [{"funding": "0.0000125", "markPx": "100", "oraclePx": "100"}],
            ],
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = HyperliquidPublicAdapter(base_url="https://test.invalid", http_client=client)
    funding = await adapter.get_funding_rates()
    await client.aclose()

    assert funding[0].funding_interval_hours == Decimal("1")
    assert funding[0].next_funding_time is not None
    assert funding[0].next_funding_time.minute == 0
    assert funding[0].next_funding_time.second == 0
    assert funding[0].next_funding_time > funding[0].timestamp
