from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.exchanges.mexc.client import (
    _decode_spot_book_ticker,
    _decode_spot_depth,
)


@pytest.mark.asyncio
async def test_mexc_default_rest_transports_use_separate_official_clients() -> None:
    adapter = MexcPublicAdapter()
    spot = await adapter._ensure_http(futures=False)
    futures = await adapter._ensure_http(futures=True)

    assert str(spot.base_url) == "https://api.mexc.com"
    assert str(futures.base_url) == "https://api.mexc.com"
    assert spot is not futures
    await adapter.close()


def _contract() -> dict[str, object]:
    return {
        "symbol": "BTC_USDT",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "futureType": 1,
        "contractSize": "0.0001",
        "priceUnit": "0.1",
        "volUnit": "1",
        "minVol": "2",
        "state": 0,
        "apiAllowed": True,
        "preMarket": False,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [("apiAllowed", False), ("preMarket", True), ("state", 4)],
)
def test_mexc_only_enables_api_tradable_contracts(field: str, value: object) -> None:
    row = _contract()
    row[field] = value

    assert not MexcPublicAdapter()._parse_future_instrument(row).is_active


@pytest.mark.asyncio
async def test_mexc_instruments_keep_contract_and_base_units_distinct() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/contract/detail/country":
            return httpx.Response(200, json={"success": True, "code": 0, "data": [_contract()]})
        assert request.url.path == "/api/v3/exchangeInfo"
        return httpx.Response(
            200,
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "status": "1",
                        "isSpotTradingAllowed": True,
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {
                                "filterType": "LOT_SIZE",
                                "stepSize": "0.00001",
                                "minQty": "0.0001",
                            },
                        ],
                    }
                ]
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = MexcPublicAdapter(base_url="https://test.invalid", http_client=client)
    instruments = await adapter.get_instruments()
    await client.aclose()

    perpetual = next(
        item for item in instruments if item.instrument_type is InstrumentType.PERPETUAL
    )
    spot = next(item for item in instruments if item.instrument_type is InstrumentType.SPOT)
    assert perpetual.contract_size == Decimal("0.0001")
    assert perpetual.step_size == Decimal("0.0001")
    assert perpetual.min_order_size == Decimal("0.0002")
    assert spot.exchange_symbol == "BTCUSDT"
    assert spot.step_size == Decimal("0.00001")


@pytest.mark.asyncio
async def test_mexc_spot_instrument_uses_documented_empty_filter_precisions() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/contract/detail/country":
            return httpx.Response(200, json={"success": True, "code": 0, "data": []})
        return httpx.Response(
            200,
            json={
                "symbols": [
                    {
                        "symbol": "TOMO3LUSDT",
                        "baseAsset": "TOMO3L",
                        "quoteAsset": "USDT",
                        "status": "ENABLED",
                        "baseAssetPrecision": 2,
                        "quotePrecision": 3,
                        "baseSizePrecision": "0.0001",
                        "quoteAmountPrecision": "5",
                        "isSpotTradingAllowed": True,
                        "filters": [],
                    }
                ]
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = MexcPublicAdapter(base_url="https://test.invalid", http_client=client)
    instruments = await adapter.get_instruments()
    await client.aclose()

    assert instruments[0].tick_size == Decimal("0.001")
    assert instruments[0].step_size == Decimal("0.01")
    assert instruments[0].min_order_size == Decimal("0.0001")


@pytest.mark.asyncio
async def test_mexc_futures_ticker_and_book_convert_contracts_to_base_quantity() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/contract/ticker":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "code": 0,
                    "data": {
                        "symbol": "BTC_USDT",
                        "lastPrice": "100000",
                        "bid1": "99999",
                        "ask1": "100001",
                        "volume24": "5000",
                        "holdVol": "7000",
                        "timestamp": 1735689600000,
                    },
                },
            )
        if request.url.path == "/api/v3/ticker/24hr":
            return httpx.Response(200, json=[])
        assert request.url.path == "/api/v1/contract/depth/BTC_USDT"
        return httpx.Response(
            200,
            json={
                "success": True,
                "code": 0,
                "data": {
                    "bids": [["99999", "10", "1"]],
                    "asks": [["100001", "20", "1"]],
                    "version": 7,
                    "timestamp": 1735689600000,
                },
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = MexcPublicAdapter(base_url="https://test.invalid", http_client=client)
    adapter._contract_sizes["BTC_USDT"] = Decimal("0.0001")
    tickers = await adapter.get_tickers()
    book = await adapter.get_orderbook("BTC_USDT", 20)
    await client.aclose()

    assert tickers[0].volume_24h == Decimal("0.5")
    assert tickers[0].open_interest == Decimal("0.7")
    assert book.bids[0].quantity == Decimal("0.0010")
    assert book.asks[0].quantity == Decimal("0.0020")
    assert book.sequence == 7


@pytest.mark.asyncio
async def test_mexc_funding_uses_exact_cycle_and_next_settlement() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "success": True,
                "code": 0,
                "data": [
                    {
                        "symbol": "BTC_USDT",
                        "fundingRate": "0.00018",
                        "collectCycle": 4,
                        "nextSettleTime": 1735704000000,
                        "idxPrice": "100000",
                        "fairPrice": "100010",
                        "timestamp": 1735689600000,
                    }
                ],
            },
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid"
    )
    adapter = MexcPublicAdapter(base_url="https://test.invalid", http_client=client)
    funding = await adapter.get_funding_rates()
    await client.aclose()

    assert funding[0].funding_interval_hours == Decimal("4")
    assert funding[0].next_funding_time == datetime(2025, 1, 1, 4, tzinfo=UTC)
    assert funding[0].funding_rate == Decimal("0.00018")


def test_mexc_spot_protobuf_book_ticker_and_depth_are_decoded() -> None:
    ticker = b"".join(
        (
            _field(1, b"99"),
            _field(2, b"2"),
            _field(3, b"101"),
            _field(4, b"3"),
        )
    )
    ticker_wrapper = b"".join(
        (
            _field(3, b"BTCUSDT"),
            _varint_field(6, 1735689600000),
            _field(315, ticker),
        )
    )
    decoded_ticker = _decode_spot_book_ticker(ticker_wrapper)
    assert decoded_ticker is not None
    assert decoded_ticker[:3] == ("BTCUSDT", Decimal("99"), Decimal("101"))

    ask = _field(1, b"101") + _field(2, b"3")
    bid = _field(1, b"99") + _field(2, b"2")
    depth = _field(1, ask) + _field(2, bid) + _field(4, b"42")
    depth_wrapper = b"".join(
        (
            _field(3, b"BTCUSDT"),
            _varint_field(6, 1735689600000),
            _field(303, depth),
        )
    )
    decoded_depth = _decode_spot_depth(depth_wrapper, 20)
    assert decoded_depth is not None
    assert decoded_depth[1][0].price == Decimal("99")
    assert decoded_depth[2][0].price == Decimal("101")
    assert decoded_depth[4] == 42


def _field(number: int, value: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(value)) + value


def _varint_field(number: int, value: int) -> bytes:
    return _varint(number << 3) + _varint(value)


def _varint(value: int) -> bytes:
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        result.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(result)
