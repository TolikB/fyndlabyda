from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import InstrumentType, Ticker
from funding_arbitrage.exchanges.gate import GatePublicAdapter


def response(payload: object) -> httpx.Response:
    return httpx.Response(200, json=payload)


def test_gate_prelaunch_contract_is_not_tradable() -> None:
    adapter = GatePublicAdapter()
    instrument = adapter._parse_future_instrument(
        {
            "name": "KODEX200_USDT",
            "quanto_multiplier": "0.0001",
            "order_price_round": "0.01",
            "order_size_min": "1",
            "funding_interval": 28_800,
            "status": "prelaunch",
            "in_delisting": False,
        }
    )

    assert instrument.is_active is False


@pytest.mark.asyncio
async def test_gate_rest_payloads_are_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/futures/usdt/contracts"):
            return response(
                [
                    {
                        "name": "BTC_USDT",
                        "type": "direct",
                        "quanto_multiplier": "0.0001",
                        "order_price_round": "0.1",
                        "order_size_min": "1",
                        "funding_interval": 28800,
                        "funding_next_apply": 1735718400,
                        "in_delisting": False,
                    },
                    {
                        "name": "KODEX200_USDT",
                        "quanto_multiplier": "0.0001",
                        "order_price_round": "0.01",
                        "order_size_min": "1",
                        "funding_interval": 28_800,
                        "status": "prelaunch",
                        "in_delisting": False,
                    },
                ]
            )
        if path.endswith("/spot/currency_pairs"):
            return response(
                [
                    {
                        "id": "BTC_USDT",
                        "base": "BTC",
                        "quote": "USDT",
                        "amount_precision": 6,
                        "precision": 2,
                        "min_base_amount": "0.00001",
                        "trade_status": "tradable",
                    }
                ]
            )
        if path.endswith("/futures/usdt/tickers"):
            return response(
                [
                    {
                        "contract": "BTC_USDT",
                        "last": "100",
                        "mark_price": "100.1",
                        "index_price": "100",
                        "funding_rate": "0.001",
                        "volume_24h_settle": "12",
                        "total_size": "300",
                        "t": 1735689600000,
                    },
                    {
                        "contract": "KODEX200_USDT",
                        "last": "0",
                        "funding_rate": "0",
                        "volume_24h_settle": "0",
                        "t": 1735689600000,
                    },
                ]
            )
        if path.endswith("/spot/tickers"):
            return response(
                [
                    {
                        "currency_pair": "BTC_USDT",
                        "last": "100",
                        "highest_bid": "99.9",
                        "lowest_ask": "100.1",
                        "quote_volume": "12",
                    }
                ]
            )
        if path.endswith("/funding_rate"):
            return response([{"t": 1735689600, "r": "0.001"}])
        if path.endswith("/order_book"):
            return response(
                {
                    "id": 42,
                    "current": 1735689600,
                    "bids": [{"p": "99.9", "s": "2"}],
                    "asks": [{"p": "100.1", "s": "3"}],
                }
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid/api/v4"
    )
    adapter = GatePublicAdapter(base_url="https://test.invalid/api/v4", http_client=client)
    instruments = await adapter.get_instruments()
    tickers = await adapter.get_tickers()
    funding = await adapter.get_funding_rates()
    history = await adapter.get_funding_history(
        "BTC_USDT", datetime(2024, 12, 31, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)
    )
    orderbook = await adapter.get_orderbook("BTC_USDT", 20)
    await client.aclose()

    assert {item.instrument_type for item in instruments} == {
        InstrumentType.SPOT,
        InstrumentType.PERPETUAL,
    }
    assert tickers[0].last_price == Decimal("100")
    assert funding[0].funding_rate_daily == Decimal("0.003")
    assert funding[0].next_funding_time == datetime(2025, 1, 1, 8, tzinfo=UTC)
    assert "KODEX200_USDT" not in {item.symbol for item in tickers}
    assert "KODEX200_USDT" not in {item.symbol for item in funding}
    assert history[0].funding_timestamp.year == 2025
    assert orderbook.sequence == 42


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("instrument_type", "payload"),
    (
        (
            InstrumentType.SPOT,
            {
                "id": 42,
                "current": 1735689600000,
                "bids": [["99.8", "0"], ["99.9", "2"]],
                "asks": [["100.1", "3"]],
            },
        ),
        (
            InstrumentType.PERPETUAL,
            {
                "id": 42,
                "current": 1735689600,
                "bids": [{"p": "99.8", "s": "0"}, {"p": "99.9", "s": "2"}],
                "asks": [{"p": "100.1", "s": "3"}],
            },
        ),
    ),
)
async def test_gate_rest_orderbook_ignores_zero_size_levels(
    instrument_type: InstrumentType,
    payload: dict[str, object],
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: response(payload)),
        base_url="https://test.invalid/api/v4",
    ) as client:
        adapter = GatePublicAdapter(
            base_url="https://test.invalid/api/v4", http_client=client
        )
        orderbook = await adapter.get_orderbook(
            "BTC_USDT", 20, instrument_type=instrument_type
        )

    assert [level.price for level in orderbook.bids] == [Decimal("99.9")]
    assert orderbook.timestamp == datetime(2025, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("instrument_type", "bids"),
    (
        (InstrumentType.SPOT, [["99.9", "-1"]]),
        (InstrumentType.PERPETUAL, [{"p": "99.9", "s": "0"}]),
    ),
)
async def test_gate_rest_orderbook_rejects_non_executable_side(
    instrument_type: InstrumentType,
    bids: list[object],
) -> None:
    payload: dict[str, object] = {
        "id": 42,
        "current": 1735689600,
        "bids": bids,
        "asks": [["100.1", "3"]],
    }
    if instrument_type is InstrumentType.PERPETUAL:
        payload["asks"] = [{"p": "100.1", "s": "3"}]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: response(payload)),
        base_url="https://test.invalid/api/v4",
    ) as client:
        adapter = GatePublicAdapter(
            base_url="https://test.invalid/api/v4", http_client=client
        )
        with pytest.raises(InvalidResponseError):
            await adapter.get_orderbook(
                "BTC_USDT", 20, instrument_type=instrument_type
            )

@pytest.mark.asyncio
async def test_gate_websocket_reconnects_after_disconnect() -> None:
    adapter = GatePublicAdapter(max_reconnects=2)
    attempts = 0

    async def fake_connection(
        _url: str, _symbols: list[str], _kind: InstrumentType
    ) -> AsyncIterator[Ticker]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disconnected")
        yield adapter._parse_future_ticker(
            {
                "contract": "BTC_USDT",
                "last": "100",
                "volume_24h": "1",
                "t": 1735689600000,
            }
        )

    async def patched_sleep(_: float) -> None:
        return None

    adapter._sleep = patched_sleep
    adapter._ticker_connection = fake_connection  # type: ignore[assignment]
    stream = adapter.stream_tickers([("BTC_USDT", InstrumentType.PERPETUAL)])
    ticker = await anext(stream)
    await stream.aclose()

    assert attempts >= 2
    assert ticker.symbol == "BTC_USDT"
    assert ticker.last_price == Decimal("100")


@pytest.mark.asyncio
async def test_gate_advances_cached_funding_time_past_ticker_timestamp() -> None:
    stale_next = datetime(2025, 1, 1, 8, tzinfo=UTC)
    ticker_time = datetime(2025, 1, 2, 1, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/futures/usdt/contracts"):
            return response(
                [
                    {
                        "name": "BTC_USDT",
                        "funding_interval": 28_800,
                        "funding_next_apply": int(stale_next.timestamp()),
                    }
                ]
            )
        if request.url.path.endswith("/futures/usdt/tickers"):
            return response(
                [
                    {
                        "contract": "BTC_USDT",
                        "funding_rate": "0.001",
                        "mark_price": "100",
                        "index_price": "100",
                        "t": int(ticker_time.timestamp() * 1000),
                    }
                ]
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid/api/v4"
    )
    adapter = GatePublicAdapter(base_url="https://test.invalid/api/v4", http_client=client)
    funding = await adapter.get_funding_rates()
    await client.aclose()

    assert funding[0].next_funding_time == datetime(2025, 1, 2, 8, tzinfo=UTC)
    assert funding[0].next_funding_time > funding[0].timestamp


@pytest.mark.asyncio
async def test_gate_refreshes_dynamic_funding_interval_metadata() -> None:
    contract_requests = 0
    first_next = datetime(2025, 1, 1, 8, tzinfo=UTC)
    refreshed_next = datetime(2025, 1, 1, 4, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal contract_requests
        if request.url.path.endswith("/futures/usdt/contracts"):
            contract_requests += 1
            interval = 28_800 if contract_requests == 1 else 14_400
            next_time = first_next if contract_requests == 1 else refreshed_next
            return response(
                [
                    {
                        "name": "BTC_USDT",
                        "type": "direct",
                        "quanto_multiplier": "0.0001",
                        "order_price_round": "0.1",
                        "order_size_min": "1",
                        "funding_interval": interval,
                        "funding_next_apply": int(next_time.timestamp()),
                        "in_delisting": False,
                    }
                ]
            )
        if request.url.path.endswith("/spot/currency_pairs"):
            return response([])
        if request.url.path.endswith("/futures/usdt/tickers"):
            return response(
                [
                    {
                        "contract": "BTC_USDT",
                        "funding_rate": "0.001",
                        "mark_price": "100",
                        "index_price": "100",
                        "t": int((refreshed_next - timedelta(hours=1)).timestamp() * 1000),
                    }
                ]
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://test.invalid/api/v4"
    )
    adapter = GatePublicAdapter(
        base_url="https://test.invalid/api/v4",
        http_client=client,
        funding_metadata_ttl_seconds=0,
    )
    await adapter.get_instruments()
    funding = await adapter.get_funding_rates()
    await client.aclose()

    assert contract_requests == 2
    assert funding[0].funding_interval_hours == Decimal("4")
    assert funding[0].next_funding_time == refreshed_next
