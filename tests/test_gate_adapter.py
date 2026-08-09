from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType, Ticker
from funding_arbitrage.exchanges.gate import GatePublicAdapter


def response(payload: object) -> httpx.Response:
    return httpx.Response(200, json=payload)


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
                        "in_delisting": False,
                    }
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
                        "funding_next_apply": 1735718400,
                        "volume_24h_settle": "12",
                        "total_size": "300",
                        "t": 1735689600000,
                    }
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
    assert history[0].funding_timestamp.year == 2025
    assert orderbook.sequence == 42


@pytest.mark.asyncio
async def test_gate_websocket_reconnects_after_disconnect() -> None:
    adapter = GatePublicAdapter(max_reconnects=2)
    attempts = 0

    async def fake_connection(_: list[str]) -> AsyncIterator[Ticker]:
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
    ticker = await anext(adapter.stream_tickers(["BTC_USDT"]))

    assert attempts == 2
    assert ticker.symbol == "BTC_USDT"
    assert ticker.last_price == Decimal("100")
