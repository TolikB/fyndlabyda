"""Regression tests for the market-data defects found in the v1 adapters."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest

from funding_arbitrage.exchanges.base.exceptions import NetworkError, RateLimitError
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    MarketKey,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.market_data.health import CircuitBreaker, VenueStatus
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.opportunity.models import CostBreakdown
from tests.builders import spot_perp_market

D = Decimal


def client(handler: Any, base_url: str = "https://test.invalid") -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=base_url)


GATE_CONTRACT = {
    "name": "BTC_USDT",
    "quanto_multiplier": "0.0001",
    "order_price_round": "0.1",
    "order_size_min": "1",
    "funding_interval": 14_400,
    "funding_next_apply": 1_790_000_000,
    "in_delisting": False,
}


async def test_gate_futures_book_is_converted_from_contracts_to_base_units() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/futures/usdt/contracts"):
            return httpx.Response(200, json=[GATE_CONTRACT])
        if path.endswith("/spot/currency_pairs"):
            return httpx.Response(200, json=[])
        if path.endswith("/futures/usdt/order_book"):
            return httpx.Response(
                200,
                json={
                    "id": 7,
                    "current": 1735689600.123,
                    "bids": [{"p": "99990", "s": 5000}],
                    "asks": [{"p": "100010", "s": 3000}],
                },
            )
        if path.endswith("/spot/order_book"):
            # Gate spot books report milliseconds.
            return httpx.Response(
                200,
                json={
                    "current": 1735689600123,
                    "bids": [["99990", "0.5"]],
                    "asks": [["100010", "0.3"]],
                },
            )
        return httpx.Response(404)

    http = client(handler, "https://test.invalid/api/v4")
    adapter = GatePublicAdapter(base_url="https://test.invalid/api/v4", http_client=http)
    futures = await adapter.get_orderbook("BTC_USDT", 20, InstrumentType.PERPETUAL)
    spot = await adapter.get_orderbook("BTC_USDT", 20, InstrumentType.SPOT)
    instruments = await adapter.get_instruments()
    await http.aclose()

    assert futures.bids[0].quantity == D("0.5")  # 5000 contracts x 0.0001 BTC
    assert futures.asks[0].quantity == D("0.3")
    assert futures.instrument_type is InstrumentType.PERPETUAL
    assert spot.timestamp == datetime(2025, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
    perp = next(item for item in instruments if item.instrument_type is InstrumentType.PERPETUAL)
    assert perp.step_size == D("0.0001") and perp.min_order_size == D("0.0001")
    assert perp.funding_interval == 4


async def test_gate_funding_uses_contract_interval_and_rolls_next_time() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/futures/usdt/contracts"):
            return httpx.Response(200, json=[GATE_CONTRACT])
        if path.endswith("/spot/currency_pairs"):
            return httpx.Response(200, json=[])
        if path.endswith("/futures/usdt/tickers"):
            return httpx.Response(
                200, json=[{"contract": "BTC_USDT", "last": "100", "funding_rate": "0.0004"}]
            )
        return httpx.Response(404)

    http = client(handler, "https://test.invalid/api/v4")
    adapter = GatePublicAdapter(base_url="https://test.invalid/api/v4", http_client=http)
    funding = await adapter.get_funding_rates()
    await http.aclose()
    snapshot = funding[0]
    assert snapshot.funding_interval_hours == D("4")
    assert snapshot.funding_rate_8h == D("0.0008")
    assert snapshot.next_funding_time is not None
    assert snapshot.next_funding_time > datetime.now(UTC)


async def test_gate_rate_limit_pauses_all_concurrent_requests() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "1"}, json={"label": "TOO_MANY"})

    http = client(handler, "https://test.invalid/api/v4")
    adapter = GatePublicAdapter(base_url="https://test.invalid/api/v4", http_client=http)
    with pytest.raises(RateLimitError) as error:
        await adapter.get_tickers()
    assert error.value.retry_after == 1.0
    started = time.monotonic()
    results = await asyncio.gather(
        *(adapter.get_tickers() for _ in range(3)), return_exceptions=True
    )
    await http.aclose()
    assert all(isinstance(result, RateLimitError) for result in results)
    # Every caller waited for the venue's cooldown instead of hammering it.
    assert time.monotonic() - started >= 0.9


async def test_okx_swap_book_uses_ct_val_and_funding_time_is_next_settlement() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/public/instruments"):
            if request.url.params["instType"] == "SWAP":
                return httpx.Response(
                    200,
                    json={
                        "code": "0",
                        "data": [
                            {
                                "instId": "BTC-USDT-SWAP",
                                "ctType": "linear",
                                "ctVal": "0.01",
                                "ctValCcy": "BTC",
                                "settleCcy": "USDT",
                                "lotSz": "0.01",
                                "minSz": "0.01",
                                "tickSz": "0.1",
                                "state": "live",
                            },
                            {
                                "instId": "BTC-USD-SWAP",
                                "ctType": "inverse",
                                "ctVal": "100",
                                "ctValCcy": "USD",
                                "settleCcy": "BTC",
                                "tickSz": "0.1",
                            },
                        ],
                    },
                )
            return httpx.Response(200, json={"code": "0", "data": []})
        if path.endswith("/market/books"):
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "bids": [["99990", "25", "0", "3"]],
                            "asks": [["100010", "40", "0", "2"]],
                            "ts": "1735689600000",
                        }
                    ],
                },
            )
        if path.endswith("/public/funding-rate"):
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "fundingRate": "0.0001",
                            "fundingTime": "1735718400000",
                            "nextFundingTime": "1735732800000",
                            "ts": "1735689600000",
                        }
                    ],
                },
            )
        return httpx.Response(404)

    http = client(handler)
    adapter = OkxPublicAdapter(base_url="https://test.invalid", http_client=http)
    instruments = await adapter.get_instruments()
    book = await adapter.get_orderbook("BTC-USDT-SWAP", 20)
    funding = await adapter.get_funding_rates()
    await http.aclose()

    assert [item.exchange_symbol for item in instruments] == ["BTC-USDT-SWAP"]
    assert instruments[0].step_size == D("0.0001")
    assert book.bids[0].quantity == D("0.25")  # 25 contracts x 0.01 BTC
    assert book.sequence is None  # the old adapter stored ms timestamps as int32 ids
    assert funding[0].next_funding_time == datetime(2025, 1, 1, 8, 0, tzinfo=UTC)
    assert funding[0].funding_interval_hours == D("4")


async def test_binance_skips_delivery_contracts_and_uses_funding_info() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/fapi/v1/fundingInfo"):
            return httpx.Response(200, json=[{"symbol": "XYZUSDT", "fundingIntervalHours": 4}])
        if path.endswith("/fapi/v1/premiumIndex"):
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "XYZUSDT",
                        "markPrice": "1",
                        "lastFundingRate": "0.0002",
                        "nextFundingTime": 1735704000000,
                        "time": 1735689600000,
                    },
                    {
                        "symbol": "BTCUSDT_250328",
                        "markPrice": "100",
                        "lastFundingRate": "",
                        "nextFundingTime": 0,
                        "time": 1735689600000,
                    },
                ],
            )
        return httpx.Response(404)

    http = client(handler)
    adapter = BinancePublicAdapter(
        spot_base_url="https://test.invalid",
        futures_base_url="https://test.invalid",
        http_client=http,
    )
    funding = await adapter.get_funding_rates()
    await http.aclose()
    assert [item.symbol for item in funding] == ["XYZUSDT"]
    assert funding[0].funding_interval_hours == D("4")
    assert funding[0].funding_rate_daily == D("0.0012")


async def test_hyperliquid_history_paginates_past_500_rows() -> None:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    hours = [start + timedelta(hours=index) for index in range(720)]

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        begin = datetime.fromtimestamp(body["startTime"] / 1000, tz=UTC)
        rows = [
            {"coin": "BTC", "fundingRate": "0.0000125", "time": int(moment.timestamp() * 1000)}
            for moment in hours
            if moment >= begin
        ][:500]
        return httpx.Response(200, json=rows)

    http = client(handler)
    adapter = HyperliquidPublicAdapter(base_url="https://test.invalid", http_client=http)
    points = await adapter.get_funding_history("BTC", start, hours[-1])
    await http.aclose()
    assert len(points) == 720
    assert points[-1].funding_timestamp == hours[-1]


def test_circuit_breaker_retries_after_cooldown() -> None:
    now = datetime(2026, 10, 1, tzinfo=UTC)
    breaker = CircuitBreaker(failure_threshold=2, base_cooldown_seconds=30)
    breaker.record_failure(now, "timeout")
    breaker.record_failure(now, "timeout")
    assert breaker.status is VenueStatus.OFFLINE
    assert not breaker.allow(now + timedelta(seconds=10))
    assert breaker.allow(now + timedelta(seconds=31))
    breaker.record_failure(now + timedelta(seconds=31), "timeout")
    assert breaker.retry_at == now + timedelta(seconds=31 + 60)
    breaker.record_success(now + timedelta(seconds=100))
    assert breaker.status is VenueStatus.ONLINE and breaker.allow(now)


def test_rate_limiter_pause_blocks_and_drains_bucket() -> None:
    limiter = RateLimiter(requests_per_second=100, burst=5)
    limiter.pause(0.2)
    assert limiter.tokens == 0
    assert limiter.blocked_until > time.monotonic()


class FailingAdapter(ExchangeAdapter):
    name = "broken"

    async def get_instruments(self) -> list[NormalizedInstrument]:
        raise NetworkError("down")

    async def get_tickers(self) -> list[Ticker]:
        raise NetworkError("down")

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        raise NetworkError("down")

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        raise NetworkError("down")

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        raise NetworkError("down")

    def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        raise NotImplementedError


class SlowAdapter(FailingAdapter):
    name = "slow"

    async def get_instruments(self) -> list[NormalizedInstrument]:
        await asyncio.sleep(5)
        return []


async def test_one_failing_or_hanging_venue_never_blanks_the_others() -> None:
    collector = MarketDataCollector(
        [MockExchangeAdapter("bybit"), FailingAdapter(), SlowAdapter()],
        venue_timeout_seconds=0.3,
    )
    started = time.monotonic()
    snapshot = await collector.collect_once()
    assert time.monotonic() - started < 2
    assert snapshot.venues["bybit"].collected
    assert not snapshot.venues["broken"].collected
    assert not snapshot.venues["slow"].collected
    assert {ticker.exchange for ticker in snapshot.tickers} == {"bybit"}
    # Spot and perpetual share "BTCUSDT"; both are addressable separately.
    spot = snapshot.ticker("bybit", "BTCUSDT", InstrumentType.SPOT)
    perp = snapshot.ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL)
    assert spot is not None and perp is not None and spot.last_price != perp.last_price
    failures = await collector.fetch_orderbooks(
        snapshot,
        [
            MarketKey("bybit", InstrumentType.SPOT, "BTCUSDT"),
            MarketKey("bybit", InstrumentType.PERPETUAL, "BTCUSDT"),
            MarketKey("broken", InstrumentType.PERPETUAL, "BTCUSDT"),
        ],
    )
    assert set(failures) == {MarketKey("broken", InstrumentType.PERPETUAL, "BTCUSDT")}
    spot_book = snapshot.orderbook("bybit", "BTCUSDT", InstrumentType.SPOT)
    perp_book = snapshot.orderbook("bybit", "BTCUSDT", InstrumentType.PERPETUAL)
    assert spot_book is not None and perp_book is not None
    assert spot_book.mid_price != perp_book.mid_price


def test_history_refresh_keeps_the_built_lookup_indexes() -> None:
    market = spot_perp_market(datetime(2026, 10, 1, 12, tzinfo=UTC))
    assert market.ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL) is not None
    refreshed = market.with_funding_history({("bybit", "BTCUSDT"): []})
    assert refreshed.funding_history == {("bybit", "BTCUSDT"): []}
    assert refreshed.__dict__["ticker_index"] is market.__dict__["ticker_index"]
    assert "funding_index" not in refreshed.__dict__  # never built, so nothing to keep
    assert refreshed.ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL) is market.ticker(
        "bybit", "BTCUSDT", InstrumentType.PERPETUAL
    )


def test_cost_total_sums_every_component() -> None:
    costs = CostBreakdown(
        entry_fees=Decimal("0.1"),
        exit_fees=Decimal("0.2"),
        entry_spread=Decimal("0.3"),
        exit_spread=Decimal("0.4"),
        entry_slippage=Decimal("0.5"),
        exit_slippage=Decimal("0.6"),
        borrowing_cost=Decimal("0.7"),
        network_cost=Decimal("0.8"),
    )
    assert costs.total == sum(costs.model_dump().values(), Decimal("0")) == Decimal("3.6")
