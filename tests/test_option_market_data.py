from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    InstrumentKey,
    InstrumentType,
    OptionQuoteSnapshot,
    OptionRight,
)
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.execution.option_fees import option_trade_fee
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.market_data.option_quotes import (
    bounded_option_chain,
    canonical_option_quote_event,
)

NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
EXPIRY = datetime(2026, 9, 25, 8, tzinfo=UTC)
EXPIRY_MS = str(int(EXPIRY.timestamp() * 1000))


def test_option_trade_fee_uses_index_notional_and_premium_cap() -> None:
    uncapped = option_trade_fee(
        option_price=Decimal("5"),
        underlying_price=Decimal("100"),
        quantity_contracts=Decimal("1"),
        contract_multiplier=Decimal("0.1"),
        fee_rate=Decimal("0.0003"),
        fee_cap_rate=Decimal("0.07"),
    )
    capped = option_trade_fee(
        option_price=Decimal("0.01"),
        underlying_price=Decimal("100"),
        quantity_contracts=Decimal("1"),
        contract_multiplier=Decimal("0.1"),
        fee_rate=Decimal("0.0003"),
        fee_cap_rate=Decimal("0.07"),
    )

    assert uncapped == Decimal("0.003")
    assert capped == Decimal("0.00007")
    with pytest.raises(ValueError):
        option_trade_fee(
            option_price=Decimal("0.01"),
            underlying_price=Decimal("100"),
            quantity_contracts=Decimal("1"),
            contract_multiplier=Decimal("0.1"),
            fee_rate=Decimal("-0.0003"),
            fee_cap_rate=Decimal("0.07"),
        )


def _bybit_response(
    result: dict[str, object],
    *,
    timestamp: str = str(int(NOW.timestamp() * 1000)),
) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "retCode": 0,
            "retMsg": "OK",
            "time": timestamp,
            "result": result,
        },
    )


def _bybit_instrument(symbol: str, right: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "Trading",
        "optionsType": right,
        "baseCoin": "BTC",
        "quoteCoin": "USD",
        "settleCoin": "USDC",
        "deliveryTime": EXPIRY_MS,
        "priceFilter": {"tickSize": "0.1"},
        "lotSizeFilter": {"qtyStep": "0.01", "minOrderQty": "0.01"},
    }


def _bybit_ticker(
    symbol: str,
    *,
    bid: str,
    ask: str,
) -> dict[str, str]:
    return {
        "symbol": symbol,
        "underlyingPrice": "60025.5",
        "bid1Price": bid,
        "bid1Size": "2.5",
        "ask1Price": ask,
        "ask1Size": "3.5",
        "markIv": "0.55",
        "openInterest": "120",
        "volume24h": "45",
    }


@pytest.mark.asyncio
async def test_bybit_option_chain_normalizes_executable_quotes_without_synthesis(
    caplog: pytest.LogCaptureFixture,
) -> None:
    call_symbol = "BTC-25SEP26-60000-C-USDT"
    put_symbol = "BTC-25SEP26-60000-P-USDT"
    invalid_symbol = "BTC-25SEP26-61000-C-USDT"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/instruments-info"):
            assert request.url.params["category"] == "option"
            assert request.url.params["baseCoin"] == "BTC"
            assert request.url.params["status"] == "Trading"
            return _bybit_response(
                {
                    "list": [
                        _bybit_instrument(call_symbol, "Call"),
                        _bybit_instrument(put_symbol, "Put"),
                        _bybit_instrument(invalid_symbol, "Call"),
                    ]
                }
            )
        if request.url.path.endswith("/tickers"):
            return _bybit_response(
                {
                    "list": [
                        _bybit_ticker(call_symbol, bid="1999", ask="2001"),
                        _bybit_ticker(put_symbol, bid="1900", ask="1902"),
                        _bybit_ticker(invalid_symbol, bid="0", ask="100"),
                    ]
                }
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://test.invalid",
    )
    adapter = BybitPublicAdapter(
        base_url="https://test.invalid",
        http_client=client,
    )
    quotes = await adapter.get_option_chain(("btc", "BTC", " "))
    await client.aclose()

    assert len(quotes) == 2
    by_right = {quote.instrument.option_right: quote for quote in quotes}
    assert set(by_right) == {OptionRight.CALL, OptionRight.PUT}
    call = by_right[OptionRight.CALL]
    assert call.instrument.strike_price == Decimal("60000")
    assert call.instrument.exchange_symbol == call_symbol
    assert call.instrument.expiry == EXPIRY
    assert call.instrument.quote_asset == "USD"
    assert call.instrument.settlement_asset == "USDC"
    assert call.bid_price == Decimal("1999")
    assert call.ask_price == Decimal("2001")
    assert call.bid_quantity == Decimal("2.5")
    assert call.ask_quantity == Decimal("3.5")
    assert call.mark_implied_volatility == Decimal("0.55")
    assert call.contract_multiplier == Decimal("1")
    assert call.price_tick == Decimal("0.1")
    assert call.quantity_step == Decimal("0.01")
    assert call.minimum_quantity == Decimal("0.01")
    assert call.exchange_timestamp == NOW
    assert all(quote.bid_price > 0 for quote in quotes)
    assert caplog.messages.count("bybit_option_quotes_skipped") == 1


def _okx_response(data: list[dict[str, str]]) -> httpx.Response:
    return httpx.Response(200, json={"code": "0", "data": data})


def _okx_instrument(symbol: str, right: str) -> dict[str, str]:
    return {
        "instId": symbol,
        "instFamily": "BTC-USD",
        "uly": "BTC-USD",
        "state": "live",
        "optType": right,
        "ctVal": "0.01",
        "ctMult": "2",
        "ctValCcy": "BTC",
        "settleCcy": "BTC",
        "expTime": EXPIRY_MS,
        "stk": "60000",
        "tickSz": "0.0001",
        "lotSz": "1",
        "minSz": "1",
    }


@pytest.mark.asyncio
async def test_okx_option_chain_converts_native_coin_prices_and_contract_size(
    caplog: pytest.LogCaptureFixture,
) -> None:
    call_symbol = "BTC-USD-260925-60000-C"
    put_symbol = "BTC-USD-260925-60000-P"
    invalid_symbol = "BTC-USD-260925-61000-C"
    ticker_ms = str(int(NOW.timestamp() * 1000))
    summary_time = NOW - timedelta(seconds=1)
    summary_ms = str(int(summary_time.timestamp() * 1000))
    index_ms = str(int((NOW - timedelta(seconds=2)).timestamp() * 1000))
    oi_time = NOW - timedelta(seconds=3)
    oi_ms = str(int(oi_time.timestamp() * 1000))

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if not path.endswith("/index-tickers"):
            assert request.url.params["instFamily"] == "BTC-USD"
        if path.endswith("/public/instruments"):
            return _okx_response(
                [
                    _okx_instrument(call_symbol, "C"),
                    _okx_instrument(put_symbol, "P"),
                    _okx_instrument(invalid_symbol, "C"),
                ]
            )
        if path.endswith("/market/tickers"):
            return _okx_response(
                [
                    {
                        "instId": call_symbol,
                        "bidPx": "0.0100",
                        "bidSz": "3",
                        "askPx": "0.0110",
                        "askSz": "4",
                        "vol24h": "5",
                        "ts": ticker_ms,
                    },
                    {
                        "instId": put_symbol,
                        "bidPx": "0.0090",
                        "bidSz": "2",
                        "askPx": "0.0100",
                        "askSz": "3",
                        "vol24h": "6",
                        "ts": ticker_ms,
                    },
                    {
                        "instId": invalid_symbol,
                        "bidPx": "0",
                        "bidSz": "2",
                        "askPx": "0.0100",
                        "askSz": "3",
                        "vol24h": "6",
                        "ts": ticker_ms,
                    },
                ]
            )
        if path.endswith("/public/opt-summary"):
            return _okx_response(
                [
                    {"instId": call_symbol, "markVol": "0.55", "ts": summary_ms},
                    {"instId": put_symbol, "markVol": "0.56", "ts": summary_ms},
                    {
                        "instId": invalid_symbol,
                        "markVol": "0.56",
                        "ts": summary_ms,
                    },
                ]
            )
        if path.endswith("/market/index-tickers"):
            assert request.url.params["instId"] == "BTC-USD"
            return _okx_response(
                [{"instId": "BTC-USD", "idxPx": "50000", "ts": index_ms}]
            )
        if path.endswith("/public/open-interest"):
            return _okx_response(
                [
                    {"instId": call_symbol, "oi": "7", "ts": oi_ms},
                    {"instId": put_symbol, "oi": "8", "ts": oi_ms},
                ]
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://test.invalid",
    )
    adapter = OkxPublicAdapter(
        base_url="https://test.invalid",
        http_client=client,
    )
    quotes = await adapter.get_option_chain(("BTC",))
    await client.aclose()

    assert len(quotes) == 2
    call = next(
        quote
        for quote in quotes
        if quote.instrument.option_right is OptionRight.CALL
    )
    assert call.instrument.strike_price == Decimal("60000")
    assert call.bid_price == Decimal("500")
    assert call.ask_price == Decimal("550")
    assert call.price_tick == Decimal("5")
    assert call.contract_multiplier == Decimal("0.02")
    assert call.native_price_multiplier == Decimal("50000")
    assert call.open_interest_contracts == Decimal("7")
    assert call.volume_contracts == Decimal("5")
    assert call.exchange_timestamp == oi_time
    assert caplog.messages.count("okx_option_quotes_skipped") == 1


def _quote(
    *,
    right: OptionRight,
    strike: str,
    expiry: datetime,
    timestamp: datetime = NOW,
    bid: str = "4",
    ask: str = "4.1",
) -> OptionQuoteSnapshot:
    return OptionQuoteSnapshot(
        instrument=InstrumentKey(
            venue="BYBIT",
            exchange_symbol=(
                f"BTC-{expiry:%d%b%y}-{strike}-{right.value[0]}"
            ).upper(),
            base_asset="BTC",
            quote_asset="USDT",
            settlement_asset="USDT",
            instrument_type=InstrumentType.OPTION,
            expiry=expiry,
            strike_price=Decimal(strike),
            option_right=right,
        ),
        underlying_price=Decimal("100"),
        bid_price=Decimal(bid),
        bid_quantity=Decimal("10"),
        ask_price=Decimal(ask),
        ask_quantity=Decimal("10"),
        mark_implied_volatility=Decimal("0.5"),
        contract_multiplier=Decimal("1"),
        price_tick=Decimal("0.1"),
        quantity_step=Decimal("1"),
        minimum_quantity=Decimal("1"),
        exchange_timestamp=timestamp,
    )


def test_option_event_is_deterministic_and_round_trips_with_typed_payload() -> None:
    quote = _quote(
        right=OptionRight.CALL,
        strike="100",
        expiry=NOW + timedelta(days=30),
    )
    first = canonical_option_quote_event(
        quote,
        source="bybit.public.option.rest",
        receive_timestamp=NOW + timedelta(milliseconds=1),
    )
    repeated = canonical_option_quote_event(
        quote,
        source="BYBIT.PUBLIC.OPTION.REST",
        receive_timestamp=NOW + timedelta(milliseconds=2),
    )
    restored = EventEnvelope[OptionQuoteSnapshot].model_validate_json(
        first.model_dump_json()
    )

    assert first.kind is EventKind.OPTION_QUOTE_SNAPSHOT
    assert first.metadata.event_id == repeated.metadata.event_id
    assert first.metadata.sequence_id == repeated.metadata.sequence_id
    assert isinstance(restored.payload, OptionQuoteSnapshot)
    assert restored.payload == quote


def test_bounded_option_chain_keeps_complete_nearest_pairs_and_deduplicates() -> None:
    near_expiry = NOW + timedelta(days=7)
    far_expiry = NOW + timedelta(days=30)
    at_money_call = _quote(
        right=OptionRight.CALL,
        strike="100",
        expiry=near_expiry,
    )
    quotes = [
        at_money_call,
        at_money_call,
        _quote(right=OptionRight.PUT, strike="100", expiry=near_expiry),
        _quote(right=OptionRight.CALL, strike="110", expiry=near_expiry),
        _quote(right=OptionRight.PUT, strike="110", expiry=near_expiry),
        _quote(right=OptionRight.CALL, strike="90", expiry=near_expiry),
        _quote(right=OptionRight.CALL, strike="100", expiry=far_expiry),
        _quote(right=OptionRight.PUT, strike="100", expiry=far_expiry),
    ]

    selected = bounded_option_chain(
        quotes,
        as_of=NOW,
        maximum_expiries=1,
        strikes_per_expiry=1,
    )

    assert len(selected) == 2
    assert {quote.instrument.option_right for quote in selected} == {
        OptionRight.CALL,
        OptionRight.PUT,
    }
    assert {quote.instrument.strike_price for quote in selected} == {
        Decimal("100")
    }
    assert {quote.instrument.expiry for quote in selected} == {near_expiry}

    conflicting = at_money_call.model_copy(update={"bid_price": Decimal("3.9")})
    with pytest.raises(ValueError, match="conflicting option quotes"):
        bounded_option_chain(
            [at_money_call, conflicting],
            as_of=NOW,
            maximum_expiries=1,
            strikes_per_expiry=1,
        )

    mismatched_settlement_put = quotes[1].model_copy(
        update={
            "instrument": quotes[1].instrument.model_copy(
                update={"settlement_asset": "USDC"}
            )
        }
    )
    assert bounded_option_chain(
        [at_money_call, mismatched_settlement_put],
        as_of=NOW,
        maximum_expiries=1,
        strikes_per_expiry=1,
    ) == []


@pytest.mark.asyncio
async def test_collector_exposes_only_fresh_option_quotes_published_to_journal() -> None:
    quotes = [
        _quote(
            right=OptionRight.CALL,
            strike="100",
            expiry=NOW + timedelta(days=30),
        ),
        _quote(
            right=OptionRight.PUT,
            strike="100",
            expiry=NOW + timedelta(days=30),
        ),
    ]

    class OptionAdapter:
        name = "bybit"

        async def get_option_chain(
            self,
            base_assets: tuple[str, ...],
        ) -> list[OptionQuoteSnapshot]:
            assert base_assets == ("BTC",)
            return quotes

    failed_events: list[EventEnvelope[OptionQuoteSnapshot]] = []

    async def failing_sink(event: EventEnvelope[OptionQuoteSnapshot]) -> None:
        failed_events.append(event)
        raise RuntimeError("journal unavailable")

    failed = MarketDataCollector(
        [OptionAdapter()],  # type: ignore[list-item]
        option_assets=("BTC",),
        option_refresh_seconds=1,
        stale_after_seconds=5,
        canonical_option_event_sink=failing_sink,
        clock=lambda: NOW,
    )
    assert await failed._load_option_quotes(OptionAdapter(), NOW) == ()  # noqa: SLF001
    assert failed._option_quote_cache == {}  # noqa: SLF001
    assert failed_events

    published: list[EventEnvelope[OptionQuoteSnapshot]] = []

    async def healthy_sink(event: EventEnvelope[OptionQuoteSnapshot]) -> None:
        published.append(event)

    healthy = MarketDataCollector(
        [OptionAdapter()],  # type: ignore[list-item]
        option_assets=("BTC",),
        option_refresh_seconds=1,
        stale_after_seconds=5,
        canonical_option_event_sink=healthy_sink,
        clock=lambda: NOW,
    )
    accepted = await healthy._load_option_quotes(OptionAdapter(), NOW)  # noqa: SLF001

    assert accepted == tuple(sorted(quotes, key=lambda item: item.instrument.canonical_id))
    assert len(published) == 2
    assert all(event.kind is EventKind.OPTION_QUOTE_SNAPSHOT for event in published)
    assert [event.payload.instrument.canonical_id for event in published] == sorted(
        event.payload.instrument.canonical_id for event in published
    )
    assert (
        await healthy._load_option_quotes(  # noqa: SLF001
            OptionAdapter(),
            NOW + timedelta(seconds=6),
        )
        == ()
    )


@pytest.mark.asyncio
async def test_collector_uses_request_completion_time_for_option_freshness() -> None:
    response_time = NOW + timedelta(milliseconds=50)
    quotes = [
        _quote(
            right=right,
            strike="100",
            expiry=NOW + timedelta(days=30),
            timestamp=response_time,
        )
        for right in (OptionRight.CALL, OptionRight.PUT)
    ]

    class OptionAdapter:
        name = "bybit"

        async def get_option_chain(
            self,
            base_assets: tuple[str, ...],
        ) -> list[OptionQuoteSnapshot]:
            assert base_assets == ("BTC",)
            return quotes

    collector = MarketDataCollector(
        [OptionAdapter()],  # type: ignore[list-item]
        option_assets=("BTC",),
        stale_after_seconds=5,
        clock=lambda: NOW + timedelta(milliseconds=100),
    )

    accepted = await collector._load_option_quotes(  # noqa: SLF001
        OptionAdapter(),
        NOW,
    )

    assert accepted == tuple(sorted(quotes, key=lambda item: item.instrument.canonical_id))
