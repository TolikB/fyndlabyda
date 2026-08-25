from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
    InstrumentType,
)
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    NormalizedInstrument,
    Ticker,
)
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.public_events import (
    CcxtPublicEventNormalizer,
    PublicDataNormalizationError,
    PublicEventAccount,
    PublicEventProfile,
    PublicEventSupervisor,
    public_event_profiles,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.services.event_writer import EventWriterFailed

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
MARKET: dict[str, object] = {
    "id": "BTCUSDT",
    "symbol": "BTC/USDT:USDT",
    "base": "BTC",
    "quote": "USDT",
    "settle": "USDT",
    "spot": False,
    "swap": True,
    "future": False,
    "contractSize": "0.001",
}


class FakePublicExchange:
    rateLimit = 50
    precisionMode = 4

    def __init__(self, *, open_interest: bool = True) -> None:
        self.has = {
            "watchTrades": True,
            "fetchOHLCV": True,
            "fetchOpenInterest": open_interest,
            "watchLiquidations": True,
            "fetchLiquidations": False,
        }
        self.markets = {"BTC/USDT:USDT": dict(MARKET)}
        self.markets_by_id = {"BTCUSDT": [dict(MARKET)]}
        self.closed = False
        self.fetch_ohlcv_calls = 0
        self.fetch_open_interest_calls = 0

    async def load_markets(
        self, reload: bool = False
    ) -> dict[str, dict[str, object]]:
        return self.markets

    async def close(self) -> None:
        self.closed = True

    def market(self, symbol: str) -> dict[str, object]:
        return self.markets[symbol]

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        *,
        limit: int,
        params: object,
    ) -> list[list[object]]:
        assert symbol == "BTC/USDT:USDT"
        assert timeframe == "1m"
        assert limit == 2
        assert params == {}
        self.fetch_ohlcv_calls += 1
        return [
            [
                int(NOW.timestamp() * 1000),
                "60000",
                "60100",
                "59900",
                "60050",
                "500",
            ]
        ]

    async def fetch_open_interest(
        self, symbol: str, *, params: object
    ) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        assert params == {}
        self.fetch_open_interest_calls += 1
        return {
            "symbol": symbol,
            "timestamp": int(NOW.timestamp() * 1000),
            "openInterestAmount": "1000",
            "openInterestValue": "60000",
        }

    async def watch_trades(self, symbol: str, *, params: object) -> object:
        await asyncio.Event().wait()
        raise AssertionError((symbol, params))

    async def watch_liquidations(self, symbol: str, *, params: object) -> object:
        await asyncio.Event().wait()
        raise AssertionError((symbol, params))


class FlakyLoadExchange(FakePublicExchange):
    def __init__(self) -> None:
        super().__init__()
        self.load_calls = 0

    async def load_markets(
        self, reload: bool = False
    ) -> dict[str, dict[str, object]]:
        del reload
        self.load_calls += 1
        if self.load_calls == 1:
            raise OSError("temporary market bootstrap failure")
        return self.markets


class ImmediateTradeExchange(FakePublicExchange):
    async def watch_trades(self, symbol: str, *, params: object) -> object:
        assert params == {}
        return {
            "id": "trade-1",
            "symbol": symbol,
            "timestamp": int(NOW.timestamp() * 1000),
            "side": "buy",
            "price": "60000",
            "amount": "1",
        }

class EventCollector:
    def __init__(self) -> None:
        self.events: list[EventEnvelope[Any]] = []

    async def __call__(self, event: EventEnvelope[Any]) -> None:
        self.events.append(event)


def _profile() -> PublicEventProfile:
    return PublicEventProfile(
        "binance",
        "linear",
        "binanceusdm",
        "future",
        InstrumentType.PERPETUAL,
        ohlcv_volume_in_contracts=True,
        open_interest_amount_in_contracts=True,
    )


def _snapshot(*, open_interest: Decimal | None = Decimal("12.5")) -> MarketSnapshot:
    instrument = NormalizedInstrument(
        exchange="binance",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=LegacyInstrumentType.PERPETUAL,
        contract_size=Decimal("0.001"),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
        funding_interval=28_800,
    )
    ticker = Ticker(
        exchange="binance",
        symbol="BTCUSDT",
        instrument_type=LegacyInstrumentType.PERPETUAL,
        last_price=Decimal("60000"),
        mark_price=Decimal("60001"),
        index_price=Decimal("59999"),
        volume_24h=Decimal("1000000"),
        open_interest=open_interest,
        timestamp=NOW,
    )
    funding = FundingSnapshot(
        exchange="binance",
        symbol="BTCUSDT",
        funding_rate=Decimal("0.0001"),
        funding_interval_hours=Decimal("8"),
        next_funding_time=datetime(2026, 8, 20, 16, 0, tzinfo=UTC),
        mark_price=Decimal("60001"),
        index_price=Decimal("59999"),
        timestamp=NOW,
    )
    return MarketSnapshot([instrument], [ticker], [funding], {}, NOW)


def test_normalizer_preserves_contract_units_and_exchange_time() -> None:
    exchange = FakePublicExchange()
    normalizer = CcxtPublicEventNormalizer(_profile(), exchange)
    timestamp = int(NOW.timestamp() * 1000)

    trade = normalizer.trade_events(
        {
            "id": "trade-1",
            "symbol": "BTC/USDT:USDT",
            "timestamp": timestamp,
            "side": "buy",
            "price": "60000",
            "amount": "2",
        },
        received_at=NOW,
    )[0]
    candle = normalizer.candle_event(
        "BTC/USDT:USDT",
        [timestamp, "60000", "60100", "59900", "60050", "500"],
        received_at=NOW,
    )
    open_interest = normalizer.open_interest_event(
        {
            "symbol": "BTC/USDT:USDT",
            "timestamp": timestamp,
            "openInterestAmount": "1000",
            "openInterestValue": "60000",
        },
        received_at=NOW,
    )
    liquidation = normalizer.liquidation_events(
        {
            "id": "liq-1",
            "symbol": "BTC/USDT:USDT",
            "timestamp": timestamp,
            "side": "sell",
            "price": "59000",
            "contracts": "3",
            "contractSize": "0.001",
            "baseValue": "0.003",
            "quoteValue": "177",
        },
        received_at=NOW,
    )[0]

    assert trade.kind is EventKind.TRADE_TICK
    assert trade.payload.quantity == Decimal("0.002")
    assert candle.payload.volume == Decimal("0.500")
    assert open_interest.payload.open_interest_base == Decimal("1.000")
    assert open_interest.payload.open_interest_quote == Decimal("60000")
    assert liquidation.kind is EventKind.LIQUIDATION_TICK
    assert liquidation.payload.quantity == Decimal("0.003")
    assert all(
        event.metadata.quality is DataQuality.VALID
        for event in (trade, candle, open_interest, liquidation)
    )


def test_liquidation_normalizer_accepts_ccxt_bybit_shape_without_side() -> None:
    profile = PublicEventProfile(
        "bybit", "linear", "bybit", "swap", InstrumentType.PERPETUAL
    )
    normalizer = CcxtPublicEventNormalizer(profile, FakePublicExchange())

    event = normalizer.liquidation_events(
        {
            "id": "bybit-liq-1",
            "symbol": "BTC/USDT:USDT",
            "timestamp": int(NOW.timestamp() * 1000),
            "price": "59000",
            "contracts": "250",
            "contractSize": "0.001",
            "baseValue": "0.25",
            "quoteValue": "14750",
        },
        received_at=NOW,
    )[0]

    assert event.payload.quantity == Decimal("0.25")
    assert event.payload.side is None
    assert event.metadata.sequence_id == "BTCUSDT:bybit-liq-1"


def test_explicit_venue_unit_contracts_prevent_double_scaling() -> None:
    profiles = {
        venue: public_event_profiles(venue)[-1]
        for venue in ("gate", "okx", "mexc", "kucoin", "htx")
    }
    assert profiles["gate"].ohlcv_volume_in_contracts is True
    assert profiles["mexc"].ohlcv_volume_in_contracts is True
    assert profiles["kucoin"].ohlcv_volume_in_contracts is True
    assert profiles["okx"].ohlcv_volume_in_contracts is False
    assert profiles["htx"].ohlcv_volume_in_contracts is False
    assert profiles["htx"].open_interest_base_volume_in_contracts is True

    okx_candle = CcxtPublicEventNormalizer(
        profiles["okx"], FakePublicExchange()
    ).candle_event(
        "BTC/USDT:USDT",
        [int(NOW.timestamp() * 1000), "1", "2", "1", "2", "500"],
        received_at=NOW,
    )
    htx_oi = CcxtPublicEventNormalizer(
        profiles["htx"], FakePublicExchange()
    ).open_interest_event(
        {
            "symbol": "BTC/USDT:USDT",
            "timestamp": int(NOW.timestamp() * 1000),
            "baseVolume": "1000",
        },
        received_at=NOW,
    )

    assert okx_candle.payload.volume == Decimal("500")
    assert htx_oi.payload.open_interest_base == Decimal("1.000")

def test_normalizer_fails_closed_for_missing_trade_identity() -> None:
    normalizer = CcxtPublicEventNormalizer(_profile(), FakePublicExchange())
    with pytest.raises(PublicDataNormalizationError, match="trade ID"):
        normalizer.trade_events(
            {
                "symbol": "BTC/USDT:USDT",
                "timestamp": int(NOW.timestamp() * 1000),
                "price": "60000",
                "amount": "1",
            },
            received_at=NOW,
        )


def test_profiles_cover_all_eight_venues_and_both_accounts_where_available() -> None:
    venues = ("binance", "bybit", "gate", "okx", "hyperliquid", "mexc", "kucoin", "htx")
    profiles = {venue: public_event_profiles(venue) for venue in venues}

    assert set(profiles) == set(venues)
    assert profiles["hyperliquid"][0].instrument_type is InstrumentType.PERPETUAL
    for venue in set(venues) - {"hyperliquid"}:
        assert {profile.instrument_type for profile in profiles[venue]} == {
            InstrumentType.SPOT,
            InstrumentType.PERPETUAL,
        }


async def test_supervisor_mirrors_exact_funding_and_only_falls_back_for_missing_oi() -> None:
    exchange = FakePublicExchange(open_interest=False)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )

    await supervisor.start()
    await supervisor.observe_snapshot(_snapshot())
    await supervisor.close()

    assert exchange.closed
    assert len(supervisor.metadata_snapshots) == 1
    assert [item.stream for item in supervisor.required_quality_streams] == [
        EventKind.FUNDING_SNAPSHOT.value
    ]
    assert supervisor.metadata_snapshots[0].rate_limit_ms == Decimal("50")
    assert [event.kind for event in collector.events] == [
        EventKind.FUNDING_SNAPSHOT,
        EventKind.OPEN_INTEREST_SNAPSHOT,
    ]
    funding = collector.events[0].payload
    assert funding.funding_rate == Decimal("0.0001")
    assert funding.funding_interval_seconds == 28_800
    assert funding.mark_price == Decimal("60001")
    assert funding.index_price == Decimal("59999")
    assert funding.next_funding_time == datetime(2026, 8, 20, 16, 0, tzinfo=UTC)


async def test_supervisor_normalizes_blank_legacy_settlement_asset() -> None:
    exchange = FakePublicExchange(open_interest=True)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    source = _snapshot(open_interest=None)
    instrument = source.instruments[0].model_copy(update={"settlement_asset": ""})
    snapshot = MarketSnapshot(
        [instrument],
        source.tickers,
        source.funding,
        source.orderbooks,
        source.captured_at,
    )

    await supervisor.observe_snapshot(snapshot)

    assert len(collector.events) == 1
    assert collector.events[0].payload.instrument.settlement_asset is None
    assert supervisor.required_quality_streams


async def test_supervisor_does_not_duplicate_open_interest_when_rest_is_supported() -> None:
    exchange = FakePublicExchange(open_interest=True)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )

    await supervisor.start()
    await supervisor.observe_snapshot(_snapshot())
    await supervisor.close()

    assert [event.kind for event in collector.events] == [EventKind.FUNDING_SNAPSHOT]


async def test_funding_uses_same_snapshot_ticker_mark_and_index() -> None:
    exchange = FakePublicExchange(open_interest=True)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    snapshot = _snapshot(open_interest=None)
    incomplete = snapshot.funding[0].model_copy(update={"mark_price": None})
    snapshot = MarketSnapshot(
        snapshot.instruments,
        snapshot.tickers,
        [incomplete],
        {},
        NOW,
    )

    await supervisor.start()
    await supervisor.observe_snapshot(snapshot)
    await supervisor.close()

    assert [event.kind for event in collector.events] == [EventKind.FUNDING_SNAPSHOT]
    funding = collector.events[0].payload
    assert funding.mark_price == Decimal("60001")
    assert funding.index_price == Decimal("59999")


async def test_incomplete_funding_is_not_published() -> None:
    exchange = FakePublicExchange(open_interest=True)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    snapshot = _snapshot(open_interest=None)
    incomplete_funding = snapshot.funding[0].model_copy(
        update={"mark_price": None, "index_price": None}
    )
    incomplete_ticker = snapshot.tickers[0].model_copy(
        update={"mark_price": None, "index_price": None}
    )
    snapshot = MarketSnapshot(
        snapshot.instruments,
        [incomplete_ticker],
        [incomplete_funding],
        {},
        NOW,
    )

    await supervisor.start()
    await supervisor.observe_snapshot(snapshot)
    await supervisor.close()

    assert collector.events == []


async def test_initial_market_load_failure_is_retried_without_restart() -> None:
    exchange = FlakyLoadExchange()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        EventCollector(),
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )

    await supervisor.start()
    for _ in range(100):
        if exchange.load_calls >= 2 and supervisor.metadata_snapshots:
            break
        await asyncio.sleep(0.01)
    await supervisor.close()

    assert exchange.load_calls >= 2
    assert len(supervisor.metadata_snapshots) == 1


async def test_durable_sink_failure_is_not_misclassified_as_websocket_reconnect() -> None:
    exchange = ImmediateTradeExchange()

    async def fail_sink(event: EventEnvelope[Any]) -> None:
        del event
        raise EventWriterFailed("journal unavailable")

    account = PublicEventAccount(_profile(), exchange)
    supervisor = PublicEventSupervisor(
        [account],
        fail_sink,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    supervisor._desired_symbols[supervisor._key(account)] = ("BTC/USDT:USDT",)

    with pytest.raises(EventWriterFailed, match="journal unavailable"):
        await supervisor._watch_symbol(account, "trades", "BTC/USDT:USDT")

async def test_supervisor_rest_recovery_publishes_candle_and_open_interest() -> None:
    exchange = FakePublicExchange(open_interest=True)
    collector = EventCollector()
    supervisor = PublicEventSupervisor(
        [PublicEventAccount(_profile(), exchange)],
        collector,
        symbol_limit=1,
        rest_interval_seconds=0.01,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )

    await supervisor.start()
    await supervisor.observe_snapshot(_snapshot())
    for _ in range(100):
        kinds = {event.kind for event in collector.events}
        if EventKind.CANDLE in kinds and EventKind.OPEN_INTEREST_SNAPSHOT in kinds:
            break
        await asyncio.sleep(0.01)
    await supervisor.close()

    assert exchange.fetch_ohlcv_calls > 0
    assert exchange.fetch_open_interest_calls > 0
    assert EventKind.CANDLE in {event.kind for event in collector.events}
    assert EventKind.OPEN_INTEREST_SNAPSHOT in {
        event.kind for event in collector.events
    }