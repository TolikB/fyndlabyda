from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
    InstrumentType,
    deterministic_event_id,
    snapshot_occurrence_id,
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
        self.fetch_ohlcv_since: list[int | None] = []
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
        since: int | None,
        limit: int,
        params: object,
    ) -> list[list[object]]:
        assert symbol == "BTC/USDT:USDT"
        assert timeframe == "1m"
        assert limit == 10
        assert params == {}
        self.fetch_ohlcv_calls += 1
        self.fetch_ohlcv_since.append(since)
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


class CursorCandleExchange(FakePublicExchange):
    def __init__(self) -> None:
        super().__init__(open_interest=False)
        self.rows: list[list[object]] = []
        self.fail_next_ohlcv = False

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        *,
        since: int | None,
        limit: int,
        params: object,
    ) -> list[list[object]]:
        assert symbol == "BTC/USDT:USDT"
        assert timeframe == "1m"
        assert limit == 10
        assert params == {}
        self.fetch_ohlcv_calls += 1
        self.fetch_ohlcv_since.append(since)
        if self.fail_next_ohlcv:
            self.fail_next_ohlcv = False
            raise OSError("temporary OHLCV failure")
        eligible = (
            self.rows
            if since is None
            else [row for row in self.rows if int(row[0]) >= since]
        )
        return eligible[:limit]

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


async def test_htx_websocket_trade_uses_exact_native_trade_id(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, factory = database
    exchange = FakePublicExchange()
    spot_market = {
        **MARKET,
        "id": "btcusdt",
        "symbol": "BTC/USDT",
        "settle": None,
        "spot": True,
        "swap": False,
        "contractSize": "1",
    }
    exchange.markets = {"BTC/USDT": spot_market}
    exchange.markets_by_id = {"btcusdt": [spot_market]}
    profile = PublicEventProfile(
        "htx",
        "spot",
        "htx",
        "spot",
        InstrumentType.SPOT,
    )
    normalizer = CcxtPublicEventNormalizer(profile, exchange)
    timestamp = int(NOW.timestamp() * 1000)
    common = {
        "id": 1.929663069621672e27,
        "symbol": "BTC/USDT",
        "timestamp": timestamp,
        "side": "buy",
    }

    events = normalizer.trade_events(
        [
            {
                **common,
                "price": "60000",
                "amount": "1",
                "info": {"id": common["id"], "tradeId": 103627592326},
            },
            {
                **common,
                "price": "60001",
                "amount": "2",
                "info": {"id": common["id"], "tradeId": 103627592327},
            },
        ],
        received_at=NOW,
    )

    assert [event.payload.trade_id for event in events] == [
        "103627592326",
        "103627592327",
    ]
    assert events[0].metadata.event_id != events[1].metadata.event_id
    replay = normalizer.trade_events(
        {
            **common,
            "price": "60000",
            "amount": "1",
            "info": {"id": common["id"], "tradeId": "103627592326"},
        },
        received_at=NOW + timedelta(seconds=1),
    )[0]
    assert replay.metadata.event_id == events[0].metadata.event_id
    assert replay.payload == events[0].payload
    async with factory() as session:
        assert await append_events(session, events) == 2
        assert await append_events(session, [replay]) == 0


def test_htx_derivative_trade_uses_native_snake_case_trade_id() -> None:
    profile = PublicEventProfile(
        "htx",
        "linear",
        "htx",
        "swap",
        InstrumentType.PERPETUAL,
    )
    event = CcxtPublicEventNormalizer(profile, FakePublicExchange()).trade_events(
        {
            "id": "aggregate-order-id",
            "symbol": "BTC/USDT:USDT",
            "timestamp": int(NOW.timestamp() * 1000),
            "side": "sell",
            "price": "60000",
            "amount": "2",
            "info": {"id": "aggregate-order-id", "trade_id": "152022944"},
        },
        received_at=NOW,
    )[0]

    assert event.payload.trade_id == "152022944"


def test_rest_candles_exclude_mutable_current_interval() -> None:
    normalizer = CcxtPublicEventNormalizer(_profile(), FakePublicExchange())
    received_at = NOW + timedelta(seconds=30)

    events = normalizer.closed_candle_events(
        "BTC/USDT:USDT",
        [
            [
                int((NOW - timedelta(minutes=1)).timestamp() * 1000),
                "60000",
                "60100",
                "59900",
                "60050",
                "500",
            ],
            [
                int(NOW.timestamp() * 1000),
                "60050",
                "60200",
                "60000",
                "60150",
                "250",
            ],
        ],
        received_at=received_at,
    )

    assert len(events) == 1
    assert events[0].payload.open_time == NOW - timedelta(minutes=1)
    assert events[0].payload.closed is True


async def test_two_rest_polls_append_each_candle_only_after_close(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, factory = database
    normalizer = CcxtPublicEventNormalizer(_profile(), FakePublicExchange())
    previous_open = NOW - timedelta(minutes=1)
    next_open = NOW + timedelta(minutes=1)
    first_poll = normalizer.closed_candle_events(
        "BTC/USDT:USDT",
        [
            [
                int(previous_open.timestamp() * 1000),
                "60000",
                "60100",
                "59900",
                "60050",
                "500",
            ],
            [
                int(NOW.timestamp() * 1000),
                "60050",
                "60200",
                "60000",
                "60100",
                "100",
            ],
        ],
        received_at=NOW + timedelta(seconds=30),
        received_monotonic_ns=100,
    )
    second_poll = normalizer.closed_candle_events(
        "BTC/USDT:USDT",
        [
            [
                int(NOW.timestamp() * 1000),
                "60050",
                "60300",
                "60000",
                "60250",
                "900",
            ],
            [
                int(next_open.timestamp() * 1000),
                "60250",
                "60400",
                "60200",
                "60300",
                "50",
            ],
        ],
        received_at=NOW + timedelta(seconds=90),
        received_monotonic_ns=101,
    )

    assert [event.payload.open_time for event in first_poll] == [previous_open]
    assert [event.payload.open_time for event in second_poll] == [NOW]
    async with factory() as session:
        assert await append_events(session, first_poll) == 1
        assert await append_events(session, second_poll) == 1
        assert await append_events(session, (*first_poll, *second_poll)) == 0


async def test_rest_poll_uses_fetch_completion_time_for_candle_closure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CompletedAt(datetime):
        @classmethod
        def now(cls, tz: object = None) -> CompletedAt:
            del tz
            return cls.fromtimestamp(
                (NOW + timedelta(seconds=66)).timestamp(),
                tz=UTC,
            )

    exchange = FakePublicExchange(open_interest=False)
    collector = EventCollector()
    account = PublicEventAccount(_profile(), exchange)
    supervisor = PublicEventSupervisor(
        [account],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    supervisor._normalizers[supervisor._key(account)] = CcxtPublicEventNormalizer(
        _profile(), exchange
    )
    monkeypatch.setattr(
        "funding_arbitrage.exchanges.public_events.datetime", CompletedAt
    )

    await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)

    candles = [event for event in collector.events if event.kind is EventKind.CANDLE]
    assert len(candles) == 1
    assert candles[0].payload.open_time == NOW
    assert candles[0].payload.closed is True


async def test_rest_candle_cursor_recovers_after_failed_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CompletedAt(datetime):
        @classmethod
        def now(cls, tz: object = None) -> CompletedAt:
            del tz
            return cls.fromtimestamp(
                (NOW + timedelta(minutes=3, seconds=6)).timestamp(),
                tz=UTC,
            )

    def row(open_time: datetime, close: str) -> list[object]:
        return [
            int(open_time.timestamp() * 1000),
            "60000",
            "60100",
            "59900",
            close,
            "500",
        ]

    exchange = CursorCandleExchange()
    exchange.rows = [
        row(NOW - timedelta(minutes=1), "60010"),
        row(NOW, "60020"),
    ]
    collector = EventCollector()
    account = PublicEventAccount(_profile(), exchange)
    supervisor = PublicEventSupervisor(
        [account],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    monkeypatch.setattr(
        "funding_arbitrage.exchanges.public_events.datetime", CompletedAt
    )

    await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)
    exchange.rows.extend(
        [
            row(NOW + timedelta(minutes=1), "60030"),
            row(NOW + timedelta(minutes=2), "60040"),
        ]
    )
    exchange.fail_next_ohlcv = True
    await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)
    await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)

    candle_opens = [
        event.payload.open_time
        for event in collector.events
        if event.kind is EventKind.CANDLE
    ]
    expected_since = int((NOW + timedelta(minutes=1)).timestamp() * 1000)
    assert candle_opens == [
        NOW - timedelta(minutes=1),
        NOW,
        NOW + timedelta(minutes=1),
        NOW + timedelta(minutes=2),
    ]
    assert exchange.fetch_ohlcv_since == [None, expected_since, expected_since]


async def test_candle_cursor_does_not_advance_after_partial_publish_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CompletedAt(datetime):
        @classmethod
        def now(cls, tz: object = None) -> CompletedAt:
            del tz
            return cls.fromtimestamp(
                (NOW + timedelta(minutes=1, seconds=6)).timestamp(),
                tz=UTC,
            )

    class FailSecondPublish:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, event: EventEnvelope[Any]) -> None:
            del event
            self.calls += 1
            if self.calls == 2:
                raise EventWriterFailed("simulated partial publish failure")

    exchange = CursorCandleExchange()
    exchange.rows = [
        [
            int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            "60000",
            "60100",
            "59900",
            "60010",
            "500",
        ],
        [
            int(NOW.timestamp() * 1000),
            "60010",
            "60200",
            "60000",
            "60150",
            "700",
        ],
    ]
    account = PublicEventAccount(_profile(), exchange)
    failing_sink = FailSecondPublish()
    supervisor = PublicEventSupervisor(
        [account],
        failing_sink,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    monkeypatch.setattr(
        "funding_arbitrage.exchanges.public_events.datetime", CompletedAt
    )

    with pytest.raises(EventWriterFailed, match="partial publish"):
        await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)
    assert supervisor._last_closed_candle_open_ms == {}

    recovered = EventCollector()
    supervisor.event_sink = recovered
    await supervisor._poll_symbol(account, "BTC/USDT:USDT", NOW)

    recovered_candles = [
        event for event in recovered.events if event.kind is EventKind.CANDLE
    ]
    assert len(recovered_candles) == 2
    assert exchange.fetch_ohlcv_since == [None, None]


def test_open_interest_observations_disambiguate_mutable_native_snapshot() -> None:
    normalizer = CcxtPublicEventNormalizer(_profile(), FakePublicExchange())
    base_row = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": int(NOW.timestamp() * 1000),
        "openInterestAmount": "1000",
        "openInterestValue": "60000",
    }
    corrected_row = {**base_row, "openInterestAmount": "1001"}

    first = normalizer.open_interest_event(
        base_row,
        received_at=NOW,
        received_monotonic_ns=100,
    )
    retry = normalizer.open_interest_event(
        base_row,
        received_at=NOW,
        received_monotonic_ns=100,
    )
    next_observation = normalizer.open_interest_event(
        corrected_row,
        received_at=NOW,
        received_monotonic_ns=101,
    )

    assert first.metadata.sequence_id == next_observation.metadata.sequence_id
    assert first.metadata.event_id == retry.metadata.event_id
    assert first.metadata.event_id != next_observation.metadata.event_id


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


async def test_required_quality_streams_survive_one_transient_snapshot_omission() -> None:
    collector = EventCollector()
    clock_values = iter((0.0, 60.0))
    supervisor = PublicEventSupervisor(
        [],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        quality_stream_retention_seconds=180,
        quality_stream_clock=lambda: next(clock_values),
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    source = _snapshot()
    await supervisor.observe_snapshot(source)
    expected = supervisor.required_quality_streams

    await supervisor.observe_snapshot(
        MarketSnapshot([], [], [], {}, source.captured_at + timedelta(seconds=60))
    )

    assert supervisor.required_quality_streams == expected


async def test_required_quality_streams_expire_after_retention_window() -> None:
    collector = EventCollector()
    clock_values = iter((0.0, 181.0))
    supervisor = PublicEventSupervisor(
        [],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        quality_stream_retention_seconds=180,
        quality_stream_clock=lambda: next(clock_values),
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    source = _snapshot()
    await supervisor.observe_snapshot(source)

    await supervisor.observe_snapshot(
        MarketSnapshot([], [], [], {}, source.captured_at + timedelta(seconds=181))
    )

    assert supervisor.required_quality_streams == ()


async def test_quality_stream_retention_ignores_regressed_snapshot_clock() -> None:
    collector = EventCollector()
    clock_values = iter((0.0, 60.0, 241.0))
    supervisor = PublicEventSupervisor(
        [],
        collector,
        symbol_limit=1,
        rest_interval_seconds=60,
        quality_stream_retention_seconds=180,
        quality_stream_clock=lambda: next(clock_values),
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    source = _snapshot()
    await supervisor.observe_snapshot(source)
    regressed_at = source.captured_at - timedelta(hours=1)
    instrument = source.instruments[0].model_copy(
        update={"exchange_symbol": "ETHUSDT", "base_asset": "ETH"}
    )
    ticker = source.tickers[0].model_copy(
        update={"symbol": "ETHUSDT", "timestamp": regressed_at}
    )
    funding = source.funding[0].model_copy(
        update={"symbol": "ETHUSDT", "timestamp": regressed_at}
    )

    await supervisor.observe_snapshot(
        MarketSnapshot([instrument], [ticker], [funding], {}, regressed_at)
    )
    assert len(supervisor.required_quality_streams) == 2

    await supervisor.observe_snapshot(
        MarketSnapshot([], [], [], {}, regressed_at - timedelta(hours=1))
    )

    assert supervisor.required_quality_streams == ()


async def test_snapshot_projection_precedes_mirrored_market_events() -> None:
    calls: list[str] = []

    async def sink(event: EventEnvelope[Any]) -> None:
        calls.append(event.kind.value)

    async def observer(_: MarketSnapshot) -> None:
        calls.append("UNIVERSE")

    supervisor = PublicEventSupervisor(
        [],
        sink,
        symbol_limit=1,
        rest_interval_seconds=60,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.1,
    )
    supervisor.set_pre_mirror_snapshot_observer(observer)

    await supervisor.observe_snapshot(_snapshot())

    assert calls[0] == "UNIVERSE"
    assert EventKind.FUNDING_SNAPSHOT.value in calls[1:]


async def test_polled_derivative_observations_do_not_reuse_event_ids() -> None:
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
    first = _snapshot()
    second = MarketSnapshot(
        first.instruments,
        [first.tickers[0].model_copy(update={"open_interest": Decimal("13.5")})],
        [
            first.funding[0].model_copy(
                update={
                    "mark_price": Decimal("60002"),
                    "index_price": Decimal("60000"),
                }
            )
        ],
        first.orderbooks,
        first.captured_at,
    )

    await supervisor._publish_snapshot_events(first, NOW, 100)
    await supervisor._publish_snapshot_events(first, NOW, 100)
    await supervisor._publish_snapshot_events(
        second,
        NOW - timedelta(seconds=1),
        101,
    )

    funding_events = [
        event for event in collector.events if event.kind is EventKind.FUNDING_SNAPSHOT
    ]
    open_interest_events = [
        event
        for event in collector.events
        if event.kind is EventKind.OPEN_INTEREST_SNAPSHOT
    ]
    assert len(funding_events) == 3
    assert len(open_interest_events) == 3
    assert len({event.metadata.sequence_id for event in funding_events}) == 1
    assert len({event.metadata.sequence_id for event in open_interest_events}) == 1
    assert funding_events[0].metadata.event_id == funding_events[1].metadata.event_id
    assert funding_events[1].metadata.event_id != funding_events[2].metadata.event_id
    assert (
        open_interest_events[0].metadata.event_id
        == open_interest_events[1].metadata.event_id
    )
    assert (
        open_interest_events[1].metadata.event_id
        != open_interest_events[2].metadata.event_id
    )
    assert funding_events[2].metadata.monotonic_ns == 101
    expected_occurrence = snapshot_occurrence_id(
        receive_timestamp=NOW,
        receive_monotonic_ns=100,
    )
    assert funding_events[0].metadata.event_id == deterministic_event_id(
        source=funding_events[0].metadata.source,
        kind=funding_events[0].kind,
        sequence_id=funding_events[0].metadata.sequence_id,
        exchange_timestamp=funding_events[0].metadata.exchange_timestamp,
        payload=funding_events[0].payload,
        occurrence_id=expected_occurrence,
    )


async def test_same_native_identity_on_two_exchanges_keeps_unique_compatible_ids() -> None:
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
    snapshot = MarketSnapshot(
        [
            source.instruments[0],
            source.instruments[0].model_copy(update={"exchange": "bybit"}),
        ],
        [
            source.tickers[0],
            source.tickers[0].model_copy(update={"exchange": "bybit"}),
        ],
        [
            source.funding[0],
            source.funding[0].model_copy(update={"exchange": "bybit"}),
        ],
        source.orderbooks,
        source.captured_at,
    )

    await supervisor._publish_snapshot_events(snapshot, NOW, 100)

    assert len(collector.events) == 2
    assert len({event.metadata.event_id for event in collector.events}) == 2
    expected_occurrence = snapshot_occurrence_id(
        receive_timestamp=NOW,
        receive_monotonic_ns=100,
    )
    for event in collector.events:
        assert event.metadata.event_id == deterministic_event_id(
            source=event.metadata.source,
            kind=event.kind,
            sequence_id=event.metadata.sequence_id,
            exchange_timestamp=event.metadata.exchange_timestamp,
            payload=event.payload,
            occurrence_id=expected_occurrence,
        )


async def test_duplicate_polled_rows_have_distinct_event_ids_within_observation() -> None:
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
    source = _snapshot()
    snapshot = MarketSnapshot(
        source.instruments,
        [
            source.tickers[0],
            source.tickers[0].model_copy(
                update={"open_interest": Decimal("13.5")}
            ),
        ],
        [
            source.funding[0],
            source.funding[0].model_copy(
                update={"mark_price": Decimal("60002")}
            ),
        ],
        source.orderbooks,
        source.captured_at,
    )

    await supervisor._publish_snapshot_events(snapshot, NOW, 100)
    first_events = collector.events.copy()
    reversed_snapshot = MarketSnapshot(
        source.instruments,
        list(reversed(snapshot.tickers)),
        list(reversed(snapshot.funding)),
        source.orderbooks,
        source.captured_at,
    )
    await supervisor._publish_snapshot_events(reversed_snapshot, NOW, 100)
    repeated_events = collector.events[4:]

    def event_identity(event: EventEnvelope[Any]) -> tuple[EventKind, str]:
        return event.kind, event.payload.model_dump_json()

    first_identity = {
        event_identity(event): event.metadata.event_id for event in first_events
    }
    repeated_identity = {
        event_identity(event): event.metadata.event_id for event in repeated_events
    }
    assert len(first_identity) == 4
    assert len(set(first_identity.values())) == 4
    assert repeated_identity == first_identity


async def test_polled_snapshot_identity_is_bounded_for_long_symbols() -> None:
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
    source = _snapshot()
    symbol = "X" * 512
    snapshot = MarketSnapshot(
        [source.instruments[0].model_copy(update={"exchange_symbol": symbol})],
        [source.tickers[0].model_copy(update={"symbol": symbol})],
        [source.funding[0].model_copy(update={"symbol": symbol})],
        source.orderbooks,
        source.captured_at,
    )

    await supervisor._publish_snapshot_events(snapshot, NOW, 100)

    assert len(collector.events) == 2
    assert all(len(event.metadata.sequence_id) <= 128 for event in collector.events)
    assert all(len(event.metadata.correlation_id) <= 128 for event in collector.events)


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
