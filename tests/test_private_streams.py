from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import ccxt.pro as ccxtpro
import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
)
from funding_arbitrage.domain.events import InstrumentType as EventInstrumentType
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.private_streams import (
    CcxtPrivateEventNormalizer,
    PrivateStreamAccount,
    PrivateStreamNormalizationError,
    PrivateStreamProfile,
    PrivateStreamSupervisor,
    private_stream_profiles,
)
from funding_arbitrage.execution.reconciliation import LiveReconciler, ReconciliationResult
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingAdapter,
    TradingOrderResult,
    VenueBalance,
    VenuePosition,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
SWAP_MARKET: dict[str, object] = {
    "id": "BTCUSDT",
    "symbol": "BTC/USDT:USDT",
    "base": "BTC",
    "quote": "USDT",
    "settle": "USDT",
    "spot": False,
    "swap": True,
    "future": False,
    "contractSize": 0.001,
}
SPOT_MARKET: dict[str, object] = {
    "id": "BTCUSDT",
    "symbol": "BTC/USDT",
    "base": "BTC",
    "quote": "USDT",
    "settle": None,
    "spot": True,
    "swap": False,
    "future": False,
    "contractSize": 1,
}


class FakeProExchange:
    def __init__(self, *, watch_positions: bool = True, fail_orders_once: bool = False) -> None:
        self.has = {
            "spot": True,
            "swap": True,
            "future": True,
            "watchOrders": True,
            "watchMyTrades": True,
            "watchBalance": True,
            "watchPositions": watch_positions,
        }
        self.markets = {
            "BTC/USDT:USDT": dict(SWAP_MARKET),
            "BTC/USDT": dict(SPOT_MARKET),
        }
        self.markets_by_id = {
            "BTCUSDT": [self.markets["BTC/USDT:USDT"], self.markets["BTC/USDT"]]
        }
        self.queues = {
            "orders": asyncio.Queue[object](),
            "fills": asyncio.Queue[object](),
            "balance": asyncio.Queue[object](),
            "positions": asyncio.Queue[object](),
        }
        self.loaded = False
        self.closed = False
        self.fail_orders_once = fail_orders_once
        self.order_failure = asyncio.Event()

    def check_required_credentials(self) -> None:
        return None

    async def load_markets(self, reload: bool = False) -> dict[str, dict[str, object]]:
        assert reload is True
        self.loaded = True
        return self.markets

    async def close(self) -> None:
        self.closed = True

    def market(self, symbol: str) -> dict[str, object]:
        if symbol in self.markets:
            return self.markets[symbol]
        for market in self.markets.values():
            if market["id"] == symbol:
                return market
        raise KeyError(symbol)

    async def watch_orders(self, **_: object) -> object:
        if self.fail_orders_once:
            self.fail_orders_once = False
            self.order_failure.set()
            raise ConnectionError("redacted test disconnect")
        return await self.queues["orders"].get()

    async def watch_my_trades(self, **_: object) -> object:
        return await self.queues["fills"].get()

    async def watch_balance(self, **_: object) -> object:
        return await self.queues["balance"].get()

    async def watch_positions(self, **_: object) -> object:
        return await self.queues["positions"].get()


class EventCollector:
    def __init__(self) -> None:
        self.events: list[EventEnvelope[Any]] = []

    async def __call__(self, event: EventEnvelope[Any]) -> None:
        self.events.append(event)


class SwitchableEventCollector(EventCollector):
    def __init__(self) -> None:
        super().__init__()
        self.failure: Exception | None = None

    async def __call__(self, event: EventEnvelope[Any]) -> None:
        if self.failure is not None:
            raise self.failure
        await super().__call__(event)


class ReconciliationAdapter:
    name = "bybit"

    async def fetch_balance(self) -> VenueBalance:
        return VenueBalance(
            exchange=self.name,
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
            timestamp=NOW,
        )

    async def fetch_positions(self) -> list[VenuePosition]:
        return []

    async def fetch_open_orders(self) -> list[TradingOrderResult]:
        return [
            TradingOrderResult(
                exchange=self.name,
                exchange_order_id="spot-order-1",
                client_order_id="",
                exchange_symbol="BTCUSDT",
                instrument_type=InstrumentType.SPOT,
                side="BUY",
                requested_base_quantity=Decimal("0.01"),
                filled_base_quantity=Decimal("0"),
                status=LiveOrderStatus.OPEN,
                timestamp=NOW,
            )
        ]


async def _wait_for_events(collector: EventCollector, count: int) -> None:
    for _ in range(100):
        if len(collector.events) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"expected {count} events, received {len(collector.events)}")


def _order() -> dict[str, object]:
    return {
        "id": "order-1",
        "clientOrderId": "fa-order-1",
        "symbol": "BTC/USDT:USDT",
        "timestamp": int(NOW.timestamp() * 1000),
        "status": "open",
        "side": "buy",
        "type": "limit",
        "amount": 2,
        "filled": 1,
        "price": 62000,
        "average": 61999,
        "reduceOnly": False,
    }


def _fill() -> dict[str, object]:
    return {
        "id": "fill-1",
        "order": "order-1",
        "symbol": "BTC/USDT:USDT",
        "timestamp": int(NOW.timestamp() * 1000),
        "side": "buy",
        "price": 61999,
        "amount": 1,
        "takerOrMaker": "taker",
        "fee": {"cost": "-0.25", "currency": "usdt"},
    }


def _position() -> dict[str, object]:
    return {
        "symbol": "BTC/USDT:USDT",
        "timestamp": int(NOW.timestamp() * 1000),
        "side": "short",
        "contracts": 3,
        "entryPrice": 62000,
        "markPrice": 61900,
        "unrealizedPnl": "0.30",
        "leverage": 1,
        "initialMargin": 185.7,
    }


def _balance() -> dict[str, object]:
    return {
        "timestamp": int(NOW.timestamp() * 1000),
        "free": {"USDT": "900"},
        "used": {"USDT": "100"},
        "total": {"USDT": "1000"},
        "debt": {"USDT": "0"},
    }


def test_normalizer_emits_typed_deterministic_private_events() -> None:
    exchange = FakeProExchange()
    normalizer = CcxtPrivateEventNormalizer("bybit", exchange, "unified")

    order = normalizer.order_events([_order()], received_at=NOW)[0]
    fill = normalizer.fill_events([_fill()], received_at=NOW)[0]
    position = normalizer.position_events([_position()], received_at=NOW)[0]
    balance = normalizer.balance_events(_balance(), received_at=NOW)[0]
    repeated = normalizer.order_events([_order()], received_at=NOW)[0]

    assert [order.kind, fill.kind, position.kind, balance.kind] == [
        EventKind.ORDER_UPDATE,
        EventKind.FILL,
        EventKind.POSITION_SNAPSHOT,
        EventKind.BALANCE_SNAPSHOT,
    ]
    assert order.payload.requested_quantity == Decimal("0.002")
    assert order.payload.filled_quantity == Decimal("0.001")
    assert fill.payload.quantity == Decimal("0.001")
    assert fill.payload.client_order_id == "fa-order-1"
    assert fill.payload.fee_amount == Decimal("0.25")
    assert position.payload.signed_quantity == Decimal("-0.003")
    assert balance.payload.total == Decimal("1000")
    assert order.metadata.event_id == repeated.metadata.event_id
    assert order.metadata.source == "BYBIT.PRIVATE.UNIFIED.ORDERS.CCXT_PRO"


def test_missing_exchange_timestamp_is_explicitly_recovering() -> None:
    exchange = FakeProExchange()
    normalizer = CcxtPrivateEventNormalizer("bybit", exchange, "unified")
    raw = _balance()
    raw.pop("timestamp")

    event = normalizer.balance_events(raw, received_at=NOW)[0]

    assert event.metadata.quality is DataQuality.RECOVERING
    assert event.payload.exchange_timestamp == NOW


def test_malformed_private_event_fails_without_inventing_market_identity() -> None:
    exchange = FakeProExchange()
    normalizer = CcxtPrivateEventNormalizer("bybit", exchange, "unified")
    raw = _order()
    raw["symbol"] = "UNKNOWN/USDT"

    with pytest.raises(PrivateStreamNormalizationError, match="unresolved"):
        normalizer.order_events([raw], received_at=NOW)


@pytest.mark.asyncio
async def test_supervisor_streams_and_reconciliation_share_one_event_journal() -> None:
    exchange = FakeProExchange()
    profile = PrivateStreamProfile(
        "bybit",
        "unified",
        "bybit",
        "swap",
        True,
        supported_instrument_types=frozenset(
            {EventInstrumentType.SPOT, EventInstrumentType.PERPETUAL}
        ),
    )
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (PrivateStreamAccount(profile, exchange),),
        {"bybit": cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )

    await supervisor.start()
    result = ReconciliationResult(
        passed=True,
        reason=None,
        balances={
            "bybit": VenueBalance(
                exchange="bybit",
                free={"USDT": Decimal("1000")},
                total={"USDT": Decimal("1000")},
                timestamp=NOW,
            )
        },
        positions=(),
        open_orders=(),
        details={},
    )
    await supervisor.ingest_reconciliation(result, observed_at=NOW)
    assert supervisor.health(NOW) == (True, None)

    await exchange.queues["orders"].put([_order()])
    await exchange.queues["fills"].put([_fill()])
    await exchange.queues["positions"].put([_position()])
    await exchange.queues["balance"].put(_balance())
    await _wait_for_events(collector, 5)

    assert {event.kind for event in collector.events} == {
        EventKind.ORDER_UPDATE,
        EventKind.FILL,
        EventKind.POSITION_SNAPSHOT,
        EventKind.BALANCE_SNAPSHOT,
    }
    await supervisor.stop()
    assert exchange.closed is True
    assert supervisor.health(NOW)[0] is False


@pytest.mark.asyncio
async def test_supervisor_health_fails_closed_during_reconnect_and_stale_checkpoint() -> None:
    exchange = FakeProExchange(fail_orders_once=True)
    profile = PrivateStreamProfile(
        "bybit",
        "unified",
        "bybit",
        "swap",
        True,
        supported_instrument_types=frozenset(
            {EventInstrumentType.SPOT, EventInstrumentType.PERPETUAL}
        ),
    )
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (PrivateStreamAccount(profile, exchange),),
        {"bybit": cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=1,
        reconnect_initial_seconds=0.2,
        reconnect_max_seconds=0.2,
    )
    await supervisor.start()
    await supervisor.ingest_reconciliation(
        ReconciliationResult(
            passed=True,
            reason=None,
            balances={"bybit": VenueBalance(exchange="bybit", timestamp=NOW)},
            positions=(),
            open_orders=(),
            details={},
        ),
        observed_at=NOW,
    )

    await asyncio.wait_for(exchange.order_failure.wait(), timeout=1)
    healthy, reason = supervisor.health(NOW)
    assert healthy is False
    assert reason == "private_stream_reconnecting:bybit:unified:orders"
    assert supervisor.health(NOW + timedelta(seconds=2)) == (
        False,
        "private_stream_reconciliation_stale",
    )
    await supervisor.stop()


@pytest.mark.asyncio
async def test_position_stream_falls_back_to_reconciled_snapshot_when_unsupported() -> None:
    spot_exchange = FakeProExchange()
    exchange = FakeProExchange(watch_positions=False)
    spot_profile, profile = private_stream_profiles("mexc")
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(spot_profile, spot_exchange),
            PrivateStreamAccount(profile, exchange),
        ),
        {"mexc": cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    await supervisor.start()
    await supervisor.ingest_reconciliation(
        ReconciliationResult(
            passed=True,
            reason=None,
            balances={"mexc": VenueBalance(exchange="mexc", timestamp=NOW)},
            positions=(
                VenuePosition(
                    exchange="mexc",
                    exchange_symbol="BTCUSDT",
                    instrument_type=InstrumentType.PERPETUAL,
                    side="SHORT",
                    base_quantity=Decimal("0.003"),
                    entry_price=Decimal("62000"),
                    mark_price=Decimal("61900"),
                    unrealized_pnl=Decimal("0.3"),
                ),
            ),
            open_orders=(
                TradingOrderResult(
                    exchange="mexc",
                    exchange_order_id="order-1",
                    client_order_id="fa-order-1",
                    exchange_symbol="BTCUSDT",
                    instrument_type=InstrumentType.PERPETUAL,
                    side="SELL",
                    requested_base_quantity=Decimal("0.003"),
                    filled_base_quantity=Decimal("0"),
                    status=LiveOrderStatus.OPEN,
                    timestamp=NOW,
                ),
            ),
            details={},
        ),
        observed_at=NOW,
    )

    assert ("mexc", "linear", "positions") not in supervisor._tasks
    assert {event.kind for event in collector.events} == {
        EventKind.POSITION_SNAPSHOT,
        EventKind.ORDER_UPDATE,
    }
    assert all("PRIVATE_REST_RECONCILIATION" in event.metadata.source for event in collector.events)
    await supervisor.stop()


@pytest.mark.parametrize("venue", ["bybit", "okx", "hyperliquid"])
@pytest.mark.asyncio
async def test_unified_profile_reconciles_spot_orders_and_derivative_positions(
    venue: str,
) -> None:
    exchange = FakeProExchange()
    profile = private_stream_profiles(venue)[0]
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (PrivateStreamAccount(profile, exchange),),
        {venue: cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    await supervisor.start()
    try:
        await supervisor.ingest_reconciliation(
            ReconciliationResult(
                passed=False,
                reason="non_terminal_live_order",
                balances={venue: VenueBalance(exchange=venue, timestamp=NOW)},
                positions=(
                    VenuePosition(
                        exchange=venue,
                        exchange_symbol="BTCUSDT",
                        instrument_type=InstrumentType.PERPETUAL,
                        side="SHORT",
                        base_quantity=Decimal("0.003"),
                        entry_price=Decimal("62000"),
                        mark_price=Decimal("61900"),
                    ),
                ),
                open_orders=(
                    TradingOrderResult(
                        exchange=venue,
                        exchange_order_id="spot-order-1",
                        client_order_id="fa-spot-order-1",
                        exchange_symbol="BTCUSDT",
                        instrument_type=InstrumentType.SPOT,
                        side="BUY",
                        requested_base_quantity=Decimal("0.01"),
                        filled_base_quantity=Decimal("0"),
                        status=LiveOrderStatus.OPEN,
                        timestamp=NOW,
                    ),
                ),
                details={},
            ),
            observed_at=NOW,
        )
    finally:
        await supervisor.stop()

    reconciled_types = {
        event.payload.instrument.instrument_type
        for event in collector.events
        if event.kind in {EventKind.ORDER_UPDATE, EventKind.POSITION_SNAPSHOT}
    }
    assert reconciled_types == {
        EventInstrumentType.SPOT,
        EventInstrumentType.PERPETUAL,
    }


def test_private_profile_matrix_covers_all_eight_cex_and_pinned_capabilities() -> None:
    venues = ("binance", "bybit", "gate", "okx", "hyperliquid", "mexc", "kucoin", "htx")
    profiles = [profile for venue in venues for profile in private_stream_profiles(venue)]
    spot = frozenset({EventInstrumentType.SPOT})
    perpetual = frozenset({EventInstrumentType.PERPETUAL})
    derivatives = frozenset(
        {EventInstrumentType.PERPETUAL, EventInstrumentType.FUTURE}
    )
    expected = {
        ("binance", "spot"): spot,
        ("binance", "linear"): derivatives,
        ("bybit", "unified"): spot | derivatives,
        ("gate", "spot"): spot,
        ("gate", "linear"): perpetual,
        ("okx", "unified"): spot | derivatives,
        ("hyperliquid", "unified"): spot | perpetual,
        ("mexc", "spot"): spot,
        ("mexc", "linear"): perpetual,
        ("kucoin", "spot"): spot,
        ("kucoin", "linear"): derivatives,
        ("htx", "spot"): spot,
        ("htx", "linear"): perpetual,
    }

    assert {profile.venue for profile in profiles} == set(venues)
    assert {profile.venue for profile in profiles if profile.positions_via_reconciliation} == {
        "mexc",
        "htx",
    }
    assert {
        (profile.venue, profile.account): profile.supported_instrument_types
        for profile in profiles
    } == expected
    capability_names = {
        EventInstrumentType.SPOT: "spot",
        EventInstrumentType.PERPETUAL: "swap",
        EventInstrumentType.FUTURE: "future",
    }
    for profile in profiles:
        exchange = getattr(ccxtpro, profile.exchange_class)(
            {"options": {"defaultType": profile.default_type}}
        )
        for instrument_type in profile.supported_instrument_types:
            assert exchange.has.get(capability_names[instrument_type]) is True
        assert exchange.has.get("watchOrders") is True
        assert exchange.has.get("watchMyTrades") is True
        assert exchange.has.get("watchBalance") is True
        if profile.watch_positions and not profile.positions_via_reconciliation:
            assert exchange.has.get("watchPositions") is True


def test_private_profile_topology_fails_closed_before_start() -> None:
    with pytest.raises(ValueError, match="unsupported private stream default type"):
        PrivateStreamProfile("bybit", "bad", "bybit", "unknown", True)

    exchange = FakeProExchange()
    spot_only = PrivateStreamProfile(
        "bybit",
        "spot",
        "bybit",
        "spot",
        False,
    )
    with pytest.raises(ValueError, match="missing=PERPETUAL"):
        PrivateStreamSupervisor(
            (PrivateStreamAccount(spot_only, exchange),),
            {"bybit": cast(TradingAdapter, object())},
            EventCollector(),
            reconciliation_max_age_seconds=90,
            reconnect_initial_seconds=0.01,
            reconnect_max_seconds=0.05,
        )

    unified = PrivateStreamProfile(
        "bybit",
        "unified",
        "bybit",
        "swap",
        True,
        supported_instrument_types=frozenset(
            {EventInstrumentType.SPOT, EventInstrumentType.PERPETUAL}
        ),
    )
    exact_supervisor = PrivateStreamSupervisor(
        (PrivateStreamAccount(unified, exchange),),
        {"bybit": cast(TradingAdapter, object())},
        EventCollector(),
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    assert exact_supervisor.reconciliation_coverage() == {
        "bybit": frozenset({"SPOT", "PERPETUAL"})
    }
    duplicate_spot = PrivateStreamProfile(
        "bybit",
        "duplicate-spot",
        "bybit",
        "spot",
        False,
    )
    with pytest.raises(ValueError, match="ambiguous=SPOT"):
        PrivateStreamSupervisor(
            (
                PrivateStreamAccount(unified, exchange),
                PrivateStreamAccount(duplicate_spot, FakeProExchange()),
            ),
            {"bybit": cast(TradingAdapter, object())},
            EventCollector(),
            reconciliation_max_age_seconds=90,
            reconnect_initial_seconds=0.01,
            reconnect_max_seconds=0.05,
        )

    with pytest.raises(ValueError, match="account identities must be unique"):
        PrivateStreamSupervisor(
            (
                PrivateStreamAccount(unified, exchange),
                PrivateStreamAccount(unified, FakeProExchange()),
            ),
            {"bybit": cast(TradingAdapter, object())},
            EventCollector(),
            reconciliation_max_age_seconds=90,
            reconnect_initial_seconds=0.01,
            reconnect_max_seconds=0.05,
        )


@pytest.mark.asyncio
async def test_declared_market_capability_is_verified_before_stream_start() -> None:
    exchange = FakeProExchange()
    exchange.has["spot"] = False
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(
                private_stream_profiles("bybit")[0],
                exchange,
            ),
        ),
        {"bybit": cast(TradingAdapter, object())},
        EventCollector(),
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )

    with pytest.raises(RuntimeError, match="lacks declared spot market capability"):
        await supervisor.start()

    assert exchange.closed is True


@pytest.mark.asyncio
async def test_missing_stream_capability_closes_clients_without_starting_tasks() -> None:
    exchange = FakeProExchange()
    exchange.has["watchBalance"] = False
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(
                private_stream_profiles("bybit")[0],
                exchange,
            ),
        ),
        {"bybit": cast(TradingAdapter, object())},
        EventCollector(),
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )

    with pytest.raises(RuntimeError, match="lacks required watchBalance capability"):
        await supervisor.start()

    assert exchange.closed is True
    assert supervisor.snapshot(NOW)["channels"] == {}
    assert supervisor.health(NOW) == (
        False,
        "private_stream_supervisor_not_running",
    )


@pytest.mark.asyncio
async def test_failed_live_reconciliation_journals_open_order_before_enforcement(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = Settings(
        _env_file=None,
        LIVE_KILL_SWITCH_FILE=str(tmp_path / "LIVE_DISABLED"),
    )
    risk = LiveRiskController(settings)
    adapter = ReconciliationAdapter()
    exchange = FakeProExchange()
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(
                private_stream_profiles("bybit")[0],
                exchange,
            ),
        ),
        {"bybit": cast(TradingAdapter, adapter)},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    reconciler = LiveReconciler(
        settings,
        {"bybit": cast(TradingAdapter, adapter)},
        factory,
        risk,
    )

    await supervisor.start()
    try:
        result = await reconciler.reconcile(raise_on_failure=False)
        assert result.passed is False
        assert result.reason is not None
        assert "non_terminal_live_order" in result.reason
        assert risk.paused

        await supervisor.ingest_reconciliation(result, observed_at=NOW)

        assert supervisor.health(NOW) == (
            False,
            "private_stream_reconciliation_failed",
        )
        order_events = [
            event for event in collector.events if event.kind is EventKind.ORDER_UPDATE
        ]
        assert len(order_events) == 1
        assert order_events[0].payload.client_order_id == "external:spot-order-1"
        assert (
            order_events[0].payload.instrument.instrument_type
            is EventInstrumentType.SPOT
        )
        with pytest.raises(LiveTradingPaused, match="non_terminal_live_order"):
            reconciler.raise_if_failed(result)
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_malformed_reconciliation_preserves_prior_facts_and_marks_unhealthy() -> None:
    exchange = FakeProExchange()
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(
                private_stream_profiles("bybit")[0],
                exchange,
            ),
        ),
        {"bybit": cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    healthy = ReconciliationResult(
        passed=True,
        reason=None,
        balances={
            "bybit": VenueBalance(
                exchange="bybit",
                free={"USDT": Decimal("1000")},
                total={"USDT": Decimal("1000")},
                timestamp=NOW,
            )
        },
        positions=(),
        open_orders=(),
        details={},
    )
    malformed = ReconciliationResult(
        passed=True,
        reason=None,
        balances=healthy.balances,
        positions=(),
        open_orders=(
            TradingOrderResult(
                exchange="bybit",
                exchange_order_id="   ",
                client_order_id="   ",
                exchange_symbol="BTCUSDT",
                instrument_type=InstrumentType.SPOT,
                side="BUY",
                requested_base_quantity=Decimal("0.01"),
                filled_base_quantity=Decimal("0"),
                status=LiveOrderStatus.OPEN,
                timestamp=NOW,
            ),
        ),
        details={},
    )

    await supervisor.start()
    try:
        await supervisor.ingest_reconciliation(
            healthy,
            observed_at=NOW - timedelta(seconds=1),
        )
        prior_balance_events = sum(
            event.kind is EventKind.BALANCE_SNAPSHOT for event in collector.events
        )

        with pytest.raises(
            PrivateStreamNormalizationError,
            match="no client or exchange identity",
        ):
            await supervisor.ingest_reconciliation(malformed, observed_at=NOW)

        assert sum(
            event.kind is EventKind.BALANCE_SNAPSHOT for event in collector.events
        ) == prior_balance_events + 1
        assert supervisor.health(NOW) == (
            False,
            "private_stream_reconciliation_failed",
        )
        assert supervisor.snapshot(NOW)["last_reconciliation_failure_reason"] == (
            "reconciliation_ingest_PrivateStreamNormalizationError"
        )
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_reconciliation_sink_failure_invalidates_previous_healthy_checkpoint() -> None:
    exchange = FakeProExchange()
    collector = SwitchableEventCollector()
    supervisor = PrivateStreamSupervisor(
        (
            PrivateStreamAccount(
                private_stream_profiles("bybit")[0],
                exchange,
            ),
        ),
        {"bybit": cast(TradingAdapter, object())},
        collector,
        reconciliation_max_age_seconds=90,
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.05,
    )
    result = ReconciliationResult(
        passed=True,
        reason=None,
        balances={
            "bybit": VenueBalance(
                exchange="bybit",
                free={"USDT": Decimal("1000")},
                total={"USDT": Decimal("1000")},
                timestamp=NOW,
            )
        },
        positions=(),
        open_orders=(),
        details={},
    )

    await supervisor.start()
    try:
        await supervisor.ingest_reconciliation(
            result,
            observed_at=NOW - timedelta(seconds=1),
        )
        collector.failure = RuntimeError("sink unavailable")

        with pytest.raises(RuntimeError, match="sink unavailable"):
            await supervisor.ingest_reconciliation(result, observed_at=NOW)

        assert supervisor.health(NOW) == (
            False,
            "private_stream_reconciliation_failed",
        )
        assert supervisor.snapshot(NOW)["last_reconciliation_failure_reason"] == (
            "reconciliation_ingest_RuntimeError"
        )
    finally:
        await supervisor.stop()


def test_private_stream_timing_configuration_is_fail_closed() -> None:
    with pytest.raises(ValidationError, match="must exceed"):
        Settings(
            _env_file=None,
            LIVE_RECONCILIATION_INTERVAL_SECONDS=30,
            LIVE_PRIVATE_STREAM_RECONCILIATION_MAX_AGE_SECONDS=30,
        )
    with pytest.raises(ValidationError, match="reconnect bounds"):
        Settings(
            _env_file=None,
            LIVE_PRIVATE_STREAM_RECONNECT_INITIAL_SECONDS=10,
            LIVE_PRIVATE_STREAM_RECONNECT_MAX_SECONDS=1,
        )
