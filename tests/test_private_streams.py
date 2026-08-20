from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import ccxt.pro as ccxtpro
import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import DataQuality, EventEnvelope, EventKind
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.private_streams import (
    CcxtPrivateEventNormalizer,
    PrivateStreamAccount,
    PrivateStreamNormalizationError,
    PrivateStreamProfile,
    PrivateStreamSupervisor,
    private_stream_profiles,
)
from funding_arbitrage.execution.reconciliation import ReconciliationResult
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingAdapter,
    TradingOrderResult,
    VenueBalance,
    VenuePosition,
)

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
    "id": "BTCUSDT_SPOT",
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
            "watchOrders": True,
            "watchMyTrades": True,
            "watchBalance": True,
            "watchPositions": watch_positions,
        }
        self.markets = {
            "BTC/USDT:USDT": dict(SWAP_MARKET),
            "BTC/USDT": dict(SPOT_MARKET),
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
    profile = PrivateStreamProfile("bybit", "unified", "bybit", "swap", True)
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
    profile = PrivateStreamProfile("bybit", "unified", "bybit", "swap", True)
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
    exchange = FakeProExchange(watch_positions=False)
    profile = PrivateStreamProfile(
        "mexc", "linear", "mexc", "swap", True, positions_via_reconciliation=True
    )
    collector = EventCollector()
    supervisor = PrivateStreamSupervisor(
        (PrivateStreamAccount(profile, exchange),),
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


def test_private_profile_matrix_covers_all_eight_cex_and_pinned_capabilities() -> None:
    venues = ("binance", "bybit", "gate", "okx", "hyperliquid", "mexc", "kucoin", "htx")
    profiles = [profile for venue in venues for profile in private_stream_profiles(venue)]

    assert {profile.venue for profile in profiles} == set(venues)
    assert {profile.venue for profile in profiles if profile.positions_via_reconciliation} == {
        "mexc",
        "htx",
    }
    for profile in profiles:
        exchange = getattr(ccxtpro, profile.exchange_class)(
            {"options": {"defaultType": profile.default_type}}
        )
        assert exchange.has.get("watchOrders") is True
        assert exchange.has.get("watchMyTrades") is True
        assert exchange.has.get("watchBalance") is True
        if profile.watch_positions and not profile.positions_via_reconciliation:
            assert exchange.has.get("watchPositions") is True


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