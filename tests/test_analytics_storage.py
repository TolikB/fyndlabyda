from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
    TradingMode,
)
from funding_arbitrage.storage.clickhouse import (
    ClickHouseHttpWriter,
    ClickHouseStoragePolicy,
    FeatureAnalyticsEvent,
    TelemetryAnalyticsEvent,
)
from funding_arbitrage.storage.ephemeral import EphemeralStatePolicy, RedisEphemeralStore

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument() -> InstrumentKey:
    return InstrumentKey(
        venue="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _event() -> EventEnvelope[TradeTick]:
    payload = TradeTick(
        instrument=_instrument(),
        trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=NOW,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id="event-1",
            exchange_timestamp=NOW,
            receive_timestamp=NOW + timedelta(milliseconds=2),
            monotonic_ns=10,
            sequence_id="1",
            source="bybit.public.trade",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


def _signal() -> SignalIntent:
    instrument = _instrument()
    return SignalIntent(
        signal_id="signal-1",
        strategy_id="funding-basis",
        mode=TradingMode.PAPER,
        signal_type=SignalType.FUNDING_BASIS,
        primary_instrument=instrument,
        side=Side.SELL,
        legs=(SignalLeg(instrument=instrument, side=Side.SELL),),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("80"),
        confidence=Decimal("0.8"),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("10"),
        estimated_cost_bps=Decimal("2"),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )


async def test_clickhouse_writer_covers_all_analytics_domains_with_auth() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text="")

    client = httpx.AsyncClient(
        base_url="http://clickhouse:8123",
        auth=("analytics", "secret-password"),
        transport=httpx.MockTransport(handler),
    )
    writer = ClickHouseHttpWriter(
        ClickHouseStoragePolicy(
            username="analytics",
            password="secret-password",
            maximum_batch_rows=10,
            maximum_batch_bytes=100_000,
        ),
        client,
    )
    feature = FeatureAnalyticsEvent(
        feature_set_version="v1",
        feature_name="EMA_20",
        instrument_id=_instrument().canonical_id,
        event_time=NOW,
        source_event_id="event-1",
        value=101.5,
        quality="VALID",
        payload={"window": 20},
    )
    telemetry = TelemetryAnalyticsEvent(
        service="runner",
        metric="cycle_latency",
        venue="BYBIT",
        event_time=NOW,
        value=4.2,
        unit="milliseconds",
        labels={"mode": "paper"},
    )

    assert await writer.write_market_events((_event(),)) == 1
    assert await writer.write_features((feature,)) == 1
    assert await writer.write_signals((_signal(),)) == 1
    assert await writer.write_telemetry((telemetry,)) == 1
    await client.aclose()

    tables = [request.url.params["query"].split()[2] for request in requests]
    assert tables == [
        "raw_market_events",
        "feature_events",
        "signal_events",
        "telemetry_events",
    ]
    expected_auth = "Basic " + base64.b64encode(b"analytics:secret-password").decode()
    assert all(request.headers["Authorization"] == expected_auth for request in requests)
    market = json.loads(requests[0].content)
    assert market["row_id"] == "event-1"
    assert market["instrument_id"] == _instrument().canonical_id
    assert len(market["payload_hash"]) == 64


async def test_clickhouse_writer_rejects_injection_oversize_and_redacts_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server leaked secret-password")

    client = httpx.AsyncClient(
        base_url="http://clickhouse:8123",
        transport=httpx.MockTransport(handler),
    )
    writer = ClickHouseHttpWriter(
        ClickHouseStoragePolicy(
            username="analytics",
            password="secret-password",
            maximum_batch_rows=1,
            maximum_batch_bytes=20,
        ),
        client,
    )
    with pytest.raises(ValueError, match="not allowlisted"):
        await writer.insert_rows("raw_market_events; DROP TABLE", ({"a": 1},))
    with pytest.raises(ValueError, match="row limit"):
        await writer.insert_rows("telemetry_events", ({"a": 1}, {"a": 2}))
    with pytest.raises(ValueError, match="byte limit"):
        await writer.insert_rows("telemetry_events", ({"payload": "x" * 100},))

    roomy = ClickHouseHttpWriter(
        ClickHouseStoragePolicy(
            username="analytics",
            password="secret-password",
            maximum_batch_bytes=1000,
        ),
        client,
    )
    with pytest.raises(RuntimeError) as error:
        await roomy.insert_rows("telemetry_events", ({"row_id": "one"},))
    assert "secret-password" not in str(error.value)
    await client.aclose()


def test_clickhouse_schema_has_explicit_retention_for_every_domain() -> None:
    sql = Path("docker/clickhouse/init/001_v1_analytics.sql").read_text(encoding="utf-8")
    assert "raw_market_events" in sql and "INTERVAL 180 DAY" in sql
    assert "feature_events" in sql and "INTERVAL 365 DAY" in sql
    assert "signal_events" in sql and "INTERVAL 730 DAY" in sql
    assert "telemetry_events" in sql and "INTERVAL 90 DAY" in sql
    assert sql.count("ReplacingMergeTree") == 4


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}

    async def set(self, name: str, value: bytes, *, ex: int) -> bool:
        self.values[name] = value
        self.ttls[name] = ex
        return True

    async def get(self, name: str) -> bytes | None:
        return self.values.get(name)

    async def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            removed += int(name in self.values)
            self.values.pop(name, None)
            self.ttls.pop(name, None)
        return removed

    async def ttl(self, name: str) -> int:
        return self.ttls.get(name, -2)


async def test_redis_store_is_ttl_bounded_and_never_accounting_authority() -> None:
    redis = FakeRedis()
    store = RedisEphemeralStore(
        redis,
        EphemeralStatePolicy(
            maximum_ttl_seconds=60,
            maximum_payload_bytes=100,
            maximum_key_length=50,
        ),
    )
    await store.put("books:bybit:btcusdt", {"sequence": 10}, ttl_seconds=30)
    assert await store.get("books:bybit:btcusdt") == {"sequence": 10}
    assert await store.assert_bounded("books:bybit:btcusdt") == 30
    assert await store.delete("books:bybit:btcusdt") is True

    with pytest.raises(ValueError, match="TTL"):
        await store.put("features:btc", {"x": 1}, ttl_seconds=61)
    with pytest.raises(ValueError, match="payload"):
        await store.put("features:btc", {"x": "z" * 200}, ttl_seconds=10)
    with pytest.raises(ValueError, match="accounting or audit authority"):
        await store.put("ledger:authoritative", {"cash": 1}, ttl_seconds=10)


async def test_redis_persistent_or_evicted_key_fails_boundedness_check() -> None:
    redis = FakeRedis()
    store = RedisEphemeralStore(redis, EphemeralStatePolicy(maximum_ttl_seconds=60))
    redis.values["funding:v1:ephemeral:books:btc"] = b"{}"
    redis.ttls["funding:v1:ephemeral:books:btc"] = -1
    with pytest.raises(ValueError, match="persistent"):
        await store.assert_bounded("books:btc")
