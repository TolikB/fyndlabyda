from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import Base
from funding_arbitrage.database.repositories.events import (
    append_event,
    load_latest_event_by_kind,
)
from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
    UniverseSelectionSnapshot,
)
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.services.multi_regime import (
    MultiRegimeEngine,
    MultiRegimeEngineConfig,
)
from funding_arbitrage.services.runtime_universe import (
    RuntimeUniversePublisher,
    build_universe_candidates,
)
from funding_arbitrage.strategies.universe import UniverseSelectorConfig

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _history(venue: str, symbol: str) -> list[FundingHistoryPoint]:
    return [
        FundingHistoryPoint(
            exchange=venue,
            symbol=symbol,
            funding_rate=Decimal("0.0005"),
            funding_timestamp=NOW - timedelta(hours=8 * (90 - index)),
        )
        for index in range(91)
    ]


def _snapshot(*, captured_at: datetime = NOW) -> MarketSnapshot:
    instruments: list[NormalizedInstrument] = []
    tickers: list[Ticker] = []
    funding: list[FundingSnapshot] = []
    books: dict[tuple[str, str, InstrumentType], OrderBook] = {}
    history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
    refreshed: dict[tuple[str, str], datetime] = {}
    for venue, symbol in (("bybit", "SOLUSDT"), ("gate", "SOL_USDT")):
        instrument = NormalizedInstrument(
            exchange=venue,
            exchange_symbol=symbol,
            base_asset="SOL",
            quote_asset="USDT",
            settlement_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            contract_size=Decimal("1"),
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            min_order_size=Decimal("0.01"),
            funding_interval=8,
        )
        instruments.append(instrument)
        tickers.append(
            Ticker(
                exchange=venue,
                symbol=symbol,
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("100"),
                best_bid=Decimal("99.99"),
                best_ask=Decimal("100.01"),
                volume_24h=Decimal("30000000"),
                open_interest=Decimal("100000"),
                timestamp=NOW,
            )
        )
        funding.append(
            FundingSnapshot(
                exchange=venue,
                symbol=symbol,
                funding_rate=Decimal("0.0005"),
                funding_interval_hours=Decimal("8"),
                next_funding_time=NOW + timedelta(hours=4),
                mark_price=Decimal("100"),
                index_price=Decimal("100"),
                timestamp=NOW,
            )
        )
        books[(venue, symbol, InstrumentType.PERPETUAL)] = OrderBook(
            exchange=venue,
            symbol=symbol,
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.99"), quantity=Decimal("2000")),),
            asks=(OrderBookLevel(price=Decimal("100.01"), quantity=Decimal("2000")),),
            timestamp=NOW,
            sequence=1,
        )
        history[(venue, symbol)] = _history(venue, symbol)
        refreshed[(venue, symbol)] = NOW
    return MarketSnapshot(
        instruments=instruments,
        tickers=tickers,
        funding=funding,
        orderbooks=books,
        captured_at=captured_at,
        funding_history=history,
        funding_history_refreshed=refreshed,
    )


def test_runtime_candidates_use_cross_venue_usd_liquidity_and_exact_history() -> None:
    candidates = build_universe_candidates(_snapshot())

    assert len(candidates) == 2
    assert {candidate.venue_count for candidate in candidates} == {2}
    assert all(candidate.data_quality is DataQuality.VALID for candidate in candidates)
    assert all(
        candidate.quote_volume_24h_usd == Decimal("30000000")
        for candidate in candidates
    )
    assert all(candidate.funding_samples == 91 for candidate in candidates)
    assert all(candidate.market_data_coverage == Decimal("1") for candidate in candidates)
    assert all(candidate.depth_within_25bps_usd > Decimal("100000") for candidate in candidates)


def test_runtime_candidates_fail_closed_when_current_evidence_is_stale() -> None:
    candidates = build_universe_candidates(
        _snapshot(captured_at=NOW + timedelta(seconds=121))
    )

    assert candidates
    assert all(candidate.data_quality is DataQuality.STALE for candidate in candidates)


def test_runtime_candidates_fail_closed_without_history_refresh_evidence() -> None:
    snapshot = replace(_snapshot(), funding_history_refreshed={})

    candidates = build_universe_candidates(snapshot)

    assert candidates
    assert all(
        candidate.data_quality is DataQuality.UNAVAILABLE for candidate in candidates
    )


def test_runtime_universe_configuration_rejects_unsafe_bounds() -> None:
    with pytest.raises(ValidationError, match="new-asset limit"):
        Settings(
            _env_file=None,
            MULTI_REGIME_UNIVERSE_MAXIMUM_ASSETS=2,
            MULTI_REGIME_UNIVERSE_MAXIMUM_NEW_ASSETS=3,
        )
    with pytest.raises(ValidationError, match="retention score"):
        Settings(
            _env_file=None,
            MULTI_REGIME_UNIVERSE_MINIMUM_ENTRY_SCORE="0.4",
            MULTI_REGIME_UNIVERSE_MINIMUM_RETENTION_SCORE="0.5",
        )


@pytest.mark.asyncio
async def test_universe_selection_is_durable_replayable_and_rebalance_bounded() -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)

    async def sink(event: EventEnvelope[Any]) -> None:
        async with factory() as session:
            await append_event(session, event)

    publisher = RuntimeUniversePublisher(
        factory,
        sink,
        selector_config=UniverseSelectorConfig(
            maximum_assets=5,
            maximum_new_assets_per_rebalance=5,
        ),
        rebalance_seconds=3600,
    )
    event = await publisher.observe_snapshot(_snapshot())

    assert event is not None
    assert event.payload.selected_assets == ("SOL",)
    async with factory() as session:
        restored_event = await load_latest_event_by_kind(
            session, EventKind.UNIVERSE_SELECTION_SNAPSHOT
        )
    assert restored_event is not None
    assert isinstance(restored_event.payload, UniverseSelectionSnapshot)
    assert restored_event.payload == event.payload

    engine = MultiRegimeEngine(
        MultiRegimeEngineConfig(assets=frozenset({"BTC"}))
    )
    engine.restore_event(restored_event)
    assert engine.active_assets == frozenset({"BTC", "SOL"})
    assert engine.latest_universe_selection == event.payload

    restarted = RuntimeUniversePublisher(
        factory,
        sink,
        selector_config=UniverseSelectorConfig(
            maximum_assets=5,
            maximum_new_assets_per_rebalance=5,
        ),
        rebalance_seconds=3600,
    )
    duplicate = await restarted.observe_snapshot(
        _snapshot(captured_at=NOW + timedelta(minutes=30))
    )
    assert duplicate is None
    assert restarted.previous is not None
    assert restarted.previous.selected_assets == ("SOL",)

    empty = await restarted.observe_snapshot(
        replace(
            _snapshot(captured_at=NOW + timedelta(hours=1)),
            funding_history_refreshed={},
        )
    )
    assert empty is not None
    assert empty.payload.selected == ()
    engine.process(empty)
    assert engine.active_assets == frozenset({"BTC"})
    assert engine.latest_universe_selection == empty.payload

    older_at = NOW - timedelta(hours=1)
    older_payload = event.payload.model_copy(
        update={
            "exchange_timestamp": older_at,
            "selection_id": "late-arriving-older-selection",
        }
    )
    older_event = EventEnvelope[UniverseSelectionSnapshot](
        kind=EventKind.UNIVERSE_SELECTION_SNAPSHOT,
        metadata=event.metadata.model_copy(
            update={
                "event_id": "evt_late_arriving_older_universe",
                "exchange_timestamp": older_at,
                "receive_timestamp": NOW + timedelta(hours=2),
                "monotonic_ns": event.metadata.monotonic_ns + 1,
                "sequence_id": "late-arriving-older-selection",
                "correlation_id": "late-arriving-older-selection",
            }
        ),
        payload=older_payload,
    )
    await sink(older_event)
    async with factory() as session:
        latest_by_event_time = await load_latest_event_by_kind(
            session, EventKind.UNIVERSE_SELECTION_SNAPSHOT
        )
    assert latest_by_event_time is not None
    assert latest_by_event_time.payload == empty.payload

    await database.dispose()
