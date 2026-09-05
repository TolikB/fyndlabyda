from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.api.routes.system import canonical_journal_status
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import Base
from funding_arbitrage.database.repositories.events import append_event, load_replay_events
from funding_arbitrage.database.repositories.journal_profiles import (
    CanonicalJournalProfileBoundary,
    CanonicalJournalProfileCoverageError,
    CanonicalJournalWriterLeaseError,
    append_canonical_journal_profile_boundary,
    assert_canonical_journal_checkpoint_compatible,
    canonical_journal_profile_spec,
    canonical_journal_writer_lease,
    load_canonical_journal_boundary_covering_row,
    load_latest_compatible_journal_window,
)
from funding_arbitrage.database.session import init_database
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
)

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _settings(**updates: object) -> Settings:
    return Settings(
        _env_file=None,
        RUN_MODE="paper_test",
        TRADING_MODE="PAPER",
        PAPER_VENUES="BYBIT",
        **updates,
    )


def _trade(sequence: int) -> EventEnvelope[TradeTick]:
    observed_at = NOW + timedelta(seconds=sequence)
    tick = TradeTick(
        instrument=INSTRUMENT,
        trade_id=str(sequence),
        price=Decimal("60000"),
        quantity=Decimal("0.1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=observed_at,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=f"profile-event-{sequence}",
            exchange_timestamp=observed_at,
            receive_timestamp=observed_at,
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            source="BYBIT.PUBLIC.TRADE",
            correlation_id="journal-profile-test",
            payload_version=1,
        ),
        payload=tick,
    )


async def _sessions() -> tuple[object, async_sessionmaker]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def test_journal_profile_hash_is_deterministic_and_contract_sensitive() -> None:
    first = canonical_journal_profile_spec(_settings())
    second = canonical_journal_profile_spec(_settings())
    contract_mutations = (
        {"PAPER_SIMULATION_VERSION": "different-version"},
        {"RELEASE_COMMIT_SHA": "a" * 40},
        {"PAPER_LOOP_INTERVAL_SECONDS": 11},
        {"PAPER_ORDERBOOK_SYMBOL_LIMIT": 11},
        {"OPTIONS_MAXIMUM_EXPIRIES": 3},
        {"PUBLIC_METADATA_REFRESH_SECONDS": 3601},
        {"MULTI_REGIME_ASSETS": "BTC,ETH,SOL"},
        {"MULTI_REGIME_DYNAMIC_UNIVERSE_ENABLED": False},
        {"MULTI_REGIME_UNIVERSE_REBALANCE_SECONDS": 3601},
        {"MULTI_REGIME_UNIVERSE_MAXIMUM_ASSETS": 21},
        {"MULTI_REGIME_UNIVERSE_MAXIMUM_NEW_ASSETS": 6},
        {"MULTI_REGIME_UNIVERSE_MAXIMUM_DATA_AGE_SECONDS": "121"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_LISTING_AGE_DAYS": "31"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_STATISTICS_DAYS": "8"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_VENUE_COUNT": 3},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_QUOTE_VOLUME_USD": "10000001"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_DEPTH_USD": "100001"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_OPEN_INTEREST_USD": "5000001"},
        {"MULTI_REGIME_UNIVERSE_MAXIMUM_SPREAD_BPS": "16"},
        {"MULTI_REGIME_UNIVERSE_MAXIMUM_SLIPPAGE_BPS": "21"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_FUNDING_SAMPLES": 21},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_DATA_COVERAGE": "0.96"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_ENTRY_SCORE": "0.56"},
        {"MULTI_REGIME_UNIVERSE_MINIMUM_RETENTION_SCORE": "0.44"},
        {"MULTI_REGIME_UNIVERSE_TARGET_FUNDING_BPS_DAILY": "11"},
        {"MULTI_REGIME_UNIVERSE_EXCLUDED_ASSETS": "BTC,ETH,USDT,USDC,BUSD"},
    )

    assert first == second
    assert first.profile == "full"
    assert len(first.config_sha256) == 64
    for mutation in contract_mutations:
        assert canonical_journal_profile_spec(_settings(**mutation)).config_sha256 != (
            first.config_sha256
        )


async def test_replay_accepts_one_contiguous_exact_profile_chain() -> None:
    engine, session_factory = await _sessions()
    spec = canonical_journal_profile_spec(_settings())
    async with canonical_journal_writer_lease(engine) as writer_lease:
        async with session_factory() as session:
            first_boundary = await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=writer_lease,
                started_at=NOW,
            )
            await append_event(session, _trade(1))
            await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=writer_lease,
                started_at=NOW + timedelta(seconds=2),
            )
            await append_event(session, _trade(3))
            replay = await load_replay_events(session, required_journal_profile=spec)
            window = await load_latest_compatible_journal_window(
                session,
                spec,
                up_to_event_row_id=2,
            )

    assert [event.metadata.event_id for event in replay] == [
        "profile-event-1",
        "profile-event-3",
    ]
    assert window.after_event_row_id == 0
    assert window.first_boundary_id == first_boundary.boundary_id
    assert window.boundary_ids[0] == first_boundary.boundary_id
    assert window.boundary_ids[-1] == window.latest_boundary_id
    assert window.started_at == NOW
    assert window.started_at.tzinfo is UTC
    await engine.dispose()  # type: ignore[union-attr]


async def test_replay_rejects_cross_profile_and_unlabeled_rows() -> None:
    engine, session_factory = await _sessions()
    full = canonical_journal_profile_spec(_settings())
    sampled = canonical_journal_profile_spec(
        _settings(
            MULTI_REGIME_ENABLED=False,
            CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS=60,
        )
    )
    async with canonical_journal_writer_lease(engine) as writer_lease:
        async with session_factory() as session:
            await append_canonical_journal_profile_boundary(
                session,
                full,
                writer_lease=writer_lease,
                started_at=NOW,
            )
            await append_event(session, _trade(1))
            sampled_boundary = await append_canonical_journal_profile_boundary(
                session,
                sampled,
                writer_lease=writer_lease,
                started_at=NOW + timedelta(seconds=2),
            )
            await append_event(session, _trade(3))

            with pytest.raises(CanonicalJournalProfileCoverageError, match="crosses"):
                await load_replay_events(session, required_journal_profile=sampled)
            sampled_only = await load_replay_events(
                session,
                start=NOW + timedelta(seconds=3),
                required_journal_profile=sampled,
            )
            window = await load_latest_compatible_journal_window(
                session,
                sampled,
                up_to_event_row_id=2,
            )

    assert [event.metadata.event_id for event in sampled_only] == ["profile-event-3"]
    assert window.after_event_row_id == 1
    assert window.first_boundary_id == sampled_boundary.boundary_id
    await engine.dispose()  # type: ignore[union-attr]

    unlabeled_engine, unlabeled_sessions = await _sessions()
    async with unlabeled_sessions() as session:
        await append_event(session, _trade(4))
        with pytest.raises(CanonicalJournalProfileCoverageError, match="metadata is missing"):
            await load_replay_events(session, required_journal_profile=full)
    await unlabeled_engine.dispose()  # type: ignore[union-attr]


async def test_writer_lease_serializes_boundary_owners_and_releases_cleanly() -> None:
    engine, session_factory = await _sessions()
    spec = canonical_journal_profile_spec(_settings())
    async with canonical_journal_writer_lease(engine) as first_lease:
        with pytest.raises(CanonicalJournalWriterLeaseError, match="another process"):
            async with canonical_journal_writer_lease(engine):
                pass
        async with session_factory() as session:
            await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=first_lease,
                started_at=NOW,
            )

    async with session_factory() as session:
        with pytest.raises(CanonicalJournalWriterLeaseError, match="active"):
            await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=first_lease,
                started_at=NOW + timedelta(seconds=1),
            )
    async with canonical_journal_writer_lease(engine):
        pass
    await engine.dispose()  # type: ignore[union-attr]


async def test_sqlite_file_writer_lease_is_database_scoped(tmp_path: Path) -> None:
    database_path = tmp_path / "journal.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    first_engine = create_async_engine(database_url)
    second_engine = create_async_engine(database_url)
    try:
        async with canonical_journal_writer_lease(first_engine):
            with pytest.raises(CanonicalJournalWriterLeaseError, match="another process"):
                async with canonical_journal_writer_lease(second_engine):
                    pass
        async with canonical_journal_writer_lease(second_engine):
            pass
    finally:
        await first_engine.dispose()
        await second_engine.dispose()


async def test_sqlite_uri_writer_configuration_fails_closed() -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite:///file:journal-profile-test?mode=memory&cache=shared&uri=true"
    )
    try:
        with pytest.raises(CanonicalJournalWriterLeaseError, match="URI databases"):
            async with canonical_journal_writer_lease(engine):
                pass
    finally:
        await engine.dispose()


async def test_checkpoint_boundary_must_strictly_cover_event_row() -> None:
    engine, session_factory = await _sessions()
    spec = canonical_journal_profile_spec(_settings())
    async with canonical_journal_writer_lease(engine) as writer_lease:
        async with session_factory() as session:
            covering = await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=writer_lease,
                started_at=NOW,
            )
            await append_event(session, _trade(1))
            latest_covering = await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=writer_lease,
                started_at=NOW + timedelta(seconds=2),
            )
            await append_event(session, _trade(3))
            next_boundary = await append_canonical_journal_profile_boundary(
                session,
                spec,
                writer_lease=writer_lease,
                started_at=NOW + timedelta(seconds=4),
            )
            selected = await load_canonical_journal_boundary_covering_row(
                session,
                spec,
                event_row_id=2,
            )
            await assert_canonical_journal_checkpoint_compatible(
                session,
                spec,
                boundary_id=latest_covering.boundary_id,
                event_row_id=2,
            )
            with pytest.raises(CanonicalJournalProfileCoverageError, match="latest covering"):
                await assert_canonical_journal_checkpoint_compatible(
                    session,
                    spec,
                    boundary_id=covering.boundary_id,
                    event_row_id=2,
                )
            with pytest.raises(CanonicalJournalProfileCoverageError, match="does not cover"):
                await assert_canonical_journal_checkpoint_compatible(
                    session,
                    spec,
                    boundary_id=next_boundary.boundary_id,
                    event_row_id=2,
                )

    assert selected.boundary_id == latest_covering.boundary_id
    await engine.dispose()  # type: ignore[union-attr]


@pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPOSITORY_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)
async def test_postgres_writer_lease_serializes_independent_process_connections() -> None:
    first_engine = create_async_engine(os.environ["DATABASE_URL"])
    second_engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with canonical_journal_writer_lease(first_engine):
            with pytest.raises(CanonicalJournalWriterLeaseError, match="another process"):
                async with canonical_journal_writer_lease(second_engine):
                    pass
        async with canonical_journal_writer_lease(second_engine):
            pass
    finally:
        await first_engine.dispose()
        await second_engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPOSITORY_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)
async def test_postgres_metadata_auto_init_is_rejected() -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        with pytest.raises(RuntimeError, match="Alembic migrations"):
            await init_database(engine)
    finally:
        await engine.dispose()


def test_profile_migration_is_append_only() -> None:
    migration = Path("migrations/versions/0018_journal_profiles.py").read_text(encoding="utf-8")

    assert "canonical_journal_profiles" in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration
    assert "funding_reject_immutable_mutation" in migration
    assert "refusing to downgrade immutable canonical journal profile evidence" in migration


def test_postgres_auto_init_is_forbidden_before_profile_boundary_write() -> None:
    with pytest.raises(ValueError, match="run Alembic first"):
        _settings(PAPER_AUTO_INIT_DATABASE=True)


def test_production_like_journal_requires_exact_release_revision() -> None:
    with pytest.raises(ValueError, match="exact RELEASE_COMMIT_SHA"):
        _settings(APP_ENV="paper_test_live_data")


async def test_journal_status_exposes_sampled_degradation_without_secrets() -> None:
    spec = canonical_journal_profile_spec(
        _settings(
            MULTI_REGIME_ENABLED=False,
            CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS=600,
        )
    )
    boundary = CanonicalJournalProfileBoundary(
        boundary_id="journal-profile-test",
        started_at=NOW,
        after_event_row_id=42,
        spec=spec,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                canonical_journal_profile=spec,
                canonical_journal_profile_boundary=boundary,
            )
        )
    )

    status = await canonical_journal_status(request)  # type: ignore[arg-type]

    assert status == {
        "recording_active": True,
        "profile": "sampled",
        "degraded": True,
        "high_frequency_events_enabled": True,
        "minimum_interval_seconds": "600",
        "simulation_versions": ["v34-cost-gated-candidate"],
        "config_sha256": spec.config_sha256,
        "boundary_id": "journal-profile-test",
        "after_event_row_id": 42,
    }
