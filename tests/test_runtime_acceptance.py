from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts import runtime_acceptance as runtime_acceptance_script
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    CanonicalEventRecord,
    ExecutionFillRecord,
    PaperRuntimeIncidentRecord,
    PositionStateRecord,
)
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.execution.base import FillStatus, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.portfolio.position import (
    PaperPosition,
    PnLBreakdown,
    PositionState,
)
from funding_arbitrage.qa.acceptance_artifacts import acceptance_replay_runner_sha256
from funding_arbitrage.qa.acceptance_provenance import RuntimeReleaseIdentity
from funding_arbitrage.qa.acceptance_window import (
    REQUIRED_FAILURE_SCENARIOS,
    REQUIRED_VENUES,
    AcceptanceObservationInput,
    DeterministicReplayEvidence,
    FailureInjectionEvidence,
    load_acceptance_seal_input,
)
from funding_arbitrage.qa.runtime_acceptance import (
    AcceptanceRuntimeAttachments,
    AcceptanceRuntimeJournalHeader,
    RuntimeAcceptanceCollector,
    _directional_fill_economics,
    acceptance_config_sha256,
    load_runtime_acceptance_journal,
)
from funding_arbitrage.services.runtime import RuntimeState


class _MemoryJournal:
    def __init__(self) -> None:
        self.header: AcceptanceRuntimeJournalHeader | None = None
        self.observations: list[AcceptanceObservationInput] = []
        self.closed = False

    def open(self, header: AcceptanceRuntimeJournalHeader) -> None:
        if self.header is not None:
            raise FileExistsError
        self.header = header

    def append(self, observation: AcceptanceObservationInput) -> None:
        self.observations.append(observation)

    def close(self) -> None:
        self.closed = True


def _settings(tmp_path: Path, mode: TradingMode = TradingMode.SHADOW) -> Settings:
    paper = mode is TradingMode.PAPER
    return Settings(
        run_mode="paper_test",
        trading_mode=mode,
        market_data_mode="live_public",
        execution_mode="paper",
        paper_autotrade=paper,
        telegram_enabled=paper,
        telegram_bot_token="123456789:test-token",
        telegram_chat_id="987654321",
        acceptance_collector_enabled=True,
        acceptance_window_id=("paper-window" if paper else "shadow-window"),
        acceptance_journal_path=str(tmp_path / "acceptance.jsonl"),
        acceptance_sample_interval_seconds=1,
    )


def _identity(settings: Settings, observed_at: datetime) -> RuntimeReleaseIdentity:
    return RuntimeReleaseIdentity(
        document_kind="acceptance-runtime-release-identity",
        schema_version=1,
        code_revision="a" * 40,
        image_digest="sha256:" + "b" * 64,
        config_sha256=acceptance_config_sha256(settings),
        runner_sha256=acceptance_replay_runner_sha256(),
        observed_at=observed_at,
    )


async def _seed_process_start(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    occurred_at: datetime,
) -> None:
    async with session_factory() as session:
        session.add(
            PaperRuntimeIncidentRecord(
                occurred_at=occurred_at,
                simulation_version=settings.paper_simulation_version,
                category="process_start",
                error_type="ProcessStart",
            )
        )
        await session.commit()


def _snapshot(captured_at: datetime, *, stale_venue: str | None = None) -> MarketSnapshot:
    tickers: list[Ticker] = []
    funding: list[FundingSnapshot] = []
    books: dict[tuple[str, str, InstrumentType], OrderBook] = {}
    for venue in REQUIRED_VENUES:
        timestamp = (
            captured_at - timedelta(seconds=31)
            if venue == stale_venue
            else captured_at
        )
        symbol = "BTCUSDT"
        tickers.append(
            Ticker(
                exchange=venue,
                symbol=symbol,
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("100"),
                best_bid=Decimal("99.9"),
                best_ask=Decimal("100.1"),
                volume_24h=Decimal("1000000"),
                timestamp=timestamp,
            )
        )
        funding.append(
            FundingSnapshot(
                exchange=venue,
                symbol=symbol,
                funding_rate=Decimal("0.0002"),
                funding_interval_hours=Decimal("8"),
                timestamp=timestamp,
            )
        )
        books[(venue, symbol, InstrumentType.PERPETUAL)] = OrderBook(
            exchange=venue,
            symbol=symbol,
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.9"), quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=Decimal("100.1"), quantity=Decimal("10")),),
            timestamp=timestamp,
        )
    return MarketSnapshot([], tickers, funding, books, captured_at)


def _fill(venue: str, side: str, timestamp: datetime) -> PaperFill:
    return PaperFill(
        client_order_id=f"client-{venue}",
        exchange=venue,
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        side=side,
        requested_quantity=Decimal("0.1"),
        filled_quantity=Decimal("0.1"),
        price=Decimal("100"),
        reference_price=Decimal("100"),
        fee=Decimal("0.02"),
        spread=Decimal("0.03"),
        slippage=Decimal("0.04"),
        status=FillStatus.FILLED,
        timestamp=timestamp,
    )


def test_acceptance_config_rejects_private_exchange_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="forbids private exchange credentials"):
        Settings(
            run_mode="paper_test",
            trading_mode=TradingMode.SHADOW,
            acceptance_collector_enabled=True,
            acceptance_window_id="shadow-window",
            acceptance_journal_path=str(tmp_path / "acceptance.jsonl"),
            bybit_api_key="private-key-must-not-be-used",
        )
    with pytest.raises(ValueError, match="forbids live-trading authorization"):
        Settings(
            run_mode="paper_test",
            trading_mode=TradingMode.SHADOW,
            acceptance_collector_enabled=True,
            acceptance_window_id="shadow-window",
            acceptance_journal_path=str(tmp_path / "acceptance.jsonl"),
            live_armed=True,
        )


def test_runtime_identity_cli_hashes_exact_effective_settings_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    output = tmp_path / "release-identity.json"
    monkeypatch.setattr(runtime_acceptance_script, "get_settings", lambda: settings)

    first_exit = runtime_acceptance_script.main(
        [
            "identity",
            "--code-revision",
            "a" * 40,
            "--image-digest",
            "sha256:" + "b" * 64,
            "--output",
            str(output),
        ]
    )
    second_exit = runtime_acceptance_script.main(
        [
            "identity",
            "--code-revision",
            "a" * 40,
            "--image-digest",
            "sha256:" + "b" * 64,
            "--output",
            str(output),
        ]
    )

    assert first_exit == 0
    assert second_exit == 2
    identity = RuntimeReleaseIdentity.model_validate_json(output.read_text())
    assert identity.config_sha256 == acceptance_config_sha256(settings)
    assert identity.runner_sha256 == acceptance_replay_runner_sha256()


def test_directional_fill_reconciliation_rejects_forged_provenance() -> None:
    source_time = datetime(2026, 8, 31, 9, 0, tzinfo=UTC)
    fill_time = source_time + timedelta(milliseconds=100)
    instrument = {
        "venue": "BYBIT",
        "exchange_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "instrument_type": "PERPETUAL",
    }
    source = CanonicalEventRecord(
        event_id="book-provenance",
        kind="BOOK_SNAPSHOT",
        source="BYBIT:BOOK",
        sequence_id="book:provenance",
        native_sequence=1,
        correlation_id="correlation-provenance",
        payload_version=1,
        quality="VALID",
        exchange_timestamp=source_time,
        receive_timestamp=source_time,
        monotonic_ns=1,
        payload_hash="d" * 64,
        payload={"instrument": instrument},
    )
    fill_payload = {
        "timestamp": fill_time.isoformat(),
        "quantity": "0.1",
        "price": "100",
        "notional": "10",
        "fee": "0.02",
        "spread_cost": "0.03",
        "impact_cost": "0.04",
        "liquidity_role": "TAKER",
    }
    payload = {
        "fill": fill_payload,
        "source_event_id": source.event_id,
        "source_event_kind": source.kind,
        "source_event_source": source.source,
        "source_event_quality": source.quality,
        "source_exchange_timestamp": source_time.isoformat(),
        "source_receive_timestamp": source_time.isoformat(),
        "source_instrument": instrument,
    }
    record = ExecutionFillRecord(
        fill_id="directional-provenance",
        simulation_version="v1-provenance",
        client_order_id="client-provenance",
        exchange_order_id="paper:client-provenance",
        venue="BYBIT",
        instrument_id="BYBIT:BTC-USDT:PERPETUAL",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("0.1"),
        fee_amount=Decimal("0.02"),
        fee_asset="USDT",
        liquidity_role="TAKER",
        exchange_timestamp=fill_time,
        receive_timestamp=fill_time,
        payload=payload,
    )

    assert _directional_fill_economics(
        record, source, maximum_book_age_seconds=120
    )[0]
    variants = (
        payload | {"source_event_source": "GATE:BOOK"},
        payload
        | {
            "source_exchange_timestamp": (
                source_time + timedelta(seconds=1)
            ).isoformat()
        },
        payload
        | {
            "source_receive_timestamp": (
                source_time + timedelta(seconds=1)
            ).isoformat()
        },
        payload | {"fill": fill_payload | {"notional": "11"}},
        payload
        | {
            "fill": fill_payload
            | {"timestamp": (fill_time + timedelta(seconds=1)).isoformat()}
        },
        payload | {"fill": fill_payload | {"liquidity_role": "MAKER"}},
    )
    for variant in variants:
        record.payload = variant
        assert not _directional_fill_economics(
            record, source, maximum_book_age_seconds=120
        )[0]
    record.payload = payload
    record.instrument_id = "BYBIT:ETH-USDT:PERPETUAL"
    assert not _directional_fill_economics(
        record, source, maximum_book_age_seconds=120
    )[0]
    record.instrument_id = "BYBIT:BTC-USDT:PERPETUAL"
    record.fee_asset = "BTC"
    assert not _directional_fill_economics(
        record, source, maximum_book_age_seconds=120
    )[0]
    record.fee_asset = "USDT"
    record.fee_amount = Decimal("-0.01")
    reconciled, fee, _, _ = _directional_fill_economics(
        record, source, maximum_book_age_seconds=120
    )
    assert reconciled is False
    assert fee == 0


async def test_collector_starts_at_zero_then_records_reconciled_costed_fills(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)
    settings = _settings(tmp_path, TradingMode.PAPER)
    runtime = RuntimeState(settings, {})
    journal = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        journal,
        now=started_at,
    )

    assert runtime.entries_allowed() is False
    await collector.start()
    first_snapshot = _snapshot(started_at)
    await collector.observe_market_snapshot(first_snapshot)

    assert runtime.entries_allowed() is True
    assert len(journal.observations) == 1
    assert all(
        getattr(journal.observations[0].counters, field) == 0
        for field in type(journal.observations[0].counters).model_fields
    )
    assert journal.observations[0].costs.total_usd == 0

    position = PaperPosition(
        opportunity_id="accepted-position",
        asset="BTC",
        capital=Decimal("10"),
        strategy="spot_perp",
        state=PositionState.OPEN,
        leg_a=_fill("bybit", "BUY", started_at),
        leg_b=_fill("gate", "SELL", started_at),
        pnl=PnLBreakdown(
            fees=Decimal("0.04"),
            spread=Decimal("0.06"),
            slippage=Decimal("0.08"),
        ),
        opened_at=started_at,
    )
    runtime.portfolio.allocate_position(position, ("bybit", "gate"), Decimal("10"))
    collector.record_strategy_evaluation([SimpleNamespace(status="confirmed")])
    collector.record_successful_cycle(first_snapshot, daily_report_sent=False)
    await collector.observe_market_snapshot(
        _snapshot(started_at + timedelta(seconds=2))
    )

    latest = journal.observations[-1]
    assert latest.counters.runner_cycles == 1
    assert latest.counters.strategy_evaluations == 1
    assert latest.counters.strategy_decisions == 1
    assert latest.counters.simulated_fills == 2
    assert latest.counters.fill_book_reconciliations == 2
    assert latest.counters.unreconciled_fills == 0
    assert latest.simulated_fill_venues == ("bybit", "gate")
    assert latest.costs.fees_usd == Decimal("0.04")
    assert latest.costs.spread_usd == Decimal("0.06")
    assert latest.costs.slippage_usd == Decimal("0.08")
    assert latest.ledger_sha256 != journal.observations[0].ledger_sha256

    await collector.close()
    assert journal.closed is True
    assert runtime.entries_allowed() is False


async def test_stale_venue_permanently_fails_window_and_disables_entries(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 11, 0, tzinfo=UTC)
    settings = _settings(tmp_path)
    runtime = RuntimeState(settings, {})
    journal = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        journal,
        now=started_at,
    )
    await collector.start()
    await collector.observe_market_snapshot(_snapshot(started_at))

    await collector.observe_market_snapshot(
        _snapshot(started_at + timedelta(seconds=2), stale_venue="gate")
    )

    failed = journal.observations[-1]
    assert failed.ready is False
    assert failed.data_quality_valid is False
    assert failed.counters.data_quality_incidents == 1
    assert failed.counters.venue_outage_incidents == 1
    assert failed.counters.stale_stream_incidents == 1
    assert "gate" not in failed.healthy_venues
    assert runtime.entries_allowed() is False
    with pytest.raises(RuntimeError, match="permanently disabled"):
        runtime.set_acceptance_entries_enabled(True)


async def test_collector_includes_directional_fill_with_canonical_book_link(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 11, 30, tzinfo=UTC)
    settings = _settings(tmp_path, TradingMode.PAPER)
    runtime = RuntimeState(settings, {})
    journal = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        journal,
        now=started_at,
    )
    await collector.start()
    await collector.observe_market_snapshot(_snapshot(started_at))
    source_instrument = {
        "venue": "bybit",
        "exchange_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "instrument_type": "PERPETUAL",
    }
    fill_time = started_at + timedelta(milliseconds=100)
    async with session_factory() as session:
        session.add(
            CanonicalEventRecord(
                event_id="book-linked-1",
                kind="BOOK_SNAPSHOT",
                source="bybit:public",
                sequence_id="book:1",
                native_sequence=1,
                correlation_id="correlation-1",
                payload_version=1,
                quality="VALID",
                exchange_timestamp=started_at,
                receive_timestamp=started_at,
                monotonic_ns=1,
                payload_hash="c" * 64,
                payload={"instrument": source_instrument},
            )
        )
        session.add(
            ExecutionFillRecord(
                fill_id="directional-fill-1",
                simulation_version=settings.paper_simulation_version,
                client_order_id="directional-client-1",
                exchange_order_id="paper:directional-client-1",
                venue="bybit",
                instrument_id="BYBIT:BTC-USDT:PERPETUAL",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("0.1"),
                fee_amount=Decimal("0.02"),
                fee_asset="USDT",
                liquidity_role="TAKER",
                exchange_timestamp=fill_time,
                receive_timestamp=fill_time,
                payload={
                    "fill": {
                        "timestamp": fill_time.isoformat(),
                        "quantity": "0.1",
                        "price": "100",
                        "notional": "10",
                        "fee": "0.02",
                        "spread_cost": "0.03",
                        "impact_cost": "0.04",
                        "liquidity_role": "TAKER",
                    },
                    "source_event_id": "book-linked-1",
                    "source_event_kind": "BOOK_SNAPSHOT",
                    "source_event_source": "bybit:public",
                    "source_event_quality": "VALID",
                    "source_exchange_timestamp": started_at.isoformat(),
                    "source_receive_timestamp": started_at.isoformat(),
                    "source_instrument": source_instrument,
                },
            )
        )
        session.add(
            PositionStateRecord(
                position_id="mrp_closed_1",
                simulation_version=settings.paper_simulation_version,
                strategy_id="trend",
                venue="bybit",
                instrument_id="BYBIT:BTC-USDT:PERPETUAL",
                status="CLOSED",
                signed_quantity=Decimal("0"),
                entry_price=Decimal("100"),
                mark_price=Decimal("101"),
                realized_pnl=Decimal("0.91"),
                unrealized_pnl=Decimal("0"),
                collateral=Decimal("0"),
                opened_at=started_at,
                closed_at=fill_time,
                updated_at=fill_time,
                payload={"structural_stop": "99", "target_price": "102"},
            )
        )
        await session.commit()

    collector.record_strategy_evaluation([SimpleNamespace(status="confirmed")])
    collector.record_successful_cycle(_snapshot(started_at), daily_report_sent=False)
    await collector.observe_market_snapshot(_snapshot(started_at + timedelta(seconds=2)))

    latest = journal.observations[-1]
    assert latest.counters.canonical_market_events == 1
    assert latest.counters.simulated_fills == 1
    assert latest.counters.fill_book_reconciliations == 1
    assert latest.counters.unreconciled_fills == 0
    assert latest.counters.closed_positions == 1
    assert latest.simulated_fill_venues == ("bybit",)
    assert latest.costs.fees_usd == Decimal("0.02")
    assert latest.costs.spread_usd == Decimal("0.03")
    assert latest.costs.slippage_usd == Decimal("0.04")


async def test_unlinked_directional_fill_permanently_fails_window(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 11, 45, tzinfo=UTC)
    settings = _settings(tmp_path, TradingMode.PAPER)
    runtime = RuntimeState(settings, {})
    journal = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        journal,
        now=started_at,
    )
    await collector.start()
    await collector.observe_market_snapshot(_snapshot(started_at))
    assert runtime.entries_allowed() is True

    fill_time = started_at + timedelta(milliseconds=100)
    async with session_factory() as session:
        session.add(
            ExecutionFillRecord(
                fill_id="directional-fill-unlinked",
                simulation_version=settings.paper_simulation_version,
                client_order_id="directional-client-unlinked",
                exchange_order_id="paper:directional-client-unlinked",
                venue="bybit",
                instrument_id="BTC-USDT-PERP",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("0.1"),
                fee_amount=Decimal("0.02"),
                fee_asset="USDT",
                liquidity_role="TAKER",
                exchange_timestamp=fill_time,
                receive_timestamp=fill_time,
                payload={
                    "fill": {
                        "timestamp": fill_time.isoformat(),
                        "quantity": "0.1",
                        "price": "100",
                        "notional": "10",
                        "fee": "0.02",
                        "spread_cost": "0.03",
                        "impact_cost": "0.04",
                        "liquidity_role": "TAKER",
                    },
                    "source_event_id": "missing-book",
                    "source_event_kind": "BOOK_SNAPSHOT",
                    "source_event_source": "bybit:public",
                    "source_event_quality": "VALID",
                    "source_exchange_timestamp": started_at.isoformat(),
                    "source_receive_timestamp": started_at.isoformat(),
                    "source_instrument": {
                        "venue": "bybit",
                        "exchange_symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "instrument_type": "PERPETUAL",
                    },
                },
            )
        )
        await session.commit()

    collector.record_successful_cycle(_snapshot(started_at), daily_report_sent=False)
    await collector.observe_market_snapshot(
        _snapshot(started_at + timedelta(seconds=2))
    )

    latest = journal.observations[-1]
    assert latest.counters.simulated_fills == 1
    assert latest.counters.fill_book_reconciliations == 0
    assert latest.counters.unreconciled_fills == 1
    assert latest.ready is False
    assert latest.data_quality_valid is False
    assert runtime.entries_allowed() is False
    with pytest.raises(RuntimeError, match="permanently disabled"):
        runtime.set_acceptance_entries_enabled(True)


async def test_runtime_journal_round_trip_rejects_truncation(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    settings = _settings(tmp_path)
    runtime = RuntimeState(settings, {})
    memory = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        memory,
        now=started_at,
    )
    await collector.start()
    await collector.observe_market_snapshot(_snapshot(started_at))
    collector.record_strategy_evaluation([SimpleNamespace(status="confirmed")])
    collector.record_successful_cycle(_snapshot(started_at), daily_report_sent=False)
    await collector.observe_market_snapshot(_snapshot(started_at + timedelta(seconds=2)))
    assert memory.header is not None

    path = tmp_path / "round-trip.jsonl"
    records = [
        memory.header.model_dump(mode="json"),
        *[
            {
                "document_kind": "acceptance-runtime-observation",
                "schema_version": 1,
                "observation": item.model_dump(mode="json"),
            }
            for item in memory.observations
        ],
    ]
    path.write_bytes(
        b"".join(
            json.dumps(item, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for item in records
        )
    )

    header, observations = load_runtime_acceptance_journal(path)
    assert header.window_id == "shadow-window"
    assert observations == tuple(memory.observations)

    truncated = tmp_path / "truncated.jsonl"
    truncated.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(ValueError, match="incomplete"):
        load_runtime_acceptance_journal(truncated)


async def test_runtime_cli_assembles_journal_and_never_overwrites(
    database: tuple[object, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, session_factory = database
    started_at = datetime(2026, 8, 31, 13, 0, tzinfo=UTC)
    settings = _settings(tmp_path)
    runtime = RuntimeState(settings, {})
    memory = _MemoryJournal()
    await _seed_process_start(session_factory, settings, started_at)
    collector = RuntimeAcceptanceCollector(
        settings,
        runtime,
        session_factory,
        _identity(settings, started_at - timedelta(seconds=1)),
        memory,
        now=started_at,
    )
    await collector.start()
    await collector.observe_market_snapshot(_snapshot(started_at))
    collector.record_strategy_evaluation([SimpleNamespace(status="confirmed")])
    collector.record_successful_cycle(_snapshot(started_at), daily_report_sent=False)
    await collector.observe_market_snapshot(_snapshot(started_at + timedelta(seconds=2)))
    assert memory.header is not None

    journal_path = tmp_path / "assemble.jsonl"
    journal_records = [
        memory.header.model_dump(mode="json"),
        *[
            {
                "document_kind": "acceptance-runtime-observation",
                "schema_version": 1,
                "observation": item.model_dump(mode="json"),
            }
            for item in memory.observations
        ],
    ]
    journal_path.write_bytes(
        b"".join(
            json.dumps(item, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for item in journal_records
        )
    )
    identity = memory.header.release_identity
    tested_at = started_at + timedelta(seconds=3)
    attachments = AcceptanceRuntimeAttachments(
        document_kind="acceptance-runtime-attachments",
        schema_version=1,
        failure_injections=tuple(
            FailureInjectionEvidence(
                scenario=scenario,
                tested_at=tested_at,
                artifact_sha256="c" * 64,
                code_revision=identity.code_revision,
                image_digest=identity.image_digest,
                config_sha256=identity.config_sha256,
                injected_count=3,
                detected_count=3,
                recovered_count=3,
                unexpected_effect_count=0,
                maximum_recovery_seconds=Decimal("1"),
            )
            for scenario in REQUIRED_FAILURE_SCENARIOS
        ),
        deterministic_replay=DeterministicReplayEvidence(
            tested_at=tested_at,
            dataset_sha256="d" * 64,
            dataset_manifest_sha256="e" * 64,
            replay_runner_sha256="f" * 64,
            replay_command_sha256="1" * 64,
            cost_policy_sha256="2" * 64,
            dataset_artifact_ref="acceptance:dataset",
            replay_runner_artifact_ref="acceptance:runner",
            first_result_sha256="3" * 64,
            second_result_sha256="3" * 64,
            event_count=10_000,
            source_start=started_at - timedelta(days=31),
            source_end=started_at - timedelta(days=1),
            venue_coverage=REQUIRED_VENUES,
            code_revision=identity.code_revision,
            image_digest=identity.image_digest,
            config_sha256=identity.config_sha256,
        ),
    )
    attachments_path = tmp_path / "attachments.json"
    attachments_path.write_text(
        json.dumps(attachments.model_dump(mode="json"), sort_keys=True),
        encoding="utf-8",
    )
    output = tmp_path / "raw.json"

    first_exit = runtime_acceptance_script.main(
        [
            "assemble",
            "--journal",
            str(journal_path),
            "--attachments",
            str(attachments_path),
            "--output",
            str(output),
        ]
    )
    second_exit = runtime_acceptance_script.main(
        [
            "assemble",
            "--journal",
            str(journal_path),
            "--attachments",
            str(attachments_path),
            "--output",
            str(output),
        ]
    )

    assert first_exit == 0
    assert second_exit == 2
    payload = load_acceptance_seal_input(output)
    assert payload.window_id == "shadow-window"
    assert payload.observations == tuple(memory.observations)
