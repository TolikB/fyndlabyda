"""Paper runner v2 against PostgreSQL: series, ledger, funding, restart, reports."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    PaperCycleRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperLedgerEntryRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
    TelegramDailyReportRecord,
)
from funding_arbitrage.database.repositories.paper import SeriesConfigMismatch
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.notifications.telegram import TelegramNotifier
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE
from funding_arbitrage.services import analytics
from funding_arbitrage.services.paper_runner import PaperTestRunner, RunnerLock
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import PaperSeriesFile
from tests.conftest import Database, FakeClock

VENUES = ("bybit", "gate", "okx", "binance", "hyperliquid")


def series_file(
    *, max_hold_hours: str = "0.05", candidate_min_rate: str = "0.0002"
) -> PaperSeriesFile:
    exit_rules = {"min_hold_hours": "0", "max_hold_hours": max_hold_hours, "exit_confirmations": 1}
    return PaperSeriesFile.model_validate(
        {
            "primary_series": "candidate",
            "series": [
                {
                    "name": "candidate",
                    "label": "candidate-test",
                    "initial_balance_usdt": "1000",
                    "position_notional_usdt": "50",
                    "max_total_notional_usdt": "100",
                    "max_open_positions": 2,
                    "entry": {"min_funding_rate_8h": candidate_min_rate},
                    "exit": exit_rules,
                },
                {
                    "name": "baseline",
                    "label": "baseline-test",
                    "initial_balance_usdt": "1000",
                    "position_notional_usdt": "50",
                    "max_total_notional_usdt": "400",
                    "max_open_positions": 8,
                    "max_positions_per_asset": 4,
                    "entry": {"min_funding_rate_8h": "0"},
                    "exit": exit_rules,
                },
            ],
        }
    )


class RecordingNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__("test-token", "123")
        self.messages: list[str] = []

    async def send_message(self, text: str) -> None:
        self.messages.append(text)


def make_runner(
    database: Database,
    clock: FakeClock,
    series: PaperSeriesFile,
    *,
    autotrade: bool = True,
    with_lock: bool = False,
    notifier: TelegramNotifier | None = None,
    **overrides: Any,
) -> PaperTestRunner:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=autotrade,
        paper_confirmation_seconds=0,
        paper_loop_interval_seconds=20,
        paper_snapshot_interval_seconds=1,
        mock_funding_interval_seconds=60,
        telegram_enabled=notifier is not None,
        telegram_bot_token="test-token" if notifier is not None else "",
        telegram_chat_id="123" if notifier is not None else "",
        **overrides,
    )
    adapters: dict[str, ExchangeAdapter] = {
        name: MockExchangeAdapter(name, funding_interval_seconds=60, clock=clock) for name in VENUES
    }
    runtime = RuntimeState(settings, adapters, series)
    return PaperTestRunner(
        settings,
        runtime,
        database.session_factory,
        series_file=series,
        engine=database.engine if with_lock else None,
        clock=clock,
        notifier=notifier,
    )


async def run_cycles(
    runner: PaperTestRunner, clock: FakeClock, count: int, step: float = 20
) -> None:
    for _ in range(count):
        cycle = await runner.cycle()
        assert cycle.status == "ok", cycle.error
        clock.advance(step)


async def count(database: Database, model: Any, **filters: Any) -> int:
    async with database.session_factory() as session:
        statement = select(func.count()).select_from(model)
        for name, value in filters.items():
            statement = statement.where(getattr(model, name) == value)
        return int(await session.scalar(statement) or 0)


async def test_cycles_open_settle_close_and_reconcile(database: Database, clock: FakeClock) -> None:
    runner = make_runner(database, clock, series_file())
    await runner.start()
    await run_cycles(runner, clock, 1)

    candidate = runner.series["candidate-test"].account
    baseline = runner.series["baseline-test"].account
    assert 1 <= len(candidate.positions) <= 2
    assert candidate.exposure <= Decimal("100")
    assert len(baseline.positions) > len(candidate.positions)
    # The candidate only takes markets paying at least 0.02% per 8h.
    for position in candidate.positions.values():
        assert position.entry_funding_rate_8h >= Decimal("0.0002")

    await run_cycles(runner, clock, 14)  # four minutes: funding every minute, 3-minute hold

    async with database.session_factory() as session:
        for label in ("candidate-test", "baseline-test"):
            result = await analytics.reconciliation(session, label)
            assert result["ok"], result
            worst = await session.scalar(
                select(func.max(PortfolioSnapshotRecord.invariant_diff)).where(
                    PortfolioSnapshotRecord.series_id == label
                )
            )
            assert Decimal(str(worst)) <= INVARIANT_TOLERANCE
        closed = await session.scalar(
            select(func.count())
            .select_from(PaperPositionRecord)
            .where(PaperPositionRecord.state == "CLOSED")
        )
        sources = set(
            (await session.execute(select(PaperFundingPaymentRecord.rate_source))).scalars()
        )
    assert closed and closed > 0
    assert sources == {"history"}
    assert await count(database, PaperFundingPaymentRecord, series_id="candidate-test") > 0
    # Every fill is one leg: two per open and two per close.
    fills = await count(database, PaperFillRecord, series_id="candidate-test")
    positions = await count(database, PaperPositionRecord, series_id="candidate-test")
    closed_candidate = await count(
        database, PaperPositionRecord, series_id="candidate-test", state="CLOSED"
    )
    assert fills == positions * 2 + closed_candidate * 2
    assert await count(database, PaperCycleRecord) == 15
    await runner.shutdown()


async def test_restart_restores_ledger_and_catches_up_missed_funding(
    database: Database, clock: FakeClock
) -> None:
    series = series_file(max_hold_hours="1")
    first = make_runner(database, clock, series)
    await first.start()
    await run_cycles(first, clock, 2)
    before = {label: item.account.snapshot(clock()) for label, item in first.series.items()}
    open_ids = {label: set(item.account.positions) for label, item in first.series.items()}
    # Simulated crash: no shutdown, no final flush beyond the last committed cycle.

    clock.advance(5 * 60)
    second = make_runner(database, clock, series)
    await second.start()
    for label, item in second.series.items():
        restored = item.account.snapshot(clock())
        assert restored.cash == before[label].cash
        assert restored.locked_capital == before[label].locked_capital
        assert set(item.account.positions) == open_ids[label]
        assert item.account.invariant_diff() <= INVARIANT_TOLERANCE

    await run_cycles(second, clock, 2)
    async with database.session_factory() as session:
        rows = (
            await session.execute(
                select(
                    PaperFundingPaymentRecord.position_id,
                    PaperFundingPaymentRecord.leg_index,
                    PaperFundingPaymentRecord.funding_timestamp,
                )
            )
        ).all()
        for label in ("candidate-test", "baseline-test"):
            assert (await analytics.reconciliation(session, label))["ok"]
    assert len(rows) == len(set(rows)), "a funding event was booked twice"
    minutes = {row.funding_timestamp for row in rows}
    assert len(minutes) >= 5, "missed settlements during the outage were not caught up"
    await second.shutdown()


async def test_changed_settings_need_a_new_series_label(
    database: Database, clock: FakeClock
) -> None:
    runner = make_runner(database, clock, series_file())
    await runner.start()
    await runner.shutdown()
    changed = make_runner(database, clock, series_file(candidate_min_rate="0.0003"))
    with pytest.raises(SeriesConfigMismatch) as error:
        await changed.start()
    assert "series.entry.min_funding_rate_8h" in str(error.value)

    # Global settings that change PnL (fees here) are part of the identity too.
    new_fees = make_runner(database, clock, series_file(), bybit_taker_fee=Decimal("0.0006"))
    with pytest.raises(SeriesConfigMismatch) as error:
        await new_fees.start()
    assert "context.fees.bybit.taker_fee" in str(error.value)


async def test_observe_mode_collects_without_positions(
    database: Database, clock: FakeClock
) -> None:
    runner = make_runner(database, clock, series_file(), autotrade=False)
    await runner.start()
    await run_cycles(runner, clock, 3)
    assert all(not item.account.positions for item in runner.series.values())
    assert await count(database, PaperLedgerEntryRecord) == 0
    async with database.session_factory() as session:
        cycles = (await session.execute(select(PaperCycleRecord))).scalars().all()
    assert len(cycles) == 3
    assert all(not cycle.autotrade and cycle.books_fetched > 0 for cycle in cycles)
    await runner.shutdown()


async def test_only_one_runner_may_hold_the_database(database: Database) -> None:
    first = RunnerLock(database.engine)
    second = RunnerLock(database.engine)
    assert await first.acquire()
    assert not await second.acquire()
    await first.release()
    assert await second.acquire()
    await second.release()


async def test_telegram_start_stop_and_single_daily_report(
    database: Database, clock: FakeClock
) -> None:
    clock.now = datetime(2026, 10, 1, 20, 55, 5, tzinfo=UTC)  # 23:55 in Kyiv
    notifier = RecordingNotifier()
    runner = make_runner(database, clock, series_file(max_hold_hours="1"), notifier=notifier)
    await runner.start()
    assert notifier.messages[0].startswith("▶️ Paper-бот запущено")
    await run_cycles(runner, clock, 45)  # until 00:10 Kyiv on 2 October
    await runner.shutdown()

    reports = [message for message in notifier.messages if message.startswith("📊")]
    assert len(reports) == 1
    report = reports[0]
    assert "Paper-звіт за 01.10.2026 (Київ) — candidate" in report
    for line in ("Результат дня:", "За весь час:", "Баланс:", "Funding:", "Витрати за день:"):
        assert line in report
    assert "Угоди: відкрито" in report
    assert notifier.messages[-1].startswith("⏹ Paper-бот зупинено")
    assert len(notifier.messages) == 3
    assert (
        await count(database, TelegramDailyReportRecord, series_id="candidate-test", status="sent")
        == 1
    )


async def test_readiness_flags_gaps_and_missing_report(
    database: Database, clock: FakeClock
) -> None:
    runner = make_runner(database, clock, series_file(max_hold_hours="1"))
    await runner.start()
    await run_cycles(runner, clock, 3)
    clock.advance(600)
    await run_cycles(runner, clock, 3)
    async with database.session_factory() as session:
        result = await analytics.readiness(
            session,
            hours=1,
            loop_interval_seconds=20,
            primary_series="candidate-test",
            now=clock(),
        )
    assert result["verdict"] == "FAIL"
    assert "snapshot_gap_exceeded" in result["reasons"]
    assert "window_not_fully_covered" in result["reasons"]
    assert "no_daily_report_sent" in result["reasons"]
    assert result["live_orders"] == 0
    assert all(item["reconciliation_ok"] for item in result["series"].values())
    await runner.shutdown()
