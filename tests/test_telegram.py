from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.notifications.telegram import (
    TelegramNotificationError,
    TelegramNotifier,
)
from funding_arbitrage.services.daily_report import (
    DailyReportService,
    _no_fill_note,
    _runner_state,
)


@pytest.mark.asyncio
async def test_telegram_notifier_uses_bot_api_without_logging_secret() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/bottest-token/sendMessage"
        assert request.read().decode().find("secret") == -1
        return httpx.Response(200, json={"ok": True, "result": {}})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.telegram.org"
    )
    notifier = TelegramNotifier(
        "test-token", "123", http_client=client, api_base_url="https://api.telegram.org"
    )
    await notifier.send_message("paper report")
    await client.aclose()


@pytest.mark.asyncio
async def test_telegram_notifier_requires_configuration() -> None:
    notifier = TelegramNotifier("", "")
    with pytest.raises(TelegramNotificationError):
        await notifier.send_message("paper report")


class ReportSession:
    def __init__(self) -> None:
        timestamp = datetime(2026, 8, 1, tzinfo=UTC)
        self.values = [
            None,
            type("Snapshot", (), {"equity": Decimal("10000"), "total_pnl": Decimal("12")})(),
            type(
                "Snapshot",
                (),
                {
                    "equity": Decimal("10012"),
                    "total_pnl": Decimal("12"),
                    "funding_pnl": Decimal("8"),
                    "fees": Decimal("1"),
                    "timestamp": timestamp,
                },
            )(),
            type("Snapshot", (), {"timestamp": timestamp})(),
            Decimal("8"),
            Decimal("1"),
            Decimal("0.2"),
            Decimal("0.5"),
            4,
            1,
            1,
            10,
            2,
            96,
            0,
            0,
        ]
        self.added: list[object] = []

    async def __aenter__(self) -> ReportSession:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def scalar(self, _statement: object) -> object:
        return self.values.pop(0)

    def add(self, value: object) -> None:
        self.added.append(value)

    async def commit(self) -> None:
        return None


class ReportSessionFactory:
    def __init__(self, session: ReportSession) -> None:
        self.session = session

    def __call__(self) -> ReportSession:
        return self.session


@pytest.mark.asyncio
async def test_daily_report_is_sent_once_for_previous_local_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        paper_initial_balance_usd=10000,
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
        telegram_timezone="UTC",
    )
    session = ReportSession()
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(session)),
    )
    sent: list[str] = []

    async def send(message: str) -> None:
        sent.append(message)

    monkeypatch.setattr(service.notifier, "send_message", send)
    result = await service.check_and_send(datetime(2026, 8, 10, 0, 1, tzinfo=UTC))

    assert result
    assert len(sent) == 1
    assert "2026-08-09" in sent[0]
    assert "DAY RESULT" in sent[0]
    assert "Net PnL: +$12.00" in sent[0]
    assert "Funding: +$8.00" in sent[0]
    assert "TOTAL — CURRENT SIMULATOR" in sent[0]
    assert "Return: +0.1200%" in sent[0]
    assert "Legacy/pre-fix simulator data is excluded." in sent[0]
    assert len(session.added) == 1


@pytest.mark.asyncio
async def test_daily_report_explains_unchanged_equity_when_no_trades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        paper_initial_balance_usd=6250,
        paper_simulation_version="v26-oos-candidate",
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
        telegram_timezone="UTC",
    )
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    unchanged = type(
        "Snapshot",
        (),
        {
            "equity": Decimal("6250"),
            "total_pnl": Decimal("0"),
            "funding_pnl": Decimal("0"),
            "fees": Decimal("0"),
            "timestamp": timestamp,
        },
    )()
    session = ReportSession()
    session.values = [
        None,
        unchanged,
        unchanged,
        unchanged,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        0,
        0,
        0,
        0,
        0,
        96,
        0,
        0,
    ]
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(session)),
    )
    sent: list[str] = []

    async def send(message: str) -> None:
        sent.append(message)

    monkeypatch.setattr(service.notifier, "send_message", send)

    assert await service.check_and_send(datetime(2026, 8, 11, 0, 1, tzinfo=UTC))
    assert "Net PnL: +$0.00" in sent[0]
    assert "No eligible paper signals were observed; equity was unchanged." in sent[0]
    assert "Unique eligible signals: 0 | Confirmed: 0" in sent[0]
    assert "Runner: OK | snapshots: 96 | cycle failures: 0 | process starts: 0" in sent[0]
    assert "simulator v26-oos-candidate" in sent[0]


@pytest.mark.asyncio
async def test_daily_report_includes_isolated_baseline_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        paper_initial_balance_usd=6250,
        paper_simulation_version="v29-oos-candidate",
        paper_comparison_enabled=True,
        paper_baseline_simulation_version="v29-oos-baseline",
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
        telegram_timezone="UTC",
    )
    timestamp = datetime(2026, 8, 14, 4, 14, tzinfo=UTC)
    candidate = type(
        "Snapshot",
        (),
        {
            "equity": Decimal("6250"),
            "total_pnl": Decimal("0"),
            "funding_pnl": Decimal("0"),
            "fees": Decimal("0"),
            "timestamp": timestamp,
        },
    )()
    baseline = type(
        "Snapshot",
        (),
        {
            "equity": Decimal("6249.635031950949948783"),
            "total_pnl": Decimal("-0.364968049050051217"),
            "funding_pnl": Decimal("0.372430000000000000"),
            "fees": Decimal("0.262500245296606355"),
            "timestamp": timestamp,
        },
    )()
    session = ReportSession()
    session.values = [
        None,
        None,
        candidate,
        candidate,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        0,
        0,
        0,
        33,
        0,
        100,
        0,
        1,
        None,
        baseline,
        baseline,
        Decimal("0.372430000000000000"),
        Decimal("0.262500245296606355"),
        Decimal("0"),
        Decimal("0"),
        2,
        1,
        0,
        100,
        0,
        1,
    ]
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(session)),
    )
    sent: list[str] = []

    async def send(message: str) -> None:
        sent.append(message)

    monkeypatch.setattr(service.notifier, "send_message", send)

    assert await service.check_and_send(datetime(2026, 8, 15, 0, 1, tzinfo=UTC))
    assert "Portfolio: candidate | simulator v29-oos-candidate" in sent[0]
    assert "Portfolio: baseline | simulator v29-oos-baseline" in sent[0]
    assert "Equity: $6249.64" in sent[0]
    assert "Net PnL: -$0.36" in sent[0]
    assert "Funding: +$0.37" in sent[0]
    assert "Fees: -$0.26" in sent[0]
    assert "Candidate and baseline are separate virtual portfolios" in sent[0]


def test_daily_report_distinguishes_unconfirmed_signals_from_no_edge() -> None:
    note = _no_fill_note(
        fills=0,
        equity_delta=Decimal("0"),
        eligible_signals=10,
        confirmed_signals=0,
        snapshot_count=96,
        cycle_failures=0,
    )

    assert note == (
        "10 eligible signal(s) were observed, but none reached confirmed state; "
        "no position was opened."
    )


def test_daily_report_does_not_call_unchanged_equity_no_edge_after_failure() -> None:
    note = _no_fill_note(
        fills=0,
        equity_delta=Decimal("0"),
        eligible_signals=0,
        confirmed_signals=0,
        snapshot_count=0,
        cycle_failures=1,
    )

    assert note is not None
    assert "runtime evidence needs attention" in note
    assert "does not prove that the market had no edge" in note
    assert _runner_state(
        snapshot_count=0,
        cycle_failures=1,
        process_starts=1,
        had_prior_snapshot=False,
    ) == "ATTENTION"
    assert _runner_state(
        snapshot_count=96,
        cycle_failures=0,
        process_starts=1,
        had_prior_snapshot=False,
    ) == "STARTED"
    assert _runner_state(
        snapshot_count=96,
        cycle_failures=0,
        process_starts=1,
        had_prior_snapshot=True,
    ) == "RESTARTED"
