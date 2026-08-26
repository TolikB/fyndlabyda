from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo

import httpx
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import TelegramDailyReportRecord
from funding_arbitrage.notifications.telegram import (
    TelegramNotificationError,
    TelegramNotifier,
    _telegram_chunks,
)
from funding_arbitrage.services.daily_report import (
    DailyReportService,
    _local_day_utc_bounds,
    _no_fill_note,
)


@pytest.mark.parametrize(
    ("report_date", "expected_hours"),
    (
        (date(2026, 3, 29), 23),
        (date(2026, 10, 25), 25),
    ),
)
def test_daily_report_uses_exact_kyiv_calendar_day_across_dst(
    report_date: date, expected_hours: int
) -> None:
    timezone = ZoneInfo("Europe/Kyiv")

    start, end = _local_day_utc_bounds(report_date, timezone)

    assert end - start == timedelta(hours=expected_hours)
    assert start.astimezone(timezone).date() == report_date
    assert end.astimezone(timezone).date() == report_date + timedelta(days=1)


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


def test_telegram_chunks_preserve_long_report_without_truncation() -> None:
    report = "\n".join(f"venue-{index}: stats" for index in range(500))

    chunks = _telegram_chunks(report)

    assert len(chunks) > 1
    assert all(len(chunk) <= 4096 for chunk in chunks)
    assert "".join(chunks) == report


@pytest.mark.asyncio
async def test_telegram_notifier_requires_configuration() -> None:
    notifier = TelegramNotifier("", "")
    with pytest.raises(TelegramNotificationError):
        await notifier.send_message("paper report")


@pytest.mark.asyncio
async def test_paper_lifecycle_notifications_are_human_readable_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
    )
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(ReportSession())),
    )
    sent: list[str] = []

    async def send(message: str) -> None:
        sent.append(message)

    monkeypatch.setattr(service.notifier, "send_message", send)

    assert await service.notify_stopped() is False
    assert await service.notify_started() is True
    assert await service.notify_started() is False
    assert await service.notify_stopped() is True
    assert await service.notify_stopped() is False
    assert sent == ["✅ Бот запущено", "⏹ Бот зупинено"]


@pytest.mark.asyncio
async def test_lifecycle_does_not_retry_ambiguous_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
    )
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(ReportSession())),
    )
    attempts = 0

    async def timeout(_message: str) -> None:
        nonlocal attempts
        attempts += 1
        raise TimeoutError("response lost after request")

    monkeypatch.setattr(service.notifier, "send_message", timeout)

    assert await service.notify_started() is False
    assert await service.notify_started() is False
    assert await service.notify_stopped() is False
    assert attempts == 1


class ReportSession:
    def __init__(self, events: list[str] | None = None) -> None:
        timestamp = datetime(2026, 8, 1, tzinfo=UTC)
        self.events = events
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
            1,
            Decimal("0.25"),
            2,
            1,
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

    async def execute(self, _statement: object) -> object:
        return type("EmptyRows", (), {"all": lambda self: []})()

    def add(self, value: object) -> None:
        self.added.append(value)

    async def commit(self) -> None:
        if self.events is not None:
            self.events.append("commit")

    async def rollback(self) -> None:
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
    events: list[str] = []
    session = ReportSession(events)
    service = DailyReportService(
        settings,
        cast(async_sessionmaker[AsyncSession], ReportSessionFactory(session)),
    )
    sent: list[str] = []

    async def send(message: str) -> None:
        events.append("send")
        sent.append(message)

    monkeypatch.setattr(service.notifier, "send_message", send)
    result = await service.check_and_send(datetime(2026, 8, 10, 0, 1, tzinfo=UTC))

    assert result
    assert len(sent) == 1
    assert "📊 Звіт про торгівлю · 2026-08-09" in sent[0]
    assert "ЗА ДЕНЬ" in sent[0]
    assert "Результат: +$12.00" in sent[0]
    assert "Фандінг: +$8.00" in sent[0]
    assert "Витрати: $1.45 (комісії $1.25, прослизання $0.20)" in sent[0]
    assert "Угоди: відкрито 2 · закрито 2" in sent[0]
    assert sent[0].count("Відкриті позиції: 2") == 1
    assert "ЗАГАЛОМ" in sent[0]
    assert "Баланс: $10012.00" in sent[0]
    assert "Результат: +$12.00 (+0.1200%)" in sent[0]
    assert "Тестовий режим · реальних ордерів немає" in sent[0]
    assert "Статус:" not in sent[0]
    assert "BYBIT" not in sent[0]
    assert "simulator" not in sent[0]
    assert "snapshots:" not in sent[0]
    assert len(session.added) == 1
    assert events == ["commit", "send", "commit"]

    session.values.append(session.added[0])
    repeated = await service.check_and_send(
        datetime(2026, 8, 10, 0, 2, tzinfo=UTC)
    )

    assert repeated is False
    assert len(sent) == 1
    assert len(session.added) == 1


@pytest.mark.asyncio
async def test_daily_report_claim_prevents_retry_after_ambiguous_delivery(
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
    attempts = 0

    async def timeout(_message: str) -> None:
        nonlocal attempts
        attempts += 1
        raise TimeoutError("response lost after request")

    monkeypatch.setattr(service.notifier, "send_message", timeout)

    assert not await service.check_and_send(datetime(2026, 8, 10, 0, 1, tzinfo=UTC))
    assert len(session.added) == 1
    record = session.added[0]
    assert isinstance(record, TelegramDailyReportRecord)
    assert record.status == "delivery_unknown"
    assert record.error == "TimeoutError"

    session.values.append(record)
    assert not await service.check_and_send(datetime(2026, 8, 10, 0, 2, tzinfo=UTC))
    assert attempts == 1


@pytest.mark.asyncio
async def test_concurrent_daily_report_claim_sends_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
        telegram_timezone="UTC",
    )

    class RaceState:
        def __init__(self) -> None:
            self.waiting = 0
            self.release = asyncio.Event()
            self.claimed = False

    class RaceSession:
        def __init__(self, state: RaceState) -> None:
            self.state = state
            self.commit_count = 0

        async def __aenter__(self) -> RaceSession:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def scalar(self, _statement: object) -> None:
            return None

        def add(self, _value: object) -> None:
            return None

        async def commit(self) -> None:
            self.commit_count += 1
            if self.commit_count > 1:
                return
            self.state.waiting += 1
            if self.state.waiting == 2:
                self.state.release.set()
            await self.state.release.wait()
            if not self.state.claimed:
                self.state.claimed = True
                return
            raise IntegrityError(
                "INSERT telegram_daily_reports",
                {},
                RuntimeError("duplicate report date"),
            )

        async def rollback(self) -> None:
            return None

    state = RaceState()

    def session_factory() -> RaceSession:
        return RaceSession(state)

    factory = cast(async_sessionmaker[AsyncSession], session_factory)
    services = [DailyReportService(settings, factory), DailyReportService(settings, factory)]
    sent: list[str] = []

    async def build_message(_session: AsyncSession, _report_date: date) -> str:
        return "daily trading report"

    async def send(message: str) -> None:
        sent.append(message)

    for service in services:
        monkeypatch.setattr(service, "_build_message", build_message)
        monkeypatch.setattr(service.notifier, "send_message", send)

    results = await asyncio.gather(
        *(service.check_and_send(datetime(2026, 8, 10, 0, 1, tzinfo=UTC)) for service in services)
    )

    assert sorted(results) == [False, True]
    assert sent == ["daily trading report"]
    await asyncio.gather(*(service.close() for service in services))


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
        0,
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
    assert "Результат: +$0.00" in sent[0]
    assert "Без угод: сигналів, що пройшли фільтри, не було." in sent[0]
    assert "Статус:" not in sent[0]
    assert "Відкриті позиції: 0" in sent[0]
    assert "v26-oos-candidate" not in sent[0]


@pytest.mark.asyncio
async def test_daily_report_omits_background_baseline_results(
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
        0,
        0,
        0,
        0,
        0,
        0,
        33,
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
    assert sent[0].count("ЗА ДЕНЬ") == 1
    assert sent[0].count("ЗАГАЛОМ") == 1
    assert "ОСНОВНА СТРАТЕГІЯ" not in sent[0]
    assert "СТРАТЕГІЯ ДЛЯ ПОРІВНЯННЯ" not in sent[0]
    assert "Баланс: $6250.00" in sent[0]
    assert "Баланс: $6249.64" not in sent[0]
    assert "Портфелі незалежні" not in sent[0]
    assert "v29-oos-" not in sent[0]


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
        "Без угод: сигналів після фільтрів 10, але жоден не підтвердився."
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
    assert "торговий цикл мають помилки" in note
    assert "потрібна перевірка" in note
