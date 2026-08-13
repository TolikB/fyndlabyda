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
from funding_arbitrage.services.daily_report import DailyReportService


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
        paper_simulation_version="v25-oos-candidate",
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
        30,
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
    assert "No eligible paper trades during the day; equity was unchanged." in sent[0]
    assert "simulator v25-oos-candidate" in sent[0]
