from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.notifications.telegram import (
    TelegramNotificationError,
    TelegramNotifier,
)
from funding_arbitrage.services.daily_report import (
    DailyReportData,
    DailyReportService,
    OpenPositionLine,
    format_daily_report,
)

TOKEN = "123456:SECRET-token"


async def test_telegram_notifier_uses_bot_api_without_logging_secret() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/bot{TOKEN}/sendMessage"
        assert TOKEN not in request.read().decode()
        return httpx.Response(200, json={"ok": True, "result": {}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    notifier = TelegramNotifier(TOKEN, "123", http_client=client)
    await notifier.send_message("paper report")
    await client.aclose()


async def test_telegram_notifier_requires_configuration() -> None:
    notifier = TelegramNotifier("", "")
    with pytest.raises(TelegramNotificationError):
        await notifier.send_message("paper report")


async def test_transport_errors_never_carry_the_token(caplog: pytest.LogCaptureFixture) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"cannot reach {request.url}")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    notifier = TelegramNotifier(TOKEN, "123", http_client=client)
    with pytest.raises(TelegramNotificationError) as error:
        await notifier.send_message("paper report")
    assert TOKEN not in str(error.value)
    # No chained exception keeps the URL alive for a traceback.
    assert error.value.__cause__ is None and error.value.__suppress_context__

    caplog.set_level(logging.DEBUG)
    assert await notifier.send_notice("start", "hello") is False
    assert TOKEN not in caplog.text
    await client.aclose()


async def test_rejected_request_reports_status_without_token() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"ok": False, "description": "Unauthorized"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    notifier = TelegramNotifier(TOKEN, "123", http_client=client)
    with pytest.raises(TelegramNotificationError) as error:
        await notifier.send_message("paper report")
    assert "401" in str(error.value)
    assert TOKEN not in str(error.value)
    await client.aclose()


def report_data() -> DailyReportData:
    return DailyReportData(
        series_name="candidate",
        series_id="candidate-sim2",
        report_date=date(2026, 10, 1),
        started_on=date(2026, 9, 30),
        initial_balance=Decimal("1000"),
        equity_start=Decimal("1001.00"),
        equity_end=Decimal("1002.50"),
        cash=Decimal("902.30"),
        locked=Decimal("100.10"),
        unrealized=Decimal("0.10"),
        funding_day=Decimal("1.90"),
        funding_total=Decimal("3.10"),
        fees_day=Decimal("0.35"),
        fees_total=Decimal("0.70"),
        slippage_day=Decimal("0.05"),
        opened=1,
        closed=1,
        closed_pnl=Decimal("-0.12"),
        open_positions=[
            OpenPositionLine(
                asset="BTC",
                strategy="spot/perp",
                venues="bybit",
                exposure=Decimal("50"),
                funding=Decimal("0.4"),
                fees=Decimal("0.07"),
            )
        ],
    )


def test_daily_report_contains_the_required_sections_only() -> None:
    message = format_daily_report(report_data())
    assert message.splitlines() == [
        "📊 Paper-звіт за 01.10.2026 (Київ) — candidate",
        "Результат дня: +1.50 USDT (+0.15%)",
        "За весь час: +2.50 USDT (+0.25%) з 30.09.2026",
        "Баланс: 1,002.50 USDT (вільно 902.30, у позиціях 100.10, нереалізовано +0.10)",
        "Funding: +1.90 за день | +3.10 усього",
        "Витрати за день: комісії 0.35, прослизання 0.05 | комісії усього 0.70",
        "Угоди: відкрито 1, закрито 1 (результат закритих -0.12)",
        "Відкриті позиції (1):",
        "• BTC spot/perp bybit — 50.00 USDT, funding +0.40, комісії 0.07",
        "Режим: лише симуляція, реальних ордерів немає",
    ]


def test_report_is_due_for_the_previous_kyiv_day() -> None:
    settings = Settings(
        telegram_enabled=True,
        telegram_bot_token=TOKEN,
        telegram_chat_id="123",
        telegram_timezone="Europe/Kyiv",
        telegram_report_hour=0,
        telegram_report_minute=5,
    )
    service = DailyReportService(
        settings,
        session_factory=None,  # type: ignore[arg-type]
        series_id="candidate-sim2",
        series_name="candidate",
        notifier=TelegramNotifier(TOKEN, "123"),
    )
    # 2 October 00:04 in Kyiv (UTC+3): not yet.
    assert service.due_date(datetime(2026, 10, 1, 21, 4, tzinfo=UTC)) is None
    assert service.due_date(datetime(2026, 10, 1, 21, 6, tzinfo=UTC)) == date(2026, 10, 1)
