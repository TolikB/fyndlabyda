from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import (
    LiveAccountSnapshotRecord,
    LiveDailyReportRecord,
    LiveFundingPaymentRecord,
    LivePositionRecord,
)
from funding_arbitrage.services.live_daily_report import (
    LiveDailyReportService,
    LiveReportDataUnavailable,
)
from tests.test_live_executor import live_settings


@pytest.mark.asyncio
async def test_live_daily_report_shows_day_and_total_actual_equity(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    async with factory() as session:
        for venue in ("bybit", "gate"):
            session.add_all(
                [
                    LiveAccountSnapshotRecord(
                        timestamp=datetime(2026, 8, 10, 20, 0, tzinfo=UTC),
                        exchange=venue,
                        equity_usd=Decimal("100"),
                        free_collateral_usd=Decimal("100"),
                        balances={},
                    ),
                    LiveAccountSnapshotRecord(
                        timestamp=datetime(2026, 8, 11, 20, 59, tzinfo=UTC),
                        exchange=venue,
                        equity_usd=Decimal("102"),
                        free_collateral_usd=Decimal("102"),
                        balances={},
                    ),
                ]
            )
        session.add(
            LiveFundingPaymentRecord(
                exchange="bybit",
                external_id="funding-1",
                exchange_symbol="BTCUSDT",
                amount=Decimal("1.50"),
                currency="USDT",
                timestamp=datetime(2026, 8, 11, 12, 0, tzinfo=UTC),
            )
        )
        session.add(
            LivePositionRecord(
                position_id="manual-position",
                intent_id="manual-intent",
                opportunity_id="manual-opportunity",
                opportunity_key="manual-key",
                strategy="cross_exchange_funding",
                asset="BTC",
                state="MANUAL_INTERVENTION",
                capital_per_leg=Decimal("100"),
                opened_at=None,
                closed_at=None,
                failure_reason="unknown_order_state",
                payload={},
            )
        )
        await session.commit()

    service = LiveDailyReportService(settings, factory)
    async with factory() as session:
        message = await service._build_message(session, date(2026, 8, 11))
    await service.close()

    assert "Net PnL after venue costs: +$4.00" in message
    assert "Net PnL since live tracking: +$4.00" in message
    assert "Current equity: $204.00" in message
    assert "Active positions: 1" in message
    assert message.count("Actual funding payments: +$1.50") == 2


@pytest.mark.asyncio
async def test_live_daily_report_refuses_partial_venue_equity(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    async with factory() as session:
        session.add(
            LiveAccountSnapshotRecord(
                timestamp=datetime(2026, 8, 11, 20, 0, tzinfo=UTC),
                exchange="bybit",
                equity_usd=Decimal("100"),
                free_collateral_usd=Decimal("100"),
                balances={},
            )
        )
        await session.commit()

    service = LiveDailyReportService(settings, factory)
    async with factory() as session:
        with pytest.raises(LiveReportDataUnavailable, match="gate"):
            await service._build_message(session, date(2026, 8, 11))
    await service.close()

class FakeNotifier:
    configured = True

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[str] = []
        self.closed = 0

    async def send_message(self, message: str) -> None:
        if self.fail:
            raise TimeoutError("synthetic telegram timeout")
        self.messages.append(message)

    async def close(self) -> None:
        self.closed += 1


async def _service_with_fake_notifier(
    settings: object,
    factory: async_sessionmaker[AsyncSession],
    *,
    fail: bool = False,
) -> tuple[LiveDailyReportService, FakeNotifier]:
    service = LiveDailyReportService(settings, factory)  # type: ignore[arg-type]
    await service.notifier.close()
    notifier = FakeNotifier(fail=fail)
    service.notifier = notifier  # type: ignore[assignment]
    return service, notifier


@pytest.mark.asyncio
async def test_live_report_alerts_are_deduplicated_and_respect_configuration(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    service, notifier = await _service_with_fake_notifier(settings, factory)

    assert await service.send_safety_alert("reconciliation") is True
    assert await service.send_safety_alert("reconciliation") is False
    assert len(notifier.messages) == 1
    assert "LIVE TRADING PAUSED" in notifier.messages[0]

    disabled = settings.model_copy(update={"telegram_enabled": False})
    disabled_service, disabled_notifier = await _service_with_fake_notifier(
        disabled,
        factory,
    )
    assert await disabled_service.send_safety_alert("disabled") is False
    assert disabled_notifier.messages == []

    await service.close()
    await disabled_service.close()
    assert notifier.closed == 1
    assert disabled_notifier.closed == 1


@pytest.mark.asyncio
async def test_check_and_send_persists_success_deduplicates_and_records_failure(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path).model_copy(
        update={
            "telegram_timezone": "UTC",
            "telegram_report_hour": 12,
            "telegram_report_minute": 0,
        }
    )
    async with factory() as session:
        for venue in ("bybit", "gate"):
            session.add_all(
                [
                    LiveAccountSnapshotRecord(
                        timestamp=datetime(2026, 8, 10, 23, tzinfo=UTC),
                        exchange=venue,
                        equity_usd=Decimal("100"),
                        free_collateral_usd=Decimal("100"),
                        balances={},
                    ),
                    LiveAccountSnapshotRecord(
                        timestamp=datetime(2026, 8, 11, 23, tzinfo=UTC),
                        exchange=venue,
                        equity_usd=Decimal("101"),
                        free_collateral_usd=Decimal("101"),
                        balances={},
                    ),
                ]
            )
        await session.commit()

    service, notifier = await _service_with_fake_notifier(settings, factory)
    assert (
        await service.check_and_send(datetime(2026, 8, 12, 11, 59, tzinfo=UTC))
        is False
    )
    report_time = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    assert await service.check_and_send(report_time) is True
    assert await service.check_and_send(report_time) is False
    assert len(notifier.messages) == 1

    failing_service, failing_notifier = await _service_with_fake_notifier(
        settings,
        factory,
        fail=True,
    )
    failure_time = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
    assert await failing_service.check_and_send(failure_time) is False
    async with factory() as session:
        failed = await session.scalar(
            select(LiveDailyReportRecord).where(
                LiveDailyReportRecord.report_date == date(2026, 8, 12)
            )
        )
        assert failed is not None
        assert failed.status == "failed"
        assert failed.error == "TimeoutError"

    failing_notifier.fail = False
    assert await failing_service.check_and_send(failure_time) is True
    async with factory() as session:
        recovered = await session.scalar(
            select(LiveDailyReportRecord).where(
                LiveDailyReportRecord.report_date == date(2026, 8, 12)
            )
        )
        assert recovered is not None
        assert recovered.status == "sent"
        assert recovered.error is None

    await service.close()
    await failing_service.close()


def test_invalid_report_timezone_falls_back_to_utc(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path).model_copy(
        update={"telegram_timezone": "Invalid/Timezone"}
    )

    service = LiveDailyReportService(settings, factory)

    assert service.timezone is UTC
