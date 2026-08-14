from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import (
    LiveAccountSnapshotRecord,
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
