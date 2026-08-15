from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
)
from funding_arbitrage.services.daily_report import DailyReportService


@pytest.mark.asyncio
async def test_daily_report_lists_every_venue_that_has_positions(database: object) -> None:
    _, session_factory = database
    venues = (
        "bybit",
        "gate",
        "okx",
        "binance",
        "hyperliquid",
        "mexc",
        "kucoin",
        "htx",
    )
    simulation_version = "venue-report-v1"
    start = datetime(2026, 8, 14, tzinfo=UTC)
    end = start + timedelta(days=1)
    async with session_factory() as session:
        for pair_index in range(0, len(venues), 2):
            position_id = f"position-{pair_index // 2}"
            pair = venues[pair_index : pair_index + 2]
            session.add(
                PaperPositionRecord(
                    position_id=position_id,
                    opportunity_id=f"opportunity-{pair_index // 2}",
                    state="OPEN",
                    asset="BTC",
                    capital=Decimal("100"),
                    simulation_version=simulation_version,
                    opened_at=start + timedelta(hours=1),
                    closed_at=None,
                    payload={},
                )
            )
            for venue in pair:
                session.add(
                    PaperFillRecord(
                        fill_id=f"fill-{venue}",
                        position_id=position_id,
                        exchange=venue,
                        symbol="BTCUSDT",
                        instrument_type="perpetual",
                        side="short",
                        filled_quantity=Decimal("0.001"),
                        price=Decimal("100000"),
                        fee=Decimal("0.10"),
                        slippage=Decimal("0.01"),
                        status="FILLED",
                        timestamp=start + timedelta(hours=1),
                        payload={},
                    )
                )
                session.add(
                    PaperFundingPaymentRecord(
                        position_id=position_id,
                        exchange=venue,
                        symbol="BTCUSDT",
                        funding_timestamp=start + timedelta(hours=8),
                        funding_rate=Decimal("0.0025"),
                        notional=Decimal("100"),
                        pnl=Decimal("0.25"),
                    )
                )
        await session.commit()

        service = DailyReportService(
            Settings(_env_file=None, PAPER_VENUES=",".join(venues)),
            session_factory,
        )
        reports = await service._load_venue_reports(
            session,
            simulation_version=simulation_version,
            start=start,
            end=end,
        )
        await service.close()

    assert tuple(report.exchange for report in reports) == venues
    for report in reports:
        assert report.day_positions == 1
        assert report.open_positions == 1
        assert report.total_positions == 1
        assert report.day_fills == 1
        assert report.total_fills == 1
        assert report.day_funding == Decimal("0.25")
        assert report.day_costs.quantize(Decimal("0.01")) == Decimal("0.11")
        assert report.day_funding_after_costs.quantize(Decimal("0.01")) == Decimal(
            "0.14"
        )

    venue_section = "\n".join(DailyReportService._venue_lines(reports))
    for venue in venues:
        assert venue.upper() in venue_section
    assert venue_section.count("FC +$0.14") == len(venues) * 2
    assert len(venue_section) < 2000
