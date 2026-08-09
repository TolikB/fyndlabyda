"""Durable daily paper-PnL report generation and Telegram scheduling."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    OpportunityRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
    TelegramDailyReportRecord,
)
from funding_arbitrage.notifications.telegram import TelegramNotifier

logger = logging.getLogger(__name__)


class DailyReportService:
    """Send one previous-calendar-day paper report after configured local midnight."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.timezone: tzinfo
        try:
            self.timezone = ZoneInfo(settings.telegram_timezone)
        except ZoneInfoNotFoundError:
            logger.warning(
                "telegram_timezone_data_missing", extra={"timezone": settings.telegram_timezone}
            )
            self.timezone = UTC
        self.notifier = TelegramNotifier(
            settings.telegram_bot_token,
            settings.telegram_chat_id,
            settings.telegram_api_base_url,
            settings.request_timeout_seconds,
        )

    async def close(self) -> None:
        await self.notifier.close()

    async def check_and_send(self, now: datetime | None = None) -> bool:
        if not self.settings.telegram_enabled or not self.notifier.configured:
            return False
        current = (now or datetime.now(UTC)).astimezone(self.timezone)
        scheduled = current.replace(
            hour=self.settings.telegram_report_hour,
            minute=self.settings.telegram_report_minute,
            second=0,
            microsecond=0,
        )
        if current < scheduled:
            return False
        report_date = current.date() - timedelta(days=1)
        async with self.session_factory() as session:
            already_sent = await session.scalar(
                select(TelegramDailyReportRecord).where(
                    TelegramDailyReportRecord.report_date == report_date,
                    TelegramDailyReportRecord.status == "sent",
                )
            )
            if already_sent is not None:
                return False
            message = await self._build_message(session, report_date)
            try:
                await self.notifier.send_message(message)
            except Exception:
                logger.exception(
                    "telegram_daily_report_failed", extra={"report_date": str(report_date)}
                )
                return False
            session.add(
                TelegramDailyReportRecord(
                    report_date=report_date,
                    status="sent",
                    sent_at=datetime.now(UTC),
                    message=message,
                )
            )
            await session.commit()
            return True

    async def _build_message(self, session: AsyncSession, report_date: date) -> str:
        start_local = datetime.combine(report_date, time.min, tzinfo=self.timezone)
        start = start_local.astimezone(UTC)
        end = (start_local + timedelta(days=1)).astimezone(UTC)
        previous = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.timestamp < start)
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        latest = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.timestamp < end)
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        funding = await session.scalar(
            select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0)).where(
                PaperFundingPaymentRecord.funding_timestamp >= start,
                PaperFundingPaymentRecord.funding_timestamp < end,
            )
        )
        fees = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.fee), 0)).where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
            )
        )
        slippage = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.slippage), 0)).where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
            )
        )
        fills = await session.scalar(
            select(func.count(PaperFillRecord.id)).where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
            )
        )
        opened = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.opened_at >= start,
                PaperPositionRecord.opened_at < end,
            )
        )
        closed = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.closed_at >= start,
                PaperPositionRecord.closed_at < end,
            )
        )
        opportunities = await session.scalar(
            select(func.count(OpportunityRecord.id)).where(
                OpportunityRecord.created_at >= start,
                OpportunityRecord.created_at < end,
            )
        )
        equity = latest.equity if latest is not None else self.settings.paper_initial_balance_usd
        previous_equity = (
            previous.equity if previous is not None else self.settings.paper_initial_balance_usd
        )
        equity_delta = equity - previous_equity
        total_pnl = latest.total_pnl if latest is not None else Decimal("0")
        return "\n".join(
            [
                f"📊 Paper Arbitrage — {report_date.isoformat()}",
                f"Equity: ${equity:.2f} ({equity_delta:+.2f} today)",
                f"Total PnL: ${total_pnl:.2f}",
                f"Funding PnL: ${Decimal(str(funding or 0)):.2f}",
                f"Fees: ${Decimal(str(fees or 0)):.2f}",
                f"Slippage: ${Decimal(str(slippage or 0)):.2f}",
                (
                    f"Fills: {int(fills or 0)} | Opened: {int(opened or 0)} "
                    f"| Closed: {int(closed or 0)}"
                ),
                f"Opportunities observed: {int(opportunities or 0)}",
                "Mode: PAPER ONLY — no live orders",
            ]
        )
