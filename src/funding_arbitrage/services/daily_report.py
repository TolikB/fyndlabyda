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


def _signed_usd(value: Decimal) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):.2f}"


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
            .where(
                PortfolioSnapshotRecord.timestamp < start,
                PortfolioSnapshotRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        latest = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.timestamp < end,
                PortfolioSnapshotRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        first = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.asc())
        )
        funding = await session.scalar(
            select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFundingPaymentRecord.position_id,
            )
            .where(
                PaperFundingPaymentRecord.funding_timestamp >= start,
                PaperFundingPaymentRecord.funding_timestamp < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        fees = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.fee), 0))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFillRecord.position_id,
            )
            .where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        slippage = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.slippage), 0))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFillRecord.position_id,
            )
            .where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        total_slippage = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.slippage), 0))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFillRecord.position_id,
            )
            .where(
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        fills = await session.scalar(
            select(func.count(PaperFillRecord.id))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFillRecord.position_id,
            )
            .where(
                PaperFillRecord.timestamp >= start,
                PaperFillRecord.timestamp < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        opened = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.opened_at >= start,
                PaperPositionRecord.opened_at < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
            )
        )
        closed = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.closed_at >= start,
                PaperPositionRecord.closed_at < end,
                PaperPositionRecord.simulation_version
                == self.settings.paper_simulation_version,
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
        total_funding = latest.funding_pnl if latest is not None else Decimal("0")
        total_fees = latest.fees if latest is not None else Decimal("0")
        total_return_percent = (
            (equity / self.settings.paper_initial_balance_usd - Decimal("1"))
            * Decimal("100")
            if self.settings.paper_initial_balance_usd > 0
            else Decimal("0")
        )
        first_seen = first.timestamp.astimezone(self.timezone) if first is not None else None
        no_trades_note = (
            "No eligible paper trades during the day; equity was unchanged."
            if int(fills or 0) == 0 and equity_delta == 0
            else None
        )
        return "\n".join(
            [
                f"📊 Paper Arbitrage — {report_date.isoformat()}",
                (
                    f"Portfolio: candidate | "
                    f"simulator {self.settings.paper_simulation_version}"
                ),
                "",
                "DAY RESULT",
                f"Net PnL: {_signed_usd(equity_delta)}",
                f"Funding: {_signed_usd(Decimal(str(funding or 0)))}",
                f"Fees: -${Decimal(str(fees or 0)):.2f}",
                f"Slippage: -${Decimal(str(slippage or 0)):.2f}",
                (
                    f"Fills: {int(fills or 0)} | Opened: {int(opened or 0)} "
                    f"| Closed: {int(closed or 0)}"
                ),
                f"Opportunities observed: {int(opportunities or 0)}",
                *([no_trades_note] if no_trades_note is not None else []),
                "",
                "TOTAL — CURRENT SIMULATOR",
                f"Equity: ${equity:.2f}",
                f"Net PnL: {_signed_usd(total_pnl)}",
                f"Return: {total_return_percent:+.4f}%",
                f"Funding: {_signed_usd(total_funding)}",
                f"Fees: -${total_fees:.2f}",
                f"Slippage: -${Decimal(str(total_slippage or 0)):.2f}",
                (
                    f"Tracking since: {first_seen.isoformat(timespec='seconds')}"
                    if first_seen is not None
                    else "Tracking since: no snapshots yet"
                ),
                "Legacy/pre-fix simulator data is excluded.",
                "Mode: PAPER ONLY — no live orders",
            ]
        )
