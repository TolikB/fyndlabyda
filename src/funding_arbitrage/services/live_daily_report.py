"""Actual account-equity daily and all-time reporting for live mode."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    LiveAccountSnapshotRecord,
    LiveDailyReportRecord,
    LiveFundingPaymentRecord,
    LivePositionRecord,
)
from funding_arbitrage.notifications.telegram import TelegramNotifier

logger = logging.getLogger(__name__)


class LiveReportDataUnavailable(RuntimeError):
    """Raised instead of publishing a misleading partial-equity report."""


def _signed_usd(value: Decimal) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):.2f}"


class LiveDailyReportService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        try:
            self.timezone: tzinfo = ZoneInfo(settings.telegram_timezone)
        except ZoneInfoNotFoundError:
            self.timezone = UTC
        self.notifier = TelegramNotifier(
            settings.telegram_bot_token.get_secret_value(),
            settings.telegram_chat_id,
            settings.telegram_api_base_url,
            settings.request_timeout_seconds,
        )
        self._alerted_reasons: set[str] = set()

    async def close(self) -> None:
        await self.notifier.close()

    async def send_safety_alert(self, reason: str) -> bool:
        if (
            reason in self._alerted_reasons
            or not self.settings.telegram_enabled
            or not self.notifier.configured
        ):
            return False
        await self.notifier.send_message(
            "🚨 LIVE TRADING PAUSED\n"
            f"Reason: {reason}\n"
            "New entries are blocked. Check exchange state and reconciliation manually."
        )
        self._alerted_reasons.add(reason)
        return True

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
            record = await session.scalar(
                select(LiveDailyReportRecord).where(
                    LiveDailyReportRecord.report_date == report_date,
                )
            )
            if record is not None and record.status == "sent":
                return False
            message = ""
            try:
                message = await self._build_message(session, report_date)
                await self.notifier.send_message(message)
            except Exception as exc:
                logger.exception("live_daily_report_failed")
                stored_message = message or (
                    f"Live report for {report_date.isoformat()} was not sent because "
                    "complete equity evidence was unavailable."
                )
                if record is None:
                    record = LiveDailyReportRecord(
                        report_date=report_date,
                        status="failed",
                        sent_at=None,
                        message=stored_message,
                        error=type(exc).__name__,
                    )
                    session.add(record)
                else:
                    record.status = "failed"
                    record.message = stored_message
                    record.error = type(exc).__name__
                await session.commit()
                return False
            if record is None:
                record = LiveDailyReportRecord(
                    report_date=report_date,
                    status="sent",
                    sent_at=datetime.now(UTC),
                    message=message,
                    error=None,
                )
                session.add(record)
            else:
                record.status = "sent"
                record.sent_at = datetime.now(UTC)
                record.message = message
                record.error = None
            await session.commit()
            return True

    async def _build_message(self, session: AsyncSession, report_date: date) -> str:
        start_local = datetime.combine(report_date, time.min, tzinfo=self.timezone)
        start = start_local.astimezone(UTC)
        end = (start_local + timedelta(days=1)).astimezone(UTC)
        start_equity = await self._equity_at_start(session, start, end)
        end_equity = await self._equity_before(session, end)
        first_equity = await self._first_equity(session)
        opened = int(
            await session.scalar(
                select(func.count(LivePositionRecord.id)).where(
                    LivePositionRecord.opened_at >= start,
                    LivePositionRecord.opened_at < end,
                )
            )
            or 0
        )
        closed = int(
            await session.scalar(
                select(func.count(LivePositionRecord.id)).where(
                    LivePositionRecord.closed_at >= start,
                    LivePositionRecord.closed_at < end,
                )
            )
            or 0
        )
        active = int(
            await session.scalar(
                select(func.count(LivePositionRecord.id)).where(
                    LivePositionRecord.state.in_(
                        ["OPEN", "OPENING", "CLOSING", "MANUAL_INTERVENTION"]
                    )
                )
            )
            or 0
        )
        day_funding = Decimal(
            str(
                await session.scalar(
                    select(func.coalesce(func.sum(LiveFundingPaymentRecord.amount), 0)).where(
                        LiveFundingPaymentRecord.timestamp >= start,
                        LiveFundingPaymentRecord.timestamp < end,
                        LiveFundingPaymentRecord.currency.in_(["USD", "USDT", "USDC"]),
                    )
                )
                or 0
            )
        )
        total_funding = Decimal(
            str(
                await session.scalar(
                    select(func.coalesce(func.sum(LiveFundingPaymentRecord.amount), 0)).where(
                        LiveFundingPaymentRecord.currency.in_(["USD", "USDT", "USDC"])
                    )
                )
                or 0
            )
        )
        day_pnl = end_equity - start_equity
        total_pnl = end_equity - first_equity
        total_return_percent = (
            total_pnl / first_equity * Decimal("100")
            if first_equity != 0
            else Decimal("0")
        )
        return "\n".join(
            [
                f"📊 Звіт про торгівлю · {report_date.isoformat()}",
                "",
                "ЗА ДЕНЬ",
                f"Результат: {_signed_usd(day_pnl)}",
                f"Фандінг: {_signed_usd(day_funding)}",
                f"Угоди: відкрито {opened} · закрито {closed}",
                "",
                "ЗАГАЛОМ",
                f"Баланс: ${end_equity:.2f}",
                f"Результат: {_signed_usd(total_pnl)} ({total_return_percent:+.4f}%)",
                f"Фандінг: {_signed_usd(total_funding)}",
                f"Відкриті позиції: {active}",
            ]
        )

    async def _equity_before(self, session: AsyncSession, before: datetime) -> Decimal:
        total = Decimal("0")
        missing: list[str] = []
        for venue in self.settings.live_venue_values:
            row = await session.scalar(
                select(LiveAccountSnapshotRecord)
                .where(
                    LiveAccountSnapshotRecord.exchange == venue,
                    LiveAccountSnapshotRecord.timestamp < before,
                )
                .order_by(LiveAccountSnapshotRecord.timestamp.desc())
            )
            if row is not None:
                total += row.equity_usd
            else:
                missing.append(venue)
        self._require_complete_equity(missing, "period_end")
        return total

    async def _equity_at_start(
        self, session: AsyncSession, start: datetime, end: datetime
    ) -> Decimal:
        total = Decimal("0")
        missing: list[str] = []
        for venue in self.settings.live_venue_values:
            row = await session.scalar(
                select(LiveAccountSnapshotRecord)
                .where(
                    LiveAccountSnapshotRecord.exchange == venue,
                    LiveAccountSnapshotRecord.timestamp < start,
                )
                .order_by(LiveAccountSnapshotRecord.timestamp.desc())
            )
            if row is None:
                row = await session.scalar(
                    select(LiveAccountSnapshotRecord)
                    .where(
                        LiveAccountSnapshotRecord.exchange == venue,
                        LiveAccountSnapshotRecord.timestamp >= start,
                        LiveAccountSnapshotRecord.timestamp < end,
                    )
                    .order_by(LiveAccountSnapshotRecord.timestamp.asc())
                )
            if row is not None:
                total += row.equity_usd
            else:
                missing.append(venue)
        self._require_complete_equity(missing, "period_start")
        return total

    async def _first_equity(self, session: AsyncSession) -> Decimal:
        total = Decimal("0")
        missing: list[str] = []
        for venue in self.settings.live_venue_values:
            row = await session.scalar(
                select(LiveAccountSnapshotRecord)
                .where(LiveAccountSnapshotRecord.exchange == venue)
                .order_by(LiveAccountSnapshotRecord.timestamp.asc())
            )
            if row is not None:
                total += row.equity_usd
            else:
                missing.append(venue)
        self._require_complete_equity(missing, "tracking_start")
        return total

    @staticmethod
    def _require_complete_equity(missing: list[str], boundary: str) -> None:
        if missing:
            raise LiveReportDataUnavailable(
                f"missing {boundary} equity for: {','.join(sorted(missing))}"
            )
