"""Telegram notices: start/stop and one previous-day report per primary series.

Nothing else is sent to Telegram; comparisons with the baseline and detailed
attribution stay in the analytics API.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperPositionRecord,
    PaperRunnerSessionRecord,
    PaperSeriesRecord,
    PortfolioSnapshotRecord,
    TelegramDailyReportRecord,
)
from funding_arbitrage.monitoring.metrics import telegram_messages_total
from funding_arbitrage.notifications.telegram import TelegramNotificationError, TelegramNotifier
from funding_arbitrage.portfolio.ledger import LedgerEntryType
from funding_arbitrage.portfolio.portfolio import AccountSnapshot
from funding_arbitrage.services.series import PaperSeriesFile

logger = logging.getLogger(__name__)

_RETRY_AFTER_FAILURE = timedelta(minutes=10)
_ZERO = Decimal("0")


def resolve_timezone(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        logger.warning("telegram_timezone_data_missing", extra={"timezone": name})
        return UTC


def _money(value: Decimal, signed: bool = False) -> str:
    return f"{value:+,.2f}" if signed else f"{value:,.2f}"


def _percent(value: Decimal, base: Decimal) -> str:
    if base <= 0:
        return "0.00%"
    return f"{value / base * Decimal('100'):+.2f}%"


@dataclass
class OpenPositionLine:
    asset: str
    strategy: str
    venues: str
    exposure: Decimal
    funding: Decimal
    fees: Decimal


@dataclass
class DailyReportData:
    series_name: str
    series_id: str
    report_date: date
    started_on: date
    initial_balance: Decimal
    equity_start: Decimal
    equity_end: Decimal
    cash: Decimal
    locked: Decimal
    unrealized: Decimal
    funding_day: Decimal
    funding_total: Decimal
    fees_day: Decimal
    fees_total: Decimal
    slippage_day: Decimal
    opened: int
    closed: int
    closed_pnl: Decimal
    open_positions: list[OpenPositionLine] = field(default_factory=list)

    @property
    def day_pnl(self) -> Decimal:
        return self.equity_end - self.equity_start

    @property
    def total_pnl(self) -> Decimal:
        return self.equity_end - self.initial_balance


def format_daily_report(data: DailyReportData) -> str:
    lines = [
        f"📊 Paper-звіт за {data.report_date:%d.%m.%Y} (Київ) — {data.series_name}",
        f"Результат дня: {_money(data.day_pnl, True)} USDT "
        f"({_percent(data.day_pnl, data.equity_start)})",
        f"За весь час: {_money(data.total_pnl, True)} USDT "
        f"({_percent(data.total_pnl, data.initial_balance)}) з {data.started_on:%d.%m.%Y}",
        f"Баланс: {_money(data.equity_end)} USDT (вільно {_money(data.cash)}, "
        f"у позиціях {_money(data.locked)}, нереалізовано {_money(data.unrealized, True)})",
        f"Funding: {_money(data.funding_day, True)} за день | "
        f"{_money(data.funding_total, True)} усього",
        f"Витрати за день: комісії {_money(data.fees_day)}, прослизання "
        f"{_money(data.slippage_day)} | комісії усього {_money(data.fees_total)}",
        f"Угоди: відкрито {data.opened}, закрито {data.closed}"
        + (f" (результат закритих {_money(data.closed_pnl, True)})" if data.closed else ""),
    ]
    if data.open_positions:
        lines.append(f"Відкриті позиції ({len(data.open_positions)}):")
        lines.extend(
            f"• {item.asset} {item.strategy} {item.venues} — {_money(item.exposure)} USDT, "
            f"funding {_money(item.funding, True)}, комісії {_money(item.fees)}"
            for item in data.open_positions
        )
    else:
        lines.append("Відкритих позицій немає")
    lines.append("Режим: лише симуляція, реальних ордерів немає")
    return "\n".join(lines)


def format_start_message(
    series_file: PaperSeriesFile,
    snapshots: dict[str, AccountSnapshot],
    *,
    autotrade: bool,
    simulator_version: str,
    previous: PaperRunnerSessionRecord | None,
    timezone: tzinfo,
) -> str:
    lines = [
        "▶️ Paper-бот запущено (лише симуляція, реальних ордерів немає)",
        "Режим: " + ("paper-торгівля увімкнена" if autotrade else "лише спостереження, без угод"),
        f"Версія симулятора: {simulator_version}",
    ]
    for config in series_file.series:
        snapshot = snapshots.get(config.label)
        if snapshot is None:
            continue
        lines.append(
            f"• {config.name} ({config.label}): баланс {_money(snapshot.equity)} USDT, "
            f"відкритих позицій {snapshot.open_positions}"
        )
    if previous is not None:
        last = previous.stopped_at or previous.last_heartbeat_at or previous.started_at
        moment = last.astimezone(timezone).strftime("%d.%m %H:%M")
        if previous.clean_stop:
            lines.append(f"Попередній запуск завершено штатно ({moment})")
        else:
            lines.append(f"Попередній запуск завершився аварійно, останній цикл {moment}")
    return "\n".join(lines)


def format_stop_message(snapshots: dict[str, AccountSnapshot], *, primary: str) -> str:
    lines = ["⏹ Paper-бот зупинено"]
    for label in sorted(snapshots, key=lambda item: (item != primary, item)):
        snapshot = snapshots[label]
        lines.append(
            f"• {label}: баланс {_money(snapshot.equity)} USDT, "
            f"відкритих позицій {snapshot.open_positions}"
        )
    return "\n".join(lines)


async def build_daily_report_data(
    session: AsyncSession,
    *,
    series_id: str,
    series_name: str,
    report_date: date,
    timezone: tzinfo,
) -> DailyReportData | None:
    series = await session.get(PaperSeriesRecord, series_id)
    if series is None or series.initial_balance is None:
        return None
    start = datetime.combine(report_date, time.min, tzinfo=timezone).astimezone(UTC)
    end = datetime.combine(report_date + timedelta(days=1), time.min, tzinfo=timezone).astimezone(
        UTC
    )
    if series.created_at >= end:
        return None
    initial = Decimal(str(series.initial_balance))

    async def snapshot_before(moment: datetime) -> PortfolioSnapshotRecord | None:
        return await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.series_id == series_id,
                PortfolioSnapshotRecord.timestamp < moment,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
            .limit(1)
        )

    async def ledger_sum(entry_type: LedgerEntryType, lower: datetime | None) -> Decimal:
        statement = select(func.coalesce(func.sum(PaperLedgerEntryRecord.amount), 0)).where(
            PaperLedgerEntryRecord.series_id == series_id,
            PaperLedgerEntryRecord.entry_type == entry_type.value,
            PaperLedgerEntryRecord.timestamp < end,
        )
        if lower is not None:
            statement = statement.where(PaperLedgerEntryRecord.timestamp >= lower)
        return Decimal(str(await session.scalar(statement) or 0))

    first = await snapshot_before(start)
    last = await snapshot_before(end)
    equity_start = Decimal(str(first.equity)) if first is not None else initial
    equity_end = Decimal(str(last.equity)) if last is not None else initial
    slippage_day = await session.scalar(
        select(func.coalesce(func.sum(PaperFillRecord.slippage), 0)).where(
            PaperFillRecord.series_id == series_id,
            PaperFillRecord.timestamp >= start,
            PaperFillRecord.timestamp < end,
        )
    )
    opened = await session.scalar(
        select(func.count(PaperPositionRecord.id)).where(
            PaperPositionRecord.series_id == series_id,
            PaperPositionRecord.opened_at >= start,
            PaperPositionRecord.opened_at < end,
        )
    )
    closed_rows = (
        (
            await session.execute(
                select(PaperPositionRecord.booked_pnl).where(
                    PaperPositionRecord.series_id == series_id,
                    PaperPositionRecord.closed_at >= start,
                    PaperPositionRecord.closed_at < end,
                )
            )
        )
        .scalars()
        .all()
    )
    open_rows = (
        (
            await session.execute(
                select(PaperPositionRecord)
                .where(
                    PaperPositionRecord.series_id == series_id,
                    PaperPositionRecord.opened_at < end,
                    or_(
                        PaperPositionRecord.closed_at.is_(None),
                        PaperPositionRecord.closed_at >= end,
                    ),
                )
                .order_by(PaperPositionRecord.opened_at)
            )
        )
        .scalars()
        .all()
    )
    lines = []
    for row in open_rows:
        legs = row.payload.get("legs", []) if isinstance(row.payload, dict) else []
        venues = "↔".join(dict.fromkeys(str(leg.get("exchange", "")) for leg in legs))
        lines.append(
            OpenPositionLine(
                asset=row.asset,
                strategy=(row.strategy or "")
                .replace("cross_exchange_funding", "perp/perp")
                .replace("spot_perp", "spot/perp"),
                venues=venues,
                exposure=Decimal(str(row.exposure or 0)),
                funding=Decimal(str(row.funding_pnl or 0)),
                fees=Decimal(str(row.fees or 0)),
            )
        )
    return DailyReportData(
        series_name=series_name,
        series_id=series_id,
        report_date=report_date,
        started_on=series.created_at.astimezone(timezone).date(),
        initial_balance=initial,
        equity_start=equity_start,
        equity_end=equity_end,
        cash=Decimal(str(last.cash)) if last is not None else initial,
        locked=Decimal(str(last.locked_capital)) if last is not None else _ZERO,
        unrealized=Decimal(str(last.unrealized_pnl or 0)) if last is not None else _ZERO,
        funding_day=await ledger_sum(LedgerEntryType.FUNDING, start),
        funding_total=await ledger_sum(LedgerEntryType.FUNDING, None),
        fees_day=-await ledger_sum(LedgerEntryType.FEE, start),
        fees_total=-await ledger_sum(LedgerEntryType.FEE, None),
        slippage_day=Decimal(str(slippage_day or 0)),
        opened=int(opened or 0),
        closed=len(closed_rows),
        closed_pnl=sum((Decimal(str(value or 0)) for value in closed_rows), _ZERO),
        open_positions=lines,
    )


class DailyReportService:
    """Send one previous-calendar-day report for the primary series."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        series_id: str,
        series_name: str,
        notifier: TelegramNotifier,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.series_id = series_id
        self.series_name = series_name
        self.notifier = notifier
        self.timezone = resolve_timezone(settings.telegram_timezone)
        self._done: set[date] = set()
        self._retry_at: datetime | None = None

    async def close(self) -> None:
        await self.notifier.close()

    def due_date(self, now: datetime) -> date | None:
        local = now.astimezone(self.timezone)
        scheduled = local.replace(
            hour=self.settings.telegram_report_hour,
            minute=self.settings.telegram_report_minute,
            second=0,
            microsecond=0,
        )
        if local < scheduled:
            return None
        return local.date() - timedelta(days=1)

    async def check_and_send(self, now: datetime | None = None) -> bool:
        if not self.settings.telegram_enabled or not self.notifier.configured:
            return False
        current = now or datetime.now(UTC)
        report_date = self.due_date(current)
        if report_date is None or report_date in self._done:
            return False
        if self._retry_at is not None and current < self._retry_at:
            return False
        try:
            async with self.session_factory() as session:
                already = await session.scalar(
                    select(TelegramDailyReportRecord.id).where(
                        TelegramDailyReportRecord.series_id == self.series_id,
                        TelegramDailyReportRecord.report_date == report_date,
                        TelegramDailyReportRecord.status == "sent",
                    )
                )
                if already is not None:
                    self._done.add(report_date)
                    return False
                data = await build_daily_report_data(
                    session,
                    series_id=self.series_id,
                    series_name=self.series_name,
                    report_date=report_date,
                    timezone=self.timezone,
                )
        except Exception:
            logger.exception("telegram_daily_report_build_failed")
            self._retry_at = current + _RETRY_AFTER_FAILURE
            return False
        if data is None:
            # The series did not exist yet on that day.
            self._done.add(report_date)
            return False
        message = format_daily_report(data)
        try:
            await self.notifier.send_message(message)
        except TelegramNotificationError as exc:
            telegram_messages_total.labels("daily_report", "failed").inc()
            logger.warning(
                "telegram_daily_report_failed",
                extra={"report_date": str(report_date), "error": str(exc)},
            )
            self._retry_at = current + _RETRY_AFTER_FAILURE
            return False
        telegram_messages_total.labels("daily_report", "sent").inc()
        self._retry_at = None
        self._done.add(report_date)
        try:
            async with self.session_factory() as session:
                await session.execute(
                    pg_insert(TelegramDailyReportRecord)
                    .values(
                        series_id=self.series_id,
                        report_date=report_date,
                        status="sent",
                        sent_at=datetime.now(UTC),
                        message=message,
                    )
                    .on_conflict_do_nothing(constraint="uq_telegram_series_report_date")
                )
                await session.commit()
        except Exception:
            # The message went out; the in-memory guard prevents a resend until restart.
            logger.exception("telegram_daily_report_ledger_failed")
        return True
