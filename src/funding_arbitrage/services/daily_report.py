"""Durable daily paper-PnL report generation and Telegram scheduling."""

from __future__ import annotations

import logging
from dataclasses import dataclass
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
    PaperRuntimeIncidentRecord,
    PortfolioSnapshotRecord,
    TelegramDailyReportRecord,
)
from funding_arbitrage.notifications.telegram import TelegramNotifier

logger = logging.getLogger(__name__)


def _local_day_utc_bounds(
    report_date: date, timezone: tzinfo
) -> tuple[datetime, datetime]:
    """Return UTC bounds for one local calendar day, including DST changes."""

    start_local = datetime.combine(report_date, time.min, tzinfo=timezone)
    end_local = datetime.combine(
        report_date + timedelta(days=1), time.min, tzinfo=timezone
    )
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


@dataclass(frozen=True)
class _PortfolioReport:
    label: str
    simulation_version: str
    equity: Decimal
    previous_equity: Decimal
    total_pnl: Decimal
    total_funding: Decimal
    total_fees: Decimal
    day_funding: Decimal
    day_fees: Decimal
    day_slippage: Decimal
    total_slippage: Decimal
    fills: int
    opened: int
    closed: int
    eligible_signals: int
    confirmed_signals: int
    snapshots: int
    cycle_failures: int
    process_starts: int
    had_prior_snapshot: bool
    first_seen: datetime | None


def _signed_usd(value: Decimal) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):.2f}"


def _no_fill_note(
    *,
    fills: int,
    equity_delta: Decimal,
    eligible_signals: int,
    confirmed_signals: int,
    snapshot_count: int,
    cycle_failures: int,
) -> str | None:
    if fills != 0 or equity_delta != 0:
        return None
    if snapshot_count == 0 or cycle_failures > 0:
        return (
            "No paper fills were recorded, but runtime evidence needs attention; "
            "unchanged equity does not prove that the market had no edge."
        )
    if confirmed_signals > 0:
        return (
            f"{confirmed_signals} confirmed signal(s) were observed, but no paper "
            "fill was produced; inspect risk and execution gates."
        )
    if eligible_signals > 0:
        return (
            f"{eligible_signals} eligible signal(s) were observed, but none reached "
            "confirmed state; no position was opened."
        )
    return "No eligible paper signals were observed; equity was unchanged."


def _runner_state(
    *,
    snapshot_count: int,
    cycle_failures: int,
    process_starts: int,
    had_prior_snapshot: bool,
) -> str:
    if snapshot_count == 0 or cycle_failures > 0:
        return "ATTENTION"
    if process_starts > 0:
        return "RESTARTED" if had_prior_snapshot else "STARTED"
    return "OK"


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
            settings.telegram_bot_token.get_secret_value(),
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
        start, end = _local_day_utc_bounds(report_date, self.timezone)
        autotrade_start = self.settings.paper_autotrade_start_utc
        signal_start = max(start, min(autotrade_start, end)) if autotrade_start else start
        candidate = await self._load_portfolio_report(
            session,
            label="candidate",
            simulation_version=self.settings.paper_simulation_version,
            start=start,
            end=end,
            signal_start=signal_start,
        )
        reports = [candidate]
        if self.settings.paper_comparison_enabled:
            reports.append(
                await self._load_portfolio_report(
                    session,
                    label="baseline",
                    simulation_version=self.settings.paper_baseline_simulation_version,
                    start=start,
                    end=end,
                    signal_start=signal_start,
                    signal_counts=(
                        candidate.eligible_signals,
                        candidate.confirmed_signals,
                    ),
                )
            )
        lines = [f"📊 Paper Arbitrage — {report_date.isoformat()}"]
        for index, report in enumerate(reports):
            if index:
                lines.append("")
            lines.extend(self._portfolio_lines(report))
        if len(reports) > 1:
            lines.extend(
                [
                    "",
                    (
                        "Candidate and baseline are separate virtual portfolios; "
                        "do not add their PnL together."
                    ),
                ]
            )
        lines.extend(
            [
                "Legacy/pre-fix simulator data is excluded.",
                "Mode: PAPER ONLY — no live orders",
            ]
        )
        return "\n".join(lines)

    async def _load_portfolio_report(
        self,
        session: AsyncSession,
        *,
        label: str,
        simulation_version: str,
        start: datetime,
        end: datetime,
        signal_start: datetime,
        signal_counts: tuple[int, int] | None = None,
    ) -> _PortfolioReport:
        previous = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.timestamp < start,
                PortfolioSnapshotRecord.simulation_version == simulation_version,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        latest = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.timestamp < end,
                PortfolioSnapshotRecord.simulation_version == simulation_version,
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        first = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.simulation_version == simulation_version)
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
                PaperPositionRecord.simulation_version == simulation_version,
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
                PaperPositionRecord.simulation_version == simulation_version,
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
                PaperPositionRecord.simulation_version == simulation_version,
            )
        )
        total_slippage = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.slippage), 0))
            .join(
                PaperPositionRecord,
                PaperPositionRecord.position_id == PaperFillRecord.position_id,
            )
            .where(PaperPositionRecord.simulation_version == simulation_version)
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
                PaperPositionRecord.simulation_version == simulation_version,
            )
        )
        opened = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.opened_at >= start,
                PaperPositionRecord.opened_at < end,
                PaperPositionRecord.simulation_version == simulation_version,
            )
        )
        closed = await session.scalar(
            select(func.count(PaperPositionRecord.id)).where(
                PaperPositionRecord.closed_at >= start,
                PaperPositionRecord.closed_at < end,
                PaperPositionRecord.simulation_version == simulation_version,
            )
        )
        if signal_counts is None:
            opportunities = await session.scalar(
                select(func.count(OpportunityRecord.id)).where(
                    OpportunityRecord.created_at >= signal_start,
                    OpportunityRecord.created_at < end,
                )
            )
            confirmed = await session.scalar(
                select(func.count(OpportunityRecord.id)).where(
                    OpportunityRecord.created_at >= signal_start,
                    OpportunityRecord.created_at < end,
                    OpportunityRecord.status == "confirmed",
                )
            )
            eligible_count = int(opportunities or 0)
            confirmed_count = int(confirmed or 0)
        else:
            eligible_count, confirmed_count = signal_counts
        snapshots = await session.scalar(
            select(func.count(PortfolioSnapshotRecord.id)).where(
                PortfolioSnapshotRecord.timestamp >= start,
                PortfolioSnapshotRecord.timestamp < end,
                PortfolioSnapshotRecord.simulation_version == simulation_version,
            )
        )
        cycle_failures = await session.scalar(
            select(func.count(PaperRuntimeIncidentRecord.id)).where(
                PaperRuntimeIncidentRecord.occurred_at >= start,
                PaperRuntimeIncidentRecord.occurred_at < end,
                PaperRuntimeIncidentRecord.simulation_version == simulation_version,
                PaperRuntimeIncidentRecord.category != "process_start",
            )
        )
        process_starts = await session.scalar(
            select(func.count(PaperRuntimeIncidentRecord.id)).where(
                PaperRuntimeIncidentRecord.occurred_at >= start,
                PaperRuntimeIncidentRecord.occurred_at < end,
                PaperRuntimeIncidentRecord.simulation_version == simulation_version,
                PaperRuntimeIncidentRecord.category == "process_start",
            )
        )
        equity = latest.equity if latest is not None else self.settings.paper_initial_balance_usd
        previous_equity = (
            previous.equity if previous is not None else self.settings.paper_initial_balance_usd
        )
        total_pnl = latest.total_pnl if latest is not None else Decimal("0")
        total_funding = latest.funding_pnl if latest is not None else Decimal("0")
        total_fees = latest.fees if latest is not None else Decimal("0")
        first_seen = first.timestamp.astimezone(self.timezone) if first is not None else None
        return _PortfolioReport(
            label=label,
            simulation_version=simulation_version,
            equity=equity,
            previous_equity=previous_equity,
            total_pnl=total_pnl,
            total_funding=total_funding,
            total_fees=total_fees,
            day_funding=Decimal(str(funding or 0)),
            day_fees=Decimal(str(fees or 0)),
            day_slippage=Decimal(str(slippage or 0)),
            total_slippage=Decimal(str(total_slippage or 0)),
            fills=int(fills or 0),
            opened=int(opened or 0),
            closed=int(closed or 0),
            eligible_signals=eligible_count,
            confirmed_signals=confirmed_count,
            snapshots=int(snapshots or 0),
            cycle_failures=int(cycle_failures or 0),
            process_starts=int(process_starts or 0),
            had_prior_snapshot=previous is not None,
            first_seen=first_seen,
        )

    def _portfolio_lines(self, report: _PortfolioReport) -> list[str]:
        equity_delta = report.equity - report.previous_equity
        total_return_percent = (
            (report.equity / self.settings.paper_initial_balance_usd - Decimal("1"))
            * Decimal("100")
            if self.settings.paper_initial_balance_usd > 0
            else Decimal("0")
        )
        no_trades_note = _no_fill_note(
            fills=report.fills,
            equity_delta=equity_delta,
            eligible_signals=report.eligible_signals,
            confirmed_signals=report.confirmed_signals,
            snapshot_count=report.snapshots,
            cycle_failures=report.cycle_failures,
        )
        runner_state = _runner_state(
            snapshot_count=report.snapshots,
            cycle_failures=report.cycle_failures,
            process_starts=report.process_starts,
            had_prior_snapshot=report.had_prior_snapshot,
        )
        return [
            (
                f"Portfolio: {report.label} | "
                f"simulator {report.simulation_version}"
            ),
            "",
            "DAY RESULT",
            f"Net PnL: {_signed_usd(equity_delta)}",
            f"Funding: {_signed_usd(report.day_funding)}",
            f"Fees: -${report.day_fees:.2f}",
            f"Slippage: -${report.day_slippage:.2f}",
            (
                f"Fills: {report.fills} | Opened: {report.opened} "
                f"| Closed: {report.closed}"
            ),
            (
                f"Unique eligible signals: {report.eligible_signals} | "
                f"Confirmed: {report.confirmed_signals}"
            ),
            (
                f"Runner: {runner_state} | snapshots: {report.snapshots} | "
                f"cycle failures: {report.cycle_failures} | "
                f"process starts: {report.process_starts}"
            ),
            *([no_trades_note] if no_trades_note is not None else []),
            "",
            "TOTAL — CURRENT SIMULATOR",
            f"Equity: ${report.equity:.2f}",
            f"Net PnL: {_signed_usd(report.total_pnl)}",
            f"Return: {total_return_percent:+.4f}%",
            f"Funding: {_signed_usd(report.total_funding)}",
            f"Fees: -${report.total_fees:.2f}",
            f"Slippage: -${report.total_slippage:.2f}",
            (
                f"Tracking since: {report.first_seen.isoformat(timespec='seconds')}"
                if report.first_seen is not None
                else "Tracking since: no snapshots yet"
            ),
        ]
