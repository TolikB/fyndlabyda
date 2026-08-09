"""Restartable production-shaped paper trading loop for the test deployment."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
    save_paper_funding_payment,
    save_paper_position,
    save_portfolio_snapshot,
)
from funding_arbitrage.execution.paper import PaperTradingExecutor
from funding_arbitrage.market_data.collector import MarketDataCollector, MarketSnapshot
from funding_arbitrage.monitoring.metrics import (
    paper_runner_cycles_total,
    paper_runner_errors_total,
    paper_runner_last_cycle_timestamp,
)
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.services.daily_report import DailyReportService
from funding_arbitrage.services.runtime import RuntimeState

logger = logging.getLogger(__name__)


class PaperTestRunner:
    """Scan, paper-fill, settle funding, close, and persist in one safe loop."""

    def __init__(
        self,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self.collector = MarketDataCollector(
            runtime.adapters.values(), settings.paper_orderbook_symbol_limit
        )
        self.executor = PaperTradingExecutor(
            fee_rate=max(fees[1] for fees in settings.fee_schedules.values())
        )
        self.stop_event = asyncio.Event()
        self._position_by_key: dict[str, str] = {}
        self._next_funding_due: dict[tuple[str, str], datetime] = {}
        self._last_history_refresh: datetime | None = None
        self.daily_report = DailyReportService(settings, session_factory)

    async def run(self) -> None:
        await self._restore_positions()
        while not self.stop_event.is_set():
            try:
                await self.cycle()
                paper_runner_cycles_total.inc()
                paper_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
            except asyncio.CancelledError:
                raise
            except Exception:
                paper_runner_errors_total.inc()
                logger.exception("paper_test_cycle_failed")
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.settings.paper_loop_interval_seconds
                )
            except TimeoutError:
                continue

    async def stop(self) -> None:
        self.stop_event.set()

    async def close(self) -> None:
        await self.stop()
        await self.daily_report.close()

    async def cycle(self) -> None:
        now = datetime.now(UTC)
        refresh_history = self._last_history_refresh is None or (
            now - self._last_history_refresh
        ).total_seconds() >= self.settings.paper_history_refresh_seconds
        snapshot = await self.collector.collect_once(include_history=refresh_history)
        if refresh_history:
            self._last_history_refresh = snapshot.captured_at
        opportunities = self.runtime.update_market(snapshot)
        await self._persist_market(snapshot, opportunities)
        await self._settle_funding(snapshot)
        await self._close_expired(snapshot)
        await self._open_confirmed(opportunities, snapshot)
        await self._persist_portfolio()
        await self.daily_report.check_and_send(snapshot.captured_at)

    async def _restore_positions(self) -> None:
        async with self.session_factory() as session:
            snapshot = await session.scalar(
                select(PortfolioSnapshotRecord).order_by(
                    PortfolioSnapshotRecord.timestamp.desc()
                )
            )
            has_snapshot = snapshot is not None
            if snapshot is not None:
                self.runtime.portfolio.restore_balances(
                    {key: Decimal(str(value)) for key, value in snapshot.balances.items()}
                )
            rows = (
                await session.execute(
                    select(PaperPositionRecord).order_by(PaperPositionRecord.id)
                )
            ).scalars()
            for row in rows:
                position = PaperPosition.model_validate(row.payload)
                if has_snapshot:
                    self.runtime.portfolio.add_position(position)
                else:
                    try:
                        self.runtime.portfolio.allocate_position(
                            position, position.allocated_venues, position.capital
                        )
                    except ValueError:
                        logger.warning(
                            "paper_position_restore_skipped",
                            extra={"position_id": position.id},
                        )
                        continue
                if position.state is PositionState.OPEN and position.opportunity_key:
                    self._position_by_key[position.opportunity_key] = position.id

    async def _persist_market(
        self, snapshot: MarketSnapshot, opportunities: list[Opportunity]
    ) -> None:
        async with self.session_factory() as session:
            await save_market_snapshot(session, snapshot)
            await save_opportunities(session, opportunities)

    async def _persist_portfolio(self) -> None:
        async with self.session_factory() as session:
            for position in self.runtime.portfolio.positions.values():
                await save_paper_position(session, position)
            await save_portfolio_snapshot(session, self.runtime.portfolio.snapshot())

    async def _open_confirmed(
        self, opportunities: list[Opportunity], snapshot: MarketSnapshot
    ) -> None:
        open_count = sum(
            position.state is PositionState.OPEN
            for position in self.runtime.portfolio.positions.values()
        )
        for opportunity in opportunities:
            if open_count >= self.settings.paper_max_open_positions:
                return
            if opportunity.status != "confirmed":
                continue
            key = OpportunityDebouncer.key(opportunity)
            if key in self._position_by_key:
                continue
            quote = next(
                (
                    quote
                    for quote in opportunity.size_quotes
                    if quote.capital >= self.settings.paper_position_size_usd
                    and quote.net_profit > 0
                    and quote.fully_filled
                ),
                None,
            )
            if quote is None:
                continue
            position = await self.executor.open(opportunity, quote.capital, snapshot)
            if position.state is not PositionState.OPEN:
                continue
            position.opportunity_key = key
            venues = (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a)
            try:
                self.runtime.portfolio.allocate_position(position, venues, quote.capital)
            except ValueError:
                continue
            self._position_by_key[key] = position.id
            open_count += 1

    async def _settle_funding(self, snapshot: MarketSnapshot) -> None:
        now = snapshot.captured_at
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            for funding in snapshot.funding:
                if not self._funding_applies(position, funding.exchange):
                    continue
                due_key = (position.id, funding.exchange)
                due = self._next_funding_due.setdefault(
                    due_key,
                    now + timedelta(seconds=self.settings.paper_settlement_interval_seconds),
                )
                if now < due:
                    continue
                async with self.session_factory() as session:
                    existing = await session.scalar(
                        select(PaperFundingPaymentRecord).where(
                            PaperFundingPaymentRecord.position_id == position.id,
                            PaperFundingPaymentRecord.exchange == funding.exchange,
                            PaperFundingPaymentRecord.funding_timestamp == due,
                        )
                    )
                    if existing is not None:
                        self._next_funding_due[due_key] = due + timedelta(
                            seconds=self.settings.paper_settlement_interval_seconds
                        )
                        continue
                    event_funding = funding.model_copy(update={"timestamp": due})
                    pnl = self.runtime.portfolio.settle_funding(
                        position.id, event_funding, position.capital
                    )
                    await save_paper_funding_payment(
                        session, position.id, event_funding, position.capital, pnl
                    )
                self._next_funding_due[due_key] = due + timedelta(
                    seconds=self.settings.paper_settlement_interval_seconds
                )

    async def _close_expired(self, snapshot: MarketSnapshot) -> None:
        now = snapshot.captured_at
        for position in list(self.runtime.portfolio.positions.values()):
            if position.state is not PositionState.OPEN or position.opened_at is None:
                continue
            if (now - position.opened_at).total_seconds() < self.settings.paper_max_hold_seconds:
                continue
            await self.executor.close(position, snapshot)
            self.runtime.portfolio.close_position(position.id)
            if position.opportunity_key:
                self._position_by_key.pop(position.opportunity_key, None)

    @staticmethod
    def _funding_applies(position: PaperPosition, exchange: str) -> bool:
        return bool(
            (position.leg_a and position.leg_a.exchange == exchange)
            or (position.leg_b and position.leg_b.exchange == exchange)
        )
