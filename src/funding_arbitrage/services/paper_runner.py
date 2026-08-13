"""Restartable production-shaped paper trading loop for the test deployment."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import and_, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
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
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
)
from funding_arbitrage.execution.base import PaperFill
from funding_arbitrage.execution.paper import PaperTradingExecutor
from funding_arbitrage.market_data.collector import MarketDataCollector, MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.monitoring.metrics import (
    paper_runner_cycle_duration_seconds,
    paper_runner_cycles_total,
    paper_runner_errors_total,
    paper_runner_last_cycle_timestamp,
    paper_runner_stage_duration_seconds,
    paper_trade_rejections_total,
)
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.opportunity.settlement import (
    settlement_continuation_allowed,
    settlement_entry_allowed,
    target_settlements,
)
from funding_arbitrage.portfolio.allocator import CapitalAllocator
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.risk.engine import RiskEngine, RiskLimits
from funding_arbitrage.services.daily_report import DailyReportService
from funding_arbitrage.services.runtime import RuntimeState

logger = logging.getLogger(__name__)


def _initialize_scan_worker() -> None:
    """Keep CPU-heavy scans below the API/event-loop scheduler priority."""

    set_priority = getattr(os, "setpriority", None)
    priority_process = getattr(os, "PRIO_PROCESS", None)
    if os.name != "posix" or set_priority is None or priority_process is None:
        return
    try:
        set_priority(priority_process, threading.get_native_id(), 10)
    except OSError:
        logger.warning("paper_scan_worker_priority_unchanged")


_SCAN_EXECUTOR = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="paper-scan",
    initializer=_initialize_scan_worker,
)


class PaperTestRunner:
    """Scan, paper-fill, settle funding, close, and persist in one safe loop."""

    def __init__(
        self,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
        collector: MarketDataCollector | None = None,
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self._owns_collector = collector is None
        self.collector = collector or MarketDataCollector(
            runtime.adapters.values(),
            settings.paper_orderbook_symbol_limit,
            settings.paper_market_asset_limit,
            settings.paper_history_symbol_limit,
            settings.market_data_stale_seconds,
            settings.market_data_mode == "live_public",
        )
        self.executor = PaperTradingExecutor(
            fees={venue: schedule[1] for venue, schedule in settings.fee_schedules.items()},
            stale_seconds=settings.market_data_stale_seconds,
            simulation_version=settings.paper_simulation_version,
            legging_move_percent=settings.paper_legging_move_percent,
        )
        self.allocator = CapitalAllocator(
            RiskEngine(
                RiskLimits(
                    max_single_opportunity_percent=(
                        settings.paper_max_single_opportunity_percent
                    ),
                    max_single_asset_percent=settings.paper_max_single_asset_percent,
                    max_single_exchange_percent=(
                        settings.paper_max_single_exchange_percent
                    ),
                    max_single_strategy_percent=(
                        settings.paper_max_single_strategy_percent
                    ),
                    max_correlated_group_percent=(
                        settings.paper_max_correlated_group_percent
                    ),
                    minimum_cash_reserve_percent=settings.paper_reserve_percent,
                )
            ),
            settings.paper_correlation_group_values,
        )
        self.stop_event = asyncio.Event()
        self._position_by_key: dict[str, str] = {}
        self._next_funding_due: dict[tuple[str, str, str], datetime] = {}
        self._last_history_refresh: datetime | None = None
        self._history_persist_snapshot_at: datetime | None = None
        self._last_history_symbols: dict[str, tuple[str, ...]] = {}
        self._last_market_persist: datetime | None = None
        self._candidate_history_symbols: dict[str, set[str]] = {}
        self._candidate_orderbook_symbols: dict[
            str, set[tuple[str, InstrumentType]]
        ] = {}
        self.daily_report = DailyReportService(settings, session_factory)

    async def run(self) -> None:
        await self.restore(restore_history=True)
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.cycle()
                paper_runner_cycle_duration_seconds.observe(time.monotonic() - started)
                paper_runner_cycles_total.inc()
                paper_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
            except asyncio.CancelledError:
                raise
            except Exception:
                paper_runner_errors_total.inc()
                logger.exception("paper_test_cycle_failed")
            await _wait_for_next_cycle(
                self.stop_event,
                started,
                self.settings.paper_loop_interval_seconds,
            )

    async def stop(self) -> None:
        self.stop_event.set()

    async def close(self) -> None:
        await self.stop()
        if self._owns_collector:
            await self.collector.close()
        await self.daily_report.close()

    async def restore(self, *, restore_history: bool) -> None:
        await self._restore_positions()
        if restore_history:
            await self._restore_funding_history()

    async def cycle(self) -> None:
        snapshot = await self.collect_snapshot()
        await self.process_snapshot(snapshot, persist_market=True)

    async def collect_snapshot(
        self, peers: tuple[PaperTestRunner, ...] = ()
    ) -> MarketSnapshot:
        now = datetime.now(UTC)
        stage_started = time.monotonic()
        runners = (self, *peers)
        required_history: dict[str, set[str]] = {}
        required_books: dict[str, set[tuple[str, InstrumentType]]] = {}
        for runner in runners:
            for venue, symbols in runner._required_funding_symbols().items():
                required_history.setdefault(venue, set()).update(symbols)
            for venue, books in runner._required_orderbook_symbols().items():
                required_books.setdefault(venue, set()).update(books)
        normalized_history = {
            venue: tuple(sorted(symbols)) for venue, symbols in required_history.items()
        }
        periodic_history_refresh = self._last_history_refresh is None or (
            now - self._last_history_refresh
        ).total_seconds() >= self.settings.paper_history_refresh_seconds
        refresh_history = periodic_history_refresh or (
            normalized_history != self._last_history_symbols
        )
        snapshot = await self.collector.collect_once(
            orderbook_symbols={
                venue: sorted(books, key=lambda value: (value[0], value[1].value))
                for venue, books in required_books.items()
            },
            include_history=refresh_history,
            history_symbols={venue: sorted(symbols) for venue, symbols in required_history.items()},
            force_history_refresh=periodic_history_refresh,
        )
        paper_runner_stage_duration_seconds.labels("collect").observe(
            time.monotonic() - stage_started
        )
        if periodic_history_refresh:
            self._last_history_refresh = snapshot.captured_at
        if refresh_history:
            self._last_history_symbols = normalized_history
            self._history_persist_snapshot_at = snapshot.captured_at
        return snapshot

    async def process_snapshot(
        self, snapshot: MarketSnapshot, *, persist_market: bool
    ) -> None:
        stage_started = time.monotonic()
        # Strategy replay is CPU-bound. Keep it off the asyncio event loop so
        # health, metrics, and scheduled Telegram reports remain responsive.
        opportunities = await asyncio.get_running_loop().run_in_executor(
            _SCAN_EXECUTOR,
            self.runtime.update_market,
            snapshot,
        )
        paper_runner_stage_duration_seconds.labels("scan").observe(
            time.monotonic() - stage_started
        )
        self._remember_candidate_symbols(
            self.runtime.opportunity_engine.last_candidates
        )
        should_persist_market = persist_market and (
            self._last_market_persist is None
            or (
                snapshot.captured_at - self._last_market_persist
            ).total_seconds()
            >= self.settings.paper_market_persist_interval_seconds
        )
        if should_persist_market:
            stage_started = time.monotonic()
            await self._persist_market(
                snapshot,
                opportunities,
                include_history=self._history_persist_snapshot_at == snapshot.captured_at,
            )
            paper_runner_stage_duration_seconds.labels("market_persist").observe(
                time.monotonic() - stage_started
            )
            self._last_market_persist = snapshot.captured_at
        stage_started = time.monotonic()
        self._accrue_borrow(snapshot.captured_at)
        await self._settle_funding(snapshot)
        await self._close_expired(snapshot)
        if self.settings.paper_autotrade:
            await self._open_confirmed(opportunities, snapshot)
        paper_runner_stage_duration_seconds.labels("paper_execution").observe(
            time.monotonic() - stage_started
        )
        self.runtime.refresh_portfolio_metrics()
        stage_started = time.monotonic()
        await self._persist_portfolio(snapshot.captured_at)
        await self.daily_report.check_and_send(snapshot.captured_at)
        paper_runner_stage_duration_seconds.labels("portfolio_persist").observe(
            time.monotonic() - stage_started
        )

    async def _restore_positions(self) -> None:
        async with self.session_factory() as session:
            snapshot = await session.scalar(
                select(PortfolioSnapshotRecord)
                .where(
                    PortfolioSnapshotRecord.simulation_version
                    == self.settings.paper_simulation_version
                )
                .order_by(PortfolioSnapshotRecord.timestamp.desc())
            )
            has_snapshot = snapshot is not None
            if snapshot is not None:
                self.runtime.portfolio.restore_balances(
                    {key: Decimal(str(value)) for key, value in snapshot.balances.items()}
                )
            rows = (
                await session.execute(
                    select(PaperPositionRecord).order_by(PaperPositionRecord.id)
                    .where(
                        PaperPositionRecord.simulation_version
                        == self.settings.paper_simulation_version
                    )
                )
            ).scalars()
            for row in rows:
                position = PaperPosition.model_validate(row.payload)
                durable_funding_pnl = await session.scalar(
                    select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0)).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
                position.pnl.funding_pnl = Decimal(str(durable_funding_pnl or 0))
                durable_funding_events = await session.scalar(
                    select(func.count(PaperFundingPaymentRecord.id)).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
                position.funding_events = int(durable_funding_events or 0)
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

    async def _restore_funding_history(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(days=30)
        venues = self.settings.paper_venue_values
        latest = (
            select(
                FundingHistoryRecord.exchange.label("exchange"),
                FundingHistoryRecord.symbol.label("symbol"),
                func.max(FundingHistoryRecord.funding_timestamp).label("latest_at"),
            )
            .where(
                FundingHistoryRecord.exchange.in_(venues),
                FundingHistoryRecord.funding_timestamp >= cutoff,
            )
            .group_by(FundingHistoryRecord.exchange, FundingHistoryRecord.symbol)
            .subquery()
        )
        async with self.session_factory() as session:
            latest_rows = list(
                (
                    await session.execute(
                        select(FundingHistoryRecord).join(
                            latest,
                            and_(
                                FundingHistoryRecord.exchange == latest.c.exchange,
                                FundingHistoryRecord.symbol == latest.c.symbol,
                                FundingHistoryRecord.funding_timestamp == latest.c.latest_at,
                            ),
                        )
                    )
                ).scalars()
            )
            selected = _rank_warm_history_symbols(
                latest_rows, self.settings.paper_market_asset_limit
            )
            if not selected:
                return
            rows = list(
                (
                    await session.execute(
                        select(FundingHistoryRecord)
                        .where(
                            tuple_(
                                FundingHistoryRecord.exchange,
                                FundingHistoryRecord.symbol,
                            ).in_(selected),
                            FundingHistoryRecord.funding_timestamp >= cutoff,
                        )
                        .order_by(
                            FundingHistoryRecord.exchange,
                            FundingHistoryRecord.symbol,
                            FundingHistoryRecord.funding_timestamp,
                        )
                    )
                ).scalars()
            )
        history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        for row in rows:
            history.setdefault((row.exchange, row.symbol), []).append(
                FundingHistoryPoint(
                    exchange=row.exchange,
                    symbol=row.symbol,
                    funding_rate=row.funding_rate,
                    funding_timestamp=row.funding_timestamp.replace(tzinfo=UTC)
                    if row.funding_timestamp.tzinfo is None
                    else row.funding_timestamp.astimezone(UTC),
                    mark_price=row.mark_price,
                )
            )
        self.collector.seed_funding_history(history)
        self._last_history_refresh = datetime.now(UTC)
        required = self._required_funding_symbols()
        self._last_history_symbols = {
            venue: tuple(sorted(symbols)) for venue, symbols in required.items()
        }
        logger.info(
            "paper_funding_history_warmed",
            extra={"event": "paper_startup", "count": len(rows)},
        )

    async def _persist_market(
        self,
        snapshot: MarketSnapshot,
        opportunities: list[Opportunity],
        *,
        include_history: bool,
    ) -> None:
        async with self.session_factory() as session:
            await save_market_snapshot(session, snapshot, include_history=include_history)
            await save_opportunities(session, opportunities)

    async def _persist_portfolio(self, timestamp: datetime) -> None:
        async with self.session_factory() as session:
            for position in self.runtime.portfolio.positions.values():
                await save_paper_position(session, position)
            await save_portfolio_snapshot(
                session, self.runtime.portfolio.snapshot(timestamp=timestamp)
            )

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
            if self.settings.paper_strategy_profile == "baseline":
                quote = next(
                    (
                        item
                        for item in opportunity.size_quotes
                        if item.capital >= self.settings.paper_position_size_usd
                        and item.net_profit > 0
                        and item.fully_filled
                    ),
                    None,
                )
                capital = quote.capital if quote is not None else Decimal("0")
            else:
                settlement_quotes = [
                    quote
                    for quote in opportunity.size_quotes
                    if self.settings.market_data_mode == "mock"
                    or settlement_entry_allowed(
                        opportunity,
                        quote,
                        snapshot,
                        snapshot.captured_at,
                        self.settings.paper_entry_window_hours,
                        self.settings.paper_min_settlement_cost_coverage,
                    )
                ]
                if not settlement_quotes:
                    paper_trade_rejections_total.labels("settlement_cost_coverage").inc()
                    continue
                capital = self.allocator.allocate(
                    opportunity.model_copy(update={"size_quotes": settlement_quotes}),
                    self.runtime.portfolio,
                    minimum_capital=self.settings.paper_position_size_usd,
                )
            if capital <= 0:
                paper_trade_rejections_total.labels("allocation_or_risk").inc()
                continue
            position = await self.executor.open(opportunity, capital, snapshot)
            if position.state is not PositionState.OPEN:
                paper_trade_rejections_total.labels("execution").inc()
                continue
            position.target_settlements = (
                (
                    snapshot.captured_at
                    + timedelta(seconds=self.settings.paper_settlement_interval_seconds),
                )
                if self.settings.market_data_mode == "mock"
                else target_settlements(opportunity, snapshot, snapshot.captured_at)
            )
            position.opportunity_key = key
            venues = (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a)
            try:
                self.runtime.portfolio.allocate_position(position, venues, capital)
            except ValueError:
                paper_trade_rejections_total.labels("venue_balance").inc()
                continue
            self._position_by_key[key] = position.id
            open_count += 1

    async def _settle_funding(self, snapshot: MarketSnapshot) -> None:
        if self.settings.market_data_mode == "mock":
            await self._settle_mock_funding(snapshot)
            return
        await self._settle_live_funding(snapshot)

    def _accrue_borrow(self, now: datetime) -> None:
        """Accrue configured spot-borrow cost over actual wall-clock holding time."""

        for position in self.runtime.portfolio.positions.values():
            if (
                position.state is not PositionState.OPEN
                or position.borrow_rate_daily <= 0
                or position.opened_at is None
            ):
                continue
            accrued_until = position.borrow_accrued_until or position.opened_at
            if now <= accrued_until:
                continue
            elapsed_days = Decimal(str((now - accrued_until).total_seconds())) / Decimal(
                "86400"
            )
            position.pnl.borrow_cost += (
                position.capital * position.borrow_rate_daily * elapsed_days
            )
            position.borrow_accrued_until = now

    async def _settle_live_funding(self, snapshot: MarketSnapshot) -> None:
        now = snapshot.captured_at
        funding_by_key = {
            (item.exchange, item.symbol): item for item in snapshot.funding
        }
        history_by_key = snapshot.funding_history or {}
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN or position.opened_at is None:
                continue
            for leg in self._funding_legs(position):
                history = history_by_key.get((leg.exchange, leg.symbol), [])
                current = funding_by_key.get((leg.exchange, leg.symbol))
                for event in sorted(history, key=lambda item: item.funding_timestamp):
                    if not position.opened_at < event.funding_timestamp <= now:
                        continue
                    interval_hours = (
                        current.funding_interval_hours
                        if current is not None
                        else _history_interval_hours(history, event)
                    )
                    event_funding = FundingSnapshot(
                        exchange=event.exchange,
                        symbol=event.symbol,
                        funding_rate=event.funding_rate,
                        funding_interval_hours=interval_hours,
                        mark_price=event.mark_price,
                        timestamp=event.funding_timestamp,
                    )
                    await self._apply_funding_event(position, leg, event_funding)

    async def _settle_mock_funding(self, snapshot: MarketSnapshot) -> None:
        now = snapshot.captured_at
        funding_by_key = {
            (item.exchange, item.symbol): item for item in snapshot.funding
        }
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            for leg in self._funding_legs(position):
                funding = funding_by_key.get((leg.exchange, leg.symbol))
                if funding is None:
                    continue
                due_key = (position.id, leg.exchange, leg.symbol)
                due = self._next_funding_due.setdefault(
                    due_key,
                    now + timedelta(seconds=self.settings.paper_settlement_interval_seconds),
                )
                if now < due:
                    continue
                await self._apply_funding_event(
                    position, leg, funding.model_copy(update={"timestamp": due})
                )
                self._next_funding_due[due_key] = due + timedelta(
                    seconds=self.settings.paper_settlement_interval_seconds
                )

    async def _apply_funding_event(
        self, position: PaperPosition, leg: PaperFill, funding: FundingSnapshot
    ) -> None:
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(PaperFundingPaymentRecord).where(
                    PaperFundingPaymentRecord.position_id == position.id,
                    PaperFundingPaymentRecord.exchange == funding.exchange,
                    PaperFundingPaymentRecord.symbol == funding.symbol,
                    PaperFundingPaymentRecord.funding_timestamp == funding.timestamp,
                )
            )
            if existing is not None:
                return
            pnl = self.runtime.portfolio.calculate_funding_pnl(
                leg.side, position.capital, funding.funding_rate
            )
            await save_paper_funding_payment(
                session, position.id, funding, position.capital, pnl
            )
            self.runtime.portfolio.settle_funding(
                position.id, funding, position.capital, leg.side
            )
            position.funding_events += 1

    def _required_funding_symbols(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {
            venue: list(symbols)
            for venue, symbols in self._candidate_history_symbols.items()
        }
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            for leg in self._funding_legs(position):
                result.setdefault(leg.exchange, []).append(leg.symbol)
        return {
            venue: list(dict.fromkeys(symbols)) for venue, symbols in result.items()
        }

    def _required_orderbook_symbols(
        self,
    ) -> dict[str, list[tuple[str, InstrumentType]]]:
        result = {
            venue: set(symbols)
            for venue, symbols in self._candidate_orderbook_symbols.items()
        }
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            for leg, instrument_type in zip(
                (position.leg_a, position.leg_b),
                (position.leg_a_type, position.leg_b_type),
                strict=True,
            ):
                if leg is not None and instrument_type is not None:
                    result.setdefault(leg.exchange, set()).add((leg.symbol, instrument_type))
        return {venue: sorted(symbols) for venue, symbols in result.items()}

    def _remember_candidate_symbols(self, opportunities: list[Opportunity]) -> None:
        history: dict[str, set[str]] = {}
        books: dict[str, set[tuple[str, InstrumentType]]] = {}
        for opportunity in opportunities:
            for venue, symbol, instrument_type in (
                (
                    opportunity.venue_a,
                    opportunity.symbol_a,
                    InstrumentType(opportunity.leg_a_type),
                ),
                (
                    opportunity.venue_b or opportunity.venue_a,
                    opportunity.symbol_b,
                    InstrumentType(opportunity.leg_b_type),
                ),
            ):
                if symbol is None:
                    continue
                books.setdefault(venue, set()).add((symbol, instrument_type))
                if instrument_type is InstrumentType.PERPETUAL:
                    history.setdefault(venue, set()).add(symbol)
        self._candidate_history_symbols = history
        self._candidate_orderbook_symbols = books

    async def _close_expired(self, snapshot: MarketSnapshot) -> None:
        now = snapshot.captured_at
        current_keys = {
            OpportunityDebouncer.key(opportunity): opportunity
            for opportunity in self.runtime.opportunities
        }
        for position in list(self.runtime.portfolio.positions.values()):
            if position.state is not PositionState.OPEN or position.opened_at is None:
                continue
            max_hold = (
                now - position.opened_at
            ).total_seconds() >= self.settings.paper_max_hold_seconds
            current = current_keys.get(position.opportunity_key or "")
            if current is None:
                position.edge_miss_count += 1
            else:
                position.edge_miss_count = 0
            edge_gone = position.edge_miss_count >= self.settings.paper_exit_edge_miss_cycles
            funding_reversed = self._funding_reversed(position, snapshot)
            adverse_basis = self._adverse_basis(position, snapshot)
            market_degraded = self._execution_degraded(position, snapshot)
            target_received = position.funding_events > 0 and any(
                target <= now for target in position.target_settlements
            )
            continue_after_target = False
            if target_received and current is not None:
                quote = min(
                    current.size_quotes,
                    key=lambda value: abs(value.capital - position.capital),
                    default=None,
                )
                if quote is not None:
                    continue_after_target = settlement_continuation_allowed(
                        current,
                        quote,
                        snapshot,
                        now,
                        self.settings.paper_min_settlement_cost_coverage,
                    )
                if continue_after_target:
                    position.target_settlements = target_settlements(current, snapshot, now)
            target_exit = target_received and not continue_after_target
            optimized_exit = (
                self.settings.paper_strategy_profile == "candidate"
                and (
                    edge_gone
                    or funding_reversed
                    or adverse_basis
                    or market_degraded
                    or target_exit
                )
            )
            if not (max_hold or optimized_exit):
                continue
            await self.executor.close(position, snapshot)
            if position.state is not PositionState.CLOSED:
                continue
            self.runtime.portfolio.close_position(position.id)
            if position.opportunity_key:
                self._position_by_key.pop(position.opportunity_key, None)

    @staticmethod
    def _execution_degraded(position: PaperPosition, snapshot: MarketSnapshot) -> bool:
        """Require enough fresh opposite-side depth to neutralize both legs."""

        for leg, instrument_type in (
            (position.leg_a, position.leg_a_type),
            (position.leg_b, position.leg_b_type),
        ):
            if leg is None or instrument_type is None:
                return True
            ticker = snapshot.ticker(leg.exchange, leg.symbol, instrument_type)
            book = snapshot.orderbook(leg.exchange, leg.symbol, instrument_type)
            if ticker is None or book is None:
                return True
            if (
                (snapshot.captured_at - ticker.timestamp).total_seconds()
                > snapshot.stale_after_seconds
                or (snapshot.captured_at - book.timestamp).total_seconds()
                > snapshot.stale_after_seconds
            ):
                return True
            close_side = OrderSide.SELL if leg.side.upper() == "BUY" else OrderSide.BUY
            if not calculate_execution_price(
                book, close_side, leg.filled_quantity
            ).is_fully_filled:
                return True
        return False

    @classmethod
    def _funding_reversed(cls, position: PaperPosition, snapshot: MarketSnapshot) -> bool:
        cashflow = Decimal("0")
        observed = 0
        funding_by_key = {(item.exchange, item.symbol): item for item in snapshot.funding}
        for leg in cls._funding_legs(position):
            funding = funding_by_key.get((leg.exchange, leg.symbol))
            if funding is None:
                continue
            observed += 1
            cashflow += (
                funding.funding_rate
                if leg.side.upper() == "SELL"
                else -funding.funding_rate
            )
        return observed > 0 and cashflow <= 0

    def _adverse_basis(self, position: PaperPosition, snapshot: MarketSnapshot) -> bool:
        market_pnl = Decimal("0")
        observed = 0
        for leg, instrument_type in (
            (position.leg_a, position.leg_a_type),
            (position.leg_b, position.leg_b_type),
        ):
            if leg is None or leg.price is None or instrument_type is None:
                continue
            ticker = snapshot.ticker(leg.exchange, leg.symbol, instrument_type)
            if ticker is None:
                return False
            observed += 1
            market_pnl += (ticker.last_price - leg.price) * leg.filled_quantity * (
                Decimal("1") if leg.side.upper() == "BUY" else Decimal("-1")
            )
        return observed == 2 and market_pnl <= -(
            position.capital * self.settings.paper_max_adverse_basis_percent
        )

    @staticmethod
    def _funding_legs(position: PaperPosition) -> tuple[PaperFill, ...]:
        leg_types = [position.leg_a_type, position.leg_b_type]
        if position.opportunity_key and any(item is None for item in leg_types):
            parts = position.opportunity_key.split(":")
            if len(parts) >= 2:
                for index, value in enumerate(parts[-2:]):
                    if leg_types[index] is None:
                        try:
                            leg_types[index] = InstrumentType(value)
                        except ValueError:
                            pass
        position.leg_a_type, position.leg_b_type = leg_types
        return tuple(
            leg
            for leg, leg_type in zip(
                (position.leg_a, position.leg_b), leg_types, strict=True
            )
            if leg is not None and leg_type is InstrumentType.PERPETUAL
        )


class SharedMarketPaperComparisonRunner:
    """Run isolated candidate/baseline ledgers from one identical public snapshot."""

    def __init__(self, candidate: PaperTestRunner, baseline: PaperTestRunner) -> None:
        if candidate.collector is not baseline.collector:
            raise ValueError("comparison runners must share one market-data collector")
        if (
            candidate.settings.paper_simulation_version
            == baseline.settings.paper_simulation_version
        ):
            raise ValueError("comparison runners require distinct simulation versions")
        self.candidate = candidate
        self.baseline = baseline
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        await self.candidate.restore(restore_history=True)
        await self.baseline.restore(restore_history=False)
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                snapshot = await self.candidate.collect_snapshot((self.baseline,))
                await asyncio.gather(
                    self.candidate.process_snapshot(snapshot, persist_market=True),
                    self.baseline.process_snapshot(snapshot, persist_market=False),
                )
                paper_runner_cycle_duration_seconds.observe(time.monotonic() - started)
                paper_runner_cycles_total.inc()
                paper_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
            except asyncio.CancelledError:
                raise
            except Exception:
                paper_runner_errors_total.inc()
                logger.exception("paper_comparison_cycle_failed")
            await _wait_for_next_cycle(
                self.stop_event,
                started,
                self.candidate.settings.paper_loop_interval_seconds,
            )

    async def stop(self) -> None:
        self.stop_event.set()

    async def close(self) -> None:
        await self.stop()
        await self.candidate.close()
        await self.baseline.close()


async def _wait_for_next_cycle(
    stop_event: asyncio.Event,
    cycle_started: float,
    interval_seconds: float,
) -> None:
    """Keep a start-to-start cadence instead of sleeping after slow collection."""

    elapsed = time.monotonic() - cycle_started
    delay = max(0.0, float(interval_seconds) - elapsed)
    if delay == 0:
        # Slow cycles should catch up promptly, while still yielding to API tasks.
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except TimeoutError:
        return


def _history_interval_hours(
    history: list[FundingHistoryPoint], event: FundingHistoryPoint
) -> Decimal:
    ordered = sorted(history, key=lambda item: item.funding_timestamp)
    index = ordered.index(event)
    if index > 0:
        seconds = (event.funding_timestamp - ordered[index - 1].funding_timestamp).total_seconds()
        if seconds > 0:
            return Decimal(str(seconds / 3600))
    return Decimal("8")


def _rank_warm_history_symbols(
    latest_rows: list[FundingHistoryRecord], limit: int
) -> set[tuple[str, str]]:
    by_venue: dict[str, list[FundingHistoryRecord]] = {}
    for row in latest_rows:
        by_venue.setdefault(row.exchange, []).append(row)
    core = {"BTC": 0, "ETH": 1, "SOL": 2}

    def asset_rank(symbol: str) -> int:
        normalized = symbol.replace("-", "").replace("_", "")
        return next(
            (rank for asset, rank in core.items() if normalized.startswith(asset)),
            len(core),
        )

    selected: set[tuple[str, str]] = set()
    for venue, rows in by_venue.items():
        ranked = sorted(
            rows,
            key=lambda row: (
                asset_rank(row.symbol),
                -abs(row.funding_rate),
                row.symbol,
            ),
        )[:limit]
        selected.update((venue, row.symbol) for row in ranked)
    return selected
