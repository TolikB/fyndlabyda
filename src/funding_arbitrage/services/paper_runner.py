"""Restartable production-shaped paper trading loop for the test deployment."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.database.repositories.ledger import (
    backfill_paper_funding_ledger,
    infer_funding_settlement_asset,
)
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
    save_paper_funding_payment,
    save_paper_position,
    save_paper_runtime_incident,
    save_portfolio_snapshot,
)
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
)
from funding_arbitrage.exchanges.public_events import PublicEventSupervisor
from funding_arbitrage.execution.base import PaperFill
from funding_arbitrage.execution.paper import PaperTradingExecutor
from funding_arbitrage.market_data.collector import (
    CanonicalBookEventSink,
    MarketDataCollector,
    MarketSnapshot,
)
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.monitoring.metrics import (
    paper_market_cycles_skipped_total,
    paper_runner_cycle_duration_seconds,
    paper_runner_cycles_total,
    paper_runner_errors_total,
    paper_runner_last_cycle_timestamp,
    paper_runner_stage_duration_seconds,
    paper_trade_rejections_total,
)
from funding_arbitrage.opportunity.debounce import (
    OpportunityDebouncer,
    canonical_exposure_key,
)
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.opportunity.settlement import (
    is_funding_strategy,
    next_settlement_rate,
    settlement_continuation_allowed,
    settlement_entry_allowed,
    target_settlement_events,
)
from funding_arbitrage.portfolio.allocator import CapitalAllocator
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.risk.engine import RiskEngine, RiskLimits
from funding_arbitrage.services.daily_report import DailyReportService
from funding_arbitrage.services.runtime import RuntimeState

if TYPE_CHECKING:
    from funding_arbitrage.qa.runtime_acceptance import RuntimeAcceptanceCollector

logger = logging.getLogger(__name__)


class IncompleteMarketSnapshotError(Exception):
    """A recoverable public-data gap that must not enter the PnL dataset."""

    def __init__(self, venues: tuple[str, ...]) -> None:
        self.venues = venues
        super().__init__(f"incomplete venues: {','.join(venues)}")


class FundingReconciliationExhaustedError(Exception):
    """A closed position exhausted its bounded final-history retries."""


async def _persist_runtime_incident(
    session_factory: async_sessionmaker[AsyncSession],
    simulation_versions: tuple[str, ...],
    category: str,
    error: Exception,
) -> None:
    """Best-effort durable failure evidence without persisting error messages."""

    try:
        async with session_factory() as session:
            await save_paper_runtime_incident(
                session,
                simulation_versions,
                category,
                type(error).__name__,
                datetime.now(UTC),
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "paper_runtime_incident_persist_failed",
            extra={"event": "paper_runtime_incident", "category": category},
        )


async def _persist_runner_start(
    session_factory: async_sessionmaker[AsyncSession],
    simulation_versions: tuple[str, ...],
) -> None:
    """Make every process epoch durable before any paper cycle can complete."""

    async with session_factory() as session:
        await save_paper_runtime_incident(
            session,
            simulation_versions,
            "process_start",
            "ProcessStart",
            datetime.now(UTC),
        )


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
        public_events: PublicEventSupervisor | None = None,
        canonical_book_event_sink: CanonicalBookEventSink | None = None,
        combined_snapshot_provider: (
            Callable[[datetime], PortfolioSnapshot | None] | None
        ) = None,
        canonical_consumer_barrier: Callable[[], Awaitable[None]] | None = None,
        acceptance_collector: RuntimeAcceptanceCollector | None = None,
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self.public_events = public_events
        self.combined_snapshot_provider = combined_snapshot_provider
        self.canonical_consumer_barrier = canonical_consumer_barrier
        self._owns_collector = collector is None
        self.collector = collector or MarketDataCollector(
            runtime.adapters.values(),
            settings.paper_orderbook_symbol_limit,
            settings.paper_market_asset_limit,
            settings.paper_history_symbol_limit,
            settings.market_data_stale_seconds,
            settings.market_data_mode == "live_public",
            canonical_book_event_sink=canonical_book_event_sink,
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
        self._position_ids_by_exposure_key: dict[str, set[str]] = {}
        self._next_funding_due: dict[tuple[str, str, str], datetime] = {}
        self._funding_settlement_assets: dict[tuple[str, str], str] = {}
        self._funding_apply_lock = asyncio.Lock()
        self._pending_funding_reconciliation_failures: dict[
            str, PaperPosition
        ] = {}
        self._last_history_refresh: datetime | None = None
        self._history_persist_snapshot_at: datetime | None = None
        self._last_history_symbols: dict[str, tuple[str, ...]] = {}
        self._last_market_persist: datetime | None = None
        self._candidate_history_symbols: dict[str, set[str]] = {}
        self._candidate_orderbook_symbols: dict[
            str, set[tuple[str, InstrumentType]]
        ] = {}
        self.daily_report = DailyReportService(settings, session_factory)
        self._restore_lock = asyncio.Lock()
        self._restored = False
        self._prepare_lock = asyncio.Lock()
        self._run_prepared = False
        self.acceptance_collector = acceptance_collector
        if self.acceptance_collector is None and settings.acceptance_collector_enabled:
            from funding_arbitrage.qa.runtime_acceptance import (
                RuntimeAcceptanceCollector,
            )

            self.acceptance_collector = RuntimeAcceptanceCollector.from_settings(
                settings,
                runtime,
                session_factory,
            )

    async def run(self) -> None:
        await self.prepare_run()
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.cycle()
                paper_runner_cycle_duration_seconds.observe(time.monotonic() - started)
                paper_runner_cycles_total.inc()
                paper_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
                if not self.stop_event.is_set():
                    await self.daily_report.notify_started()
            except asyncio.CancelledError:
                raise
            except IncompleteMarketSnapshotError as error:
                paper_market_cycles_skipped_total.labels("incomplete_venue").inc()
                logger.warning(
                    "paper_market_cycle_skipped",
                    extra={
                        "event": "paper_market_gap",
                        "exchanges": list(error.venues),
                    },
                )
            except Exception as error:
                paper_runner_errors_total.inc()
                logger.exception("paper_test_cycle_failed")
                if self.acceptance_collector is not None:
                    try:
                        await self.acceptance_collector.record_runner_failure()
                    except Exception:
                        logger.exception("acceptance_runtime_failure_record_failed")
                await _persist_runtime_incident(
                    self.session_factory,
                    (self.settings.paper_simulation_version,),
                    "paper_cycle",
                    error,
                )
            await _wait_for_next_cycle(
                self.stop_event,
                started,
                self.settings.paper_loop_interval_seconds,
            )

    async def prepare_run(self) -> None:
        """Prepare restart evidence and acceptance state before the runner task starts."""

        async with self._prepare_lock:
            if self._run_prepared:
                return
            await self.restore(restore_history=True)
            await _persist_runner_start(
                self.session_factory,
                (self.settings.paper_simulation_version,),
            )
            if self.acceptance_collector is not None:
                await self.acceptance_collector.start()
            self._run_prepared = True

    async def stop(self) -> None:
        self.stop_event.set()

    async def close(self, *, send_stopped: bool = True) -> None:
        await self.stop()
        errors: list[Exception] = []
        resources_closed_cleanly = True
        if self._owns_collector:
            try:
                await self.collector.close()
            except Exception as error:
                resources_closed_cleanly = False
                errors.append(error)
                logger.exception(
                    "paper_shutdown_component_failed",
                    extra={"component": "market_data_collector"},
                )
        if self.public_events is not None:
            try:
                await self.public_events.close()
            except Exception as error:
                resources_closed_cleanly = False
                errors.append(error)
                logger.exception(
                    "paper_shutdown_component_failed",
                    extra={"component": "public_event_supervisor"},
                )
        if self.acceptance_collector is not None:
            try:
                await self.acceptance_collector.close()
            except Exception as error:
                resources_closed_cleanly = False
                errors.append(error)
                logger.exception(
                    "paper_shutdown_component_failed",
                    extra={"component": "acceptance_collector"},
                )
        if send_stopped and resources_closed_cleanly:
            try:
                await self.daily_report.notify_stopped()
            except Exception as error:
                errors.append(error)
                logger.exception(
                    "paper_shutdown_component_failed",
                    extra={"component": "stopped_notification"},
                )
        try:
            await self.daily_report.close()
        except Exception as error:
            errors.append(error)
            logger.exception(
                "paper_shutdown_component_failed",
                extra={"component": "telegram_notifier"},
            )
        if errors:
            raise ExceptionGroup("paper runner shutdown failed", errors)

    async def restore(self, *, restore_history: bool) -> None:
        async with self._restore_lock:
            if self._restored:
                return
            await self._restore_positions()
            await self._restore_funding_ledger()
            if restore_history:
                await self._restore_funding_history()
            self._restored = True

    async def cycle(self) -> None:
        snapshot = await self.collect_snapshot()
        await self.process_snapshot(snapshot, persist_market=True)

    async def collect_snapshot(
        self, peers: tuple[PaperTestRunner, ...] = ()
    ) -> MarketSnapshot:
        now = datetime.now(UTC)
        stage_started = time.monotonic()
        if self.public_events is not None:
            await self.public_events.start()
        runners = (self, *peers)
        required_history: dict[str, set[str]] = {}
        forced_history: dict[str, set[str]] = {}
        required_books: dict[str, set[tuple[str, InstrumentType]]] = {}
        discovery_books: dict[str, set[tuple[str, InstrumentType]]] = {}
        for runner in runners:
            for venue, symbols in runner._required_funding_symbols(now).items():
                required_history.setdefault(venue, set()).update(symbols)
            for venue, symbols in runner._due_funding_symbols(now).items():
                forced_history.setdefault(venue, set()).update(symbols)
            for venue, books in runner._open_position_orderbook_symbols().items():
                required_books.setdefault(venue, set()).update(books)
            for venue, candidate_books in runner._candidate_orderbook_symbols.items():
                discovery_books.setdefault(venue, set()).update(candidate_books)
        for runner in runners:
            await runner._persist_pending_funding_reconciliation_failures()
        normalized_history = {
            venue: tuple(sorted(symbols)) for venue, symbols in required_history.items()
        }
        periodic_history_refresh = self._last_history_refresh is None or (
            now - self._last_history_refresh
        ).total_seconds() >= self.settings.paper_history_refresh_seconds
        refresh_history = bool(forced_history) or periodic_history_refresh or (
            normalized_history != self._last_history_symbols
        )
        snapshot = await self.collector.collect_once(
            orderbook_symbols={
                venue: sorted(books, key=lambda value: (value[0], value[1].value))
                for venue, books in required_books.items()
            },
            discovery_orderbook_symbols={
                venue: sorted(books, key=lambda value: (value[0], value[1].value))
                for venue, books in discovery_books.items()
            },
            include_history=refresh_history,
            history_symbols={venue: sorted(symbols) for venue, symbols in required_history.items()},
            force_history_refresh=periodic_history_refresh,
            force_history_symbols={
                venue: sorted(symbols) for venue, symbols in forced_history.items()
            },
        )
        if self.acceptance_collector is not None:
            await self.acceptance_collector.observe_market_snapshot(snapshot)
        paper_runner_stage_duration_seconds.labels("collect").observe(
            time.monotonic() - stage_started
        )
        if snapshot.incomplete_venues:
            raise IncompleteMarketSnapshotError(snapshot.incomplete_venues)
        missing_mark_venues = {
            venue
            for runner in runners
            for venue in runner._missing_mark_venues(snapshot)
        }
        if missing_mark_venues:
            if self.acceptance_collector is not None:
                await self.acceptance_collector.record_market_gap(
                    tuple(sorted(missing_mark_venues))
                )
            raise IncompleteMarketSnapshotError(tuple(sorted(missing_mark_venues)))
        if self.public_events is not None:
            await self.public_events.observe_snapshot(snapshot)
        if self.canonical_consumer_barrier is not None:
            await self.canonical_consumer_barrier()
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
        if self.acceptance_collector is not None:
            self.acceptance_collector.record_strategy_evaluation(opportunities)
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
        self._mark_open_positions(snapshot)
        await self._close_expired(snapshot)
        if self._autotrade_enabled(snapshot.captured_at):
            await self._open_confirmed(opportunities, snapshot)
        paper_runner_stage_duration_seconds.labels("paper_execution").observe(
            time.monotonic() - stage_started
        )
        self.runtime.refresh_portfolio_metrics()
        stage_started = time.monotonic()
        await self._persist_portfolio(snapshot.captured_at)
        daily_report_sent = await self.daily_report.check_and_send(
            snapshot.captured_at
        )
        self.runtime.last_completed_snapshot = snapshot
        if self.acceptance_collector is not None:
            self.acceptance_collector.record_successful_cycle(
                snapshot,
                daily_report_sent=daily_report_sent,
            )
        paper_runner_stage_duration_seconds.labels("portfolio_persist").observe(
            time.monotonic() - stage_started
        )

    def _autotrade_enabled(self, now: datetime) -> bool:
        start = self.settings.paper_autotrade_start_utc
        return (
            self.settings.paper_autotrade
            and self.runtime.entries_allowed()
            and (start is None or now >= start)
        )

    def _mark_open_positions(self, snapshot: MarketSnapshot) -> None:
        for position in self.runtime.portfolio.positions.values():
            if position.state is PositionState.OPEN:
                self.executor.mark_to_market(position, snapshot)

    def _missing_mark_venues(self, snapshot: MarketSnapshot) -> tuple[str, ...]:
        """Identify open legs that cannot be marked before a shared cycle mutates state."""

        missing: set[str] = set()
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            for leg, instrument_type in zip(
                (position.leg_a, position.leg_b),
                (position.leg_a_type, position.leg_b_type),
                strict=True,
            ):
                if leg is None or instrument_type is None:
                    continue
                ticker = snapshot.ticker(leg.exchange, leg.symbol, instrument_type)
                if ticker is None or (
                    snapshot.captured_at - ticker.timestamp
                ).total_seconds() > self.executor.stale_seconds:
                    missing.add(leg.exchange)
        return tuple(sorted(missing))

    @staticmethod
    def _position_exposure_key(position: PaperPosition) -> str | None:
        if position.exposure_key:
            return position.exposure_key
        if position.leg_a is None or position.leg_b is None:
            return None
        leg_a_type = position.leg_a_type or position.leg_a.instrument_type
        leg_b_type = position.leg_b_type or position.leg_b.instrument_type
        if leg_a_type is None or leg_b_type is None:
            return None
        return canonical_exposure_key(
            position.asset,
            (position.leg_a.exchange, position.leg_a.symbol, str(leg_a_type)),
            (position.leg_b.exchange, position.leg_b.symbol, str(leg_b_type)),
        )

    def _register_open_position(self, position: PaperPosition) -> None:
        if position.opportunity_key:
            self._position_by_key[position.opportunity_key] = position.id
        exposure_key = self._position_exposure_key(position)
        if exposure_key is None:
            return
        position.exposure_key = exposure_key
        self._position_ids_by_exposure_key.setdefault(exposure_key, set()).add(
            position.id
        )

    def _unregister_open_position(self, position: PaperPosition) -> None:
        if position.opportunity_key:
            self._position_by_key.pop(position.opportunity_key, None)
        exposure_key = self._position_exposure_key(position)
        if exposure_key is None:
            return
        position_ids = self._position_ids_by_exposure_key.get(exposure_key)
        if position_ids is None:
            return
        position_ids.discard(position.id)
        if not position_ids:
            self._position_ids_by_exposure_key.pop(exposure_key, None)

    async def _restore_positions(self) -> None:
        async with self.session_factory() as session:
            snapshot = await session.scalar(
                select(PortfolioSnapshotRecord)
                .where(
                    PortfolioSnapshotRecord.simulation_version
                    == self.settings.paper_simulation_version,
                    PortfolioSnapshotRecord.snapshot_scope == "legacy",
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
                funding_payments = list(
                    (
                        await session.execute(
                            select(PaperFundingPaymentRecord).where(
                                PaperFundingPaymentRecord.position_id == position.id
                            )
                        )
                    )
                    .scalars()
                )
                position.pnl.funding_pnl = sum(
                    (Decimal(str(payment.pnl)) for payment in funding_payments),
                    Decimal("0"),
                )
                position.funding_events = len(funding_payments)
                for payment in funding_payments:
                    self._mark_funding_settled(
                        position,
                        payment.exchange,
                        payment.symbol,
                        payment.funding_timestamp,
                    )
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
                if position.state is PositionState.CLOSED:
                    self.runtime.portfolio.total_realized_pnl += position.pnl.total_pnl
                if position.state is PositionState.OPEN:
                    self._register_open_position(position)

    async def _restore_funding_ledger(self) -> None:
        """Project pre-existing durable payments before accepting a new cycle."""

        async with self.session_factory() as session:
            inserted = await backfill_paper_funding_ledger(
                session,
                simulation_version=self.settings.paper_simulation_version,
            )
        if inserted:
            logger.info(
                "paper_funding_ledger_backfilled",
                extra={"event": "paper_startup", "count": inserted},
            )

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
        legacy = self.runtime.portfolio.snapshot(timestamp=timestamp)
        combined = (
            self.combined_snapshot_provider(timestamp)
            if self.combined_snapshot_provider is not None
            else None
        )
        if (
            self.combined_snapshot_provider is not None
            and combined is None
        ):
            raise RuntimeError("combined paper snapshot provider returned no snapshot")
        if combined is not None and combined.simulation_version != legacy.simulation_version:
            raise RuntimeError("combined paper snapshot crossed simulation versions")
        async with self.session_factory() as session:
            for position in self.runtime.portfolio.positions.values():
                await save_paper_position(session, position)
            if combined is None:
                await save_portfolio_snapshot(session, legacy)
            else:
                await save_portfolio_snapshot(
                    session,
                    legacy,
                    snapshot_scope="legacy",
                    commit=False,
                )
                await save_portfolio_snapshot(
                    session,
                    combined,
                    snapshot_scope="combined",
                )

    def _funding_locked_capital(self) -> Decimal:
        return sum(
            (
                position.capital * Decimal("2")
                for position in self.runtime.portfolio.positions.values()
                if position.state
                in {
                    PositionState.OPENING,
                    PositionState.OPEN,
                    PositionState.CLOSING,
                }
                and is_funding_strategy(position.strategy)
            ),
            Decimal("0"),
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
            exposure_key = OpportunityDebouncer.exposure_key(opportunity)
            if (
                key in self._position_by_key
                or exposure_key in self._position_ids_by_exposure_key
            ):
                self._record_trade_rejection("duplicate_exposure", opportunity)
                continue
            if is_funding_strategy(opportunity.strategy):
                settlement_rate = next_settlement_rate(
                    opportunity,
                    snapshot,
                    snapshot.captured_at,
                )
                if (
                    settlement_rate is None
                    or settlement_rate < self.settings.paper_minimum_funding_rate
                ):
                    self._record_trade_rejection(
                        "minimum_funding_rate",
                        opportunity,
                    )
                    continue
                funding_locked = self._funding_locked_capital()
                allowed_quotes = [
                    quote
                    for quote in opportunity.size_quotes
                    if funding_locked + quote.capital * Decimal("2")
                    <= self.settings.paper_max_funding_capital_usd
                ]
                if not allowed_quotes:
                    self._record_trade_rejection("funding_cap", opportunity)
                    continue
                opportunity = opportunity.model_copy(
                    update={"size_quotes": allowed_quotes}
                )
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
                    self._record_trade_rejection(
                        "settlement_cost_coverage", opportunity
                    )
                    continue
                allocation = self.allocator.decide(
                    opportunity.model_copy(update={"size_quotes": settlement_quotes}),
                    self.runtime.portfolio,
                    # The fixed configured size belongs to the baseline. The
                    # candidate must be free to choose any profitable executable
                    # quote from the configured depth grid, including $100 when
                    # a larger quote loses its settlement edge to slippage.
                    minimum_capital=Decimal("0"),
                )
                capital = allocation.capital
            if capital <= 0:
                reason = (
                    allocation.risk_reasons[0]
                    if self.settings.paper_strategy_profile == "candidate"
                    and allocation.risk_reasons
                    else (
                        allocation.reason
                        if self.settings.paper_strategy_profile == "candidate"
                        else "no_viable_size_quote"
                    )
                )
                self._record_trade_rejection(
                    reason or "allocation",
                    opportunity,
                    risk_reasons=(
                        allocation.risk_reasons
                        if self.settings.paper_strategy_profile == "candidate"
                        else ()
                    ),
                )
                continue
            position = await self.executor.open(opportunity, capital, snapshot)
            if position.state is not PositionState.OPEN:
                self._record_trade_rejection("execution", opportunity)
                continue
            if self.settings.market_data_mode == "mock":
                due = snapshot.captured_at + timedelta(
                    seconds=self.settings.paper_settlement_interval_seconds
                )
                position.target_funding_events = {
                    self._funding_key(leg.exchange, leg.symbol): due
                    for leg in self._funding_legs(position)
                }
            else:
                position.target_funding_events = target_settlement_events(
                    opportunity, snapshot, snapshot.captured_at
                )
            position.target_settlements = tuple(
                sorted(set(position.target_funding_events.values()))
            )
            position.opportunity_key = key
            position.exposure_key = exposure_key
            venues = (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a)
            try:
                self.runtime.portfolio.allocate_position(position, venues, capital)
            except ValueError:
                self._record_trade_rejection("venue_balance", opportunity)
                continue
            self._register_open_position(position)
            open_count += 1

    def _record_trade_rejection(
        self,
        reason: str,
        opportunity: Opportunity,
        *,
        risk_reasons: tuple[str, ...] = (),
    ) -> None:
        if self.acceptance_collector is not None:
            self.acceptance_collector.record_risk_rejection()
        profile = self.settings.paper_strategy_profile
        paper_trade_rejections_total.labels(profile, reason).inc()
        logger.info(
            "paper_trade_rejected",
            extra={
                "event": "paper_trade_rejected",
                "profile": profile,
                "reason": reason,
                "risk_reasons": risk_reasons,
                "asset": opportunity.asset,
                "strategy": str(opportunity.strategy),
                "venue_a": opportunity.venue_a,
                "venue_b": opportunity.venue_b,
                "opportunity_id": opportunity.id,
            },
        )

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
            if position.opened_at is None:
                continue
            if position.state is PositionState.OPEN:
                effective_end = now
            elif (
                position.state is PositionState.CLOSED
                and position.closed_at is not None
                and self._funding_reconciliation_active(position, now)
            ):
                effective_end = min(now, position.closed_at)
            else:
                continue
            for leg in self._funding_legs(position):
                history = history_by_key.get((leg.exchange, leg.symbol), [])
                current = funding_by_key.get((leg.exchange, leg.symbol))
                for event in sorted(history, key=lambda item: item.funding_timestamp):
                    if not position.opened_at < event.funding_timestamp <= effective_end:
                        continue
                    marker = self._funding_event_marker(
                        event.exchange, event.symbol, event.funding_timestamp
                    )
                    if marker in position.settled_funding_events:
                        continue
                    interval_hours = _history_interval_hours(history, event) or (
                        current.funding_interval_hours
                        if current is not None
                        else Decimal("8")
                    )
                    event_funding = FundingSnapshot(
                        exchange=event.exchange,
                        symbol=event.symbol,
                        funding_rate=event.funding_rate,
                        funding_interval_hours=interval_hours,
                        mark_price=event.mark_price,
                        timestamp=event.funding_timestamp,
                    )
                    self._funding_settlement_assets[(leg.exchange, leg.symbol)] = (
                        self._funding_settlement_asset(snapshot, leg)
                    )
                    await self._apply_funding_event(
                        position,
                        leg,
                        event_funding,
                        history_event=event,
                    )
            self._complete_funding_reconciliation_after_poll(position, snapshot)

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
                self._funding_settlement_assets[(leg.exchange, leg.symbol)] = (
                    self._funding_settlement_asset(snapshot, leg)
                )
                await self._apply_funding_event(
                    position,
                    leg,
                    funding.model_copy(update={"timestamp": due}),
                )
                self._next_funding_due[due_key] = due + timedelta(
                    seconds=self.settings.paper_settlement_interval_seconds
                )

    @staticmethod
    def _funding_settlement_asset(
        snapshot: MarketSnapshot,
        leg: PaperFill,
    ) -> str:
        instrument = snapshot.instrument(
            leg.exchange,
            leg.symbol,
            leg.instrument_type,
        )
        if instrument is not None:
            settlement_asset = instrument.settlement_asset or instrument.quote_asset
            if settlement_asset.strip():
                return settlement_asset
        return infer_funding_settlement_asset(leg.exchange, leg.symbol)

    async def _apply_funding_event(
        self,
        position: PaperPosition,
        leg: PaperFill,
        funding: FundingSnapshot,
        *,
        history_event: FundingHistoryPoint | None = None,
    ) -> None:
        if position.opened_at is None or funding.timestamp <= position.opened_at:
            raise ValueError("funding event is outside the position holding window")
        if position.state is PositionState.CLOSED and (
            position.closed_at is None or funding.timestamp > position.closed_at
        ):
            raise ValueError("funding event is outside the position holding window")
        if position.state not in {PositionState.OPEN, PositionState.CLOSED}:
            raise ValueError("funding event requires an open or closed position")
        async with self._funding_apply_lock:
            await self._apply_funding_event_locked(
                position,
                leg,
                funding,
                history_event=history_event,
            )

    async def _apply_funding_event_locked(
        self,
        position: PaperPosition,
        leg: PaperFill,
        funding: FundingSnapshot,
        *,
        history_event: FundingHistoryPoint | None,
    ) -> None:
        marker = self._funding_event_marker(
            funding.exchange, funding.symbol, funding.timestamp
        )
        if marker in position.settled_funding_events:
            return
        calculated_pnl = self.runtime.portfolio.calculate_funding_pnl(
            leg.side, position.capital, funding.funding_rate
        )
        durable_settlement_asset = self._funding_settlement_assets.get(
            (leg.exchange, leg.symbol)
        ) or infer_funding_settlement_asset(
            leg.exchange,
            leg.symbol,
        )
        async with self.session_factory() as session:
            payment = await save_paper_funding_payment(
                session,
                position.id,
                funding,
                position.capital,
                calculated_pnl,
                history_event=history_event,
                ledger_asset=durable_settlement_asset,
                ledger_strategy_id=position.strategy or "LEGACY_FUNDING",
            )
        durable_funding = FundingSnapshot(
            exchange=payment.exchange,
            symbol=payment.symbol,
            funding_rate=Decimal(str(payment.funding_rate)),
            funding_interval_hours=funding.funding_interval_hours,
            timestamp=payment.funding_timestamp,
        )
        marker = self._funding_event_marker(
            payment.exchange, payment.symbol, durable_funding.timestamp
        )
        if marker not in position.settled_funding_events:
            self.runtime.portfolio.settle_recorded_funding(
                position.id,
                durable_funding,
                Decimal(str(payment.pnl)),
            )
            position.funding_events += 1
        self._mark_funding_settled(
            position,
            payment.exchange,
            payment.symbol,
            durable_funding.timestamp,
        )

    @staticmethod
    def _funding_key(exchange: str, symbol: str) -> str:
        return f"{exchange}|{symbol}"

    @classmethod
    def _funding_event_marker(
        cls, exchange: str, symbol: str, timestamp: datetime
    ) -> str:
        normalized = (
            timestamp.replace(tzinfo=UTC)
            if timestamp.tzinfo is None
            else timestamp.astimezone(UTC)
        )
        return (
            f"{cls._funding_key(exchange, symbol)}|"
            f"{normalized.isoformat(timespec='microseconds')}"
        )

    @classmethod
    def _mark_funding_settled(
        cls,
        position: PaperPosition,
        exchange: str,
        symbol: str,
        timestamp: datetime,
    ) -> None:
        normalized = (
            timestamp.replace(tzinfo=UTC)
            if timestamp.tzinfo is None
            else timestamp.astimezone(UTC)
        )
        position.settled_funding_events.add(
            cls._funding_event_marker(exchange, symbol, normalized)
        )
        key = cls._funding_key(exchange, symbol)
        previous = position.settled_funding_at.get(key)
        if previous is None or normalized > previous:
            position.settled_funding_at[key] = normalized

    def _schedule_funding_reconciliation(
        self, position: PaperPosition, observed_at: datetime
    ) -> None:
        if (
            self.settings.market_data_mode == "mock"
            or position.state is not PositionState.CLOSED
            or position.closed_at is None
            or position.funding_reconciliation_until is not None
        ):
            return
        deadline = position.closed_at + timedelta(
            seconds=self.settings.paper_funding_reconciliation_window_seconds
        )
        position.funding_reconciliation_until = deadline
        position.funding_reconciliation_next_poll_at = min(observed_at, deadline)
        position.funding_reconciliation_completed_at = None
        position.funding_reconciliation_post_deadline_attempts = 0
        position.funding_reconciliation_failed_at = None
        position.funding_reconciliation_failure_reason = None

    def _funding_reconciliation_active(
        self, position: PaperPosition, _observed_at: datetime
    ) -> bool:
        if position.state is not PositionState.CLOSED or position.closed_at is None:
            return False
        if position.funding_reconciliation_completed_at is not None:
            return False
        if position.funding_reconciliation_failed_at is not None:
            return False
        deadline = position.funding_reconciliation_until
        if deadline is None:
            return False
        return True

    def _complete_funding_reconciliation_after_poll(
        self, position: PaperPosition, snapshot: MarketSnapshot
    ) -> None:
        if position.state is not PositionState.CLOSED:
            return
        deadline = position.funding_reconciliation_until
        if deadline is None or snapshot.captured_at < deadline:
            return
        required_keys = {
            (leg.exchange, leg.symbol) for leg in self._funding_legs(position)
        }
        if not required_keys:
            return
        refreshed_at = snapshot.funding_history_refreshed
        if any(
            key not in refreshed_at
            or self._normalize_funding_timestamp(refreshed_at[key]) < deadline
            for key in required_keys
        ):
            return
        position.funding_reconciliation_completed_at = snapshot.captured_at
        position.funding_reconciliation_next_poll_at = None

    def _fail_funding_reconciliation(
        self, position: PaperPosition, observed_at: datetime
    ) -> None:
        position.funding_reconciliation_failed_at = observed_at
        position.funding_reconciliation_failure_reason = (
            "post_deadline_attempts_exhausted"
        )
        position.funding_reconciliation_next_poll_at = None
        self._pending_funding_reconciliation_failures[position.id] = position
        logger.error(
            "paper_funding_reconciliation_exhausted",
            extra={
                "event": "paper_funding_reconciliation",
                "position_id": position.id,
                "attempts": position.funding_reconciliation_post_deadline_attempts,
            },
        )

    async def _persist_pending_funding_reconciliation_failures(self) -> None:
        for position_id, position in tuple(
            self._pending_funding_reconciliation_failures.items()
        ):
            async with self.session_factory() as session:
                await save_paper_position(session, position, commit=False)
                await save_paper_runtime_incident(
                    session,
                    (self.settings.paper_simulation_version,),
                    "funding_reconciliation",
                    FundingReconciliationExhaustedError.__name__,
                    datetime.now(UTC),
                    commit=False,
                )
                await session.commit()
            self._pending_funding_reconciliation_failures.pop(position_id, None)

    @staticmethod
    def _normalize_funding_timestamp(timestamp: datetime) -> datetime:
        return (
            timestamp.replace(tzinfo=UTC)
            if timestamp.tzinfo is None
            else timestamp.astimezone(UTC)
        )

    def _due_funding_symbols(self, now: datetime) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for position in self.runtime.portfolio.positions.values():
            if self._funding_reconciliation_active(position, now):
                next_poll_at = position.funding_reconciliation_next_poll_at
                if next_poll_at is None or now >= next_poll_at:
                    deadline = position.funding_reconciliation_until
                    if deadline is not None and now >= deadline:
                        attempts = (
                            position.funding_reconciliation_post_deadline_attempts
                        )
                        max_attempts = (
                            self.settings.paper_funding_reconciliation_max_post_deadline_attempts
                        )
                        if attempts >= max_attempts:
                            self._fail_funding_reconciliation(position, now)
                            continue
                        position.funding_reconciliation_post_deadline_attempts += 1
                    for leg in self._funding_legs(position):
                        result.setdefault(leg.exchange, []).append(leg.symbol)
                    next_poll_at = now + timedelta(
                        seconds=self.settings.paper_funding_reconciliation_poll_seconds
                    )
                    if deadline is not None and now < deadline:
                        position.funding_reconciliation_next_poll_at = min(
                            next_poll_at, deadline
                        )
                    else:
                        position.funding_reconciliation_next_poll_at = next_poll_at
                continue
            if position.state is not PositionState.OPEN:
                continue
            if position.target_funding_events:
                for key, target in position.target_funding_events.items():
                    settled = position.settled_funding_at.get(key)
                    if target > now or (settled is not None and settled >= target):
                        continue
                    exchange, symbol = key.split("|", maxsplit=1)
                    result.setdefault(exchange, []).append(symbol)
                continue
            if any(target <= now for target in position.target_settlements):
                for leg in self._funding_legs(position):
                    result.setdefault(leg.exchange, []).append(leg.symbol)
        return {
            venue: list(dict.fromkeys(symbols)) for venue, symbols in result.items()
        }

    def _required_funding_symbols(
        self, now: datetime | None = None
    ) -> dict[str, list[str]]:
        observed_at = now or datetime.now(UTC)
        result: dict[str, list[str]] = {
            venue: list(symbols)
            for venue, symbols in self._candidate_history_symbols.items()
        }
        for position in self.runtime.portfolio.positions.values():
            if position.state is not PositionState.OPEN and not (
                self._funding_reconciliation_active(position, observed_at)
            ):
                continue
            for leg in self._funding_legs(position):
                result.setdefault(leg.exchange, []).append(leg.symbol)
        return {
            venue: list(dict.fromkeys(symbols)) for venue, symbols in result.items()
        }

    def _open_position_orderbook_symbols(
        self,
    ) -> dict[str, list[tuple[str, InstrumentType]]]:
        result: dict[str, set[tuple[str, InstrumentType]]] = {}
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
            target_due = any(
                target <= now for target in position.target_settlements
            )
            pending_target_funding = bool(
                self._pending_target_funding(position, now)
            )
            target_received = target_due and not pending_target_funding and (
                bool(position.target_funding_events) or position.funding_events > 0
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
                    position.target_funding_events = target_settlement_events(
                        current, snapshot, now
                    )
                    position.target_settlements = tuple(
                        sorted(set(position.target_funding_events.values()))
                    )
            target_exit = target_received and not continue_after_target
            exit_reason: str | None = None
            if max_hold:
                exit_reason = "max_hold"
            elif self.settings.paper_strategy_profile == "candidate":
                exit_reason = next(
                    (
                        reason
                        for reason, triggered in (
                            ("edge_gone", edge_gone),
                            ("funding_reversed", funding_reversed),
                            ("adverse_basis", adverse_basis),
                            ("market_degraded", market_degraded),
                            ("target_settlement", target_exit),
                        )
                        if triggered
                    ),
                    None,
                )
            if exit_reason is not None and position.exit_requested_at is None:
                # Latch the first risk/strategy exit request. A degraded book can
                # make an immediate fill impossible; persisting this state makes
                # the runner retry once executable liquidity returns, including
                # after a process restart, without inventing a paper fill.
                position.exit_requested_at = now
                position.exit_requested_reason = exit_reason
            if position.exit_requested_at is None:
                continue
            await self.executor.close(position, snapshot)
            if position.state is not PositionState.CLOSED:
                continue
            self.runtime.portfolio.close_position(position.id)
            self._schedule_funding_reconciliation(position, now)
            self._unregister_open_position(position)

    @staticmethod
    def _pending_target_funding(
        position: PaperPosition, now: datetime
    ) -> tuple[str, ...]:
        if not position.target_funding_events:
            return ()
        return tuple(
            key
            for key, target in position.target_funding_events.items()
            if target <= now
            and (
                position.settled_funding_at.get(key) is None
                or position.settled_funding_at[key] < target
            )
        )

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
        self.last_completed_snapshot: MarketSnapshot | None = None

    async def restore(self) -> None:
        await self.candidate.restore(restore_history=True)
        await self.baseline.restore(restore_history=False)

    async def run(self) -> None:
        await self.restore()
        await _persist_runner_start(
            self.candidate.session_factory,
            (
                self.candidate.settings.paper_simulation_version,
                self.baseline.settings.paper_simulation_version,
            ),
        )
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                snapshot = await self.candidate.collect_snapshot((self.baseline,))
                await self.process_snapshot(snapshot)
                paper_runner_cycle_duration_seconds.observe(time.monotonic() - started)
                paper_runner_cycles_total.inc()
                paper_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
                if not self.stop_event.is_set():
                    await self.candidate.daily_report.notify_started()
            except asyncio.CancelledError:
                raise
            except IncompleteMarketSnapshotError as error:
                paper_market_cycles_skipped_total.labels("incomplete_venue").inc()
                logger.warning(
                    "paper_comparison_market_cycle_skipped",
                    extra={
                        "event": "paper_market_gap",
                        "exchanges": list(error.venues),
                    },
                )
            except Exception as error:
                paper_runner_errors_total.inc()
                logger.exception("paper_comparison_cycle_failed")
                await _persist_runtime_incident(
                    self.candidate.session_factory,
                    (
                        self.candidate.settings.paper_simulation_version,
                        self.baseline.settings.paper_simulation_version,
                    ),
                    "comparison_cycle",
                    error,
                )
            await _wait_for_next_cycle(
                self.stop_event,
                started,
                self.candidate.settings.paper_loop_interval_seconds,
            )

    async def process_snapshot(self, snapshot: MarketSnapshot) -> None:
        """Publish a shared completion marker only after both ledgers finish."""

        await asyncio.gather(
            self.candidate.process_snapshot(snapshot, persist_market=True),
            self.baseline.process_snapshot(snapshot, persist_market=False),
        )
        self.last_completed_snapshot = snapshot

    async def stop(self) -> None:
        self.stop_event.set()

    async def close(self) -> None:
        await self.stop()
        errors: list[Exception] = []
        try:
            await self.baseline.close(send_stopped=False)
        except Exception as error:
            errors.append(error)
            logger.exception(
                "paper_comparison_shutdown_component_failed",
                extra={"component": "baseline"},
            )
        try:
            await self.candidate.close(send_stopped=not errors)
        except Exception as error:
            errors.append(error)
            logger.exception(
                "paper_comparison_shutdown_component_failed",
                extra={"component": "candidate"},
            )
        if errors:
            raise ExceptionGroup("paper comparison shutdown failed", errors)


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
) -> Decimal | None:
    ordered = sorted(history, key=lambda item: item.funding_timestamp)
    index = ordered.index(event)
    if index > 0:
        seconds = (event.funding_timestamp - ordered[index - 1].funding_timestamp).total_seconds()
        if seconds > 0:
            return Decimal(str(seconds / 3600))
    return None


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
