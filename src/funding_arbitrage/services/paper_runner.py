"""Restartable paper-trading loop: one shared market snapshot, independent series.

Each cycle:

1. collect instruments (cached), tickers, and funding from every venue concurrently;
2. pre-scan on economics only, then fetch fresh order books and funding history for
   the few markets a decision may need (open legs and top candidates);
3. full scan and opportunity confirmation (shared by all series);
4. per series: settle funding from venue history, exit, enter, check the invariant;
5. persist every series' ledger changes and the cycle record in one transaction.

Nothing here can send an exchange order: the adapters are public-data only and the
simulator fills against the fetched books.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import perf_counter

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.repositories.market_data import (
    insert_funding_snapshots,
    insert_orderbooks,
    insert_tickers,
    prune_market_data,
    upsert_exchanges,
    upsert_funding_history,
    upsert_instruments,
    upsert_opportunities,
)
from funding_arbitrage.database.repositories.paper import (
    CycleRecord,
    ensure_series,
    heartbeat_runner_session,
    insert_account_snapshots,
    insert_cycle,
    load_series_state,
    persist_account_changes,
    start_runner_session,
    stop_runner_session,
)
from funding_arbitrage.exchanges.base.models import InstrumentType, MarketKey, OrderBook
from funding_arbitrage.execution.paper import (
    SIMULATOR_VERSION,
    FillRejected,
    PaperExecutionSimulator,
)
from funding_arbitrage.market_data.collector import FundingKey, MarketDataCollector, MarketSnapshot
from funding_arbitrage.monitoring.metrics import (
    funding_pnl_total,
    paper_cash,
    paper_close_deferrals_total,
    paper_entry_rejections_total,
    paper_equity,
    paper_invariant_diff,
    paper_locked_capital,
    paper_persistence_failures_total,
    paper_pnl_total,
    paper_positions_open,
    paper_runner_cycle_seconds,
    paper_runner_cycles_total,
    paper_runner_errors_total,
    paper_runner_last_cycle_timestamp,
)
from funding_arbitrage.notifications.telegram import TelegramNotifier
from funding_arbitrage.opportunity.filters import (
    FilterStage,
    OpportunityFilterConfig,
    passes_filters,
)
from funding_arbitrage.opportunity.models import Opportunity, OpportunityStatus, StrategyName
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE, AccountSnapshot, PaperAccount
from funding_arbitrage.portfolio.position import PaperPosition
from funding_arbitrage.services.daily_report import (
    DailyReportService,
    format_start_message,
    format_stop_message,
)
from funding_arbitrage.services.funding_settlement import FundingSettler
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import (
    PaperSeriesFile,
    SeriesConfig,
    load_series_file,
    simulation_context,
)

logger = logging.getLogger(__name__)

_RUNNER_LOCK_KEY = 0x46554E44494E47  # "FUNDING"
_PRUNE_EVERY = timedelta(hours=24)
_CAP_HEADROOM = Decimal("0.995")


class RunnerAlreadyActive(RuntimeError):
    """Another runner process holds the database lock."""


class RunnerLock:
    """Session-level PostgreSQL advisory lock: one runner may write a database."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self._connection: AsyncConnection | None = None

    async def acquire(self) -> bool:
        connection = await self.engine.connect()
        acquired = await connection.scalar(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": _RUNNER_LOCK_KEY}
        )
        await connection.commit()
        if not acquired:
            await connection.close()
            return False
        self._connection = connection
        return True

    async def release(self) -> None:
        if self._connection is None:
            return
        try:
            await self._connection.execute(
                text("SELECT pg_advisory_unlock(:key)"), {"key": _RUNNER_LOCK_KEY}
            )
            await self._connection.commit()
        finally:
            await self._connection.close()
            self._connection = None


@dataclass
class SeriesRuntime:
    config: SeriesConfig
    account: PaperAccount
    entry_filter: OpportunityFilterConfig
    close_deferred_since: dict[str, datetime] = field(default_factory=dict)
    rejections: Counter[str] = field(default_factory=Counter)


class PaperTestRunner:
    """Scan, paper-fill, settle funding, close, and persist in one safe loop."""

    def __init__(
        self,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        series_file: PaperSeriesFile | None = None,
        engine: AsyncEngine | None = None,
        clock: Callable[[], datetime] | None = None,
        notifier: TelegramNotifier | None = None,
        release_hash: str | None = None,
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self.clock = clock or (lambda: datetime.now(UTC))
        self.series_file = (
            series_file or runtime.series_file or load_series_file(settings.paper_series_file)
        )
        self.collector = MarketDataCollector(
            runtime.adapters.values(),
            venue_timeout_seconds=settings.market_venue_timeout_seconds,
            instrument_refresh_seconds=settings.market_instrument_refresh_seconds,
            history_ttl_seconds=settings.market_history_ttl_seconds,
            book_depth=settings.paper_book_depth,
            clock=self.clock,
        )
        self.simulator = PaperExecutionSimulator(
            settings.fee_schedules,
            max_book_age_seconds=settings.paper_max_book_age_seconds,
            max_slippage_fraction=settings.paper_max_fill_slippage_percent / Decimal("100"),
        )
        self.settler = FundingSettler(
            self.collector.funding_history_between,
            grace_seconds=settings.paper_funding_grace_seconds,
            poll_seconds=settings.paper_funding_poll_seconds,
        )
        self.notifier = notifier or TelegramNotifier(
            settings.telegram_bot_token,
            settings.telegram_chat_id,
            settings.telegram_api_base_url,
            settings.request_timeout_seconds,
        )
        primary = self.series_file.primary
        self.daily_report = DailyReportService(
            settings,
            session_factory,
            series_id=primary.label,
            series_name=primary.name,
            notifier=self.notifier,
        )
        self.lock = RunnerLock(engine) if engine is not None else None
        self.release_hash = release_hash
        self.series: dict[str, SeriesRuntime] = {}
        self.stop_event = asyncio.Event()
        self.started = False
        self.fatal_error: str | None = None
        self.persistence_ok = True
        self.last_success_at: datetime | None = None
        self.last_cycle: CycleRecord | None = None
        self._session_id: int | None = None
        self._recorded: set[str] = set()
        self._last_snapshot_persist: datetime | None = None
        self._last_funding_persist: datetime | None = None
        self._persisted_instruments: dict[str, datetime] = {}
        self._opportunity_persisted_at: dict[str, datetime] = {}
        self._last_prune: datetime | None = None
        runtime.runner = self

    # ================================================================ lifecycle
    async def start(self) -> None:
        if self.lock is not None and not await self.lock.acquire():
            raise RunnerAlreadyActive(
                "another paper runner already writes to this database; refusing to start"
            )
        now = self.clock()
        context = simulation_context(self.settings)
        async with self.session_factory() as session:
            for config in self.series_file.series:
                # A series starts with its first paper trade window, not with observation.
                if await ensure_series(
                    session,
                    series_id=config.label,
                    name=config.name,
                    simulator_version=SIMULATOR_VERSION,
                    config_hash=config.config_hash(SIMULATOR_VERSION, context),
                    config=config.identity(SIMULATOR_VERSION, context),
                    initial_balance=config.initial_balance_usdt,
                    now=now,
                    create=self.settings.paper_autotrade,
                ):
                    self._recorded.add(config.label)
            await session.commit()
            for config in self.series_file.series:
                restored = await load_series_state(session, config.label)
                account = PaperAccount(
                    config.label,
                    config.initial_balance_usdt,
                    totals=restored.totals,
                    open_positions=restored.open_positions,
                    closed_positions_pnl=restored.closed_positions_pnl,
                    closed_slippage=restored.closed_slippage,
                )
                diff = account.invariant_diff()
                if diff > INVARIANT_TOLERANCE:
                    account.halted_reason = "restore_invariant_violation"
                    logger.error(
                        "paper_series_restore_invariant_violation",
                        extra={"series": config.label, "invariant_diff": str(diff)},
                    )
                self.series[config.label] = SeriesRuntime(
                    config=config,
                    account=account,
                    entry_filter=config.filter_config(self.settings.paper_confirmation_seconds),
                )
            self._session_id, previous = await start_runner_session(
                session,
                now=now,
                simulator_version=SIMULATOR_VERSION,
                release_hash=self.release_hash,
                autotrade=self.settings.paper_autotrade,
                series=[config.label for config in self.series_file.series],
            )
            await session.commit()
        self.runtime.accounts = {label: item.account for label, item in self.series.items()}
        self.started = True
        logger.info(
            "paper_runner_started",
            extra={
                "series": list(self.series),
                "autotrade": self.settings.paper_autotrade,
                "simulator_version": SIMULATOR_VERSION,
            },
        )
        await self._notify(
            "start",
            format_start_message(
                self.series_file,
                {label: item.account.snapshot(now) for label, item in self.series.items()},
                autotrade=self.settings.paper_autotrade,
                simulator_version=SIMULATOR_VERSION,
                previous=previous,
                timezone=self.daily_report.timezone,
            ),
        )

    async def run(self) -> None:
        try:
            await self.start()
        except Exception as exc:
            self.fatal_error = f"{type(exc).__name__}: {exc}"
            logger.exception("paper_runner_start_failed")
            if self.lock is not None:
                await self.lock.release()
            return
        try:
            while not self.stop_event.is_set():
                await self.cycle()
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=self.settings.paper_loop_interval_seconds
                    )
                except TimeoutError:
                    continue
        finally:
            await self.shutdown()

    async def stop(self) -> None:
        self.stop_event.set()

    async def shutdown(self) -> None:
        if not self.started:
            await self.notifier.close()
            await self.daily_report.close()
            return
        self.started = False
        now = self.clock()
        try:
            async with self.session_factory() as session:
                for item in self.series.values():
                    await persist_account_changes(session, item.account, now)
                await insert_account_snapshots(
                    session,
                    [
                        item.account.snapshot(now)
                        for label, item in self.series.items()
                        if label in self._recorded
                    ],
                )
                if self._session_id is not None:
                    await stop_runner_session(session, self._session_id, now)
                await session.commit()
            for item in self.series.values():
                item.account.clear_pending()
        except Exception:
            logger.exception("paper_runner_final_persist_failed")
        await self._notify(
            "stop",
            format_stop_message(
                {label: item.account.snapshot(now) for label, item in self.series.items()},
                primary=self.series_file.primary.label,
            ),
        )
        await self.notifier.close()
        await self.daily_report.close()
        if self.lock is not None:
            await self.lock.release()
        logger.info("paper_runner_stopped")

    async def _notify(self, kind: str, message: str) -> None:
        if self.settings.telegram_enabled and self.notifier.configured:
            await self.notifier.send_notice(kind, message)

    # ==================================================================== cycle
    async def cycle(self) -> CycleRecord:
        started = self.clock()
        timer = perf_counter()
        stage = "collect"
        incidents: list[str] = []
        opportunities: list[Opportunity] = []
        books_fetched = 0
        snapshot: MarketSnapshot | None = None
        try:
            self.collector.set_priority_symbols(self._priority_symbols())
            snapshot = await self.collector.collect_once()
            stage = "scan"
            engine = self.runtime.opportunity_engine
            pre = await asyncio.to_thread(engine.scan, snapshot, FilterStage.PRE)
            stage = "books"
            book_keys, history_keys = self._decision_inputs(pre)
            failures = await self.collector.fetch_orderbooks(snapshot, book_keys)
            books_fetched = len(book_keys) - len(failures)
            fetched_history = await self.collector.ensure_funding_history(
                history_keys, self.settings.paper_history_requests_per_cycle
            )
            snapshot = dataclasses.replace(snapshot, funding_history=self.collector.history_view())
            stage = "scan"
            opportunities = await asyncio.to_thread(engine.scan, snapshot, FilterStage.FULL)
            self.runtime.update_market(snapshot, opportunities)
            stage = "series"
            decided_at = self.clock()
            for item in self.series.values():
                incidents.extend(
                    await self._process_series(item, snapshot, opportunities, decided_at)
                )
            stage = "persist"
            evidence = self._evidence_books(snapshot)
            venues_failed = sorted(
                name for name, state in snapshot.venues.items() if not state.collected
            )
            cycle = CycleRecord(
                started_at=started,
                finished_at=self.clock(),
                status="degraded" if venues_failed or not self.persistence_ok else "ok",
                venues_ok=sorted(
                    name for name, state in snapshot.venues.items() if state.collected
                ),
                venues_failed=venues_failed,
                opportunities=len(opportunities),
                books_fetched=books_fetched,
                autotrade=self.settings.paper_autotrade,
                incidents=incidents,
                stage=None,
            )
            if await self._persist_paper(decided_at, cycle):
                self.last_success_at = cycle.finished_at
                paper_runner_last_cycle_timestamp.set(cycle.finished_at.timestamp())
            await self._persist_market(
                decided_at, snapshot, opportunities, evidence, fetched_history
            )
            stage = "report"
            await self.daily_report.check_and_send(decided_at)
            paper_runner_cycles_total.inc()
            self.last_cycle = cycle
            return cycle
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            paper_runner_errors_total.labels(stage).inc()
            logger.exception("paper_test_cycle_failed", extra={"stage": stage})
            cycle = CycleRecord(
                started_at=started,
                finished_at=self.clock(),
                status="error",
                stage=stage,
                error=f"{type(exc).__name__}: {exc}",
                venues_ok=sorted(
                    name
                    for name, state in (snapshot.venues if snapshot else {}).items()
                    if state.collected
                ),
                venues_failed=sorted(
                    name
                    for name, state in (snapshot.venues if snapshot else {}).items()
                    if not state.collected
                ),
                opportunities=len(opportunities),
                books_fetched=books_fetched,
                autotrade=self.settings.paper_autotrade,
                incidents=incidents,
            )
            await self._persist_error_cycle(cycle)
            self.last_cycle = cycle
            return cycle
        finally:
            paper_runner_cycle_seconds.observe(perf_counter() - timer)

    # ------------------------------------------------------------ decisions
    def _priority_symbols(self) -> dict[str, set[str]]:
        symbols: dict[str, set[str]] = {}
        for item in self.series.values():
            for position in item.account.positions.values():
                for leg in position.legs:
                    if leg.is_perpetual:
                        symbols.setdefault(leg.exchange, set()).add(leg.symbol)
        return symbols

    def _decision_inputs(self, pre: list[Opportunity]) -> tuple[list[MarketKey], list[FundingKey]]:
        """Books for every open leg and the top candidates any series could take."""

        keys: list[MarketKey] = []
        for item in self.series.values():
            for position in item.account.positions.values():
                keys.extend(
                    MarketKey(leg.exchange, leg.instrument_type, leg.symbol)
                    for leg in position.legs
                )
        history: list[FundingKey] = []
        chosen = 0
        for opportunity in pre:
            if chosen >= self.settings.paper_book_candidates_per_cycle:
                break
            if not any(self._series_may_take(item, opportunity) for item in self.series.values()):
                continue
            chosen += 1
            for plan in self.simulator.legs_for(opportunity):
                keys.append(MarketKey(plan.exchange, plan.instrument_type, plan.symbol))
                if plan.instrument_type is InstrumentType.PERPETUAL:
                    history.append((plan.exchange, plan.symbol))
        return keys, [key for key in history if not self.collector.history_fresh(key)]

    def _series_may_take(self, item: SeriesRuntime, opportunity: Opportunity) -> bool:
        config = item.config
        if StrategyName(opportunity.strategy) not in config.strategies:
            return False
        if (
            opportunity.funding_rate_8h < config.entry.min_funding_rate_8h
            or opportunity.net_apr < config.entry.min_net_apr
        ):
            return False
        account = item.account
        held = list(account.positions.values())
        if any(position.opportunity_key == opportunity.key for position in held):
            return False
        if (
            sum(position.asset == opportunity.asset for position in held)
            >= config.max_positions_per_asset
        ):
            return False
        if not self.settings.paper_autotrade:
            # Observe mode still evaluates books and history for the best markets.
            return True
        return (
            len(account.positions) < config.max_open_positions
            and account.exposure + config.minimum_notional <= config.max_total_notional_usdt
        )

    async def _process_series(
        self,
        item: SeriesRuntime,
        snapshot: MarketSnapshot,
        opportunities: list[Opportunity],
        now: datetime,
    ) -> list[str]:
        account = item.account
        incidents: list[str] = []
        for position in list(account.positions.values()):
            self.settler.observe(position, snapshot, now)
            incidents.extend(await self.settler.settle(account, position, snapshot, now))
        for position in list(account.positions.values()):
            incidents.extend(self._maybe_close(item, position, snapshot, now))
        if self.settings.paper_autotrade and account.halted_reason is None and self.persistence_ok:
            self._open_entries(item, snapshot, opportunities, now)
        diff = account.invariant_diff()
        paper_invariant_diff.labels(account.series_id).set(float(diff))
        if diff > INVARIANT_TOLERANCE and account.halted_reason is None:
            account.halted_reason = "invariant_violation"
            incidents.append(f"invariant_violation:{account.series_id}:{diff}")
            logger.error(
                "paper_invariant_violation",
                extra={"series": account.series_id, "invariant_diff": str(diff)},
            )
        return incidents

    def _current_edge(self, position: PaperPosition, snapshot: MarketSnapshot) -> Decimal | None:
        """Funding per 8h the position currently earns (None when a feed is missing)."""

        edge = Decimal("0")
        for _, leg in position.perpetual_legs:
            funding = snapshot.funding_for(leg.exchange, leg.symbol)
            if funding is None or not snapshot.venue_collected(leg.exchange):
                return None
            edge += -leg.direction * funding.funding_rate_8h
        return edge

    def _exit_reason(
        self, config: SeriesConfig, position: PaperPosition, snapshot: MarketSnapshot, now: datetime
    ) -> str | None:
        held_hours = Decimal(str((now - position.opened_at).total_seconds())) / Decimal("3600")
        if held_hours >= config.exit.max_hold_hours:
            return "max_hold"
        edge = self._current_edge(position, snapshot)
        if edge is None:
            return None
        if edge < config.exit.exit_funding_rate_8h:
            position.low_edge_streak += 1
        else:
            position.low_edge_streak = 0
        if (
            held_hours >= config.exit.min_hold_hours
            and position.low_edge_streak >= config.exit.exit_confirmations
        ):
            return "funding_edge_decay"
        return None

    def _maybe_close(
        self, item: SeriesRuntime, position: PaperPosition, snapshot: MarketSnapshot, now: datetime
    ) -> list[str]:
        reason = self._exit_reason(item.config, position, snapshot, now)
        if reason is None:
            item.close_deferred_since.pop(position.id, None)
            return []
        if position.funding_due(now):
            # Book the settlement that already happened before leaving the position.
            return []
        try:
            fills = self.simulator.close(position, snapshot, now)
        except FillRejected as exc:
            since = item.close_deferred_since.setdefault(position.id, now)
            paper_close_deferrals_total.labels(item.account.series_id).inc()
            waited = (now - since).total_seconds()
            if waited >= self.settings.paper_close_defer_alert_seconds:
                logger.warning(
                    "paper_close_deferred",
                    extra={
                        "series": item.account.series_id,
                        "position_id": position.id,
                        "reason": exc.reason,
                        "waited_seconds": int(waited),
                    },
                )
                return [f"close_deferred:{position.id}:{exc.reason}"]
            return []
        item.close_deferred_since.pop(position.id, None)
        booked = item.account.close_position(position, fills, reason, now)
        logger.info(
            "paper_position_closed",
            extra={
                "series": item.account.series_id,
                "position_id": position.id,
                "asset": position.asset,
                "reason": reason,
                "booked_pnl": str(booked),
            },
        )
        return []

    def _open_entries(
        self,
        item: SeriesRuntime,
        snapshot: MarketSnapshot,
        opportunities: list[Opportunity],
        now: datetime,
    ) -> None:
        config = item.config
        account = item.account
        held_keys = {position.opportunity_key for position in account.positions.values()}
        assets = Counter(position.asset for position in account.positions.values())
        for opportunity in opportunities:
            if len(account.positions) >= config.max_open_positions:
                return
            room = config.max_total_notional_usdt - account.exposure
            if room < config.minimum_notional:
                return
            if opportunity.status != OpportunityStatus.CONFIRMED:
                continue
            if StrategyName(opportunity.strategy) not in config.strategies:
                continue
            if not passes_filters(opportunity, item.entry_filter, FilterStage.FULL):
                continue
            if opportunity.key in held_keys or assets[opportunity.asset] >= (
                config.max_positions_per_asset
            ):
                continue
            # Leave room for the VWAP above mid so the fill stays under the hard cap.
            target = min(config.position_notional_usdt, room * _CAP_HEADROOM)
            try:
                position, fills = self.simulator.open(
                    opportunity,
                    target,
                    snapshot,
                    now,
                    series_id=account.series_id,
                    perp_leverage=config.perp_leverage,
                )
            except FillRejected as exc:
                self._reject(item, exc.reason)
                continue
            if account.exposure + position.exposure > config.max_total_notional_usdt:
                self._reject(item, "exposure_cap")
                continue
            try:
                account.open_position(position, fills)
            except ValueError:
                self._reject(item, "insufficient_cash")
                continue
            self.settler.observe(position, snapshot, now)
            held_keys.add(opportunity.key)
            assets[opportunity.asset] += 1
            logger.info(
                "paper_position_opened",
                extra={
                    "series": account.series_id,
                    "position_id": position.id,
                    "strategy": position.strategy,
                    "asset": position.asset,
                    "exposure": str(position.exposure),
                    "funding_rate_8h": str(opportunity.funding_rate_8h),
                },
            )

    @staticmethod
    def _reject(item: SeriesRuntime, reason: str) -> None:
        item.rejections[reason] += 1
        paper_entry_rejections_total.labels(item.account.series_id, reason).inc()

    def _evidence_books(self, snapshot: MarketSnapshot) -> list[OrderBook]:
        keys = {
            MarketKey(fill.exchange, fill.instrument_type, fill.symbol)
            for item in self.series.values()
            for fill in item.account.pending_fills
        }
        return [book for key, book in snapshot.orderbooks.items() if key in keys]

    # ------------------------------------------------------------- persistence
    async def _persist_paper(self, now: datetime, cycle: CycleRecord) -> bool:
        interval = self.settings.paper_snapshot_interval_seconds
        due = (
            self._last_snapshot_persist is None
            or (now - self._last_snapshot_persist).total_seconds() >= interval
            or any(item.account.has_pending for item in self.series.values())
        )
        snapshots = {label: item.account.snapshot(now) for label, item in self.series.items()}
        self._publish_metrics(snapshots)
        try:
            async with self.session_factory() as session:
                for item in self.series.values():
                    await persist_account_changes(session, item.account, now)
                if due:
                    await insert_account_snapshots(
                        session,
                        [
                            snapshot
                            for label, snapshot in snapshots.items()
                            if label in self._recorded
                        ],
                    )
                await insert_cycle(session, cycle)
                if self._session_id is not None:
                    await heartbeat_runner_session(session, self._session_id, now)
                await session.commit()
        except Exception:
            # Pending ledger changes stay queued and are retried next cycle; no new
            # entries are opened until the database accepts writes again.
            self.persistence_ok = False
            paper_persistence_failures_total.inc()
            logger.exception("paper_persistence_failed")
            return False
        for item in self.series.values():
            item.account.clear_pending()
        if due:
            self._last_snapshot_persist = now
        self.persistence_ok = True
        return True

    async def _persist_error_cycle(self, cycle: CycleRecord) -> None:
        try:
            async with self.session_factory() as session:
                await insert_cycle(session, cycle)
                await session.commit()
        except Exception:
            logger.warning("paper_error_cycle_not_persisted")

    async def _persist_market(
        self,
        now: datetime,
        snapshot: MarketSnapshot,
        opportunities: list[Opportunity],
        evidence: list[OrderBook],
        fetched_history: list[FundingKey],
    ) -> None:
        settings = self.settings
        refreshed = self.collector.instrument_refreshes()
        venues = [
            venue for venue, at in refreshed.items() if self._persisted_instruments.get(venue) != at
        ]
        funding_due = (
            self._last_funding_persist is None
            or (now - self._last_funding_persist).total_seconds()
            >= settings.market_persist_funding_seconds
        )
        window = timedelta(seconds=settings.opportunity_persist_seconds)
        confirmed = [
            opportunity
            for opportunity in opportunities
            if opportunity.status == OpportunityStatus.CONFIRMED
            and now
            - self._opportunity_persisted_at.get(opportunity.key, datetime.min.replace(tzinfo=UTC))
            >= window
        ]
        try:
            async with self.session_factory() as session:
                if venues:
                    await upsert_instruments(
                        session,
                        [item for item in snapshot.instruments if item.exchange in venues],
                    )
                if funding_due:
                    await insert_funding_snapshots(session, snapshot.funding)
                    if settings.market_persist_tickers:
                        await insert_tickers(session, snapshot.tickers)
                    await upsert_exchanges(session, snapshot)
                if fetched_history and snapshot.funding_history:
                    await upsert_funding_history(
                        session,
                        [
                            point
                            for key in fetched_history
                            for point in snapshot.funding_history.get(key, [])
                        ],
                    )
                if evidence:
                    await insert_orderbooks(session, evidence)
                if confirmed:
                    await upsert_opportunities(session, confirmed)
                await session.commit()
            for venue in venues:
                self._persisted_instruments[venue] = refreshed[venue]
            if funding_due:
                self._last_funding_persist = now
            for opportunity in confirmed:
                self._opportunity_persisted_at[opportunity.key] = now
            if self._last_prune is None or now - self._last_prune >= _PRUNE_EVERY:
                horizon = now - timedelta(days=settings.market_data_retention_days)
                async with self.session_factory() as session:
                    removed = await prune_market_data(session, horizon)
                self._last_prune = now
                logger.info("market_data_pruned", extra={"removed": removed})
        except Exception:
            logger.warning("market_data_persistence_failed", exc_info=True)

    def _publish_metrics(self, snapshots: dict[str, AccountSnapshot]) -> None:
        for label, snapshot in snapshots.items():
            paper_equity.labels(label).set(float(snapshot.equity))
            paper_cash.labels(label).set(float(snapshot.cash))
            paper_locked_capital.labels(label).set(float(snapshot.locked_capital))
            paper_pnl_total.labels(label).set(float(snapshot.total_pnl))
            funding_pnl_total.labels(label).set(float(snapshot.funding_pnl))
            paper_positions_open.labels(label).set(snapshot.open_positions)

    # ------------------------------------------------------------------ health
    def healthy(self, now: datetime | None = None) -> bool:
        if not self.started or self.fatal_error is not None or self.last_success_at is None:
            return False
        limit = max(120.0, self.settings.paper_loop_interval_seconds * 6)
        return ((now or self.clock()) - self.last_success_at).total_seconds() <= limit
