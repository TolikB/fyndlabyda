"""Production-shaped live runner with preflight, reconciliation, and actual equity."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    LiveAccountSnapshotRecord,
    LiveFundingPaymentRecord,
)
from funding_arbitrage.database.repositories.live import (
    load_active_live_positions,
    save_live_account_snapshots,
    save_live_funding_payments,
)
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
)
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.private_streams import PrivateStreamSupervisor
from funding_arbitrage.exchanges.public_events import PublicEventSupervisor
from funding_arbitrage.execution.live import LiveExecutionError, LiveTradingExecutor
from funding_arbitrage.execution.reconciliation import LiveReconciler, ReconciliationResult
from funding_arbitrage.execution.trading import (
    LivePosition,
    LivePositionState,
    TradingAdapter,
    VenueBalance,
    VenuePosition,
)
from funding_arbitrage.market_data.collector import (
    CanonicalBookEventSink,
    CanonicalOptionEventSink,
    MarketDataCollector,
    MarketSnapshot,
)
from funding_arbitrage.monitoring.metrics import (
    live_drawdown_fraction,
    live_drawdown_limit_utilization,
    live_equity,
    live_exposure_limit_utilization,
    live_funding_payments_total,
    live_funding_poll_errors_total,
    live_gross_exposure_usd,
    live_pnl,
    live_positions_open,
    live_reconciliation_failures_total,
    live_reconciliation_healthy,
    live_runner_cycles_total,
    live_runner_errors_total,
    live_runner_last_cycle_timestamp,
    live_trade_rejections_total,
    live_trading_paused,
)
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.models import Opportunity, SizeQuote
from funding_arbitrage.opportunity.settlement import (
    settlement_continuation_allowed,
    settlement_entry_allowed,
    target_settlements,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused
from funding_arbitrage.services.decision_pipeline import FundingLiveDecisionService
from funding_arbitrage.services.live_daily_report import LiveDailyReportService
from funding_arbitrage.services.runtime import RuntimeState

logger = logging.getLogger(__name__)


class LiveTradingRunner:
    def __init__(
        self,
        settings: Settings,
        runtime: RuntimeState,
        session_factory: async_sessionmaker[AsyncSession],
        trading_adapters: dict[str, TradingAdapter],
        private_streams: PrivateStreamSupervisor | None = None,
        public_events: PublicEventSupervisor | None = None,
        canonical_book_event_sink: CanonicalBookEventSink | None = None,
        canonical_option_event_sink: CanonicalOptionEventSink | None = None,
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self.session_factory = session_factory
        self.trading_adapters = trading_adapters
        self.private_streams = private_streams
        self.public_events = public_events
        self._base_entry_health = runtime.entry_health
        if private_streams is not None:
            runtime.entry_health = self._entry_health
        public_adapters = [runtime.adapters[name] for name in settings.live_venue_values]
        self.collector = MarketDataCollector(
            public_adapters,
            settings.paper_orderbook_symbol_limit,
            settings.paper_market_asset_limit,
            settings.paper_history_symbol_limit,
            settings.market_data_stale_seconds,
            True,
            option_assets=(
                settings.multi_regime_asset_values
                if settings.options_market_data_enabled
                else ()
            ),
            option_refresh_seconds=settings.options_refresh_seconds,
            option_maximum_expiries=settings.options_maximum_expiries,
            option_strikes_per_expiry=settings.options_strikes_per_expiry,
            canonical_book_event_sink=canonical_book_event_sink,
            canonical_option_event_sink=canonical_option_event_sink,
        )
        self.risk = LiveRiskController(settings)
        self.executor = LiveTradingExecutor(
            settings,
            trading_adapters,
            session_factory,
            self.risk,
            metadata_registry=(
                public_events.metadata_registry if public_events is not None else None
            ),
            private_reconciliation_coverage=(
                private_streams.reconciliation_coverage()
                if private_streams is not None
                else {}
            ),
        )
        self.decision_pipeline = FundingLiveDecisionService(settings, self.risk)
        self.reconciler = LiveReconciler(
            settings, trading_adapters, session_factory, self.risk
        )
        self.daily_report = LiveDailyReportService(settings, session_factory)
        self.stop_event = asyncio.Event()
        self.positions: dict[str, LivePosition] = {}
        self._position_by_key: dict[str, str] = {}
        self._candidate_books: dict[str, set[tuple[str, InstrumentType]]] = {}
        self._candidate_history: dict[str, set[str]] = {}
        self._last_history_refresh: datetime | None = None
        self._last_history_symbols: dict[str, tuple[str, ...]] = {}
        self._last_market_persist: datetime | None = None
        self._last_reconciliation: datetime | None = None
        self._last_account_snapshot: datetime | None = None
        self._balances: dict[str, VenueBalance] = {}
        self._venue_positions: tuple[VenuePosition, ...] = ()
        self.initialized = False
        self.startup_error: str | None = None
        self._process_lock_session: AsyncSession | None = None
        self._funding_cursors: dict[str, datetime] = {}
        self._funding_floors: dict[str, datetime] = {}

    async def run(self) -> None:
        try:
            self.risk.verify_interlock_storage()
            await self._acquire_process_lock()
            await asyncio.gather(
                *(adapter.initialize() for adapter in self.trading_adapters.values())
            )
            await asyncio.gather(
                *(adapter.preflight() for adapter in self.trading_adapters.values())
            )
            if self.private_streams is not None:
                await self.private_streams.start()
            if self.public_events is not None:
                await self.public_events.start()
            await self._restore_positions()
            result, reconciled_at = await self._reconcile_and_journal(startup=True)
            live_reconciliation_healthy.set(1)
            self._balances = result.balances
            self._venue_positions = result.positions
            self._last_reconciliation = reconciled_at
            await self._restore_risk_baselines()
            await self._restore_funding_cursors()
            await self._poll_funding_payments(datetime.now(UTC))
            self.initialized = True
        except asyncio.CancelledError:
            raise
        except LiveTradingPaused:
            self.startup_error = "live_startup_reconciliation_paused"
            live_runner_errors_total.labels("startup_reconciliation").inc()
            logger.exception("live_startup_reconciliation_failed")
            await self._alert_if_paused()
            return
        except Exception:
            self.startup_error = "live_startup_failed"
            self.risk.trip("live_startup_failed")
            live_runner_errors_total.labels("startup").inc()
            logger.exception("live_startup_failed")
            await self._alert_if_paused()
            return

        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.cycle()
                live_runner_cycles_total.inc()
                live_runner_last_cycle_timestamp.set(datetime.now(UTC).timestamp())
            except asyncio.CancelledError:
                raise
            except LiveTradingPaused:
                live_runner_errors_total.labels("safety_pause").inc()
                await self._alert_if_paused()
            except Exception:
                live_runner_errors_total.labels("cycle").inc()
                logger.exception("live_cycle_failed")
                await self._alert_if_paused()
            live_trading_paused.set(1 if self.risk.paused else 0)
            remaining = self.settings.live_loop_interval_seconds - (
                time.monotonic() - started
            )
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=max(remaining, 0.01))
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self.stop_event.set()

    async def _reconcile_and_journal(
        self, *, startup: bool = False
    ) -> tuple[ReconciliationResult, datetime]:
        result = await self.reconciler.reconcile(
            startup=startup,
            raise_on_failure=False,
        )
        observed_at = datetime.now(UTC)
        if self.private_streams is not None:
            try:
                await self.private_streams.ingest_reconciliation(
                    result,
                    observed_at=observed_at,
                )
            except Exception:
                self.risk.trip("private_reconciliation_journal_failed")
                raise
        self.reconciler.raise_if_failed(result)
        return result, observed_at

    async def close(self) -> None:
        await self.stop()
        await self.collector.close()
        await self.daily_report.close()
        if self.private_streams is not None:
            await self.private_streams.stop()
        if self.public_events is not None:
            await self.public_events.close()
        await asyncio.gather(
            *(adapter.close() for adapter in self.trading_adapters.values()),
            return_exceptions=True,
        )
        await self._release_process_lock()

    async def cycle(self) -> None:
        now = datetime.now(UTC)
        history_symbols = {
            venue: tuple(sorted(symbols))
            for venue, symbols in self._candidate_history.items()
        }
        periodic_history_refresh = self._last_history_refresh is None or (
            now - self._last_history_refresh
        ).total_seconds() >= self.settings.paper_history_refresh_seconds
        refresh_history = (
            periodic_history_refresh
            or history_symbols != self._last_history_symbols
        )
        snapshot = await self.collector.collect_once(
            orderbook_symbols=self._required_books(),
            include_history=refresh_history,
            history_symbols={venue: list(symbols) for venue, symbols in history_symbols.items()},
            force_history_refresh=periodic_history_refresh,
        )
        self._require_complete_market_snapshot(snapshot)
        if self.public_events is not None:
            await self.public_events.observe_snapshot(snapshot)
        if refresh_history:
            self._last_history_refresh = snapshot.captured_at
            self._last_history_symbols = history_symbols
        opportunities = await asyncio.to_thread(self.runtime.update_market, snapshot)
        self._remember_candidates(self.runtime.opportunity_engine.last_candidates)
        await self._persist_market_if_due(snapshot, opportunities)
        reconciliation_due = self._reconciliation_due(snapshot.captured_at)
        if reconciliation_due:
            try:
                result, _ = await self._reconcile_and_journal()
            except Exception:
                live_reconciliation_healthy.set(0)
                live_reconciliation_failures_total.inc()
                raise
            live_reconciliation_healthy.set(1)
            self._balances = result.balances
            self._venue_positions = result.positions
            self._last_reconciliation = snapshot.captured_at
        else:
            self._balances = await self._fetch_fresh_balances()
        await self._record_equity(snapshot)
        if reconciliation_due:
            await self._poll_funding_payments(snapshot.captured_at)
        await self._close_positions(opportunities, snapshot)
        if (
            not self.risk.paused
            and self.settings.live_autotrade
            and self.runtime.entries_allowed()
        ):
            self._balances = await self._fetch_fresh_balances()
            await self._open_positions(opportunities, snapshot)
        live_positions_open.set(
            sum(
                position.state is LivePositionState.OPEN
                for position in self.positions.values()
            )
        )
        gross_exposure = sum(
            (
                position.capital_per_leg * Decimal("2")
                for position in self.positions.values()
                if position.state
                not in {LivePositionState.CLOSED, LivePositionState.FAILED}
            ),
            Decimal("0"),
        )
        live_gross_exposure_usd.set(float(gross_exposure))
        live_exposure_limit_utilization.set(
            float(gross_exposure / self.settings.live_max_total_notional_usd)
        )
        await self.daily_report.check_and_send(snapshot.captured_at)
        self.runtime.last_completed_snapshot = snapshot

    def _entry_health(self) -> tuple[bool, str | None]:
        if self._base_entry_health is not None:
            healthy, reason = self._base_entry_health()
            if not healthy:
                return False, reason
        if self.private_streams is None:
            return True, None
        return self.private_streams.health()

    async def _restore_positions(self) -> None:
        async with self.session_factory() as session:
            positions = await load_active_live_positions(session)
        self.positions = {position.position_id: position for position in positions}
        self._position_by_key = {
            position.opportunity_key: position.position_id
            for position in positions
            if position.state is LivePositionState.OPEN
        }

    async def _restore_risk_baselines(self) -> None:
        async with self.session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        LiveAccountSnapshotRecord.timestamp,
                        func.sum(LiveAccountSnapshotRecord.equity_usd),
                    )
                    .group_by(LiveAccountSnapshotRecord.timestamp)
                    .order_by(LiveAccountSnapshotRecord.timestamp.asc())
                )
            ).all()
        if not rows:
            return
        normalized = [
            (timestamp, Decimal(str(equity))) for timestamp, equity in rows
        ]
        current_day = datetime.now(UTC).astimezone(self.risk.timezone).date()
        day_start = datetime.combine(
            current_day, datetime.min.time(), tzinfo=self.risk.timezone
        ).astimezone(UTC)
        before_day = [equity for timestamp, equity in normalized if timestamp < day_start]
        in_day = [equity for timestamp, equity in normalized if timestamp >= day_start]
        day_start_equity = (
            before_day[-1]
            if before_day
            else in_day[0]
            if in_day
            else normalized[-1][1]
        )
        self.risk.restore_baselines(
            starting_equity=normalized[0][1],
            high_water_equity=max(equity for _, equity in normalized),
            day_start_equity=day_start_equity,
            equity_day=current_day,
        )

    async def _restore_funding_cursors(self) -> None:
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            for venue in self.trading_adapters:
                latest_payment = await session.scalar(
                    select(func.max(LiveFundingPaymentRecord.timestamp)).where(
                        LiveFundingPaymentRecord.exchange == venue
                    )
                )
                first_snapshot = await session.scalar(
                    select(func.min(LiveAccountSnapshotRecord.timestamp)).where(
                        LiveAccountSnapshotRecord.exchange == venue
                    )
                )
                floor = first_snapshot or now
                self._funding_floors[venue] = floor
                self._funding_cursors[venue] = latest_payment or floor

    async def _poll_funding_payments(self, now: datetime) -> None:
        venue_names = sorted(self.trading_adapters)
        overlap = timedelta(hours=1)
        rows = await asyncio.gather(
            *(
                self.trading_adapters[venue].fetch_funding_payments(
                    max(
                        self._funding_cursors.get(venue, now) - overlap,
                        self._funding_floors.get(venue, now),
                    )
                )
                for venue in venue_names
            ),
            return_exceptions=True,
        )
        failures: list[str] = []
        for venue, row in zip(venue_names, rows, strict=True):
            if isinstance(row, BaseException):
                failures.append(venue)
                live_funding_poll_errors_total.labels(venue).inc()
                logger.warning(
                    "live_funding_poll_failed",
                    extra={"exchange": venue, "error": type(row).__name__},
                )
                continue
            async with self.session_factory() as session:
                inserted = await save_live_funding_payments(session, row)
            if inserted:
                live_funding_payments_total.labels(venue).inc(inserted)
            self._funding_cursors[venue] = now
        if failures:
            self.risk.trip("funding_history_poll_failed:" + ",".join(failures))
            raise LiveTradingPaused(
                self.risk.paused_reason or "funding_history_poll_failed"
            )

    async def _acquire_process_lock(self) -> None:
        session = self.session_factory()
        try:
            dialect = session.get_bind().dialect.name
            if dialect != "postgresql":
                self._process_lock_session = session
                return
            acquired = await session.scalar(
                text("SELECT pg_try_advisory_lock(5068047852292196165)")
            )
            if acquired is not True:
                raise RuntimeError("another live runner holds the PostgreSQL process lock")
            self._process_lock_session = session
        except Exception:
            await session.close()
            raise

    async def _release_process_lock(self) -> None:
        session = self._process_lock_session
        if session is None:
            return
        try:
            if session.get_bind().dialect.name == "postgresql":
                await session.execute(
                    text("SELECT pg_advisory_unlock(5068047852292196165)")
                )
        finally:
            await session.close()
            self._process_lock_session = None

    async def _persist_market_if_due(
        self, snapshot: MarketSnapshot, opportunities: list[Opportunity]
    ) -> None:
        if self._last_market_persist is not None and (
            snapshot.captured_at - self._last_market_persist
        ).total_seconds() < self.settings.live_market_persist_interval_seconds:
            return
        async with self.session_factory() as session:
            await save_market_snapshot(
                session,
                snapshot,
                include_history=self._last_history_refresh == snapshot.captured_at,
            )
            await save_opportunities(session, opportunities)
        self._last_market_persist = snapshot.captured_at

    async def _record_equity(self, snapshot: MarketSnapshot) -> None:
        total_equity = Decimal("0")
        periodic_persist = self._last_account_snapshot is None or (
            snapshot.captured_at - self._last_account_snapshot
        ).total_seconds() >= self.settings.live_account_snapshot_interval_seconds
        account_snapshots: list[tuple[VenueBalance, Decimal, Decimal]] = []
        for venue, balance in self._balances.items():
            equity = (
                balance.equity_usd
                if balance.equity_usd is not None
                else self._balance_equity(balance, snapshot)
                + balance.unrealized_pnl_usd
            )
            if equity < self.settings.live_min_venue_equity_usd:
                self.risk.trip(f"venue_equity_below_minimum:{venue}")
            free = (
                balance.free_collateral_usd
                if balance.free_collateral_usd is not None
                else sum(
                    (balance.available(currency) for currency in ("USD", "USDT", "USDC")),
                    Decimal("0"),
                )
            )
            total_equity += equity
            account_snapshots.append((balance, equity, free))
        previous_high_water = self.risk.state.high_water_equity
        new_high_water = (
            previous_high_water is None or total_equity > previous_high_water
        )
        if periodic_persist or new_high_water:
            async with self.session_factory() as session:
                await save_live_account_snapshots(
                    session, account_snapshots, snapshot.captured_at
                )
            self._last_account_snapshot = snapshot.captured_at
        self.risk.update_equity(total_equity, snapshot.captured_at)
        live_equity.set(float(total_equity))
        high_water = self.risk.state.high_water_equity or total_equity
        drawdown = (
            (high_water - total_equity) / high_water
            if high_water > 0
            else Decimal("0")
        )
        live_drawdown_fraction.set(float(drawdown))
        live_drawdown_limit_utilization.set(
            float(drawdown / self.settings.live_max_drawdown_percent)
        )
        start = self.risk.state.starting_equity or total_equity
        live_pnl.set(float(total_equity - start))
        await self._alert_if_paused()

    async def _fetch_fresh_balances(self) -> dict[str, VenueBalance]:
        venue_names = sorted(self.trading_adapters)
        rows = await asyncio.gather(
            *(
                self.trading_adapters[venue].fetch_balance()
                for venue in venue_names
            ),
            return_exceptions=True,
        )
        failures = [
            venue
            for venue, row in zip(venue_names, rows, strict=True)
            if isinstance(row, BaseException)
        ]
        if failures:
            self.risk.trip("balance_refresh_failed:" + ",".join(failures))
            raise LiveTradingPaused(self.risk.paused_reason or "balance_refresh_failed")
        invalid = [
            venue
            for venue, row in zip(venue_names, rows, strict=True)
            if not isinstance(row, VenueBalance) or row.exchange != venue
        ]
        if invalid:
            reason = "balance_identity_mismatch:" + ",".join(invalid)
            self.risk.trip(reason)
            raise LiveTradingPaused(reason)
        return {
            venue: row
            for venue, row in zip(venue_names, rows, strict=True)
            if isinstance(row, VenueBalance)
        }

    def _require_complete_market_snapshot(self, snapshot: MarketSnapshot) -> None:
        if not snapshot.incomplete_venues:
            return
        reason = "market_snapshot_incomplete:" + ",".join(snapshot.incomplete_venues)
        self.risk.trip(reason)
        raise LiveTradingPaused(reason)

    def _balance_equity(
        self, balance: VenueBalance, snapshot: MarketSnapshot
    ) -> Decimal:
        value = Decimal("0")
        for currency, amount in balance.total.items():
            if amount == 0:
                continue
            if amount < 0:
                reason = f"negative_equity_asset:{balance.exchange}:{currency}"
                self.risk.trip(reason)
                raise LiveTradingPaused(reason)
            if currency in {"USD", "USDT", "USDC"}:
                value += amount
                continue
            prices = [
                ticker.last_price
                for ticker in snapshot.tickers
                if ticker.exchange == balance.exchange
                and ticker.instrument_type is InstrumentType.SPOT
                and ticker.last_price > 0
                and (
                    snapshot.captured_at - ticker.timestamp
                ).total_seconds() <= snapshot.stale_after_seconds
                and any(
                    instrument.exchange == ticker.exchange
                    and instrument.exchange_symbol == ticker.symbol
                    and instrument.base_asset == currency
                    and instrument.quote_asset in {"USD", "USDT", "USDC"}
                    and instrument.instrument_type is InstrumentType.SPOT
                    for instrument in snapshot.instruments
                )
            ]
            if not prices:
                reason = f"unpriced_equity_asset:{balance.exchange}:{currency}"
                self.risk.trip(reason)
                raise LiveTradingPaused(reason)
            value += amount * prices[0]
        return value

    async def _open_positions(
        self, opportunities: list[Opportunity], snapshot: MarketSnapshot
    ) -> None:
        open_positions = [
            position
            for position in self.positions.values()
            if position.state is LivePositionState.OPEN
        ]
        open_notional = sum(
            (
                leg.filled_base_quantity * leg.average_price
                for position in open_positions
                for leg in (position.leg_a, position.leg_b)
                if leg is not None
            ),
            Decimal("0"),
        )
        ranked = sorted(
            opportunities,
            key=lambda item: (item.opportunity_score, item.net_apr),
            reverse=True,
        )
        for opportunity in ranked:
            if len(open_positions) >= self.settings.live_max_open_positions:
                return
            if opportunity.status != "confirmed":
                continue
            key = OpportunityDebouncer.key(opportunity)
            if key in self._position_by_key:
                continue
            quote = self._select_size(opportunity, snapshot)
            if quote is None:
                live_trade_rejections_total.labels("size_or_expected_profit").inc()
                continue
            concentration_reason = self._concentration_rejection(
                opportunity, quote, open_positions
            )
            if concentration_reason is not None:
                live_trade_rejections_total.labels(concentration_reason).inc()
                continue
            try:
                approval = self.decision_pipeline.approve(
                    opportunity,
                    quote,
                    snapshot,
                    key,
                    now=snapshot.captured_at,
                )
                position = await self.executor.open_position(
                    approval,
                    snapshot,
                    self._balances,
                    open_notional,
                )
            except (LiveExecutionError, LiveTradingPaused, KeyError, ValueError) as exc:
                live_trade_rejections_total.labels(type(exc).__name__).inc()
                logger.warning("live_entry_rejected", extra={"reason": str(exc)})
                continue
            self.positions[position.position_id] = position
            if position.state is LivePositionState.OPEN:
                self._position_by_key[key] = position.position_id
                open_positions.append(position)
                open_notional += sum(
                    (
                        leg.filled_base_quantity * leg.average_price
                        for leg in (position.leg_a, position.leg_b)
                        if leg is not None
                    ),
                    Decimal("0"),
                )
            elif position.state is LivePositionState.MANUAL_INTERVENTION:
                await self._alert_if_paused()
                return

    def _select_size(
        self, opportunity: Opportunity, snapshot: MarketSnapshot
    ) -> SizeQuote | None:
        cap = min(
            self.settings.live_default_position_size_usd,
            self.settings.live_max_order_notional_usd,
        )
        candidates = [
            quote
            for quote in opportunity.size_quotes
            if quote.fully_filled
            and quote.net_profit > 0
            and quote.capital <= cap
            and settlement_entry_allowed(
                opportunity,
                quote,
                snapshot,
                snapshot.captured_at,
                self.settings.live_entry_window_hours,
                self.settings.live_min_settlement_cost_coverage,
            )
        ]
        if not candidates:
            return None
        quote = max(candidates, key=lambda item: item.net_profit)
        if quote.net_profit < self.settings.live_min_expected_profit_usd:
            return None
        return quote

    def _concentration_rejection(
        self,
        opportunity: Opportunity,
        quote: SizeQuote,
        open_positions: list[LivePosition],
    ) -> str | None:
        def position_notional(position: LivePosition) -> Decimal:
            return sum(
                (
                    leg.filled_base_quantity * leg.average_price
                    for leg in (position.leg_a, position.leg_b)
                    if leg is not None
                ),
                Decimal("0"),
            )

        candidate_notional = quote.capital * Decimal("2")
        asset_notional = sum(
            (
                position_notional(position)
                for position in open_positions
                if position.asset == opportunity.asset.upper()
            ),
            Decimal("0"),
        ) + candidate_notional
        if asset_notional > self.settings.live_max_asset_notional_usd:
            return "asset_notional_limit"

        strategy_notional = sum(
            (
                position_notional(position)
                for position in open_positions
                if position.strategy == str(opportunity.strategy)
            ),
            Decimal("0"),
        ) + candidate_notional
        if strategy_notional > self.settings.live_max_strategy_notional_usd:
            return "strategy_notional_limit"

        venue_notional: dict[str, Decimal] = {}
        for position in open_positions:
            for leg in (position.leg_a, position.leg_b):
                if leg is None:
                    continue
                venue_notional[leg.exchange] = venue_notional.get(
                    leg.exchange, Decimal("0")
                ) + leg.filled_base_quantity * leg.average_price
        for venue in (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a):
            venue_notional[venue] = venue_notional.get(venue, Decimal("0")) + quote.capital
        if any(
            value > self.settings.live_max_venue_notional_usd
            for value in venue_notional.values()
        ):
            return "venue_notional_limit"

        group = next(
            (
                assets
                for assets in self.settings.paper_correlation_group_values
                if opportunity.asset.upper() in assets
            ),
            frozenset({opportunity.asset.upper()}),
        )
        correlated_notional = sum(
            (
                position_notional(position)
                for position in open_positions
                if position.asset in group
            ),
            Decimal("0"),
        ) + candidate_notional
        if correlated_notional > self.settings.live_max_correlated_notional_usd:
            return "correlated_notional_limit"
        return None

    async def _close_positions(
        self, opportunities: list[Opportunity], snapshot: MarketSnapshot
    ) -> None:
        by_key = {
            OpportunityDebouncer.key(opportunity): opportunity
            for opportunity in opportunities
        }
        now = snapshot.captured_at
        for position in list(self.positions.values()):
            if position.state is not LivePositionState.OPEN or position.opened_at is None:
                continue
            current = by_key.get(position.opportunity_key)
            position.edge_miss_count = (
                position.edge_miss_count + 1 if current is None else 0
            )
            max_hold = (
                now - position.opened_at
            ).total_seconds() >= self.settings.live_max_hold_seconds
            edge_gone = (
                position.edge_miss_count >= self.settings.live_exit_edge_miss_cycles
            )
            funding_reversed = self._funding_reversed(position, snapshot)
            adverse_basis = self._adverse_basis(position, snapshot)
            target_due = any(
                target + timedelta(seconds=self.settings.live_settlement_grace_seconds)
                <= now
                for target in position.target_settlements
            )
            continue_holding = False
            if target_due and current is not None:
                quote = min(
                    current.size_quotes,
                    key=lambda item: abs(item.capital - position.capital_per_leg),
                    default=None,
                )
                if quote is not None:
                    continue_holding = settlement_continuation_allowed(
                        current,
                        quote,
                        snapshot,
                        now,
                        self.settings.live_min_settlement_cost_coverage,
                    )
                    if continue_holding:
                        position.target_settlements = target_settlements(
                            current, snapshot, now
                        )
            if not (
                (self.risk.paused and self.settings.live_liquidate_on_pause)
                or max_hold
                or edge_gone
                or funding_reversed
                or adverse_basis
                or (target_due and not continue_holding)
            ):
                continue
            try:
                closed = await self.executor.close_position(position, snapshot)
            except (LiveExecutionError, LiveTradingPaused, ValueError) as exc:
                logger.error("live_close_failed", extra={"reason": str(exc)})
                self.risk.trip("live_close_failed")
                await self._alert_if_paused()
                return
            self.positions[position.position_id] = closed
            if closed.state is LivePositionState.CLOSED:
                self._position_by_key.pop(position.opportunity_key, None)
            else:
                await self._alert_if_paused()
                return

    def _required_books(self) -> dict[str, list[tuple[str, InstrumentType]]]:
        result = {venue: set(values) for venue, values in self._candidate_books.items()}
        for position in self.positions.values():
            if position.state is not LivePositionState.OPEN:
                continue
            for leg in (position.leg_a, position.leg_b):
                if leg is not None:
                    result.setdefault(leg.exchange, set()).add(
                        (leg.exchange_symbol, leg.instrument_type)
                    )
        return {
            venue: sorted(values, key=lambda item: (item[0], item[1].value))
            for venue, values in result.items()
        }

    def _remember_candidates(self, opportunities: list[Opportunity]) -> None:
        books: dict[str, set[tuple[str, InstrumentType]]] = {}
        history: dict[str, set[str]] = {}
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
                if venue not in self.trading_adapters or symbol is None:
                    continue
                books.setdefault(venue, set()).add((symbol, instrument_type))
                if instrument_type is InstrumentType.PERPETUAL:
                    history.setdefault(venue, set()).add(symbol)
        self._candidate_books = books
        self._candidate_history = history

    def _reconciliation_due(self, now: datetime) -> bool:
        return self._last_reconciliation is None or (
            now - self._last_reconciliation
        ).total_seconds() >= self.settings.live_reconciliation_interval_seconds

    @staticmethod
    def _funding_reversed(position: LivePosition, snapshot: MarketSnapshot) -> bool:
        observed = 0
        cashflow = Decimal("0")
        for leg in (position.leg_a, position.leg_b):
            if leg is None or leg.instrument_type is not InstrumentType.PERPETUAL:
                continue
            funding = snapshot.funding_rate(leg.exchange, leg.exchange_symbol)
            if funding is None:
                continue
            observed += 1
            cashflow += (
                funding.funding_rate
                if leg.side.upper() == "SELL"
                else -funding.funding_rate
            )
        return observed > 0 and cashflow <= 0

    def _adverse_basis(self, position: LivePosition, snapshot: MarketSnapshot) -> bool:
        pnl = Decimal("0")
        observed = 0
        for leg in (position.leg_a, position.leg_b):
            if leg is None:
                continue
            ticker = snapshot.ticker(
                leg.exchange, leg.exchange_symbol, leg.instrument_type
            )
            if ticker is None:
                return False
            observed += 1
            direction = Decimal("1") if leg.side.upper() == "BUY" else Decimal("-1")
            pnl += (
                ticker.last_price - leg.average_price
            ) * leg.filled_base_quantity * direction
        return observed == 2 and pnl <= -(
            position.capital_per_leg * self.settings.live_max_adverse_basis_percent
        )

    async def _alert_if_paused(self) -> None:
        reason = self.risk.paused_reason
        if reason:
            try:
                await self.daily_report.send_safety_alert(reason)
            except Exception:
                logger.exception("live_safety_alert_failed")
