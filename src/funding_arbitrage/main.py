"""FastAPI entry point."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app

from funding_arbitrage.ai import load_decision_support_artifacts
from funding_arbitrage.api.routes.analytics import router as analytics_router
from funding_arbitrage.api.routes.backtests import (
    market_backtest_recovery_loop,
)
from funding_arbitrage.api.routes.backtests import (
    router as backtests_router,
)
from funding_arbitrage.api.routes.control import router as control_router
from funding_arbitrage.api.routes.market_data import router as market_data_router
from funding_arbitrage.api.routes.multi_regime import router as multi_regime_router
from funding_arbitrage.api.routes.opportunities import router as opportunities_router
from funding_arbitrage.api.routes.portfolio import router as portfolio_router
from funding_arbitrage.api.routes.scan import router as scan_router
from funding_arbitrage.api.routes.system import router as system_router
from funding_arbitrage.api.routes.websocket import router as websocket_router
from funding_arbitrage.backtest.fills import FillModelPolicy
from funding_arbitrage.config import Settings, get_settings
from funding_arbitrage.database.repositories.audit import DatabaseControlPlaneAuditSink
from funding_arbitrage.database.repositories.backtest_jobs import (
    DurableMarketReplayJobStore,
)
from funding_arbitrage.database.repositories.control_plane import (
    DatabaseControlPlaneIdempotencyStore,
)
from funding_arbitrage.database.repositories.market_data import (
    load_portfolio_equity_high_water,
)
from funding_arbitrage.database.session import create_database, init_database
from funding_arbitrage.domain.decisions import SignalType
from funding_arbitrage.domain.events import EventKind, TradingMode
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.private_streams import create_private_stream_supervisor
from funding_arbitrage.exchanges.public_events import create_public_event_supervisor
from funding_arbitrage.exchanges.trading import create_trading_adapters
from funding_arbitrage.execution.advanced_paper import AdvancedStrategyPaperBroker
from funding_arbitrage.execution.directional_paper import DirectionalPaperBroker
from funding_arbitrage.internal_tls import create_internal_ssl_context
from funding_arbitrage.logging import configure_logging
from funding_arbitrage.market_data.quality import DataQualityMonitor
from funding_arbitrage.monitoring.metrics import api_errors_total, api_request_latency_seconds
from funding_arbitrage.security.control_plane import (
    ControlPlaneMiddleware,
    ControlPlanePolicy,
    ControlPlaneSecurity,
)
from funding_arbitrage.security.rate_limit import create_control_plane_rate_limiter
from funding_arbitrage.security.revocation import create_token_revocation_store
from funding_arbitrage.services.event_router import CanonicalEventRouter
from funding_arbitrage.services.event_writer import CanonicalEventWriter
from funding_arbitrage.services.live_runner import LiveTradingRunner
from funding_arbitrage.services.multi_regime import (
    MultiRegimeEngine,
    MultiRegimeEngineConfig,
)
from funding_arbitrage.services.multi_regime_runtime import (
    DurableMultiRegimeRuntime,
    RuntimeAdvancedRiskContextProvider,
    RuntimePortfolioRiskContextProvider,
    RuntimeStrategyExecutionSnapshotProvider,
    RuntimeSupplementalStrategyContextProvider,
)
from funding_arbitrage.services.paper_runner import (
    PaperTestRunner,
    SharedMarketPaperComparisonRunner,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.runtime_decision_support import (
    EquityHighWaterDrawdown,
    RuntimeDecisionSupportConfig,
    RuntimeDecisionSupportProvider,
    fresh_equity_drawdown,
)
from funding_arbitrage.services.runtime_universe import RuntimeUniversePublisher
from funding_arbitrage.services.strategy_suite import PAPER_EXECUTABLE_SIGNAL_TYPES
from funding_arbitrage.storage.clickhouse import (
    ClickHouseHttpWriter,
    ClickHouseStoragePolicy,
)
from funding_arbitrage.storage.replication import (
    ClickHouseDecisionReplicator,
    ClickHouseEventReplicator,
)
from funding_arbitrage.strategies.universe import UniverseSelectorConfig

logger = logging.getLogger(__name__)


async def _attempt_shutdown(
    component: str,
    operation: Callable[[], Awaitable[object]],
    failures: list[BaseException],
) -> None:
    try:
        await operation()
    except asyncio.CancelledError as error:
        failures.append(error)
        logger.warning("shutdown_component_cancelled", extra={"component": component})
    except Exception as error:
        failures.append(error)
        logger.exception("shutdown_component_failed", extra={"component": component})


def _finalize_shutdown(
    failures: list[BaseException],
    primary_error: BaseException | None,
) -> None:
    if not failures:
        return
    if primary_error is not None:
        failure_types = ", ".join(type(error).__name__ for error in failures)
        primary_error.add_note(f"application shutdown also failed: {failure_types}")
        return
    raise BaseExceptionGroup("application shutdown failed", failures)


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or get_settings()
    engine, session_factory = create_database(active_settings)
    market_replay_job_store = DurableMarketReplayJobStore(session_factory)
    market_replay_worker_id = "market-worker-" + uuid4().hex
    event_writer = CanonicalEventWriter(
        session_factory,
        queue_size=active_settings.canonical_event_queue_size,
        batch_size=active_settings.canonical_event_batch_size,
        flush_interval_seconds=(active_settings.canonical_event_flush_interval_seconds),
    )
    clickhouse_writer: ClickHouseHttpWriter | None = None
    clickhouse_replicator: ClickHouseEventReplicator | None = None
    clickhouse_decision_replicator: ClickHouseDecisionReplicator | None = None
    if active_settings.clickhouse_enabled:
        tls_context = create_internal_ssl_context(active_settings)
        if tls_context is None:
            raise RuntimeError("ClickHouse analytics requires internal mTLS")
        clickhouse_writer = ClickHouseHttpWriter(
            ClickHouseStoragePolicy(
                url=active_settings.clickhouse_url,
                database=active_settings.clickhouse_database,
                username=active_settings.clickhouse_username,
                password=active_settings.clickhouse_password,
                request_timeout_seconds=(
                    active_settings.clickhouse_request_timeout_seconds
                ),
                maximum_batch_rows=active_settings.clickhouse_replication_batch_size,
            ),
            tls_context=tls_context,
        )
        clickhouse_replicator = ClickHouseEventReplicator(
            session_factory,
            clickhouse_writer,
            batch_size=active_settings.clickhouse_replication_batch_size,
            poll_seconds=active_settings.clickhouse_replication_poll_seconds,
        )
        clickhouse_decision_replicator = ClickHouseDecisionReplicator(
            session_factory,
            clickhouse_writer,
            batch_size=active_settings.clickhouse_replication_batch_size,
            poll_seconds=active_settings.clickhouse_replication_poll_seconds,
        )
    event_quality_monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=active_settings.market_data_stale_seconds),
        unavailable_after=timedelta(
            seconds=active_settings.market_data_stale_seconds * 3
        ),
        stream_timeouts={
            "BOOK": (
                timedelta(seconds=active_settings.orderbook_stream_stale_seconds),
                timedelta(seconds=active_settings.orderbook_stream_stale_seconds * 3),
            ),
            EventKind.FUNDING_SNAPSHOT.value: (
                timedelta(seconds=active_settings.funding_snapshot_stale_seconds),
                timedelta(seconds=active_settings.funding_snapshot_stale_seconds * 3),
            )
        },
    )
    event_router = CanonicalEventRouter(event_writer, event_quality_monitor)
    multi_regime_runtime: DurableMultiRegimeRuntime | None = None
    decision_support_provider: RuntimeDecisionSupportProvider | None = None
    decision_support_drawdown_tracker: EquityHighWaterDrawdown | None = None
    live_runner_for_decision_support: LiveTradingRunner | None = None
    universe_publisher: RuntimeUniversePublisher | None = None
    paper_broker: DirectionalPaperBroker | None = None
    advanced_paper_broker: AdvancedStrategyPaperBroker | None = None
    adapters = create_public_adapters(
        active_settings, canonical_book_event_sink=event_router.publish
    )
    public_events = (
        create_public_event_supervisor(active_settings, event_router.publish)
        if active_settings.market_data_mode == "live_public"
        and active_settings.run_mode in {"paper_test", "live"}
        else None
    )

    def entry_health() -> tuple[bool, str | None]:
        if event_writer.failed:
            return False, "canonical_event_journal_unavailable"
        if clickhouse_replicator is not None and not clickhouse_replicator.healthy:
            return False, clickhouse_replicator.health_reason
        if (
            clickhouse_decision_replicator is not None
            and not clickhouse_decision_replicator.healthy
        ):
            return False, clickhouse_decision_replicator.health_reason
        if multi_regime_runtime is not None and not multi_regime_runtime.healthy:
            return False, (
                "multi_regime_runtime_failed:"
                + (multi_regime_runtime.failure_reason or "unknown")
            )
        if public_events is not None:
            configured_venues = (
                active_settings.live_venue_values
                if active_settings.run_mode == "live"
                else active_settings.paper_venue_values
            )
            healthy, _ = event_router.venue_streams_usable(
                public_events.required_quality_streams,
                configured_venues,
                ("BOOK", EventKind.FUNDING_SNAPSHOT.value),
                now=datetime.now(UTC),
            )
            if not healthy:
                return False, "canonical_market_data_quality_unhealthy"
        return True, None

    runtime = RuntimeState(active_settings, adapters, entry_health=entry_health)
    if (
        active_settings.multi_regime_enabled
        and active_settings.mode_contract.strategy_evaluation_enabled
    ):
        runtime_mode = active_settings.effective_trading_mode
        if runtime_mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE}:
            runtime_mode = TradingMode.SHADOW
        paper_policies = {
            venue: FillModelPolicy(
                maker_fee_bps=maker_fee * 10_000,
                taker_fee_bps=taker_fee * 10_000,
                order_latency_ms=active_settings.multi_regime_paper_latency_ms,
                maximum_participation_rate=(
                    active_settings.multi_regime_paper_maximum_participation_rate
                ),
                impact_coefficient_bps=(
                    active_settings.multi_regime_paper_impact_coefficient_bps
                ),
            )
            for venue, (maker_fee, taker_fee) in (
                active_settings.fee_schedules.items()
            )
        }
        paper_execution_enabled = (
            runtime_mode is TradingMode.PAPER
            and active_settings.multi_regime_paper_execution_enabled
        )
        paper_broker = (
            DirectionalPaperBroker(
                paper_policies,
                simulation_version=active_settings.paper_simulation_version,
            )
            if paper_execution_enabled
            else None
        )
        advanced_paper_broker = (
            AdvancedStrategyPaperBroker(
                paper_policies,
                simulation_version=active_settings.paper_simulation_version,
            )
            if paper_execution_enabled
            else None
        )
        risk_provider = RuntimePortfolioRiskContextProvider(
            runtime,
            paper_broker,
            advanced_paper_broker,
        )
        supplemental_provider = RuntimeSupplementalStrategyContextProvider(
            runtime,
            paper_broker,
            advanced_paper_broker,
        )
        execution_snapshot_provider = RuntimeStrategyExecutionSnapshotProvider(
            runtime
        )
        advanced_risk_provider = RuntimeAdvancedRiskContextProvider(
            runtime,
            paper_broker,
            advanced_paper_broker,
        )
        if active_settings.decision_support_enabled:
            artifacts = load_decision_support_artifacts(
                active_settings.decision_support_artifact_root,
                active_settings.decision_support_artifact_bundle_file,
                expected_file_sha256=(
                    active_settings.decision_support_artifact_sha256
                ),
                maximum_bytes=(
                    active_settings.decision_support_artifact_maximum_bytes
                ),
            )

            if active_settings.run_mode != "live":
                decision_support_drawdown_tracker = EquityHighWaterDrawdown(
                    active_settings.paper_initial_balance_usd
                )

            def decision_support_drawdown(timestamp: datetime) -> Decimal:
                if active_settings.run_mode == "live":
                    live_runner = live_runner_for_decision_support
                    if live_runner is None or not live_runner.initialized:
                        raise RuntimeError(
                            "live decision-support equity is not initialized"
                        )
                    live_state = live_runner.risk.state
                    return fresh_equity_drawdown(
                        current_equity=live_state.current_equity,
                        high_water_equity=live_state.high_water_equity,
                        observed_at=live_state.current_equity_observed_at,
                        evaluated_at=timestamp,
                        maximum_age_seconds=Decimal(
                            str(
                                max(
                                    active_settings.live_loop_interval_seconds * 3,
                                    active_settings.request_timeout_seconds * 2,
                                )
                            )
                        ),
                    )
                combined = (
                    multi_regime_runtime.combined_portfolio_snapshot(timestamp)
                    if multi_regime_runtime is not None
                    else None
                )
                portfolio = combined or runtime.portfolio.snapshot(timestamp)
                assert decision_support_drawdown_tracker is not None
                return decision_support_drawdown_tracker.observe(portfolio.equity)

            decision_support_provider = RuntimeDecisionSupportProvider(
                artifacts,
                RuntimeDecisionSupportConfig(
                    meta_label_enabled=(
                        active_settings.decision_support_meta_label_enabled
                    ),
                    rl_enabled=active_settings.decision_support_rl_enabled,
                    meta_label_maximum_feature_zscore=(
                        active_settings.decision_support_meta_label_maximum_feature_zscore
                    ),
                    intent_feature_maximum_age_seconds=Decimal(
                        active_settings.multi_regime_stale_after_seconds
                    ),
                    technical_feature_maximum_age_seconds=Decimal(
                        active_settings.multi_regime_strategy_interval_seconds
                        + active_settings.multi_regime_source_interval_seconds
                    ),
                    orderflow_feature_maximum_age_seconds=Decimal(
                        active_settings.multi_regime_stale_after_seconds
                    ),
                    regime_feature_maximum_age_seconds=Decimal(
                        active_settings.multi_regime_regime_interval_seconds
                        + active_settings.multi_regime_source_interval_seconds
                    ),
                    derivatives_feature_maximum_age_seconds=Decimal(
                        active_settings.funding_snapshot_stale_seconds
                    ),
                    rl_maximum_state_age_seconds=(
                        active_settings.decision_support_rl_maximum_state_age_seconds
                    ),
                    rl_maximum_drawdown_fraction=(
                        active_settings.decision_support_rl_maximum_drawdown_fraction
                    ),
                ),
                drawdown_provider=decision_support_drawdown,
                reconciliation_health_provider=lambda: entry_health()[0],
            )
        multi_regime_engine = MultiRegimeEngine(
            MultiRegimeEngineConfig(
                mode=runtime_mode,
                assets=active_settings.multi_regime_asset_values,
                source_interval_seconds=(
                    active_settings.multi_regime_source_interval_seconds
                ),
                strategy_interval_seconds=(
                    active_settings.multi_regime_strategy_interval_seconds
                ),
                regime_interval_seconds=(
                    active_settings.multi_regime_regime_interval_seconds
                ),
                stale_after_seconds=(
                    active_settings.multi_regime_stale_after_seconds
                ),
                estimated_cost_bps=active_settings.multi_regime_estimated_cost_bps,
            ),
            risk_context_provider=risk_provider,
            # The mature legacy paper pipeline remains the sole funding execution
            # owner. The canonical suite still evaluates funding contexts, but it
            # cannot open a duplicate position or double-count a settlement.
            executable_signal_types=(
                PAPER_EXECUTABLE_SIGNAL_TYPES - {SignalType.FUNDING_BASIS}
            ),
            supplemental_context_provider=supplemental_provider,
            decision_support_provider=decision_support_provider,
            execution_snapshot_provider=execution_snapshot_provider,
            advanced_risk_context_provider=advanced_risk_provider,
        )
        multi_regime_runtime = DurableMultiRegimeRuntime(
            multi_regime_engine,
            session_factory,
            paper_broker=paper_broker,
            advanced_paper_broker=advanced_paper_broker,
            runtime_state=runtime,
        )
        event_router.subscribe(multi_regime_runtime.publish)
        if public_events is not None:
            universe_publisher = RuntimeUniversePublisher(
                session_factory,
                event_router.publish,
                selector_config=UniverseSelectorConfig(
                    maximum_assets=(
                        active_settings.multi_regime_universe_maximum_assets
                    ),
                    maximum_new_assets_per_rebalance=(
                        active_settings.multi_regime_universe_maximum_new_assets
                    ),
                    maximum_data_age_seconds=(
                        active_settings.multi_regime_universe_maximum_data_age_seconds
                    ),
                    minimum_listing_age_days=(
                        active_settings.multi_regime_universe_minimum_listing_age_days
                    ),
                    minimum_statistics_days=(
                        active_settings.multi_regime_universe_minimum_statistics_days
                    ),
                    minimum_venue_count=(
                        active_settings.multi_regime_universe_minimum_venue_count
                    ),
                    minimum_quote_volume_24h_usd=(
                        active_settings.multi_regime_universe_minimum_quote_volume_usd
                    ),
                    minimum_depth_within_25bps_usd=(
                        active_settings.multi_regime_universe_minimum_depth_usd
                    ),
                    minimum_open_interest_usd=(
                        active_settings.multi_regime_universe_minimum_open_interest_usd
                    ),
                    maximum_spread_bps=(
                        active_settings.multi_regime_universe_maximum_spread_bps
                    ),
                    maximum_slippage_10k_bps=(
                        active_settings.multi_regime_universe_maximum_slippage_bps
                    ),
                    minimum_funding_samples=(
                        active_settings.multi_regime_universe_minimum_funding_samples
                    ),
                    minimum_market_data_coverage=(
                        active_settings.multi_regime_universe_minimum_data_coverage
                    ),
                    minimum_entry_score=(
                        active_settings.multi_regime_universe_minimum_entry_score
                    ),
                    minimum_retention_score=(
                        active_settings.multi_regime_universe_minimum_retention_score
                    ),
                    target_funding_potential_bps_daily=(
                        active_settings.multi_regime_universe_target_funding_bps_daily
                    ),
                    excluded_assets=(
                        active_settings.multi_regime_universe_excluded_asset_values
                    ),
                ),
                rebalance_seconds=(
                    active_settings.multi_regime_universe_rebalance_seconds
                ),
                enabled=active_settings.multi_regime_dynamic_universe_enabled,
            )
            public_events.set_pre_mirror_snapshot_observer(
                universe_publisher.observe_snapshot
            )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal live_runner_for_decision_support
        configure_logging(active_settings.log_level)
        app.state.adapters = adapters
        app.state.adapter = adapters["bybit"]
        app.state.session_factory = session_factory
        app.state.runtime = runtime
        app.state.event_writer = event_writer
        app.state.event_router = event_router
        app.state.event_quality_monitor = event_quality_monitor
        app.state.multi_regime_runtime = multi_regime_runtime
        app.state.decision_support_provider = decision_support_provider
        app.state.decision_support_drawdown_tracker = (
            decision_support_drawdown_tracker
        )
        app.state.universe_publisher = universe_publisher
        app.state.clickhouse_replicator = clickhouse_replicator
        app.state.clickhouse_decision_replicator = clickhouse_decision_replicator
        app.state.public_events = public_events
        runner: PaperTestRunner | SharedMarketPaperComparisonRunner | LiveTradingRunner | None = (
            None
        )
        baseline_runtime: RuntimeState | None = None
        task: asyncio.Task[None] | None = None
        app.state.live_runner = None
        if active_settings.run_mode == "paper_test" and active_settings.paper_auto_init_database:
            await init_database(engine)
        event_writer.start()
        if clickhouse_replicator is not None:
            analytics_task = asyncio.create_task(
                clickhouse_replicator.run(), name="clickhouse-event-replicator"
            )
            runtime.background_tasks.add(analytics_task)
            analytics_task.add_done_callback(runtime.background_tasks.discard)
        if clickhouse_decision_replicator is not None:
            decision_analytics_task = asyncio.create_task(
                clickhouse_decision_replicator.run(),
                name="clickhouse-decision-replicator",
            )
            runtime.background_tasks.add(decision_analytics_task)
            decision_analytics_task.add_done_callback(
                runtime.background_tasks.discard
            )
        recovery_task = asyncio.create_task(
            market_backtest_recovery_loop(
                runtime,
                market_replay_job_store,
                market_replay_worker_id,
            ),
            name="market-replay-recovery",
        )
        runtime.background_tasks.add(recovery_task)
        recovery_task.add_done_callback(runtime.background_tasks.discard)
        if active_settings.run_mode == "paper_test":
            candidate_runner = PaperTestRunner(
                active_settings,
                runtime,
                session_factory,
                public_events=public_events,
                canonical_book_event_sink=event_router.publish,
                canonical_option_event_sink=event_router.publish,
                combined_snapshot_provider=(
                    multi_regime_runtime.combined_portfolio_snapshot
                    if paper_broker is not None
                    and multi_regime_runtime is not None
                    else None
                ),
                canonical_consumer_barrier=(
                    multi_regime_runtime.flush
                    if multi_regime_runtime is not None
                    else None
                ),
            )
            if active_settings.paper_comparison_enabled:
                baseline_settings = active_settings.model_copy(
                    update={
                        "paper_strategy_profile": "baseline",
                        "paper_simulation_version": (
                            active_settings.paper_baseline_simulation_version
                        ),
                        "telegram_enabled": False,
                    }
                )
                baseline_runtime = RuntimeState(
                    baseline_settings,
                    adapters,
                    emit_metrics=False,
                    entry_health=entry_health,
                )
                baseline_runner = PaperTestRunner(
                    baseline_settings,
                    baseline_runtime,
                    session_factory,
                    collector=candidate_runner.collector,
                )
                runner = SharedMarketPaperComparisonRunner(candidate_runner, baseline_runner)
                app.state.baseline_runtime = baseline_runtime
            else:
                runner = candidate_runner
            app.state.paper_runner = runner
            app.state.baseline_runtime = baseline_runtime
            if isinstance(runner, SharedMarketPaperComparisonRunner):
                await runner.restore()
            else:
                await runner.restore(restore_history=True)
            if multi_regime_runtime is not None:
                await multi_regime_runtime.restore_features(
                    start=datetime.now(UTC)
                    - timedelta(hours=active_settings.multi_regime_restore_hours)
                )
            if decision_support_drawdown_tracker is not None:
                preferred_scope = "combined" if paper_broker is not None else "legacy"
                async with session_factory() as session:
                    high_water = await load_portfolio_equity_high_water(
                        session,
                        simulation_version=active_settings.paper_simulation_version,
                        preferred_scope=preferred_scope,
                    )
                if high_water is not None:
                    decision_support_drawdown_tracker.restore(high_water)
                current = (
                    multi_regime_runtime.combined_portfolio_snapshot(
                        datetime.now(UTC)
                    )
                    if multi_regime_runtime is not None
                    else None
                ) or runtime.portfolio.snapshot(datetime.now(UTC))
                decision_support_drawdown_tracker.observe(current.equity)
            if (
                isinstance(runner, PaperTestRunner)
                and runner.acceptance_collector is not None
            ):
                await runner.prepare_run()
            task = asyncio.create_task(runner.run(), name="paper-test-runner")
        elif active_settings.run_mode == "live":
            if multi_regime_runtime is not None:
                await multi_regime_runtime.restore_features(
                    start=datetime.now(UTC)
                    - timedelta(hours=active_settings.multi_regime_restore_hours)
                )
            trading_adapters = create_trading_adapters(active_settings)
            private_streams = create_private_stream_supervisor(
                active_settings, trading_adapters, event_router.publish
            )
            runner = LiveTradingRunner(
                active_settings,
                runtime,
                session_factory,
                trading_adapters,
                private_streams,
                public_events,
                canonical_book_event_sink=event_router.publish,
                canonical_option_event_sink=event_router.publish,
            )
            live_runner_for_decision_support = runner
            app.state.live_runner = runner
            task = asyncio.create_task(runner.run(), name="live-trading-runner")
        primary_error: BaseException | None = None
        try:
            if multi_regime_runtime is not None:
                multi_regime_runtime.start()
            yield
        except BaseException as error:
            primary_error = error
            raise
        finally:
            shutdown_failures: list[BaseException] = []
            background_tasks = tuple(runtime.background_tasks)
            for background_task in background_tasks:
                background_task.cancel()
            if background_tasks:
                await _attempt_shutdown(
                    "background_tasks",
                    lambda: asyncio.gather(
                        *background_tasks,
                        return_exceptions=True,
                    ),
                    shutdown_failures,
                )
            if runner is not None:
                await _attempt_shutdown("runner_stop", runner.stop, shutdown_failures)
            if task is not None:
                try:
                    shutdown_timeout = (
                        max(
                            60.0,
                            active_settings.live_order_timeout_seconds * 2
                            + active_settings.request_timeout_seconds
                            + 10.0,
                        )
                        if active_settings.run_mode == "live"
                        else 10.0
                    )
                    await asyncio.wait_for(task, timeout=shutdown_timeout)
                except TimeoutError:
                    task.cancel()
                    await _attempt_shutdown(
                        "runner_task_cancel",
                        lambda: asyncio.gather(task, return_exceptions=True),
                        shutdown_failures,
                    )
                except asyncio.CancelledError as error:
                    shutdown_failures.append(error)
                    logger.warning(
                        "shutdown_component_cancelled",
                        extra={"component": "runner_task"},
                    )
                except Exception as error:
                    shutdown_failures.append(error)
                    logger.exception(
                        "shutdown_component_failed",
                        extra={"component": "runner_task"},
                    )
            if runner is not None:
                await _attempt_shutdown("runner_close", runner.close, shutdown_failures)
            if multi_regime_runtime is not None:
                await _attempt_shutdown(
                    "multi_regime_runtime",
                    multi_regime_runtime.stop,
                    shutdown_failures,
                )
            for venue, adapter in adapters.items():
                await _attempt_shutdown(
                    f"public_adapter:{venue}",
                    adapter.close,
                    shutdown_failures,
                )
            await _attempt_shutdown("event_writer", event_writer.stop, shutdown_failures)
            await _attempt_shutdown(
                "control_plane_rate_limiter",
                control_plane_rate_limiter.close,
                shutdown_failures,
            )
            await _attempt_shutdown(
                "control_plane_token_revocation_store",
                control_plane_token_revocation_store.close,
                shutdown_failures,
            )
            if clickhouse_writer is not None:
                await _attempt_shutdown(
                    "clickhouse_writer",
                    clickhouse_writer.close,
                    shutdown_failures,
                )
            await _attempt_shutdown("database_engine", engine.dispose, shutdown_failures)
            _finalize_shutdown(shutdown_failures, primary_error)

    app = FastAPI(title="Funding Arbitrage Bot", version="0.1.0", lifespan=lifespan)
    control_plane_security = ControlPlaneSecurity(ControlPlanePolicy.from_settings(active_settings))
    control_plane_audit_sink = DatabaseControlPlaneAuditSink(session_factory)
    control_plane_idempotency_store = DatabaseControlPlaneIdempotencyStore(
        session_factory,
        active_settings.control_plane_idempotency_ttl_seconds,
    )
    control_plane_rate_limiter = create_control_plane_rate_limiter(active_settings)
    control_plane_token_revocation_store = create_token_revocation_store(active_settings)
    app.state.control_plane_security = control_plane_security
    app.state.control_plane_audit_sink = control_plane_audit_sink
    app.state.control_plane_idempotency_store = control_plane_idempotency_store
    app.state.control_plane_rate_limiter = control_plane_rate_limiter
    app.state.control_plane_token_revocation_store = control_plane_token_revocation_store
    app.state.market_replay_job_store = market_replay_job_store
    app.state.market_replay_worker_id = market_replay_worker_id
    app.add_middleware(
        ControlPlaneMiddleware,
        security=control_plane_security,
        audit_sink=control_plane_audit_sink,
        idempotency_store=control_plane_idempotency_store,
        rate_limiter=control_plane_rate_limiter,
        token_revocation_store=control_plane_token_revocation_store,
    )

    @app.middleware("http")
    async def observe_http(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = perf_counter()
        try:
            return await call_next(request)
        except Exception:
            api_errors_total.labels(request.url.path).inc()
            raise
        finally:
            api_request_latency_seconds.labels(request.method, request.url.path).observe(
                perf_counter() - started
            )

    app.include_router(market_data_router)
    app.include_router(multi_regime_router)
    app.include_router(system_router)
    app.include_router(opportunities_router)
    app.include_router(portfolio_router)
    app.include_router(analytics_router)
    app.include_router(backtests_router)
    app.include_router(control_router)
    app.include_router(websocket_router)
    app.include_router(scan_router)
    app.mount("/metrics", make_asgi_app())
    app.mount("/dashboard", StaticFiles(directory="dashboard", html=True), name="dashboard")

    @app.get("/health")
    async def health() -> dict[str, object]:
        autotrade_start = active_settings.paper_autotrade_start_utc
        now = datetime.now(UTC)
        return {
            "status": "ok",
            "environment": active_settings.app_env,
            \
            "execution_mode": active_settings.execution_mode,
            "paper_autotrade_enabled": active_settings.paper_autotrade,
            "paper_autotrade_start_utc": (
                autotrade_start.astimezone(UTC).isoformat().replace("+00:00", "Z")
                if autotrade_start is not None
                else None
            ),
            "paper_autotrade_active": active_settings.paper_autotrade
            and (autotrade_start is None or now >= autotrade_start),
        }

    @app.get("/health/ready")
    async def ready() -> dict[str, object]:
        if active_settings.control_plane_security_enabled:
            try:
                await control_plane_audit_sink.probe()
                await control_plane_idempotency_store.probe()
                await control_plane_rate_limiter.probe()
                await control_plane_token_revocation_store.probe()
            except Exception as error:
                raise HTTPException(
                    status_code=503,
                    detail="control-plane security storage is unavailable",
                ) from error
        try:
            await market_replay_job_store.probe()
        except Exception as error:
            raise HTTPException(
                status_code=503,
                detail="market replay job persistence is unavailable",
            ) from error
        entry_pipeline_healthy, _ = entry_health()
        if not entry_pipeline_healthy:
            raise HTTPException(
                status_code=503,
                detail="canonical market-data pipeline is unavailable",
            )
        paper_runner = getattr(app.state, "paper_runner", None)
        snapshot = (
            paper_runner.last_completed_snapshot
            if isinstance(paper_runner, SharedMarketPaperComparisonRunner)
            else runtime.last_completed_snapshot
        )
        if active_settings.run_mode == "paper_test":
            if snapshot is None:
                raise HTTPException(
                    status_code=503,
                    detail="paper runner has not completed a cycle",
                )
            age = (datetime.now(UTC) - snapshot.captured_at).total_seconds()
            max_age = max(
                active_settings.market_data_stale_seconds * 3,
                active_settings.paper_loop_interval_seconds * 3,
                300,
            )
            if age > max_age:
                raise HTTPException(status_code=503, detail="paper market snapshot is stale")
            healthy_venues = {ticker.exchange for ticker in snapshot.tickers}
            minimum_venues = min(3, len(active_settings.paper_venue_values))
            if len(healthy_venues) < minimum_venues:
                raise HTTPException(
                    status_code=503,
                    detail="fewer than three venues supplied usable market data",
                )
        elif active_settings.run_mode == "live":
            live_runner: LiveTradingRunner | None = app.state.live_runner
            if live_runner is None or not live_runner.initialized:
                detail = (
                    live_runner.startup_error
                    if live_runner is not None and live_runner.startup_error
                    else "live private preflight has not completed"
                )
                raise HTTPException(status_code=503, detail=detail)
            if live_runner.risk.paused:
                raise HTTPException(
                    status_code=503,
                    detail=f"live trading paused: {live_runner.risk.paused_reason}",
                )
            if snapshot is None:
                raise HTTPException(status_code=503, detail="live market cycle not completed")
            age = (datetime.now(UTC) - snapshot.captured_at).total_seconds()
            if age > max(
                active_settings.market_data_stale_seconds * 3,
                active_settings.live_loop_interval_seconds * 3,
            ):
                raise HTTPException(status_code=503, detail="live market snapshot is stale")
            reconciliation = live_runner.reconciler.last_result
            if reconciliation is None or not reconciliation.passed:
                raise HTTPException(status_code=503, detail="private reconciliation has not passed")
            private_streams = live_runner.private_streams
            if private_streams is None:
                raise HTTPException(status_code=503, detail="private streams are not configured")
            private_healthy, private_reason = private_streams.health()
            if not private_healthy:
                raise HTTPException(
                    status_code=503,
                    detail=private_reason or "private streams are unhealthy",
                )
        return {
            "status": "ready",
            "run_mode": active_settings.run_mode,
            "market_data_mode": active_settings.market_data_mode,
            "last_market_snapshot": snapshot.captured_at if snapshot else None,
            "comparison_enabled": (
                active_settings.paper_comparison_enabled
                if active_settings.run_mode == "paper_test"
                else False
            ),
            "healthy_venues": (
                sorted({ticker.exchange for ticker in snapshot.tickers}) if snapshot else []
            ),
        }

    return app


app = create_app()
