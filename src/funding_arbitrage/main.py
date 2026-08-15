"""FastAPI entry point."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app

from funding_arbitrage.api.routes.analytics import router as analytics_router
from funding_arbitrage.api.routes.backtests import router as backtests_router
from funding_arbitrage.api.routes.market_data import router as market_data_router
from funding_arbitrage.api.routes.opportunities import router as opportunities_router
from funding_arbitrage.api.routes.portfolio import router as portfolio_router
from funding_arbitrage.api.routes.scan import router as scan_router
from funding_arbitrage.api.routes.system import router as system_router
from funding_arbitrage.api.routes.websocket import router as websocket_router
from funding_arbitrage.config import Settings, get_settings
from funding_arbitrage.database.session import create_database, init_database
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.trading import create_trading_adapters
from funding_arbitrage.logging import configure_logging
from funding_arbitrage.monitoring.metrics import api_errors_total, api_request_latency_seconds
from funding_arbitrage.services.live_runner import LiveTradingRunner
from funding_arbitrage.services.paper_runner import (
    PaperTestRunner,
    SharedMarketPaperComparisonRunner,
)
from funding_arbitrage.services.runtime import RuntimeState


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or get_settings()
    engine, session_factory = create_database(active_settings)
    adapters = create_public_adapters(active_settings)
    runtime = RuntimeState(active_settings, adapters)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(active_settings.log_level)
        app.state.adapters = adapters
        app.state.adapter = adapters["bybit"]
        app.state.session_factory = session_factory
        app.state.runtime = runtime
        runner: (
            PaperTestRunner
            | SharedMarketPaperComparisonRunner
            | LiveTradingRunner
            | None
        ) = None
        baseline_runtime: RuntimeState | None = None
        task: asyncio.Task[None] | None = None
        app.state.live_runner = None
        if active_settings.run_mode == "paper_test":
            if active_settings.paper_auto_init_database:
                await init_database(engine)
            candidate_runner = PaperTestRunner(active_settings, runtime, session_factory)
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
                    baseline_settings, adapters, emit_metrics=False
                )
                baseline_runner = PaperTestRunner(
                    baseline_settings,
                    baseline_runtime,
                    session_factory,
                    collector=candidate_runner.collector,
                )
                runner = SharedMarketPaperComparisonRunner(
                    candidate_runner, baseline_runner
                )
                app.state.baseline_runtime = baseline_runtime
            else:
                runner = candidate_runner
            app.state.paper_runner = runner
            app.state.baseline_runtime = baseline_runtime
            task = asyncio.create_task(runner.run(), name="paper-test-runner")
        elif active_settings.run_mode == "live":
            trading_adapters = create_trading_adapters(active_settings)
            runner = LiveTradingRunner(
                active_settings,
                runtime,
                session_factory,
                trading_adapters,
            )
            app.state.live_runner = runner
            task = asyncio.create_task(runner.run(), name="live-trading-runner")
        try:
            yield
        finally:
            for background_task in runtime.background_tasks:
                background_task.cancel()
            if runtime.background_tasks:
                await asyncio.gather(*runtime.background_tasks, return_exceptions=True)
            if runner is not None:
                await runner.stop()
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
                    await asyncio.gather(task, return_exceptions=True)
            if runner is not None:
                await runner.close()
            for adapter in adapters.values():
                await adapter.close()
            await engine.dispose()

    app = FastAPI(title="Funding Arbitrage Bot", version="0.1.0", lifespan=lifespan)

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
    app.include_router(system_router)
    app.include_router(opportunities_router)
    app.include_router(portfolio_router)
    app.include_router(analytics_router)
    app.include_router(backtests_router)
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
            "run_mode": active_settings.run_mode,
            "market_data_mode": active_settings.market_data_mode,
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
        snapshot = runtime.last_completed_snapshot
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
            comparison_runtime = getattr(app.state, "baseline_runtime", None)
            if (
                comparison_runtime is not None
                and comparison_runtime.last_completed_snapshot is not snapshot
            ):
                raise HTTPException(
                    status_code=503,
                    detail="baseline has not processed the candidate market snapshot",
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
                raise HTTPException(
                    status_code=503, detail="private reconciliation has not passed"
                )
        return {
            "status": "ready",
            "run_mode": active_settings.run_mode,
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
