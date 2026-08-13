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
from funding_arbitrage.logging import configure_logging
from funding_arbitrage.monitoring.metrics import api_errors_total, api_request_latency_seconds
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
        runner: PaperTestRunner | SharedMarketPaperComparisonRunner | None = None
        baseline_runtime: RuntimeState | None = None
        task: asyncio.Task[None] | None = None
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
        try:
            yield
        finally:
            for background_task in runtime.background_tasks:
                background_task.cancel()
            if runtime.background_tasks:
                await asyncio.gather(*runtime.background_tasks, return_exceptions=True)
            if runner is not None:
                await runner.close()
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=10)
                except TimeoutError:
                    task.cancel()
            for adapter in adapters.values():
                await adapter.close()
            await engine.dispose()

    app = FastAPI(title="Funding Arbitrage Research Bot", version="0.1.0", lifespan=lifespan)

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
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "environment": active_settings.app_env,
            "run_mode": active_settings.run_mode,
            "market_data_mode": active_settings.market_data_mode,
            "execution_mode": active_settings.execution_mode,
        }

    @app.get("/health/ready")
    async def ready() -> dict[str, object]:
        snapshot = runtime.latest_snapshot
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
                and comparison_runtime.latest_snapshot is not snapshot
            ):
                raise HTTPException(
                    status_code=503,
                    detail="baseline has not processed the candidate market snapshot",
                )
        return {
            "status": "ready",
            "run_mode": active_settings.run_mode,
            "last_market_snapshot": snapshot.captured_at if snapshot else None,
            "comparison_enabled": active_settings.paper_comparison_enabled,
            "healthy_venues": (
                sorted({ticker.exchange for ticker in snapshot.tickers}) if snapshot else []
            ),
        }

    return app


app = create_app()
