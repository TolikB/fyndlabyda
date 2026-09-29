"""FastAPI entry point."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
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
from funding_arbitrage.config import (
    Settings,
    assert_public_data_only,
    deprecated_variables,
    get_settings,
)
from funding_arbitrage.database.session import create_database, init_database
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.execution.paper import SIMULATOR_VERSION
from funding_arbitrage.logging import configure_logging
from funding_arbitrage.monitoring.metrics import api_errors_total, api_request_latency_seconds
from funding_arbitrage.services.paper_runner import PaperTestRunner
from funding_arbitrage.services.release_manifest import ManifestCheck, check_manifest
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import load_series_file

logger = logging.getLogger(__name__)

_RUNNER_STOP_TIMEOUT_SECONDS = 30


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or get_settings()
    engine, session_factory = create_database(active_settings)
    adapters = create_public_adapters(active_settings)
    series_file = (
        load_series_file(active_settings.paper_series_file)
        if active_settings.run_mode == "paper_test"
        else None
    )
    runtime = RuntimeState(active_settings, adapters, series_file)
    release = ManifestCheck(ok=False, problems=["not checked"])

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal release
        configure_logging(active_settings.log_level)
        # Paper-only deployment: refuse to start with any exchange credential present.
        assert_public_data_only(os.environ)
        deprecated = deprecated_variables(os.environ)
        if deprecated:
            logger.warning(
                "deprecated_settings_ignored",
                extra={"variables": deprecated, "hint": "see config/paper_series.yaml"},
            )
        release = check_manifest(Path("."), runtime_only=True)
        logger.info(
            "release_manifest_checked",
            extra={
                "ok": release.ok,
                "runtime_sha256": release.runtime_sha256,
                "problems": release.problems[:10],
            },
        )
        app.state.adapters = adapters
        app.state.session_factory = session_factory
        app.state.runtime = runtime
        runner: PaperTestRunner | None = None
        task: asyncio.Task[None] | None = None
        if active_settings.run_mode == "paper_test":
            if active_settings.paper_auto_init_database:
                await init_database(engine)
            runner = PaperTestRunner(
                active_settings,
                runtime,
                session_factory,
                series_file=series_file,
                engine=engine,
                release_hash=release.runtime_sha256 if release.ok else None,
            )
            app.state.paper_runner = runner
            task = asyncio.create_task(runner.run(), name="paper-test-runner")
        try:
            yield
        finally:
            if runner is not None:
                await runner.stop()
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=_RUNNER_STOP_TIMEOUT_SECONDS)
                except TimeoutError:
                    task.cancel()
            for adapter in adapters.values():
                await adapter.close()
            await engine.dispose()

    app = FastAPI(title="Funding Arbitrage Research Bot", version="0.2.0", lifespan=lifespan)

    @app.middleware("http")
    async def observe_http(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = perf_counter()
        failed = False
        try:
            return await call_next(request)
        except Exception:
            failed = True
            raise
        finally:
            # Route templates (not raw URLs) keep metric label cardinality bounded.
            path = getattr(request.scope.get("route"), "path", "unmatched")
            if failed:
                api_errors_total.labels(path).inc()
            api_request_latency_seconds.labels(request.method, path).observe(
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
    @app.get("/health/live")
    async def health() -> dict[str, object]:
        return {
            "status": "ok",
            "environment": active_settings.app_env,
            "run_mode": active_settings.run_mode,
            "market_data_mode": active_settings.market_data_mode,
            "execution_mode": active_settings.execution_mode,
            "autotrade": active_settings.paper_autotrade,
            "simulator_version": SIMULATOR_VERSION,
            "release_manifest_ok": release.ok,
            "release_runtime_sha256": release.runtime_sha256,
        }

    @app.get("/health/ready")
    async def ready() -> dict[str, object]:
        runner = runtime.runner
        if active_settings.run_mode == "paper_test":
            if runner is None or not runner.healthy():
                raise HTTPException(
                    status_code=503,
                    detail={
                        "status": "not_ready",
                        "fatal_error": runner.fatal_error if runner else "runner not created",
                        "last_success_at": (
                            runner.last_success_at.isoformat()
                            if runner and runner.last_success_at
                            else None
                        ),
                        "persistence_ok": runner.persistence_ok if runner else None,
                    },
                )
        last_cycle = runner.last_cycle if runner else None
        return {
            "status": "ready",
            "run_mode": active_settings.run_mode,
            "last_market_snapshot": runtime.latest_snapshot.captured_at
            if runtime.latest_snapshot
            else None,
            "last_cycle_status": last_cycle.status if last_cycle else None,
            "last_cycle_ms": last_cycle.duration_ms if last_cycle else None,
            "venues_failed": last_cycle.venues_failed if last_cycle else [],
            "halted_series": {
                series_id: account.halted_reason
                for series_id, account in runtime.accounts.items()
                if account.halted_reason
            },
        }

    return app


app = create_app()
