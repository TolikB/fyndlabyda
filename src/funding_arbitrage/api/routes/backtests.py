from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.api.schemas.backtests import (
    BacktestRequest,
    MarketReplayRequest,
    PaperReplayRequest,
)
from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.database_replay import DatabasePaperReplay
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import BacktestEvent, PositionEvent
from funding_arbitrage.backtest.historical_replay import HistoricalMarketReplay
from funding_arbitrage.config import get_settings
from funding_arbitrage.database.models import BacktestResultRecord, BacktestRunRecord
from funding_arbitrage.database.repositories.backtest_jobs import (
    BacktestJobLeaseLost,
    DurableMarketReplayJobStore,
    MarketReplayJobSnapshot,
)
from funding_arbitrage.database.repositories.market_data import save_backtest_result
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()
market_replay_semaphore = asyncio.Semaphore(1)
logger = logging.getLogger(__name__)
_JOB_RECOVERY_INTERVAL_SECONDS = 15.0


@router.post("/backtests/compare-market")
async def compare_market_backtest(
    request: MarketReplayRequest,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    return await _run_market_comparison(request, runtime, session)


@router.post(
    "/backtests/compare-market/jobs",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_market_backtest_job(
    payload: MarketReplayRequest,
    http_request: Request,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> dict[str, object]:
    store = _job_store(http_request)
    worker_id = _worker_id(http_request)
    job_id = str(uuid4())
    snapshot = await store.create(job_id, payload.model_dump(mode="json"))
    _cache_job(runtime, snapshot)
    _schedule_market_comparison_job(
        job_id,
        payload,
        runtime,
        store,
        worker_id,
    )
    return snapshot.public_dict()


@router.get("/backtests/compare-market/jobs/{job_id}")
async def get_market_backtest_job(
    job_id: str,
    http_request: Request,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> dict[str, object]:
    snapshot = await _job_store(http_request).get(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="market replay job not found")
    _cache_job(runtime, snapshot)
    return snapshot.public_dict()


def _schedule_market_comparison_job(
    job_id: str,
    request: MarketReplayRequest,
    runtime: RuntimeState,
    store: DurableMarketReplayJobStore,
    worker_id: str,
) -> bool:
    task_name = f"market-replay-{job_id}"
    if any(task.get_name() == task_name and not task.done() for task in runtime.background_tasks):
        return False
    task = asyncio.create_task(
        _execute_market_comparison_job(
            job_id,
            request,
            runtime,
            store,
            worker_id,
        ),
        name=task_name,
    )
    runtime.background_tasks.add(task)
    task.add_done_callback(runtime.background_tasks.discard)
    return True


async def _execute_market_comparison_job(
    job_id: str,
    request: MarketReplayRequest,
    runtime: RuntimeState,
    store: DurableMarketReplayJobStore,
    worker_id: str,
) -> None:
    if not await store.claim(job_id, worker_id):
        return
    snapshot = await store.get(job_id)
    if snapshot is not None:
        _cache_job(runtime, snapshot)

    async def progress(stage: str) -> None:
        await store.progress(job_id, worker_id, stage)
        current = await store.get(job_id)
        if current is not None:
            _cache_job(runtime, current)

    try:
        result = await _run_with_lease_heartbeat(
            job_id,
            worker_id,
            store,
            _run_market_comparison(
                request,
                runtime,
                store.session_factory,
                job_id=job_id,
                progress=progress,
            ),
        )
        await store.complete(job_id, worker_id, result)
    except asyncio.CancelledError:
        await asyncio.shield(store.requeue_interrupted(job_id, worker_id))
        raise
    except BacktestJobLeaseLost:
        logger.warning("market replay lease lost", extra={"job_id": job_id})
        return
    except Exception as error:
        error_code = type(getattr(error, "orig", error)).__name__
        try:
            await store.fail(job_id, worker_id, error_code)
        except BacktestJobLeaseLost:
            logger.warning("market replay failure after lease loss", extra={"job_id": job_id})
            return
        logger.exception("market replay job failed", extra={"job_id": job_id})
    finally:
        current = await store.get(job_id)
        if current is not None:
            _cache_job(runtime, current)


async def _run_with_lease_heartbeat(
    job_id: str,
    worker_id: str,
    store: DurableMarketReplayJobStore,
    operation: Coroutine[Any, Any, dict[str, object]],
) -> dict[str, object]:
    operation_task = asyncio.create_task(
        operation, name=f"market-replay-work-{job_id}"
    )
    heartbeat_task = asyncio.create_task(
        _heartbeat_market_job(job_id, worker_id, store),
        name=f"market-replay-heartbeat-{job_id}",
    )
    try:
        tasks: set[asyncio.Task[Any]] = {operation_task, heartbeat_task}
        done, _ = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat_task in done:
            heartbeat_error = heartbeat_task.exception()
            if heartbeat_error is None:
                raise RuntimeError("market replay heartbeat stopped unexpectedly")
            raise heartbeat_error
        return await operation_task
    finally:
        for task in (operation_task, heartbeat_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, heartbeat_task, return_exceptions=True)


async def _heartbeat_market_job(
    job_id: str,
    worker_id: str,
    store: DurableMarketReplayJobStore,
) -> None:
    interval = max(5.0, store.lease_seconds / 3)
    while True:
        await asyncio.sleep(interval)
        await store.heartbeat(job_id, worker_id)


async def resume_market_backtest_jobs(
    runtime: RuntimeState,
    store: DurableMarketReplayJobStore,
    worker_id: str,
) -> int:
    scheduled = 0
    for snapshot in await store.recoverable():
        _cache_job(runtime, snapshot)
        try:
            payload = MarketReplayRequest.model_validate(snapshot.request_payload)
        except ValueError:
            if await store.claim(snapshot.id, worker_id):
                await store.fail(snapshot.id, worker_id, "InvalidPersistedRequest")
            continue
        if _schedule_market_comparison_job(
            snapshot.id,
            payload,
            runtime,
            store,
            worker_id,
        ):
            scheduled += 1
    return scheduled


async def market_backtest_recovery_loop(
    runtime: RuntimeState,
    store: DurableMarketReplayJobStore,
    worker_id: str,
) -> None:
    while True:
        try:
            await resume_market_backtest_jobs(runtime, store, worker_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("market replay recovery sweep failed")
        await asyncio.sleep(_JOB_RECOVERY_INTERVAL_SECONDS)


async def _run_market_comparison(
    request: MarketReplayRequest,
    runtime: RuntimeState,
    session_source: AsyncSession | async_sessionmaker[AsyncSession],
    job: dict[str, object] | None = None,
    job_id: str | None = None,
    progress: Callable[[str], Awaitable[None]] | None = None,
) -> dict[str, object]:
    if isinstance(session_source, AsyncSession):
        return await _run_market_comparison_session(
            request,
            runtime,
            session_source,
            job=job,
            job_id=job_id,
            progress=progress,
        )
    async with session_source() as session:
        return await _run_market_comparison_session(
            request,
            runtime,
            session,
            job=job,
            job_id=job_id,
            progress=progress,
        )


async def _run_market_comparison_session(
    request: MarketReplayRequest,
    runtime: RuntimeState,
    session: AsyncSession,
    *,
    job: dict[str, object] | None = None,
    job_id: str | None = None,
    progress: Callable[[str], Awaitable[None]] | None = None,
) -> dict[str, object]:
    replay = HistoricalMarketReplay()
    async with market_replay_semaphore:
        await _update_job_stage(job, "loading_dataset", progress)
        dataset = await replay.load(session, request.start, request.end)
        settings = get_settings()
        await _update_job_stage(job, "simulating_baseline", progress)
        baseline = await asyncio.to_thread(
            replay.simulate,
            dataset,
            "baseline",
            request.initial_capital,
            settings,
        )
        await _update_job_stage(job, "simulating_candidate", progress)
        candidate = await asyncio.to_thread(
            replay.simulate,
            dataset,
            "candidate",
            request.initial_capital,
            settings,
        )
    await _update_job_stage(job, "saving_results", progress)
    comparison = compare_paper_datasets(baseline, candidate, request.initial_capital)
    run_ids: dict[str, str] = {}
    for profile, profile_dataset in (
        ("baseline", baseline),
        ("candidate", candidate),
    ):
        started_at = datetime.now(UTC)
        result = BacktestEngine().run(
            profile_dataset.events,
            request.initial_capital,
            {**request.model_dump(mode="json"), "profile": profile},
            profile_dataset.dataset_version,
        )
        run_id = str(uuid4()) if job_id is None else f"{job_id}:{profile}"
        run_ids[profile] = run_id
        runtime.backtests[run_id] = result
        await save_backtest_result(
            session,
            run_id,
            result,
            started_at,
            {**request.model_dump(mode="json"), "profile": profile},
        )
    return {
        "dataset_version": dataset.dataset_version,
        "coverage": dataset.coverage,
        "run_ids": run_ids,
        "positions": {
            "baseline": baseline.position_count,
            "candidate": candidate.position_count,
        },
        "attribution": {
            "baseline": baseline.attribution,
            "candidate": candidate.attribution,
        },
        "comparison": comparison,
    }


async def _update_job_stage(
    job: dict[str, object] | None,
    stage: str,
    progress: Callable[[str], Awaitable[None]] | None,
) -> None:
    if job is not None:
        job.update(
            status="running",
            stage=stage,
            updated_at=datetime.now(UTC).isoformat(),
        )
    if progress is not None:
        await progress(stage)


def _cache_job(runtime: RuntimeState, snapshot: MarketReplayJobSnapshot) -> None:
    runtime.market_replay_jobs[snapshot.id] = snapshot.public_dict()


def _job_store(request: Request) -> DurableMarketReplayJobStore:
    return cast(DurableMarketReplayJobStore, request.app.state.market_replay_job_store)


def _worker_id(request: Request) -> str:
    return cast(str, request.app.state.market_replay_worker_id)

@router.post("/backtests/replay-paper")
async def replay_paper_backtest(
    request: PaperReplayRequest,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    started_at = datetime.now(UTC)
    dataset = await DatabasePaperReplay().load(
        session,
        request.simulation_version,
        request.start,
        request.end,
    )
    result = BacktestEngine().run(
        dataset.events,
        request.initial_capital,
        request.model_dump(mode="json"),
        dataset.dataset_version,
    )
    run_id = str(uuid4())
    runtime.backtests[run_id] = result
    await save_backtest_result(
        session,
        run_id,
        result,
        started_at,
        request.model_dump(mode="json"),
    )
    return {
        "id": run_id,
        "simulation_version": request.simulation_version,
        "dataset_version": dataset.dataset_version,
        "position_count": dataset.position_count,
        "metrics": result.metrics.model_dump(mode="json"),
        "attribution": dataset.attribution,
    }


@router.post("/backtests")
async def create_backtest(
    request: BacktestRequest,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    events: list[BacktestEvent] = [
        PositionEvent(
            timestamp=datetime.strptime(f"{month}-01", "%Y-%m-%d").replace(tzinfo=UTC),
            position_id=month,
            state="CLOSED",
            pnl=value,
        )
        for month, value in sorted(request.monthly_pnl.items())
    ]
    result = BacktestEngine().run(
        events, request.initial_capital, request.model_dump(), request.dataset_version
    )
    run_id = str(uuid4())
    runtime.backtests[run_id] = result
    await save_backtest_result(
        session, run_id, result, datetime.now(UTC), request.model_dump(mode="json")
    )
    return {
        "id": run_id,
        "config_hash": result.config_hash,
        "dataset_version": result.dataset_version,
        "metrics": result.metrics.model_dump(mode="json"),
    }


@router.get("/backtests/{run_id}")
async def get_backtest(
    run_id: str,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    result = runtime.backtests.get(run_id)
    if result is None:
        row = await session.scalar(
            select(BacktestResultRecord).where(BacktestResultRecord.run_id == run_id)
        )
        run = await session.scalar(
            select(BacktestRunRecord).where(BacktestRunRecord.run_id == run_id)
        )
        if row is None or run is None:
            raise HTTPException(status_code=404, detail="backtest not found")
        return {
            "config_hash": run.config_hash,
            "dataset_version": run.dataset_version,
            "metrics": row.metrics,
        }
    return {
        "config_hash": result.config_hash,
        "dataset_version": result.dataset_version,
        "metrics": result.metrics.model_dump(mode="json"),
    }
