from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated
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
from funding_arbitrage.database.repositories.market_data import save_backtest_result
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()
market_replay_semaphore = asyncio.Semaphore(1)


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
    job_id = str(uuid4())
    now = datetime.now(UTC).isoformat()
    job: dict[str, object] = {
        "id": job_id,
        "status": "queued",
        "stage": "waiting_for_worker",
        "created_at": now,
        "updated_at": now,
    }
    runtime.market_replay_jobs[job_id] = job
    session_factory: async_sessionmaker[AsyncSession] = (
        http_request.app.state.session_factory
    )
    task = asyncio.create_task(
        _execute_market_comparison_job(
            job_id,
            payload,
            runtime,
            session_factory,
        ),
        name=f"market-replay-{job_id}",
    )
    runtime.background_tasks.add(task)
    task.add_done_callback(runtime.background_tasks.discard)
    return dict(job)


@router.get("/backtests/compare-market/jobs/{job_id}")
async def get_market_backtest_job(
    job_id: str,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> dict[str, object]:
    job = runtime.market_replay_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="market replay job not found")
    return dict(job)


async def _execute_market_comparison_job(
    job_id: str,
    request: MarketReplayRequest,
    runtime: RuntimeState,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    job = runtime.market_replay_jobs[job_id]
    try:
        async with session_factory() as session:
            result = await _run_market_comparison(request, runtime, session, job)
        job.update(
            status="completed",
            stage="completed",
            result=result,
            updated_at=datetime.now(UTC).isoformat(),
        )
    except asyncio.CancelledError:
        job.update(
            status="cancelled",
            stage="cancelled",
            updated_at=datetime.now(UTC).isoformat(),
        )
        raise
    except Exception as exc:
        root_cause = getattr(exc, "orig", exc)
        job.update(
            status="failed",
            stage="failed",
            error=f"{type(exc).__name__}: {root_cause}",
            updated_at=datetime.now(UTC).isoformat(),
        )


async def _run_market_comparison(
    request: MarketReplayRequest,
    runtime: RuntimeState,
    session: AsyncSession,
    job: dict[str, object] | None = None,
) -> dict[str, object]:
    replay = HistoricalMarketReplay()
    async with market_replay_semaphore:
        _update_job_stage(job, "loading_dataset")
        dataset = await replay.load(session, request.start, request.end)
        settings = get_settings()
        _update_job_stage(job, "simulating_baseline")
        baseline = await asyncio.to_thread(
            replay.simulate,
            dataset,
            "baseline",
            request.initial_capital,
            settings,
        )
        _update_job_stage(job, "simulating_candidate")
        candidate = await asyncio.to_thread(
            replay.simulate,
            dataset,
            "candidate",
            request.initial_capital,
            settings,
        )
    _update_job_stage(job, "saving_results")
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
        run_id = str(uuid4())
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


def _update_job_stage(job: dict[str, object] | None, stage: str) -> None:
    if job is None:
        return
    job.update(
        status="running",
        stage=stage,
        updated_at=datetime.now(UTC).isoformat(),
    )


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
