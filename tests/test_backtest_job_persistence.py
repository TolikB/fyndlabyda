from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.api.routes import backtests as backtest_routes
from funding_arbitrage.api.schemas.backtests import MarketReplayRequest
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.database.models import (
    BacktestResultRecord,
    BacktestRunRecord,
    Base,
)
from funding_arbitrage.database.repositories.backtest_jobs import (
    DurableMarketReplayJobStore,
)
from funding_arbitrage.database.repositories.market_data import save_backtest_result


def _request() -> MarketReplayRequest:
    end = datetime(2026, 8, 20, tzinfo=UTC)
    return MarketReplayRequest(
        initial_capital=Decimal("15000"),
        start=end - timedelta(days=30),
        end=end,
    )


async def _store(tmp_path: Path) -> tuple[object, DurableMarketReplayJobStore]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'market-jobs.db').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return engine, DurableMarketReplayJobStore(factory, lease_seconds=15)


async def test_job_persists_and_only_one_worker_can_claim(tmp_path: Path) -> None:
    engine, store = await _store(tmp_path)
    payload = _request().model_dump(mode="json")
    created = await store.create("job-one", payload)
    assert created.status == "queued"

    recreated = DurableMarketReplayJobStore(store.session_factory, lease_seconds=15)
    persisted = await recreated.get("job-one")
    assert persisted is not None
    assert persisted.request_payload == payload

    claims = await asyncio.gather(
        store.claim("job-one", "worker-a"),
        recreated.claim("job-one", "worker-b"),
    )
    assert sorted(claims) == [False, True]
    claimed = await store.get("job-one")
    assert claimed is not None
    assert claimed.status == "running"
    assert claimed.attempts == 1
    await engine.dispose()


async def test_get_job_uses_database_after_runtime_restart(tmp_path: Path) -> None:
    engine, store = await _store(tmp_path)
    payload = _request().model_dump(mode="json")
    await store.create("job-restart", payload)
    assert await store.claim("job-restart", "worker-a")
    result = {"dataset_version": "dataset-v1", "comparison": {"passed": True}}
    await store.complete("job-restart", "worker-a", result)

    restarted_runtime = SimpleNamespace(market_replay_jobs={})
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                market_replay_job_store=DurableMarketReplayJobStore(
                    store.session_factory, lease_seconds=15
                ),
                market_replay_worker_id="worker-b",
            )
        )
    )
    response = await backtest_routes.get_market_backtest_job(
        "job-restart", request, restarted_runtime
    )
    assert response["status"] == "completed"
    assert response["result"] == result
    assert restarted_runtime.market_replay_jobs["job-restart"]["status"] == "completed"
    await engine.dispose()


async def test_recovery_reschedules_persisted_queued_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, store = await _store(tmp_path)
    payload = _request().model_dump(mode="json")
    await store.create("job-queued", payload)
    scheduled: list[tuple[str, str]] = []

    def fake_schedule(
        job_id: str,
        request: MarketReplayRequest,
        runtime: object,
        current_store: DurableMarketReplayJobStore,
        worker_id: str,
    ) -> bool:
        del request, runtime
        assert current_store is store
        scheduled.append((job_id, worker_id))
        return True

    monkeypatch.setattr(backtest_routes, "_schedule_market_comparison_job", fake_schedule)
    runtime = SimpleNamespace(market_replay_jobs={}, background_tasks=set())
    count = await backtest_routes.resume_market_backtest_jobs(
        runtime, store, "worker-restarted"
    )
    assert count == 1
    assert scheduled == [("job-queued", "worker-restarted")]
    assert runtime.market_replay_jobs["job-queued"]["status"] == "queued"
    await engine.dispose()


async def test_cancelled_job_is_requeued_for_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, store = await _store(tmp_path)
    payload = _request()
    await store.create("job-cancel", payload.model_dump(mode="json"))
    started = asyncio.Event()

    async def blocking_run(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        started.set()
        await asyncio.Event().wait()
        return {}

    monkeypatch.setattr(backtest_routes, "_run_market_comparison", blocking_run)
    runtime = SimpleNamespace(
        market_replay_jobs={},
        background_tasks=set(),
        backtests={},
    )
    task = asyncio.create_task(
        backtest_routes._execute_market_comparison_job(
            "job-cancel",
            payload,
            runtime,
            store,
            "worker-a",
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    persisted = await store.get("job-cancel")
    assert persisted is not None
    assert persisted.status == "queued"
    assert persisted.stage == "interrupted_shutdown"
    assert [item.id for item in await store.recoverable()] == ["job-cancel"]
    await engine.dispose()

async def test_backtest_result_persistence_is_idempotent_and_conflict_safe(
    tmp_path: Path,
) -> None:
    engine, store = await _store(tmp_path)
    result = BacktestEngine().run(
        [],
        Decimal("15000"),
        {"profile": "baseline"},
        "dataset-v1",
    )
    run_id = "deterministic-job:baseline"
    async with store.session_factory() as session:
        await save_backtest_result(session, run_id, result, datetime.now(UTC))
        await save_backtest_result(session, run_id, result, datetime.now(UTC))
        run_count = await session.scalar(
            select(func.count()).select_from(BacktestRunRecord)
        )
        result_count = await session.scalar(
            select(func.count()).select_from(BacktestResultRecord)
        )
    assert run_count == 1
    assert result_count == 1

    conflicting = BacktestEngine().run(
        [],
        Decimal("15000"),
        {"profile": "candidate"},
        "dataset-v1",
    )
    async with store.session_factory() as session:
        with pytest.raises(ValueError, match="reproducibility data"):
            await save_backtest_result(
                session,
                run_id,
                conflicting,
                datetime.now(UTC),
            )
    await engine.dispose()
