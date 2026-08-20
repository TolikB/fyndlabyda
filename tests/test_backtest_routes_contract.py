from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.api.routes import backtests as routes
from funding_arbitrage.api.schemas.backtests import (
    BacktestRequest,
    MarketReplayRequest,
    PaperReplayRequest,
)
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.database.models import BacktestResultRecord, BacktestRunRecord
from funding_arbitrage.database.repositories.backtest_jobs import (
    BacktestJobLeaseLost,
    MarketReplayJobSnapshot,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _request() -> MarketReplayRequest:
    return MarketReplayRequest(
        initial_capital=Decimal("10000"),
        start=NOW - timedelta(days=30),
        end=NOW,
    )


def _snapshot(
    *,
    job_id: str = "job-1",
    status: str = "queued",
    stage: str = "waiting_for_worker",
    request_payload: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> MarketReplayJobSnapshot:
    return MarketReplayJobSnapshot(
        id=job_id,
        status=status,
        stage=stage,
        request_payload=request_payload or _request().model_dump(mode="json"),
        result=result,
        attempts=0,
        created_at=NOW,
        updated_at=NOW,
    )


class FakeStore:
    lease_seconds = 15

    def __init__(
        self,
        *,
        claim: bool = True,
        snapshots: list[MarketReplayJobSnapshot] | None = None,
    ) -> None:
        self.claim_result = claim
        self.snapshots = snapshots or [_snapshot()]
        self.current = self.snapshots[0] if self.snapshots else None
        self.calls: list[tuple[str, object]] = []
        self.session_factory = SimpleNamespace()

    async def create(
        self,
        job_id: str,
        request_payload: dict[str, Any],
    ) -> MarketReplayJobSnapshot:
        self.current = _snapshot(job_id=job_id, request_payload=request_payload)
        self.calls.append(("create", job_id))
        return self.current

    async def get(self, job_id: str) -> MarketReplayJobSnapshot | None:
        self.calls.append(("get", job_id))
        if self.current is not None and self.current.id == job_id:
            return self.current
        return None

    async def claim(self, job_id: str, worker_id: str) -> bool:
        self.calls.append(("claim", worker_id))
        if self.claim_result and self.current is not None:
            self.current = self.current.model_copy(
                update={"status": "running", "stage": "loading_dataset"}
            )
        return self.claim_result

    async def progress(self, job_id: str, worker_id: str, stage: str) -> None:
        self.calls.append(("progress", stage))
        if self.current is not None:
            self.current = self.current.model_copy(update={"stage": stage})

    async def heartbeat(self, job_id: str, worker_id: str) -> None:
        self.calls.append(("heartbeat", worker_id))

    async def complete(
        self,
        job_id: str,
        worker_id: str,
        result: dict[str, Any],
    ) -> None:
        self.calls.append(("complete", result))
        if self.current is not None:
            self.current = self.current.model_copy(
                update={"status": "completed", "stage": "completed", "result": result}
            )

    async def fail(self, job_id: str, worker_id: str, error_code: str) -> None:
        self.calls.append(("fail", error_code))
        if self.current is not None:
            self.current = self.current.model_copy(
                update={"status": "failed", "stage": "failed", "error_code": error_code}
            )

    async def requeue_interrupted(self, job_id: str, worker_id: str) -> bool:
        self.calls.append(("requeue", worker_id))
        return True

    async def recoverable(self) -> list[MarketReplayJobSnapshot]:
        return self.snapshots


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(
        market_replay_jobs={},
        background_tasks=set(),
        backtests={},
    )


def _http_request(store: FakeStore, worker: str = "worker-1") -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                market_replay_job_store=store,
                market_replay_worker_id=worker,
            )
        )
    )


async def test_compare_create_get_and_missing_job_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    store = FakeStore()
    request = _request()

    async def fake_comparison(*args: object, **kwargs: object) -> dict[str, object]:
        return {"comparison": "ok"}

    scheduled: list[str] = []

    def fake_schedule(job_id: str, *args: object, **kwargs: object) -> bool:
        scheduled.append(job_id)
        return True

    monkeypatch.setattr(routes, "_run_market_comparison", fake_comparison)
    assert await routes.compare_market_backtest(
        request,
        runtime,
        SimpleNamespace(),
    ) == {"comparison": "ok"}

    monkeypatch.setattr(routes, "_schedule_market_comparison_job", fake_schedule)
    created = await routes.create_market_backtest_job(
        request,
        _http_request(store),
        runtime,
    )
    job_id = str(created["id"])
    assert scheduled == [job_id]
    assert runtime.market_replay_jobs[job_id]["status"] == "queued"

    fetched = await routes.get_market_backtest_job(
        job_id,
        _http_request(store),
        runtime,
    )
    assert fetched["id"] == job_id

    with pytest.raises(HTTPException) as exc_info:
        await routes.get_market_backtest_job(
            "missing",
            _http_request(store),
            runtime,
        )
    assert exc_info.value.status_code == 404


async def test_scheduler_prevents_duplicate_active_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    release = asyncio.Event()

    async def blocking_execute(*args: object, **kwargs: object) -> None:
        await release.wait()

    monkeypatch.setattr(routes, "_execute_market_comparison_job", blocking_execute)
    store = FakeStore()

    assert routes._schedule_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        store,
        "worker-1",
    )
    assert not routes._schedule_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        store,
        "worker-1",
    )
    release.set()
    await asyncio.gather(*tuple(runtime.background_tasks))
    await asyncio.sleep(0)
    assert runtime.background_tasks == set()


async def test_job_execution_claim_progress_completion_and_claim_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    unclaimed = FakeStore(claim=False)
    await routes._execute_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        unclaimed,
        "worker-1",
    )
    assert ("complete", {"ok": True}) not in unclaimed.calls

    store = FakeStore()

    async def fake_run(
        *args: object,
        progress: Any = None,
        **kwargs: object,
    ) -> dict[str, object]:
        assert progress is not None
        await progress("simulating_candidate")
        return {"ok": True}

    async def parked_heartbeat(*args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(routes, "_run_market_comparison", fake_run)
    monkeypatch.setattr(routes, "_heartbeat_market_job", parked_heartbeat)
    await routes._execute_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        store,
        "worker-1",
    )
    assert ("progress", "simulating_candidate") in store.calls
    assert ("complete", {"ok": True}) in store.calls
    assert runtime.market_replay_jobs["job-1"]["status"] == "completed"


@pytest.mark.parametrize(
    ("error", "expected_failure"),
    [
        (RuntimeError("synthetic"), "RuntimeError"),
        (BacktestJobLeaseLost("lost"), None),
    ],
)
async def test_job_execution_classifies_failure_and_lease_loss(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_failure: str | None,
) -> None:
    runtime = _runtime()
    store = FakeStore()

    async def failing_run(*args: object, **kwargs: object) -> dict[str, object]:
        raise error

    async def parked_heartbeat(*args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(routes, "_run_market_comparison", failing_run)
    monkeypatch.setattr(routes, "_heartbeat_market_job", parked_heartbeat)
    await routes._execute_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        store,
        "worker-1",
    )

    failures = [value for name, value in store.calls if name == "fail"]
    assert failures == ([] if expected_failure is None else [expected_failure])


async def test_job_failure_after_lease_loss_is_not_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()

    class LosingStore(FakeStore):
        async def fail(self, job_id: str, worker_id: str, error_code: str) -> None:
            raise BacktestJobLeaseLost("lost during fail")

    store = LosingStore()

    async def failing_run(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("synthetic")

    async def parked_heartbeat(*args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(routes, "_run_market_comparison", failing_run)
    monkeypatch.setattr(routes, "_heartbeat_market_job", parked_heartbeat)
    await routes._execute_market_comparison_job(
        "job-1",
        _request(),
        runtime,
        store,
        "worker-1",
    )


async def test_lease_heartbeat_wrapper_handles_success_stop_and_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeStore()

    async def parked(*args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(routes, "_heartbeat_market_job", parked)
    assert await routes._run_with_lease_heartbeat(
        "job-1",
        "worker-1",
        store,
        _return_value({"ok": True}),
    ) == {"ok": True}

    async def stopped(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(routes, "_heartbeat_market_job", stopped)
    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        await routes._run_with_lease_heartbeat(
            "job-1",
            "worker-1",
            store,
            _never(),
        )

    async def lost(*args: object, **kwargs: object) -> None:
        raise BacktestJobLeaseLost("heartbeat lost")

    monkeypatch.setattr(routes, "_heartbeat_market_job", lost)
    with pytest.raises(BacktestJobLeaseLost, match="heartbeat lost"):
        await routes._run_with_lease_heartbeat(
            "job-1",
            "worker-1",
            store,
            _never(),
        )


async def _return_value(value: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(0)
    return value


async def _never() -> dict[str, object]:
    await asyncio.Event().wait()
    return {}


async def test_resume_rejects_invalid_persisted_payload_and_schedules_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid = _snapshot(job_id="invalid", request_payload={"initial_capital": "100"})
    valid = _snapshot(job_id="valid")
    store = FakeStore(snapshots=[invalid, valid])
    runtime = _runtime()
    scheduled: list[str] = []

    def schedule(job_id: str, *args: object, **kwargs: object) -> bool:
        scheduled.append(job_id)
        return True

    monkeypatch.setattr(routes, "_schedule_market_comparison_job", schedule)
    count = await routes.resume_market_backtest_jobs(runtime, store, "worker-1")

    assert count == 1
    assert scheduled == ["valid"]
    assert ("fail", "InvalidPersistedRequest") in store.calls


async def test_recovery_loop_propagates_cancellation_after_failed_sweep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def failing_resume(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("synthetic recovery failure")

    async def cancel_sleep(delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(routes, "resume_market_backtest_jobs", failing_resume)
    monkeypatch.setattr(routes.asyncio, "sleep", cancel_sleep)

    with pytest.raises(asyncio.CancelledError):
        await routes.market_backtest_recovery_loop(
            _runtime(),
            FakeStore(),
            "worker-1",
        )
    assert calls == 1


class FakeHistoricalReplay:
    async def load(
        self,
        session: AsyncSession,
        start: datetime,
        end: datetime,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            dataset_version="market-dataset",
            coverage={"days": 30},
        )

    def simulate(
        self,
        dataset: object,
        profile: str,
        initial_capital: Decimal,
        settings: object,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            events=[],
            dataset_version=f"market-dataset:{profile}",
            position_count=1 if profile == "baseline" else 2,
            attribution={"profile": profile},
        )


async def test_market_comparison_runs_both_profiles_and_persists_results(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, factory = database
    runtime = _runtime()
    saved: list[str] = []
    stages: list[str] = []

    async def save(
        session: AsyncSession,
        run_id: str,
        result: object,
        started_at: datetime,
        config: object,
    ) -> None:
        saved.append(run_id)

    async def progress(stage: str) -> None:
        stages.append(stage)

    monkeypatch.setattr(routes, "HistoricalMarketReplay", FakeHistoricalReplay)
    monkeypatch.setattr(routes, "get_settings", lambda: object())
    monkeypatch.setattr(routes, "save_backtest_result", save)
    monkeypatch.setattr(
        routes,
        "compare_paper_datasets",
        lambda baseline, candidate, capital: {"passed": True},
    )

    result = await routes._run_market_comparison(
        _request(),
        runtime,
        factory,
        job={},
        job_id="job-1",
        progress=progress,
    )

    assert result["dataset_version"] == "market-dataset"
    assert result["positions"] == {"baseline": 1, "candidate": 2}
    assert result["comparison"] == {"passed": True}
    assert saved == ["job-1:baseline", "job-1:candidate"]
    assert stages == [
        "loading_dataset",
        "simulating_baseline",
        "simulating_candidate",
        "saving_results",
    ]
    assert len(runtime.backtests) == 2

    async with factory() as session:
        direct = await routes._run_market_comparison(
            _request(),
            runtime,
            session,
            job_id="job-2",
        )
    assert direct["run_ids"] == {
        "baseline": "job-2:baseline",
        "candidate": "job-2:candidate",
    }


class FakePaperReplay:
    async def load(
        self,
        session: AsyncSession,
        simulation_version: str,
        start: datetime | None,
        end: datetime | None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            events=[],
            dataset_version="paper-dataset",
            position_count=3,
            attribution={"funding": "1"},
        )


async def test_replay_create_and_get_backtest_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    saved: list[str] = []

    async def save(
        session: object,
        run_id: str,
        result: object,
        started_at: datetime,
        config: object,
    ) -> None:
        saved.append(run_id)

    monkeypatch.setattr(routes, "DatabasePaperReplay", FakePaperReplay)
    monkeypatch.setattr(routes, "save_backtest_result", save)
    session = SimpleNamespace()

    replay = await routes.replay_paper_backtest(
        PaperReplayRequest(initial_capital=Decimal("1000")),
        runtime,
        session,
    )
    assert replay["dataset_version"] == "paper-dataset"
    assert replay["position_count"] == 3

    created = await routes.create_backtest(
        BacktestRequest(
            initial_capital=Decimal("1000"),
            monthly_pnl={"2026-01": Decimal("5")},
        ),
        runtime,
        session,
    )
    run_id = str(created["id"])
    assert (await routes.get_backtest(run_id, runtime, session))["metrics"][
        "net_profit_after_costs"
    ] == "5"
    assert len(saved) == 2


class SequenceSession:
    def __init__(self, values: list[object | None]) -> None:
        self.values = values

    async def scalar(self, statement: object) -> object | None:
        return self.values.pop(0)


async def test_get_backtest_uses_database_fallback_and_404() -> None:
    runtime = _runtime()
    result = BacktestEngine().run([], Decimal("1000"), {}, "dataset")
    run = BacktestRunRecord(
        run_id="persisted",
        config_hash=result.config_hash,
        dataset_version=result.dataset_version,
        git_commit=result.git_commit,
        started_at=NOW,
        finished_at=NOW,
        status="completed",
    )
    row = BacktestResultRecord(
        run_id="persisted",
        metrics={"net_profit_after_costs": "0"},
        monthly_distribution={},
        created_at=NOW,
    )
    persisted = await routes.get_backtest(
        "persisted",
        runtime,
        SequenceSession([row, run]),
    )
    assert persisted["dataset_version"] == "dataset"

    with pytest.raises(HTTPException) as exc_info:
        await routes.get_backtest(
            "missing",
            runtime,
            SequenceSession([None, None]),
        )
    assert exc_info.value.status_code == 404

