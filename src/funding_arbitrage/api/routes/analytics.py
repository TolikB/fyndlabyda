"""Paper analytics API: summaries, comparison, attribution, reconciliation, readiness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.api.schemas.backtests import IncomeTargetRequest
from funding_arbitrage.backtest.income_target import income_target_analysis
from funding_arbitrage.database.models import PaperSeriesRecord
from funding_arbitrage.services import analytics
from funding_arbitrage.services.daily_report import resolve_timezone
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


def _income_target(runtime: RuntimeState, portfolio: Decimal, target: Decimal) -> dict[str, object]:
    results = list(runtime.backtests.values())
    monthly = [value for result in results for value in result.metrics.monthly_returns.values()]
    drawdown = max((result.metrics.max_drawdown for result in results), default=Decimal("0"))
    return income_target_analysis(monthly, portfolio, target, drawdown).model_dump(mode="json")


async def _series(session: AsyncSession, series_id: str) -> PaperSeriesRecord:
    record = await session.get(PaperSeriesRecord, series_id)
    if record is None:
        raise HTTPException(status_code=404, detail="series not found")
    return record


async def _resolve(session: AsyncSession, key: str) -> PaperSeriesRecord:
    """Accept a series label or a series name (newest series with that name)."""

    record = await session.get(PaperSeriesRecord, key)
    if record is not None:
        return record
    candidates = [item for item in await analytics.list_series(session) if item.name == key]
    if not candidates:
        raise HTTPException(status_code=404, detail=f"series '{key}' not found")
    return candidates[-1]


@router.get("/analytics/performance")
async def performance(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        series_id: {
            "equity": snapshot.equity,
            "cash": snapshot.cash,
            "locked_capital": snapshot.locked_capital,
            "unrealized_pnl": snapshot.unrealized_pnl,
            "total_pnl": snapshot.total_pnl,
            "invariant_diff": snapshot.invariant_diff,
        }
        for series_id, snapshot in (
            (series_id, account.snapshot(now)) for series_id, account in runtime.accounts.items()
        )
    }


@router.get("/analytics/funding")
async def funding(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    snapshots = runtime.latest_snapshot.funding if runtime.latest_snapshot else []
    return {
        "count": len(snapshots),
        "snapshots": [item.model_dump(mode="json") for item in snapshots],
    }


@router.get("/analytics/series")
async def series_list(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    include_legacy: bool = False,
) -> list[dict[str, object]]:
    return [
        await analytics.series_summary(session, record)
        for record in await analytics.list_series(session, include_legacy)
    ]


@router.get("/analytics/series/{series_id}")
async def series_detail(
    series_id: str, session: Annotated[AsyncSession, Depends(get_session_factory)]
) -> dict[str, object]:
    return await analytics.series_summary(session, await _resolve(session, series_id))


@router.get("/analytics/series/{series_id}/equity")
async def series_equity(
    series_id: str,
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    hours: Annotated[int | None, Query(ge=1, le=24 * 400)] = None,
    limit: Annotated[int, Query(ge=1, le=20_000)] = 2000,
) -> list[dict[str, object]]:
    record = await _resolve(session, series_id)
    since = datetime.now(UTC) - timedelta(hours=hours) if hours else None
    return await analytics.equity_curve(session, record.series_id, since, limit)


@router.get("/analytics/series/{series_id}/attribution")
async def series_attribution(
    series_id: str, session: Annotated[AsyncSession, Depends(get_session_factory)]
) -> dict[str, object]:
    record = await _resolve(session, series_id)
    return await analytics.attribution(session, record.series_id)


@router.get("/analytics/series/{series_id}/reconciliation")
async def series_reconciliation(
    series_id: str,
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> dict[str, object]:
    record = await _resolve(session, series_id)
    result = await analytics.reconciliation(session, record.series_id)
    account = runtime.accounts.get(record.series_id)
    if account is not None:
        result["in_memory_invariant_diff"] = str(account.invariant_diff())
        result["pending_unpersisted_entries"] = len(account.pending_ledger)
        result["halted_reason"] = account.halted_reason
    return result


@router.get("/analytics/compare")
async def compare(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    a: str = "candidate",
    b: str = "baseline",
) -> dict[str, object]:
    first = await _resolve(session, a)
    second = await _resolve(session, b)
    timezone = resolve_timezone(runtime.settings.telegram_timezone)
    days_a = await analytics.daily_equity(
        session, first.series_id, timezone, Decimal(str(first.initial_balance or 0))
    )
    days_b = await analytics.daily_equity(
        session, second.series_id, timezone, Decimal(str(second.initial_balance or 0))
    )
    return {
        "a": await analytics.series_summary(session, first),
        "b": await analytics.series_summary(session, second),
        "daily_pnl": [
            {
                "date": day,
                "a": str(days_a.get(day, Decimal("0"))),
                "b": str(days_b.get(day, Decimal("0"))),
                "difference": str(days_a.get(day, Decimal("0")) - days_b.get(day, Decimal("0"))),
            }
            for day in sorted(set(days_a) | set(days_b))
        ],
    }


@router.get("/analytics/readiness")
async def readiness(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    hours: Annotated[int, Query(ge=1, le=24 * 60)] = 72,
    max_gap_seconds: Annotated[float, Query(gt=0)] = 300.0,
) -> dict[str, object]:
    primary = runtime.series_file.primary.label if runtime.series_file else None
    return await analytics.readiness(
        session,
        hours=hours,
        loop_interval_seconds=runtime.settings.paper_loop_interval_seconds,
        primary_series=primary,
        max_gap_seconds=max_gap_seconds,
    )


@router.get("/analytics/paper")
async def paper_statistics(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> dict[str, object]:
    """Primary series summary and recent equity curve (dashboard shortcut)."""

    if runtime.series_file is None:
        raise HTTPException(status_code=404, detail="no paper series configured")
    record = await _series(session, runtime.series_file.primary.label)
    return {
        "summary": await analytics.series_summary(session, record),
        "equity_curve": await analytics.equity_curve(session, record.series_id, None, limit),
    }


@router.post("/analytics/income-target")
async def income_target(
    request: IncomeTargetRequest, runtime: Annotated[RuntimeState, Depends(get_runtime)]
) -> dict[str, object]:
    return _income_target(runtime, request.portfolio, request.monthly_target)


@router.get("/analytics/income-target")
async def income_target_get(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    portfolio: Annotated[Decimal, Query(gt=0)],
    monthly_target: Annotated[Decimal, Query(ge=0)],
) -> dict[str, object]:
    return _income_target(runtime, portfolio, monthly_target)
