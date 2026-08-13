from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.api.schemas.backtests import IncomeTargetRequest
from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.database_replay import DatabasePaperReplay
from funding_arbitrage.backtest.income_target import income_target_analysis
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.opportunity.debounce import canonical_exposure_key
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


def _open_exposure_safety(
    rows: Iterable[PaperPositionRecord],
) -> dict[str, int]:
    """Summarize persisted exposure-key coverage and duplicate open positions."""

    exposure_counts: Counter[str] = Counter()
    missing = 0
    unverifiable = 0
    mismatched = 0
    for row in rows:
        exposure_key = row.payload.get("exposure_key")
        if not isinstance(exposure_key, str) or not exposure_key:
            missing += 1
            continue
        exposure_counts[exposure_key] += 1
        canonical_key = _canonical_key_from_payload(row.payload)
        if canonical_key is None:
            unverifiable += 1
            continue
        if exposure_key != canonical_key:
            mismatched += 1
    return {
        "open_exposure_key_count": len(exposure_counts),
        "open_positions_missing_exposure_key_count": missing,
        "open_positions_unverifiable_exposure_key_count": unverifiable,
        "open_positions_mismatched_exposure_key_count": mismatched,
        "duplicate_open_exposure_count": sum(
            count - 1 for count in exposure_counts.values() if count > 1
        ),
    }


def _canonical_key_from_payload(payload: dict[str, object]) -> str | None:
    asset = payload.get("asset")
    leg_a = payload.get("leg_a")
    leg_b = payload.get("leg_b")
    if (
        not isinstance(asset, str)
        or not isinstance(leg_a, dict)
        or not isinstance(leg_b, dict)
    ):
        return None
    leg_a_type = payload.get("leg_a_type") or leg_a.get("instrument_type")
    leg_b_type = payload.get("leg_b_type") or leg_b.get("instrument_type")
    leg_values = (
        leg_a.get("exchange"),
        leg_a.get("symbol"),
        leg_a_type,
        leg_b.get("exchange"),
        leg_b.get("symbol"),
        leg_b_type,
    )
    if not all(isinstance(value, str) and value for value in leg_values):
        return None
    return canonical_exposure_key(
        asset,
        (str(leg_values[0]), str(leg_values[1]), str(leg_values[2])),
        (str(leg_values[3]), str(leg_values[4]), str(leg_values[5])),
    )


def _historical_exposure_safety(
    rows: Iterable[PaperPositionRecord],
) -> dict[str, int]:
    """Detect any canonical exposure intervals that overlapped in the ledger."""

    intervals: dict[str, list[tuple[datetime, datetime | None]]] = defaultdict(list)
    missing = 0
    unverifiable = 0
    mismatched = 0
    missing_interval = 0
    for row in rows:
        exposure_key = row.payload.get("exposure_key")
        if not isinstance(exposure_key, str) or not exposure_key:
            missing += 1
        canonical_key = _canonical_key_from_payload(row.payload)
        if canonical_key is None:
            unverifiable += 1
            continue
        if isinstance(exposure_key, str) and exposure_key and exposure_key != canonical_key:
            mismatched += 1
        if row.opened_at is None:
            missing_interval += 1
            continue
        intervals[canonical_key].append((row.opened_at, row.closed_at))

    overlaps = 0
    for values in intervals.values():
        active_end: datetime | None = None
        has_interval = False
        for opened_at, closed_at in sorted(values, key=lambda value: value[0]):
            if has_interval and (active_end is None or opened_at < active_end):
                overlaps += 1
            if not has_interval:
                active_end = closed_at
                has_interval = True
            elif active_end is not None:
                active_end = (
                    None
                    if closed_at is None
                    else max(active_end, closed_at)
                )
    return {
        "historical_positions_missing_exposure_key_count": missing,
        "historical_positions_unverifiable_exposure_key_count": unverifiable,
        "historical_positions_mismatched_exposure_key_count": mismatched,
        "historical_positions_missing_interval_count": missing_interval,
        "overlapping_exposure_interval_count": overlaps,
    }


@router.get("/analytics/compare")
async def compare_paper_profiles(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    initial_capital: Annotated[Decimal, Query(gt=0)] = Decimal("6250"),
    baseline_version: Annotated[str | None, Query()] = None,
    candidate_version: Annotated[str | None, Query()] = None,
    start: Annotated[datetime | None, Query()] = None,
    end: Annotated[datetime | None, Query()] = None,
) -> dict[str, object]:
    active_baseline = baseline_version or runtime.settings.paper_baseline_simulation_version
    active_candidate = candidate_version or runtime.settings.paper_simulation_version
    loader = DatabasePaperReplay()
    baseline = await loader.load(session, active_baseline, start, end)
    candidate = await loader.load(session, active_candidate, start, end)
    return compare_paper_datasets(baseline, candidate, initial_capital)


@router.get("/analytics/attribution")
async def paper_attribution(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    simulation_version: Annotated[str | None, Query()] = None,
    start: Annotated[datetime | None, Query()] = None,
    end: Annotated[datetime | None, Query()] = None,
) -> dict[str, object]:
    active_version = simulation_version or runtime.settings.paper_simulation_version
    dataset = await DatabasePaperReplay().load(
        session, active_version, start, end
    )
    return {
        "simulation_version": active_version,
        "dataset_version": dataset.dataset_version,
        "position_count": dataset.position_count,
        "attribution": dataset.attribution,
    }


def _income_target(runtime: RuntimeState, portfolio: Decimal, target: Decimal) -> dict[str, object]:
    results = list(runtime.backtests.values())
    monthly = [value for result in results for value in result.metrics.monthly_returns.values()]
    drawdown = max((result.metrics.max_drawdown for result in results), default=Decimal("0"))
    return income_target_analysis(monthly, portfolio, target, drawdown).model_dump(mode="json")


@router.get("/analytics/performance")
async def performance(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    snapshot = runtime.portfolio.snapshot()
    return {
        "equity": snapshot.equity,
        "cash": snapshot.cash,
        "locked_capital": snapshot.locked_capital,
        "total_pnl": snapshot.total_pnl,
    }


@router.get("/analytics/funding")
async def funding(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    snapshots = runtime.latest_snapshot.funding if runtime.latest_snapshot else []
    return {
        "count": len(snapshots),
        "snapshots": [item.model_dump(mode="json") for item in snapshots],
    }


@router.get("/analytics/paper")
async def paper_statistics(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    simulation_version: Annotated[str | None, Query()] = None,
) -> dict[str, object]:
    active_version = simulation_version or runtime.settings.paper_simulation_version
    snapshots = (
        await session.execute(
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.simulation_version == active_version)
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
            .limit(limit)
        )
    ).scalars()
    fills = await session.scalar(
        select(func.count(PaperFillRecord.id))
        .join(PaperPositionRecord, PaperPositionRecord.position_id == PaperFillRecord.position_id)
        .where(PaperPositionRecord.simulation_version == active_version)
    )
    position_rows = list(
        (
            await session.execute(
                select(PaperPositionRecord).where(
                    PaperPositionRecord.simulation_version == active_version,
                )
            )
        ).scalars()
    )
    open_position_rows = [row for row in position_rows if row.state == "OPEN"]
    exposure_safety = _open_exposure_safety(open_position_rows)
    historical_exposure_safety = _historical_exposure_safety(position_rows)
    funding_pnl = await session.scalar(
        select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0))
        .join(
            PaperPositionRecord,
            PaperPositionRecord.position_id == PaperFundingPaymentRecord.position_id,
        )
        .where(PaperPositionRecord.simulation_version == active_version)
    )
    fees = await session.scalar(
        select(func.coalesce(func.sum(PaperFillRecord.fee), 0))
        .join(PaperPositionRecord, PaperPositionRecord.position_id == PaperFillRecord.position_id)
        .where(PaperPositionRecord.simulation_version == active_version)
    )
    ordered = list(reversed(list(snapshots)))
    return {
        "simulation_version": active_version,
        "snapshot_count": len(ordered),
        "fill_count": int(fills or 0),
        "position_count": len(position_rows),
        "open_position_count": len(open_position_rows),
        **exposure_safety,
        **historical_exposure_safety,
        "funding_pnl": str(funding_pnl or 0),
        "fees": str(fees or 0),
        "equity_curve": [
            {
                "timestamp": row.timestamp,
                "equity": str(row.equity),
                "total_pnl": str(row.total_pnl),
                "funding_pnl": str(row.funding_pnl),
                "fees": str(row.fees),
            }
            for row in ordered
        ],
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
