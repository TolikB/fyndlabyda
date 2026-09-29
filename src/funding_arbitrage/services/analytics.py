"""Read-only paper analytics: series summaries, attribution, reconciliation, readiness."""

from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, datetime, timedelta, tzinfo
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.config import credential_variables
from funding_arbitrage.database.models import (
    LEGACY_SERIES_ID,
    PaperCycleRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperLedgerEntryRecord,
    PaperPositionRecord,
    PaperSeriesRecord,
    PortfolioSnapshotRecord,
    TelegramDailyReportRecord,
)
from funding_arbitrage.portfolio.ledger import LedgerEntryType
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE

_ZERO = Decimal("0")


def _dec(value: object) -> Decimal:
    return Decimal(str(value)) if value is not None else _ZERO


def _json_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.00000001")), "f")


async def list_series(
    session: AsyncSession, include_legacy: bool = False
) -> list[PaperSeriesRecord]:
    statement = select(PaperSeriesRecord).order_by(PaperSeriesRecord.created_at)
    if not include_legacy:
        statement = statement.where(PaperSeriesRecord.series_id != LEGACY_SERIES_ID)
    return list((await session.execute(statement)).scalars())


async def latest_snapshot(session: AsyncSession, series_id: str) -> PortfolioSnapshotRecord | None:
    return await session.scalar(
        select(PortfolioSnapshotRecord)
        .where(PortfolioSnapshotRecord.series_id == series_id)
        .order_by(PortfolioSnapshotRecord.timestamp.desc())
        .limit(1)
    )


async def series_summary(session: AsyncSession, series: PaperSeriesRecord) -> dict[str, Any]:
    initial = _dec(series.initial_balance)
    snapshot = await latest_snapshot(session, series.series_id)
    closed = (
        await session.execute(
            select(
                PaperPositionRecord.booked_pnl,
                PaperPositionRecord.opened_at,
                PaperPositionRecord.closed_at,
            ).where(
                PaperPositionRecord.series_id == series.series_id,
                PaperPositionRecord.state == "CLOSED",
            )
        )
    ).all()
    open_count = await session.scalar(
        select(func.count(PaperPositionRecord.id)).where(
            PaperPositionRecord.series_id == series.series_id,
            PaperPositionRecord.state == "OPEN",
        )
    )
    wins = sum(1 for row in closed if _dec(row.booked_pnl) > 0)
    holds = [
        (row.closed_at - row.opened_at).total_seconds() / 3600
        for row in closed
        if row.closed_at is not None and row.opened_at is not None
    ]
    equity = _dec(snapshot.equity) if snapshot is not None else initial
    return {
        "series_id": series.series_id,
        "name": series.name,
        "simulator_version": series.simulator_version,
        "config_hash": series.config_hash,
        "started_at": series.created_at,
        "initial_balance": _json_decimal(initial),
        "as_of": snapshot.timestamp if snapshot is not None else None,
        "equity": _json_decimal(equity),
        "cash": _json_decimal(_dec(snapshot.cash if snapshot else initial)),
        "locked_capital": _json_decimal(_dec(snapshot.locked_capital if snapshot else 0)),
        "unrealized_pnl": _json_decimal(_dec(snapshot.unrealized_pnl if snapshot else 0)),
        "realized_pnl": _json_decimal(_dec(snapshot.realized_pnl if snapshot else 0)),
        "total_pnl": _json_decimal(equity - initial),
        "return_percent": _json_decimal((equity - initial) / initial * 100 if initial else _ZERO),
        "funding_pnl": _json_decimal(_dec(snapshot.funding_pnl if snapshot else 0)),
        "fees": _json_decimal(_dec(snapshot.fees if snapshot else 0)),
        "slippage": _json_decimal(_dec(snapshot.slippage if snapshot else 0)),
        "exposure": _json_decimal(_dec(snapshot.exposure if snapshot else 0)),
        "open_positions": int(open_count or 0),
        "closed_positions": len(closed),
        "win_rate": round(wins / len(closed), 4) if closed else None,
        "average_hold_hours": round(sum(holds) / len(holds), 2) if holds else None,
        "invariant_diff": _json_decimal(_dec(snapshot.invariant_diff if snapshot else 0)),
    }


async def equity_curve(
    session: AsyncSession, series_id: str, since: datetime | None, limit: int
) -> list[dict[str, Any]]:
    statement = (
        select(PortfolioSnapshotRecord)
        .where(PortfolioSnapshotRecord.series_id == series_id)
        .order_by(PortfolioSnapshotRecord.timestamp.desc())
        .limit(limit)
    )
    if since is not None:
        statement = statement.where(PortfolioSnapshotRecord.timestamp >= since)
    rows = list((await session.execute(statement)).scalars())
    return [
        {
            "timestamp": row.timestamp,
            "equity": _json_decimal(_dec(row.equity)),
            "cash": _json_decimal(_dec(row.cash)),
            "locked_capital": _json_decimal(_dec(row.locked_capital)),
            "unrealized_pnl": _json_decimal(_dec(row.unrealized_pnl)),
            "total_pnl": _json_decimal(_dec(row.total_pnl)),
            "funding_pnl": _json_decimal(_dec(row.funding_pnl)),
            "fees": _json_decimal(_dec(row.fees)),
        }
        for row in reversed(rows)
    ]


async def daily_equity(
    session: AsyncSession, series_id: str, timezone: tzinfo, initial: Decimal
) -> dict[str, Decimal]:
    """Last equity of each local calendar day."""

    rows = (
        await session.execute(
            select(PortfolioSnapshotRecord.timestamp, PortfolioSnapshotRecord.equity)
            .where(PortfolioSnapshotRecord.series_id == series_id)
            .order_by(PortfolioSnapshotRecord.timestamp)
        )
    ).all()
    closing: dict[str, Decimal] = {}
    for timestamp, equity in rows:
        closing[timestamp.astimezone(timezone).date().isoformat()] = _dec(equity)
    result: dict[str, Decimal] = {}
    previous = initial
    for day in sorted(closing):
        result[day] = closing[day] - previous
        previous = closing[day]
    return result


_ATTRIBUTION_FIELDS = ("booked_pnl", "funding_pnl", "fees", "slippage", "exposure")


async def attribution(session: AsyncSession, series_id: str) -> dict[str, Any]:
    rows = (
        (
            await session.execute(
                select(PaperPositionRecord).where(PaperPositionRecord.series_id == series_id)
            )
        )
        .scalars()
        .all()
    )
    groups: dict[str, dict[str, dict[str, Decimal]]] = {"strategy": {}, "venues": {}, "asset": {}}
    for row in rows:
        legs = row.payload.get("legs", []) if isinstance(row.payload, dict) else []
        venues = "↔".join(dict.fromkeys(str(leg.get("exchange", "")) for leg in legs))
        for dimension, key in (
            ("strategy", row.strategy or "unknown"),
            ("venues", venues or "unknown"),
            ("asset", row.asset),
        ):
            bucket = groups[dimension].setdefault(
                key, {name: _ZERO for name in ("positions", "open", *_ATTRIBUTION_FIELDS)}
            )
            bucket["positions"] += 1
            bucket["open"] += 1 if row.state == "OPEN" else 0
            for name in _ATTRIBUTION_FIELDS:
                bucket[name] += _dec(getattr(row, name))
    sources = (
        await session.execute(
            select(
                PaperFundingPaymentRecord.rate_source,
                PaperFundingPaymentRecord.price_source,
                func.count(PaperFundingPaymentRecord.id),
                func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0),
            )
            .where(PaperFundingPaymentRecord.series_id == series_id)
            .group_by(PaperFundingPaymentRecord.rate_source, PaperFundingPaymentRecord.price_source)
        )
    ).all()
    result: dict[str, Any] = {
        dimension: {
            key: {
                "positions": int(bucket["positions"]),
                "open": int(bucket["open"]),
                **{name: _json_decimal(bucket[name]) for name in _ATTRIBUTION_FIELDS},
            }
            for key, bucket in sorted(values.items())
        }
        for dimension, values in groups.items()
    }
    result["funding_sources"] = [
        {
            "rate_source": rate_source,
            "price_source": price_source,
            "events": int(count),
            "pnl": _json_decimal(_dec(total)),
        }
        for rate_source, price_source, count, total in sources
    ]
    return result


async def reconciliation(session: AsyncSession, series_id: str) -> dict[str, Any]:
    """Cross-check the ledger against fills, funding payments, positions, and snapshots."""

    series = await session.get(PaperSeriesRecord, series_id)
    if series is None:
        return {"series_id": series_id, "ok": False, "checks": {"series_exists": False}}
    initial = _dec(series.initial_balance)
    totals: dict[str, Decimal] = {kind.value: _ZERO for kind in LedgerEntryType}
    for entry_type, amount in (
        await session.execute(
            select(PaperLedgerEntryRecord.entry_type, func.sum(PaperLedgerEntryRecord.amount))
            .where(PaperLedgerEntryRecord.series_id == series_id)
            .group_by(PaperLedgerEntryRecord.entry_type)
        )
    ).all():
        totals[entry_type] = _dec(amount)
    fills_fee = _dec(
        await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.fee), 0)).where(
                PaperFillRecord.series_id == series_id
            )
        )
    )
    funding_paid = _dec(
        await session.scalar(
            select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0)).where(
                PaperFundingPaymentRecord.series_id == series_id
            )
        )
    )
    open_capital = _dec(
        await session.scalar(
            select(func.coalesce(func.sum(PaperPositionRecord.capital), 0)).where(
                PaperPositionRecord.series_id == series_id, PaperPositionRecord.state == "OPEN"
            )
        )
    )
    booked = _dec(
        await session.scalar(
            select(func.coalesce(func.sum(PaperPositionRecord.booked_pnl), 0)).where(
                PaperPositionRecord.series_id == series_id
            )
        )
    )
    locked_ledger = -(
        totals[LedgerEntryType.COLLATERAL_LOCK.value]
        + totals[LedgerEntryType.COLLATERAL_RELEASE.value]
    )
    realized_ledger = (
        totals[LedgerEntryType.FEE.value]
        + totals[LedgerEntryType.FUNDING.value]
        + totals[LedgerEntryType.REALIZED_PNL.value]
    )
    cash_ledger = initial + sum(totals.values(), _ZERO)
    snapshot = await latest_snapshot(session, series_id)
    differences = {
        "fees_vs_fills": abs(-totals[LedgerEntryType.FEE.value] - fills_fee),
        "funding_vs_payments": abs(totals[LedgerEntryType.FUNDING.value] - funding_paid),
        "locked_vs_open_positions": abs(locked_ledger - open_capital),
        "realized_vs_positions": abs(realized_ledger - booked),
    }
    if snapshot is not None:
        differences["snapshot_cash_vs_ledger"] = abs(_dec(snapshot.cash) - cash_ledger)
        differences["snapshot_equity_identity"] = abs(
            _dec(snapshot.equity)
            - (_dec(snapshot.cash) + _dec(snapshot.locked_capital) + _dec(snapshot.unrealized_pnl))
        )
    return {
        "series_id": series_id,
        "tolerance": str(INVARIANT_TOLERANCE),
        "ok": all(value <= INVARIANT_TOLERANCE for value in differences.values()),
        "ledger": {key: _json_decimal(value) for key, value in totals.items()},
        "cash_from_ledger": _json_decimal(cash_ledger),
        "locked_from_ledger": _json_decimal(locked_ledger),
        "realized_from_ledger": _json_decimal(realized_ledger),
        "differences": {key: _json_decimal(value) for key, value in differences.items()},
        "snapshot_at": snapshot.timestamp if snapshot is not None else None,
    }


async def readiness(
    session: AsyncSession,
    *,
    hours: int,
    loop_interval_seconds: float,
    primary_series: str | None,
    max_gap_seconds: float = 300.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Acceptance check for the paper launch (default window: 72 hours)."""

    current = now or datetime.now(UTC)
    window_start = current - timedelta(hours=hours)
    cycles = (
        await session.execute(
            select(
                PaperCycleRecord.started_at,
                PaperCycleRecord.status,
                PaperCycleRecord.autotrade,
                PaperCycleRecord.venues_ok,
                PaperCycleRecord.venues_failed,
                PaperCycleRecord.incidents,
            )
            .where(PaperCycleRecord.started_at >= window_start)
            .order_by(PaperCycleRecord.started_at)
        )
    ).all()
    reasons: list[str] = []
    statuses = Counter(row.status for row in cycles)
    first = cycles[0].started_at if cycles else None
    moments = [window_start, *(row.started_at for row in cycles), current]
    gaps = [
        (later - earlier).total_seconds()
        for earlier, later in zip(moments, moments[1:], strict=False)
    ]
    inner_gaps = [
        (later.started_at - earlier.started_at).total_seconds()
        for earlier, later in zip(cycles, cycles[1:], strict=False)
    ]
    max_gap = max(inner_gaps, default=0.0)
    expected = hours * 3600 / loop_interval_seconds if loop_interval_seconds > 0 else 0
    venue_ok: Counter[str] = Counter()
    venue_seen: Counter[str] = Counter()
    incidents: Counter[str] = Counter()
    for row in cycles:
        for venue in row.venues_ok or []:
            venue_ok[venue] += 1
            venue_seen[venue] += 1
        for venue in row.venues_failed or []:
            venue_seen[venue] += 1
        for incident in row.incidents or []:
            incidents[str(incident).split(":", 1)[0]] += 1
    if not cycles:
        reasons.append("no_cycles_in_window")
    elif first is not None and (first - window_start).total_seconds() > max_gap_seconds:
        reasons.append("window_not_fully_covered")
    if max_gap > max_gap_seconds or (cycles and gaps[-1] > max_gap_seconds):
        reasons.append("snapshot_gap_exceeded")
    if statuses.get("error"):
        reasons.append("uncontrolled_cycle_errors")
    observe_cycles = sum(1 for row in cycles if not row.autotrade)
    if observe_cycles:
        # Acceptance is about paper trading; observation-only cycles do not count.
        reasons.append("observe_mode_in_window")
    if incidents.get("invariant_violation"):
        reasons.append("invariant_violation_incident")
    if incidents.get("funding_event_unresolved"):
        reasons.append("funding_event_unresolved")
    series_rows = await list_series(session)
    series_checks: dict[str, Any] = {}
    for series in series_rows:
        worst = _dec(
            await session.scalar(
                select(func.max(PortfolioSnapshotRecord.invariant_diff)).where(
                    PortfolioSnapshotRecord.series_id == series.series_id,
                    PortfolioSnapshotRecord.timestamp >= window_start,
                )
            )
        )
        check = await reconciliation(session, series.series_id)
        series_checks[series.series_id] = {
            "max_invariant_diff": _json_decimal(worst),
            "reconciliation_ok": check["ok"],
        }
        if worst > INVARIANT_TOLERANCE:
            reasons.append(f"invariant_diff:{series.series_id}")
        if not check["ok"]:
            reasons.append(f"reconciliation:{series.series_id}")
    report_sent = None
    if primary_series is not None:
        report_sent = await session.scalar(
            select(func.count(TelegramDailyReportRecord.id)).where(
                TelegramDailyReportRecord.series_id == primary_series,
                TelegramDailyReportRecord.status == "sent",
                TelegramDailyReportRecord.sent_at >= window_start,
            )
        )
        if not report_sent:
            reasons.append("no_daily_report_sent")
    credentials = credential_variables(os.environ)
    if credentials:
        reasons.append("exchange_credentials_present")
    return {
        "verdict": "PASS" if not reasons else "FAIL",
        "reasons": reasons,
        "window": {"start": window_start, "end": current, "hours": hours},
        "cycles": {
            "total": len(cycles),
            "expected": int(expected),
            "coverage": round(len(cycles) / expected, 4) if expected else None,
            "by_status": dict(statuses),
            "observe_only": observe_cycles,
            "first": first,
            "max_gap_seconds": round(max_gap, 1),
            "gap_threshold_seconds": max_gap_seconds,
        },
        "venue_availability": {
            venue: round(venue_ok[venue] / count, 4) for venue, count in sorted(venue_seen.items())
        },
        "incidents": dict(incidents),
        "series": series_checks,
        "daily_reports_sent": int(report_sent or 0) if primary_series else None,
        # No private endpoint or signing code exists; credentials would stop startup.
        "live_orders": 0,
        "exchange_credentials_present": credentials,
    }
