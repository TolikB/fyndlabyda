"""Operational CLI for the read-only research and paper service."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import uvicorn

from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import BacktestEvent, PositionEvent
from funding_arbitrage.config import assert_public_data_only, get_settings
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
)
from funding_arbitrage.database.session import create_database, init_database
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.execution.paper import SIMULATOR_VERSION
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.services import analytics
from funding_arbitrage.services.preflight import overall_status, run_preflight
from funding_arbitrage.services.release_manifest import (
    MANIFEST_PATH,
    build_manifest,
    check_manifest,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import load_series_file


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))


def read_monthly_pnl(path: str) -> dict[str, Decimal]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(month): Decimal(str(value)) for month, value in payload.items()}


async def collect_once() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    await init_database(engine)
    adapters = create_public_adapters(settings)
    try:
        snapshot = await MarketDataCollector(adapters.values()).collect_once()
        async with session_factory() as session:
            await save_market_snapshot(session, snapshot, tickers=settings.market_persist_tickers)
    finally:
        for adapter in adapters.values():
            await adapter.close()
        await engine.dispose()
    print(
        f"instruments={len(snapshot.instruments)} tickers={len(snapshot.tickers)} "
        f"funding={len(snapshot.funding)} exchanges={','.join(adapters)}"
    )


async def scan_once() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    await init_database(engine)
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    try:
        snapshot = await MarketDataCollector(adapters.values()).collect_once(include_history=True)
        opportunities = runtime.scan(snapshot)
        async with session_factory() as session:
            await save_market_snapshot(session, snapshot, tickers=False)
            await save_opportunities(session, opportunities)
        _print([item.model_dump(mode="json") for item in opportunities])
    finally:
        for adapter in adapters.values():
            await adapter.close()
        await engine.dispose()


async def backtest_once(monthly_pnl_path: str | None, initial_capital: Decimal) -> None:
    monthly: dict[str, Decimal] = {}
    if monthly_pnl_path:
        monthly = read_monthly_pnl(monthly_pnl_path)
    events: list[BacktestEvent] = [
        PositionEvent(
            timestamp=datetime.strptime(f"{month}-01", "%Y-%m-%d").replace(tzinfo=UTC),
            position_id=month,
            state="CLOSED",
            pnl=value,
        )
        for month, value in sorted(monthly.items())
    ]
    result = BacktestEngine().run(
        events,
        initial_capital,
        {"monthly_pnl": {key: str(value) for key, value in monthly.items()}},
        dataset_version="cli",
    )
    _print(result.metrics.model_dump(mode="json"))


async def paper_status() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    try:
        async with session_factory() as session:
            _print(
                [
                    await analytics.series_summary(session, record)
                    for record in await analytics.list_series(session, include_legacy=True)
                ]
            )
    finally:
        await engine.dispose()


async def preflight() -> tuple[int, dict[str, Any]]:
    settings = get_settings()
    assert_public_data_only(os.environ)
    adapters = create_public_adapters(settings)
    try:
        reports = await run_preflight(adapters)
    finally:
        for adapter in adapters.values():
            await adapter.close()
    status = overall_status(reports)
    payload: dict[str, Any] = {
        "generated_at": datetime.now(UTC),
        "market_data_mode": settings.market_data_mode,
        "status": status,
        "feeds": [report.as_dict() for report in reports],
    }
    for report in reports:
        details = report.details
        print(
            f"{report.status:4} {report.venue:<12} {report.market:<10} "
            f"instruments={details.get('instruments_active', '-')} "
            f"tickers={details.get('tickers', '-')} "
            f"book_age={details.get('book_age_s', '-')}s "
            f"history_age={details.get('history_latest_age_h', '-')}h"
        )
        for problem in report.problems:
            print(f"      {problem}")
    print(f"OVERALL {status} ({len(reports)} feeds)")
    return (0 if status != "FAIL" else 1), payload


async def readiness(hours: int, max_gap_seconds: float, primary: str | None) -> int:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    try:
        async with session_factory() as session:
            result = await analytics.readiness(
                session,
                hours=hours,
                loop_interval_seconds=settings.paper_loop_interval_seconds,
                primary_series=primary,
                max_gap_seconds=max_gap_seconds,
            )
    finally:
        await engine.dispose()
    _print(result)
    return 0 if result["verdict"] == "PASS" else 1


async def reconcile(series: str | None) -> int:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    ok = True
    try:
        async with session_factory() as session:
            records = await analytics.list_series(session)
            results = [
                await analytics.reconciliation(session, record.series_id)
                for record in records
                if series is None or series in (record.series_id, record.name)
            ]
    finally:
        await engine.dispose()
    ok = all(item["ok"] for item in results)
    _print(results)
    return 0 if ok else 1


def release_manifest(action: str, verification_path: str | None) -> int:
    root = Path(".")
    if action == "write":
        verification: dict[str, Any] = {}
        if verification_path:
            verification = json.loads(Path(verification_path).read_text(encoding="utf-8"))
        manifest = build_manifest(root, SIMULATOR_VERSION, verification)
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        print(
            f"written {MANIFEST_PATH}: runtime={manifest['runtime_sha256']} "
            f"deployment={manifest['deployment_sha256']}"
        )
        return 0
    result = check_manifest(root)
    if result.ok:
        print(f"release manifest OK: runtime={result.runtime_sha256}")
        return 0
    print("release manifest MISMATCH:")
    for problem in result.problems:
        print(f"  {problem}")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(prog="funding-arbitrage")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("api", help="run the API (and the paper runner in paper_test mode)")
    commands.add_parser("collect", help="one public REST collection and DB write")
    commands.add_parser("scan", help="collection, ranking, and opportunity history write")
    backtest = commands.add_parser("backtest", help="monthly PnL backtest metrics")
    backtest.add_argument("--monthly-pnl", default=None)
    backtest.add_argument("--initial-capital", default="1000")
    commands.add_parser("paper", help="paper series summaries from the database")
    check = commands.add_parser("preflight", help="verify every public feed")
    check.add_argument("--json", dest="json_path", default=None)
    ready = commands.add_parser("readiness", help="72-hour acceptance check")
    ready.add_argument("--hours", type=int, default=72)
    ready.add_argument("--max-gap-seconds", type=float, default=300.0)
    rec = commands.add_parser("reconcile", help="ledger reconciliation per series")
    rec.add_argument("--series", default=None)
    manifest = commands.add_parser("release-manifest", help="write or check release evidence")
    manifest.add_argument("action", choices=("write", "check"))
    manifest.add_argument("--verification", default=None)
    args = parser.parse_args()

    exit_code = 0
    if args.command == "collect":
        asyncio.run(collect_once())
    elif args.command == "scan":
        asyncio.run(scan_once())
    elif args.command == "backtest":
        asyncio.run(backtest_once(args.monthly_pnl, Decimal(args.initial_capital)))
    elif args.command == "paper":
        asyncio.run(paper_status())
    elif args.command == "preflight":
        exit_code, payload = asyncio.run(preflight())
        if args.json_path:
            Path(args.json_path).write_text(
                json.dumps(payload, indent=2, default=str, ensure_ascii=False), encoding="utf-8"
            )
    elif args.command == "readiness":
        series_file = get_settings().paper_series_file
        primary = (
            load_series_file(series_file).primary.label if Path(series_file).exists() else None
        )
        exit_code = asyncio.run(readiness(args.hours, args.max_gap_seconds, primary))
    elif args.command == "reconcile":
        exit_code = asyncio.run(reconcile(args.series))
    elif args.command == "release-manifest":
        exit_code = release_manifest(args.action, args.verification)
    else:
        uvicorn.run("funding_arbitrage.main:app", host="0.0.0.0", port=8000, reload=False)
    sys.exit(exit_code)
