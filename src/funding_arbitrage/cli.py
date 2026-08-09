"""Minimal operational CLI for the phase-one read-only service."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import uvicorn

from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import BacktestEvent, PositionEvent
from funding_arbitrage.config import get_settings
from funding_arbitrage.database.repositories.market_data import (
    save_funding_snapshots,
    save_instruments,
    save_market_snapshot,
    save_opportunities,
    save_portfolio_snapshot,
    save_tickers,
)
from funding_arbitrage.database.session import create_database, init_database
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.services.runtime import RuntimeState


def read_monthly_pnl(path: str) -> dict[str, Decimal]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(month): Decimal(str(value)) for month, value in payload.items()}


async def collect_once() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    await init_database(engine)
    adapters = create_public_adapters(settings)
    totals = {"instruments": 0, "tickers": 0, "funding": 0}
    try:
        for adapter in adapters.values():
            instruments = await adapter.get_instruments()
            tickers = await adapter.get_tickers()
            funding = await adapter.get_funding_rates()
            async with session_factory() as session:
                await save_instruments(session, instruments)
                await save_tickers(session, tickers)
                await save_funding_snapshots(session, funding)
            totals["instruments"] += len(instruments)
            totals["tickers"] += len(tickers)
            totals["funding"] += len(funding)
    finally:
        for adapter in adapters.values():
            await adapter.close()
    await engine.dispose()
    print(
        f"instruments={totals['instruments']} tickers={totals['tickers']} "
        f"funding={totals['funding']} exchanges={','.join(adapters)}"
    )


async def scan_once() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    await init_database(engine)
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    try:
        snapshot = await MarketDataCollector(adapters.values()).collect_once(include_history=True)
        opportunities = runtime.update_market(snapshot)
        async with session_factory() as session:
            await save_market_snapshot(session, snapshot)
            await save_opportunities(session, opportunities)
            await save_portfolio_snapshot(session, runtime.portfolio.snapshot())
        print(json.dumps([item.model_dump(mode="json") for item in opportunities], indent=2))
    finally:
        for adapter in adapters.values():
            await adapter.close()
        await engine.dispose()


async def backtest_once(monthly_pnl_path: str | None) -> None:
    settings = get_settings()
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
        settings.paper_initial_balance_usd,
        {"monthly_pnl": {key: str(value) for key, value in monthly.items()}},
        dataset_version="cli",
    )
    print(json.dumps(result.metrics.model_dump(mode="json"), indent=2))


def paper_status() -> None:
    settings = get_settings()
    portfolio = RuntimeState(settings, {}).portfolio
    print(json.dumps(portfolio.snapshot().model_dump(mode="json"), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="funding-arbitrage")
    parser.add_argument(
        "command", choices=("api", "collect", "scan", "backtest", "paper"), default="api", nargs="?"
    )
    parser.add_argument(
        "--monthly-pnl",
        default=None,
        help="JSON file mapping YYYY-MM to decimal PnL; used by the safe backtest command",
    )
    args = parser.parse_args()
    if args.command == "collect":
        asyncio.run(collect_once())
    elif args.command == "scan":
        asyncio.run(scan_once())
    elif args.command == "backtest":
        asyncio.run(backtest_once(args.monthly_pnl))
    elif args.command == "paper":
        paper_status()
    else:
        uvicorn.run("funding_arbitrage.main:app", host="0.0.0.0", port=8000, reload=False)
