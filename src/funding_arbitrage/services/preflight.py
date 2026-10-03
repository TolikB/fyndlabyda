"""Preflight of every public feed before a series may trade.

A feed is one venue/market pair (bybit spot, bybit perpetual, ...). For each feed
the check proves the data the simulator depends on is present and fresh:
instruments, tickers, funding with a next settlement time, a fresh non-crossed
order book with plausible base-unit depth, and recent funding history.
"""

from __future__ import annotations

import asyncio
import statistics
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    Ticker,
)

Status = Literal["PASS", "WARN", "FAIL"]
_RANK = {"PASS": 0, "WARN": 1, "FAIL": 2}
EXPECTED_MARKETS: dict[str, frozenset[InstrumentType]] = {
    venue: frozenset({InstrumentType.SPOT, InstrumentType.PERPETUAL})
    for venue in ("bybit", "gate", "okx", "binance")
}
EXPECTED_MARKETS["hyperliquid"] = frozenset({InstrumentType.PERPETUAL})


@dataclass
class FeedReport:
    venue: str
    market: str
    status: Status = "PASS"
    details: dict[str, Any] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    def flag(self, level: Status, problem: str) -> None:
        self.problems.append(f"{level}: {problem}")
        if _RANK[level] > _RANK[self.status]:
            self.status = level

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "market": self.market,
            "status": self.status,
            "details": self.details,
            "problems": self.problems,
        }


@dataclass
class PreflightLimits:
    max_ticker_age_seconds: float = 120.0
    max_book_age_seconds: float = 10.0
    max_mid_deviation: Decimal = Decimal("0.01")
    min_top_depth_usd: Decimal = Decimal("1000")
    request_timeout_seconds: float = 45.0


async def _timed(coroutine: Any, seconds: float) -> Any:
    async with asyncio.timeout(seconds):
        return await coroutine


async def check_venue(
    adapter: ExchangeAdapter,
    limits: PreflightLimits,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[FeedReport]:
    venue = adapter.name
    try:
        instruments: list[NormalizedInstrument] = await _timed(
            adapter.get_instruments(), limits.request_timeout_seconds
        )
        tickers: list[Ticker] = await _timed(adapter.get_tickers(), limits.request_timeout_seconds)
        funding: list[FundingSnapshot] = await _timed(
            adapter.get_funding_rates(), limits.request_timeout_seconds
        )
    except Exception as exc:
        report = FeedReport(venue=venue, market="all")
        report.flag("FAIL", f"venue unreachable: {type(exc).__name__}: {exc}"[:300])
        return [report]
    now = clock()
    reports: list[FeedReport] = []
    markets = sorted(
        {item.instrument_type for item in instruments if item.is_active},
        key=lambda value: value.value,
    )
    for missing in sorted(
        EXPECTED_MARKETS.get(venue, frozenset()) - set(markets), key=lambda value: value.value
    ):
        report = FeedReport(venue=venue, market=missing.value.lower())
        report.flag("FAIL", "expected market has no active instruments")
        reports.append(report)
    funding_index = {item.symbol: item for item in funding}
    for market in markets:
        if market is InstrumentType.FUTURE:
            continue
        report = FeedReport(venue=venue, market=market.value.lower())
        active = {
            item.exchange_symbol: item
            for item in instruments
            if item.instrument_type is market and item.is_active
        }
        report.details["instruments_active"] = len(active)
        market_tickers = [item for item in tickers if item.instrument_type is market]
        report.details["tickers"] = len(market_tickers)
        if not market_tickers:
            report.flag("FAIL", "no tickers")
            reports.append(report)
            continue
        top = sorted(
            (item for item in market_tickers if item.symbol in active),
            key=lambda item: item.volume_24h,
            reverse=True,
        )[:20]
        ages = [(now - item.timestamp).total_seconds() for item in top]
        if ages:
            report.details["ticker_age_median_s"] = round(statistics.median(ages), 1)
            report.details["ticker_age_max_s"] = round(max(ages), 1)
            if statistics.median(ages) > limits.max_ticker_age_seconds:
                report.flag("FAIL", "top tickers are stale")
            elif max(ages) > limits.max_ticker_age_seconds:
                report.flag("WARN", "some top tickers are stale")
        crossed = sum(
            1
            for item in market_tickers
            if item.best_bid is not None
            and item.best_ask is not None
            and item.best_bid > item.best_ask
        )
        if crossed:
            report.flag("WARN", f"{crossed} tickers with bid above ask")
        if market is InstrumentType.PERPETUAL:
            _check_funding(report, [funding_index[s] for s in active if s in funding_index], now)
        if not top:
            report.flag("FAIL", "no ticker matches an active instrument")
            reports.append(report)
            continue
        sample = top[0]
        report.details["sample_symbol"] = sample.symbol
        await _check_book(adapter, report, sample, market, limits, clock)
        if market is InstrumentType.PERPETUAL:
            await _check_history(
                adapter, report, sample.symbol, funding_index.get(sample.symbol), limits, clock
            )
        reports.append(report)
    if not reports:
        report = FeedReport(venue=venue, market="all")
        report.flag("FAIL", "no active instruments")
        reports.append(report)
    return reports


def _check_funding(report: FeedReport, items: list[FundingSnapshot], now: datetime) -> None:
    report.details["funding_rates"] = len(items)
    if not items:
        report.flag("FAIL", "no funding rates for active perpetuals")
        return
    with_next = [item for item in items if item.next_funding_time is not None]
    ratio = len(with_next) / len(items)
    report.details["funding_with_next_time"] = round(ratio, 3)
    if not with_next:
        report.flag("FAIL", "no funding rate carries a next settlement time")
    elif ratio < 0.9:
        report.flag("WARN", "some funding rates lack a next settlement time")
    past = [
        item
        for item in with_next
        if item.next_funding_time and item.next_funding_time < now - timedelta(minutes=5)
    ]
    if past:
        report.flag("WARN", f"{len(past)} next funding times are in the past")
    intervals: dict[str, int] = {}
    for item in items:
        key = f"{item.funding_interval_hours.normalize()}h"
        intervals[key] = intervals.get(key, 0) + 1
    report.details["funding_intervals"] = intervals
    extreme = max(abs(item.funding_rate_8h) for item in items)
    report.details["funding_rate_8h_abs_max"] = str(extreme)
    if extreme > Decimal("0.02"):
        report.flag("WARN", "a funding rate above 2% per 8h (check the symbol)")


async def _check_book(
    adapter: ExchangeAdapter,
    report: FeedReport,
    sample: Ticker,
    market: InstrumentType,
    limits: PreflightLimits,
    clock: Callable[[], datetime],
) -> None:
    try:
        book = await _timed(
            adapter.get_orderbook(sample.symbol, 50, market), limits.request_timeout_seconds
        )
    except Exception as exc:
        report.flag("FAIL", f"order book unavailable: {type(exc).__name__}: {exc}"[:300])
        return
    age = book.age_seconds(clock())
    report.details["book_age_s"] = round(age, 2)
    report.details["book_levels"] = [len(book.bids), len(book.asks)]
    if not book.bids or not book.asks or book.mid_price is None:
        report.flag("FAIL", "empty order book")
        return
    if age > limits.max_book_age_seconds:
        report.flag("FAIL", f"order book is {age:.1f}s old")
    deviation = abs(book.mid_price / sample.last_price - 1)
    report.details["book_mid_vs_last"] = str(deviation.quantize(Decimal("0.00001")))
    if deviation > limits.max_mid_deviation:
        report.flag("FAIL", "book mid is far from the last trade (symbol or type mismatch)")
    depth = sum(
        (level.price * level.quantity for level in (*book.bids[:10], *book.asks[:10])),
        Decimal("0"),
    )
    report.details["top10_depth_usd"] = str(depth.quantize(Decimal("0.01")))
    if depth < limits.min_top_depth_usd:
        report.flag("WARN", "top-10 depth below 1000 USD (contract size normalization?)")


async def _check_history(
    adapter: ExchangeAdapter,
    report: FeedReport,
    symbol: str,
    funding: FundingSnapshot | None,
    limits: PreflightLimits,
    clock: Callable[[], datetime],
) -> None:
    now = clock()
    try:
        points = await _timed(
            adapter.get_funding_history(symbol, now - timedelta(days=3), now),
            limits.request_timeout_seconds,
        )
    except Exception as exc:
        report.flag("FAIL", f"funding history unavailable: {type(exc).__name__}: {exc}"[:300])
        return
    report.details["history_points_3d"] = len(points)
    if not points:
        report.flag("FAIL", "no funding history in the last 3 days")
        return
    latest = max(point.funding_timestamp for point in points)
    lag = (now - latest).total_seconds() / 3600
    report.details["history_latest_age_h"] = round(lag, 2)
    interval = float(funding.funding_interval_hours) if funding is not None else 8.0
    if lag > interval * 1.5 + 0.25:
        report.flag("WARN", "latest funding history point is older than 1.5 intervals")


async def run_preflight(
    adapters: dict[str, ExchangeAdapter],
    limits: PreflightLimits | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[FeedReport]:
    active_limits = limits or PreflightLimits()
    results = await asyncio.gather(
        *(check_venue(adapter, active_limits, clock) for adapter in adapters.values())
    )
    return [report for reports in results for report in reports]


def overall_status(reports: list[FeedReport]) -> Status:
    if not reports:
        return "FAIL"
    worst: Status = "PASS"
    for report in reports:
        if _RANK[report.status] > _RANK[worst]:
            worst = report.status
    return worst
