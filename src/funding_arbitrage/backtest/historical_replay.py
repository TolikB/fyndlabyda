"""No-look-ahead replay over historical candles and settled funding events."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.backtest.database_replay import PaperReplayDataset
from funding_arbitrage.backtest.events import (
    BacktestEvent,
    FillEvent,
    FundingEvent,
    MarketEvent,
    OpportunityEvent,
    PositionEvent,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    InstrumentRecord,
    MarketCandleRecord,
)
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity, SizeQuote
from funding_arbitrage.opportunity.settlement import (
    settlement_continuation_allowed,
    settlement_entry_allowed,
    target_settlement_events,
    target_settlements,
)
from funding_arbitrage.risk.engine import RiskEngine, RiskLimits


@dataclass(frozen=True)
class HistoricalDataset:
    instruments: list[NormalizedInstrument]
    candles: list[MarketCandleRecord]
    funding: list[FundingHistoryPoint]
    dataset_version: str
    coverage: dict[str, object]


@dataclass
class _ReplayPosition:
    position_id: str
    opportunity: Opportunity
    capital: Decimal
    opened_at: datetime
    entry_a: Decimal
    entry_b: Decimal
    target_settlements: tuple[datetime, ...]
    target_funding_events: dict[str, datetime]
    settled_funding_at: dict[str, datetime] = field(default_factory=dict)
    funding_events: int = 0
    edge_miss_count: int = 0
    funding_pnl: Decimal = Decimal("0")
    entry_fees: Decimal = Decimal("0")
    entry_spread: Decimal = Decimal("0")
    entry_slippage: Decimal = Decimal("0")


class HistoricalMarketReplay:
    """Load a canonical DB slice and simulate profiles from identical inputs."""

    async def load(
        self, session: AsyncSession, start: datetime, end: datetime
    ) -> HistoricalDataset:
        start = _utc(start)
        end = _utc(end)
        if start >= end:
            raise ValueError("start must be before end")
        candle_rows = list(
            (
                await session.execute(
                    select(MarketCandleRecord)
                    .where(
                        MarketCandleRecord.close_time >= start,
                        MarketCandleRecord.close_time < end,
                        MarketCandleRecord.is_closed.is_(True),
                    )
                    .order_by(
                        MarketCandleRecord.close_time,
                        MarketCandleRecord.exchange,
                        MarketCandleRecord.symbol,
                        MarketCandleRecord.instrument_type,
                    )
                )
            ).scalars()
        )
        funding_rows = list(
            (
                await session.execute(
                    select(FundingHistoryRecord)
                    .where(
                        FundingHistoryRecord.funding_timestamp >= start - timedelta(days=30),
                        FundingHistoryRecord.funding_timestamp < end,
                    )
                    .order_by(
                        FundingHistoryRecord.funding_timestamp,
                        FundingHistoryRecord.exchange,
                        FundingHistoryRecord.symbol,
                    )
                )
            ).scalars()
        )
        identities = {
            (row.exchange, row.symbol, row.instrument_type) for row in candle_rows
        }
        instrument_rows = list((await session.execute(select(InstrumentRecord))).scalars())
        instruments = [
            _instrument(row)
            for row in instrument_rows
            if (row.exchange, row.exchange_symbol, row.instrument_type) in identities
        ]
        perpetual_symbols = {
            (row.exchange, row.symbol)
            for row in candle_rows
            if row.instrument_type == InstrumentType.PERPETUAL.value
        }
        funding = _funding_points(funding_rows, perpetual_symbols)
        version = _dataset_digest(candle_rows, funding)
        return HistoricalDataset(
            instruments=instruments,
            candles=candle_rows,
            funding=funding,
            dataset_version=f"market-db-sha256:{version}",
            coverage=_coverage(candle_rows, funding, start, end),
        )

    def load_portable(
        self,
        directory: str | Path,
        start: datetime,
        end: datetime,
    ) -> HistoricalDataset:
        """Load a deterministic public-data export without a database connection."""

        start = _utc(start)
        end = _utc(end)
        if start >= end:
            raise ValueError("start must be before end")
        root = Path(directory)
        candle_rows = [
            candle
            for candle in _portable_candles(root / "candles.csv.gz")
            if start <= _utc(candle.close_time) < end and candle.is_closed
        ]
        candle_rows.sort(
            key=lambda row: (
                _utc(row.close_time),
                row.exchange,
                row.symbol,
                row.instrument_type,
            )
        )
        identities = {
            (row.exchange, row.symbol, row.instrument_type) for row in candle_rows
        }
        instruments = [
            item
            for item in _portable_instruments(root / "instruments.csv.gz")
            if (item.exchange, item.exchange_symbol, item.instrument_type.value)
            in identities
        ]
        perpetual_symbols = {
            (row.exchange, row.symbol)
            for row in candle_rows
            if row.instrument_type == InstrumentType.PERPETUAL.value
        }
        funding = [
            point
            for point in _portable_funding(root / "funding.csv.gz")
            if start - timedelta(days=30) <= point.funding_timestamp < end
            and (point.exchange, point.symbol) in perpetual_symbols
        ]
        funding.sort(
            key=lambda point: (
                point.funding_timestamp,
                point.exchange,
                point.symbol,
            )
        )
        version = _dataset_digest(candle_rows, funding)
        return HistoricalDataset(
            instruments=instruments,
            candles=candle_rows,
            funding=funding,
            dataset_version=f"market-db-sha256:{version}",
            coverage=_coverage(candle_rows, funding, start, end),
        )

    def simulate(
        self,
        dataset: HistoricalDataset,
        profile: str,
        initial_capital: Decimal,
        settings: Settings,
    ) -> PaperReplayDataset:
        if profile not in {"baseline", "candidate"}:
            raise ValueError("profile must be baseline or candidate")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        engine = _opportunity_engine(settings, profile)
        risk_engine = RiskEngine(
            RiskLimits(
                max_single_opportunity_percent=(
                    settings.paper_max_single_opportunity_percent
                ),
                max_single_asset_percent=settings.paper_max_single_asset_percent,
                max_single_exchange_percent=settings.paper_max_single_exchange_percent,
                max_single_strategy_percent=settings.paper_max_single_strategy_percent,
                max_correlated_group_percent=(
                    settings.paper_max_correlated_group_percent
                ),
                minimum_cash_reserve_percent=settings.paper_reserve_percent,
            )
        )
        candles_by_time: dict[datetime, list[MarketCandleRecord]] = defaultdict(list)
        for candle in dataset.candles:
            candles_by_time[_candle_timestamp(candle)].append(candle)
        funding_by_time: dict[datetime, list[FundingHistoryPoint]] = defaultdict(list)
        funding_by_symbol: dict[tuple[str, str], list[FundingHistoryPoint]] = defaultdict(list)
        for point in dataset.funding:
            funding_by_time[point.funding_timestamp].append(point)
            funding_by_symbol[(point.exchange, point.symbol)].append(point)
        for points in funding_by_symbol.values():
            points.sort(key=lambda value: value.funding_timestamp)
        funding_times_by_symbol = {
            key: [point.funding_timestamp for point in points]
            for key, points in funding_by_symbol.items()
        }
        settlement_times = sorted(funding_by_time)
        settlement_cursor = 0

        events: list[BacktestEvent] = []
        if dataset.candles:
            first = min(dataset.candles, key=lambda item: _utc(item.open_time))
            last = max(dataset.candles, key=_candle_timestamp)
            events.extend(
                [
                    MarketEvent(
                        event_id=f"{profile}:dataset-start",
                        timestamp=_utc(first.open_time),
                        exchange=first.exchange,
                        symbol=first.symbol,
                        price=first.open,
                    ),
                    MarketEvent(
                        event_id=f"{profile}:dataset-end",
                        timestamp=_candle_timestamp(last),
                        exchange=last.exchange,
                        symbol=last.symbol,
                        price=last.close,
                    ),
                ]
            )
        attribution: dict[str, dict[str, dict[str, Decimal]]] = {
            "strategy": {},
            "exchange": {},
            "asset": {},
        }
        positions: dict[str, _ReplayPosition] = {}
        last_prices: dict[tuple[str, str, str], Decimal] = {}
        realized = Decimal("0")
        serial = 0
        for timestamp in sorted(candles_by_time):
            current_candles = candles_by_time[timestamp]
            for candle in current_candles:
                last_prices[(candle.exchange, candle.symbol, candle.instrument_type)] = candle.close
                events.append(
                    MarketEvent(
                        event_id=(
                            f"{profile}:market:{candle.exchange}:{candle.symbol}:"
                            f"{candle.instrument_type}:{_utc(candle.open_time).isoformat()}"
                        ),
                        timestamp=timestamp,
                        exchange=candle.exchange,
                        symbol=candle.symbol,
                        price=candle.close,
                        mark_price=candle.close,
                    )
                )
            while (
                settlement_cursor < len(settlement_times)
                and settlement_times[settlement_cursor] <= timestamp
            ):
                funding_time = settlement_times[settlement_cursor]
                settlement_cursor += 1
                payments = funding_by_time[funding_time]
                for point in payments:
                    for position in positions.values():
                        pnl = _funding_payment(position, point)
                        if pnl is None:
                            continue
                        position.funding_events += 1
                        position.funding_pnl += pnl
                        event_key = f"{point.exchange}|{point.symbol}"
                        previous = position.settled_funding_at.get(event_key)
                        if previous is None or funding_time > previous:
                            position.settled_funding_at[event_key] = funding_time
                        realized += pnl
                        events.append(
                            FundingEvent(
                                event_id=f"{profile}:funding:{position.position_id}:{point.exchange}:{point.symbol}:{funding_time.isoformat()}",
                                timestamp=funding_time,
                                exchange=point.exchange,
                                symbol=point.symbol,
                                rate=point.funding_rate,
                                notional=position.capital,
                                pnl=pnl,
                            )
                        )
            snapshot = _snapshot(
                dataset.instruments,
                current_candles,
                funding_by_symbol,
                funding_times_by_symbol,
                timestamp,
            )
            opportunities = engine.scan(snapshot)
            by_key = {_opportunity_key(item): item for item in opportunities}
            for key, position in list(positions.items()):
                current = by_key.get(key)
                age = timestamp - position.opened_at
                if current is None:
                    position.edge_miss_count += 1
                else:
                    position.edge_miss_count = 0
                edge_gone = (
                    position.edge_miss_count >= settings.paper_exit_edge_miss_cycles
                )
                target_due = any(
                    target <= timestamp for target in position.target_settlements
                )
                pending_target = any(
                    target <= timestamp
                    and (
                        position.settled_funding_at.get(key) is None
                        or position.settled_funding_at[key] < target
                    )
                    for key, target in position.target_funding_events.items()
                )
                target_received = target_due and not pending_target and (
                    bool(position.target_funding_events) or position.funding_events > 0
                )
                adverse_basis = _current_market_pnl(position, last_prices) <= -(
                    position.capital * settings.paper_max_adverse_basis_percent
                )
                continue_after_target = False
                if target_received and current is not None:
                    quote = min(
                        current.size_quotes,
                        key=lambda value: abs(value.capital - position.capital),
                        default=None,
                    )
                    if quote is not None:
                        continue_after_target = settlement_continuation_allowed(
                            current,
                            quote,
                            snapshot,
                            timestamp,
                            settings.paper_min_settlement_cost_coverage,
                        )
                    if continue_after_target:
                        position.target_funding_events = target_settlement_events(
                            current, snapshot, timestamp
                        )
                        position.target_settlements = tuple(
                            sorted(set(position.target_funding_events.values()))
                        )
                target_exit = target_received and not continue_after_target
                max_hold = age >= timedelta(seconds=settings.paper_max_hold_seconds)
                should_close = max_hold if profile == "baseline" else (
                    max_hold
                    or edge_gone
                    or adverse_basis
                    or target_exit
                    or _replay_execution_degraded(current, position.capital)
                    or (
                        current is not None
                        and _funding_sign_changed(position.opportunity, current)
                    )
                )
                if should_close:
                    realized += _close_position(
                        position, timestamp, last_prices, events, attribution
                    )
                    positions.pop(key)
            locked = sum(
                position.capital * Decimal(_venue_count(position.opportunity))
                for position in positions.values()
            )
            available = initial_capital + realized - locked
            occupied_assets = {position.opportunity.asset for position in positions.values()}
            for opportunity in opportunities:
                if (
                    len(positions) >= settings.paper_max_open_positions
                    or opportunity.asset in occupied_assets
                ):
                    continue
                quote = _select_quote(
                    opportunity,
                    profile,
                    available,
                    initial_capital,
                    positions,
                    snapshot,
                    timestamp,
                    risk_engine,
                    settings,
                    settings.paper_correlation_group_values,
                )
                if quote is None:
                    continue
                required = quote.capital * Decimal(_venue_count(opportunity))
                spendable = max(
                    Decimal("0"),
                    available
                    - initial_capital
                    * settings.paper_reserve_percent
                    / Decimal("100"),
                )
                if required > spendable:
                    continue
                serial += 1
                position_id = f"{profile}-{serial:08d}"
                target_events = target_settlement_events(
                    opportunity, snapshot, timestamp
                )
                targets = tuple(sorted(set(target_events.values())))
                position = _ReplayPosition(
                    position_id=position_id,
                    opportunity=opportunity,
                    capital=quote.capital,
                    opened_at=timestamp,
                    entry_a=opportunity.price_a,
                    entry_b=opportunity.price_b,
                    target_settlements=targets,
                    target_funding_events=target_events,
                    entry_fees=quote.costs.entry_fees,
                    entry_spread=quote.costs.entry_spread,
                    entry_slippage=quote.costs.entry_slippage,
                )
                key = _opportunity_key(opportunity)
                positions[key] = position
                occupied_assets.add(opportunity.asset)
                entry_cost = (
                    quote.costs.entry_fees
                    + quote.costs.entry_spread
                    + quote.costs.entry_slippage
                    + quote.costs.legging_cost
                )
                realized -= entry_cost
                available -= required + entry_cost
                events.extend(
                    [
                        OpportunityEvent(
                            event_id=f"{profile}:opportunity:{position_id}",
                            timestamp=timestamp,
                            opportunity_id=position_id,
                            net_edge=opportunity.net_edge,
                        ),
                        FillEvent(
                            event_id=f"{profile}:entry:{position_id}",
                            timestamp=timestamp,
                            position_id=position_id,
                            notional=quote.capital * Decimal("2"),
                            fee=quote.costs.entry_fees,
                            spread=quote.costs.entry_spread,
                            slippage=(
                                quote.costs.entry_slippage
                                + quote.costs.legging_cost
                            ),
                        ),
                        PositionEvent(
                            event_id=f"{profile}:open:{position_id}",
                            timestamp=timestamp,
                            position_id=position_id,
                            state="OPEN",
                        ),
                    ]
                )
        if candles_by_time:
            end_at = max(candles_by_time)
            for position in positions.values():
                _close_position(position, end_at, last_prices, events, attribution)
        return PaperReplayDataset(
            events=events,
            dataset_version=f"{dataset.dataset_version}:{profile}",
            attribution=attribution,
            position_count=serial,
            observation_start=(
                min(_candle_timestamp(item) for item in dataset.candles)
                if dataset.candles
                else None
            ),
            observation_end=(
                max(_candle_timestamp(item) for item in dataset.candles)
                if dataset.candles
                else None
            ),
        )


def _funding_points(
    rows: list[FundingHistoryRecord],
    perpetual_symbols: set[tuple[str, str]],
) -> list[FundingHistoryPoint]:
    return [
        FundingHistoryPoint(
            exchange=row.exchange,
            symbol=row.symbol,
            funding_rate=row.funding_rate,
            funding_timestamp=_utc(row.funding_timestamp),
            mark_price=row.mark_price,
        )
        for row in rows
        if (row.exchange, row.symbol) in perpetual_symbols
    ]


def _portable_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"portable dataset has no CSV header: {path}")
        rows: list[dict[str, str]] = []
        for row in reader:
            if any(value is None for value in row.values()):
                raise ValueError(f"portable dataset has a malformed row: {path}")
            rows.append({key: value or "" for key, value in row.items()})
        return rows


def _portable_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _utc(parsed)


def _portable_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"t", "true", "1"}:
        return True
    if normalized in {"f", "false", "0"}:
        return False
    raise ValueError(f"invalid portable boolean: {value!r}")


def _portable_candles(path: Path) -> list[MarketCandleRecord]:
    return [
        MarketCandleRecord(
            exchange=row["exchange"],
            symbol=row["symbol"],
            instrument_type=row["instrument_type"],
            interval_minutes=int(row["interval_minutes"]),
            open_time=_portable_datetime(row["open_time"]),
            close_time=_portable_datetime(row["close_time"]),
            open=Decimal(row["open"]),
            high=Decimal(row["high"]),
            low=Decimal(row["low"]),
            close=Decimal(row["close"]),
            volume=Decimal(row["volume"]),
            is_closed=_portable_bool(row["is_closed"]),
        )
        for row in _portable_rows(path)
    ]


def _portable_funding(path: Path) -> list[FundingHistoryPoint]:
    return [
        FundingHistoryPoint(
            exchange=row["exchange"],
            symbol=row["symbol"],
            funding_rate=Decimal(row["funding_rate"]),
            funding_timestamp=_portable_datetime(row["funding_timestamp"]),
            mark_price=(
                Decimal(row["mark_price"]) if row["mark_price"] else None
            ),
        )
        for row in _portable_rows(path)
    ]


def _portable_instruments(path: Path) -> list[NormalizedInstrument]:
    return [
        NormalizedInstrument(
            exchange=row["exchange"],
            exchange_symbol=row["exchange_symbol"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            instrument_type=InstrumentType(row["instrument_type"]),
            settlement_asset=row["settlement_asset"] or None,
            contract_size=Decimal(row["contract_size"]),
            tick_size=Decimal(row["tick_size"]),
            step_size=Decimal(row["step_size"]),
            min_order_size=Decimal(row["min_order_size"]),
            funding_interval=(
                int(row["funding_interval"]) if row["funding_interval"] else None
            ),
            expiry=(
                _portable_datetime(row["expiry"]) if row["expiry"] else None
            ),
            is_active=_portable_bool(row["is_active"]),
        )
        for row in _portable_rows(path)
    ]


def _opportunity_engine(settings: Settings, profile: str) -> OpportunityEngine:
    return OpportunityEngine(
        cost_engine=CostEngine(
            fees={
                venue: FeeSchedule(maker_fee=fees[0], taker_fee=fees[1])
                for venue, fees in settings.fee_schedules.items()
            },
            borrowing_cost_daily=settings.scanner_borrowing_cost_daily,
            legging_cost_percent=settings.paper_legging_move_percent,
        ),
        filter_config=OpportunityFilterConfig(
            minimum_net_apr=settings.scanner_minimum_net_apr,
            minimum_liquidity_score=settings.scanner_minimum_liquidity_score,
            maximum_slippage_percent=settings.scanner_maximum_slippage_percent,
            maximum_spread_percent=settings.scanner_maximum_spread_percent,
            minimum_funding_samples=settings.scanner_minimum_funding_samples,
            minimum_opportunity_duration_seconds=0,
        ),
        funding_horizon_hours=settings.paper_funding_horizon_hours,
        allow_spot_short=settings.scanner_allow_spot_short,
        forecast_mode=profile,
        diagnostic_quote_limit=0,
    )


def _snapshot(
    instruments: list[NormalizedInstrument],
    candles: list[MarketCandleRecord],
    funding_by_symbol: dict[tuple[str, str], list[FundingHistoryPoint]],
    funding_times_by_symbol: dict[tuple[str, str], list[datetime]],
    timestamp: datetime,
) -> MarketSnapshot:
    tickers: list[Ticker] = []
    books: dict[tuple[str, str, InstrumentType], OrderBook] = {}
    live_identities: set[tuple[str, str, InstrumentType]] = set()
    for row in candles:
        instrument_type = InstrumentType(row.instrument_type)
        spread = Decimal("0.0005")
        bid = row.close * (Decimal("1") - spread / Decimal("2"))
        ask = row.close * (Decimal("1") + spread / Decimal("2"))
        quote_volume = max(row.volume * row.close, Decimal("10000"))
        quantity = quote_volume * Decimal("0.01") / row.close
        ticker = Ticker(
            exchange=row.exchange,
            symbol=row.symbol,
            instrument_type=instrument_type,
            last_price=row.close,
            mark_price=row.close,
            index_price=row.close,
            best_bid=bid,
            best_ask=ask,
            volume_24h=quote_volume * Decimal("24"),
            timestamp=timestamp,
        )
        tickers.append(ticker)
        books[(row.exchange, row.symbol, instrument_type)] = OrderBook(
            exchange=row.exchange,
            symbol=row.symbol,
            instrument_type=instrument_type,
            bids=tuple(
                OrderBookLevel(
                    price=bid * (Decimal("1") - Decimal(level) * Decimal("0.0005")),
                    quantity=quantity,
                )
                for level in range(3)
            ),
            asks=tuple(
                OrderBookLevel(
                    price=ask * (Decimal("1") + Decimal(level) * Decimal("0.0005")),
                    quantity=quantity,
                )
                for level in range(3)
            ),
            timestamp=timestamp,
        )
        live_identities.add((row.exchange, row.symbol, instrument_type))
    ticker_prices = {(item.exchange, item.symbol): item.last_price for item in tickers}
    active_instruments = [
        item
        for item in instruments
        if (item.exchange, item.exchange_symbol, item.instrument_type) in live_identities
    ]
    snapshots: list[FundingSnapshot] = []
    history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
    for item in active_instruments:
        if item.instrument_type is not InstrumentType.PERPETUAL:
            continue
        points = funding_by_symbol.get((item.exchange, item.exchange_symbol), [])
        point_times = funding_times_by_symbol.get((item.exchange, item.exchange_symbol), [])
        split = bisect_right(point_times, timestamp)
        past = points[:split]
        history[(item.exchange, item.exchange_symbol)] = past
        if not past:
            continue
        interval = Decimal(item.funding_interval or _infer_interval(points))
        snapshots.append(
            FundingSnapshot(
                exchange=item.exchange,
                symbol=item.exchange_symbol,
                funding_rate=past[-1].funding_rate,
                funding_interval_hours=interval,
                next_funding_time=(
                    points[split].funding_timestamp if split < len(points) else None
                ),
                mark_price=ticker_prices.get((item.exchange, item.exchange_symbol)),
                timestamp=timestamp,
            )
        )
    return MarketSnapshot(
        instruments=active_instruments,
        tickers=tickers,
        funding=snapshots,
        orderbooks=books,
        captured_at=timestamp,
        funding_history=history,
        stale_after_seconds=3601,
    )


def _select_quote(
    opportunity: Opportunity,
    profile: str,
    available: Decimal,
    initial_capital: Decimal,
    positions: dict[str, _ReplayPosition],
    snapshot: MarketSnapshot,
    timestamp: datetime,
    risk_engine: RiskEngine,
    settings: Settings,
    correlation_groups: tuple[frozenset[str], ...],
) -> SizeQuote | None:
    viable = [
        quote
        for quote in opportunity.size_quotes
        if quote.fully_filled and quote.net_profit > 0 and quote.capital <= available
    ]
    if profile == "baseline":
        return min(viable, key=lambda quote: abs(quote.capital - Decimal("250")), default=None)
    leg_venues = (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a)
    venues = tuple(dict.fromkeys(leg_venues))
    asset_exposure = sum(
        (
            position.capital * Decimal(_venue_count(position.opportunity))
            for position in positions.values()
            if position.opportunity.asset == opportunity.asset
        ),
        Decimal("0"),
    )
    strategy_exposure = sum(
        (
            position.capital * Decimal(_venue_count(position.opportunity))
            for position in positions.values()
            if position.opportunity.strategy == opportunity.strategy
        ),
        Decimal("0"),
    )
    normalized_asset = opportunity.asset.upper()
    correlation_group = next(
        (
            group
            for group in correlation_groups
            if normalized_asset in group
        ),
        frozenset({normalized_asset}),
    )
    correlated_exposure = sum(
        (
            position.capital * Decimal(_venue_count(position.opportunity))
            for position in positions.values()
            if position.opportunity.asset.upper() in correlation_group
        ),
        Decimal("0"),
    )
    for quote in sorted(
        viable, key=lambda item: (item.net_profit, item.net_apr), reverse=True
    ):
        if not settlement_entry_allowed(
            opportunity,
            quote,
            snapshot,
            timestamp,
            settings.paper_entry_window_hours,
            settings.paper_min_settlement_cost_coverage,
        ):
            continue
        total_capital = quote.capital * Decimal(_venue_count(opportunity))
        assessments = [
            risk_engine.assess(
                opportunity,
                total_capital,
                initial_capital,
                asset_exposure=asset_exposure,
                exchange_exposure=_exchange_position_exposure(positions, venue),
                exchange_increment=quote.capital * Decimal(leg_venues.count(venue)),
                strategy_exposure=strategy_exposure,
                correlated_exposure=correlated_exposure,
                cash=available,
                cash_required=total_capital,
            )
            for venue in venues
        ]
        if all(assessment.approved for assessment in assessments):
            return quote
    return None


def _replay_execution_degraded(
    current: Opportunity | None, capital: Decimal
) -> bool:
    if current is None:
        return False
    quote = min(
        current.size_quotes,
        key=lambda value: abs(value.capital - capital),
        default=None,
    )
    return quote is None or not quote.fully_filled


def _target_settlements(
    opportunity: Opportunity, snapshot: MarketSnapshot, timestamp: datetime
) -> tuple[datetime, ...]:
    return target_settlements(opportunity, snapshot, timestamp)


def _funding_payment(
    position: _ReplayPosition, point: FundingHistoryPoint
) -> Decimal | None:
    opportunity = position.opportunity
    if point.exchange == opportunity.venue_a and point.symbol == opportunity.symbol_a:
        side = opportunity.leg_a_side
    elif point.exchange == opportunity.venue_b and point.symbol == opportunity.symbol_b:
        side = opportunity.leg_b_side
    else:
        return None
    direction = Decimal("1") if side == "SELL" else Decimal("-1")
    return position.capital * point.funding_rate * direction


def _close_position(
    position: _ReplayPosition,
    timestamp: datetime,
    prices: dict[tuple[str, str, str], Decimal],
    events: list[BacktestEvent],
    attribution: dict[str, dict[str, dict[str, Decimal]]],
) -> Decimal:
    opportunity = position.opportunity
    exit_a = prices.get(
        (opportunity.venue_a, opportunity.symbol_a or "", opportunity.leg_a_type),
        position.entry_a,
    )
    exit_b = prices.get(
        (
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b or "",
            opportunity.leg_b_type,
        ),
        position.entry_b,
    )
    pnl_a = _leg_pnl(position.capital, position.entry_a, exit_a, opportunity.leg_a_side)
    pnl_b = _leg_pnl(position.capital, position.entry_b, exit_b, opportunity.leg_b_side)
    quote = min(
        opportunity.size_quotes,
        key=lambda value: abs(value.capital - position.capital),
    )
    holding_hours = max(
        Decimal("0"),
        Decimal(str((timestamp - position.opened_at).total_seconds())) / Decimal("3600"),
    )
    borrow_cost = (
        quote.costs.borrowing_cost
        * holding_hours
        / opportunity.expected_holding_hours
    )
    market_pnl = pnl_a + pnl_b
    basis_pnl = (
        market_pnl
        if opportunity.strategy in {"spot_perp", "futures_basis"}
        else Decimal("0")
    )
    mismatch_pnl = market_pnl - basis_pnl
    components = {
        "funding_pnl": position.funding_pnl,
        "basis_pnl": basis_pnl,
        "price_mismatch_pnl": mismatch_pnl,
        "fees": position.entry_fees + quote.costs.exit_fees,
        "spread": position.entry_spread + quote.costs.exit_spread,
        "slippage": position.entry_slippage + quote.costs.exit_slippage,
        "borrow_cost": borrow_cost,
        "legging_cost": quote.costs.legging_cost,
        "other_costs": quote.costs.network_cost,
    }
    total_cost = sum(
        (
            components["fees"],
            components["spread"],
            components["slippage"],
            components["borrow_cost"],
            components["legging_cost"],
            components["other_costs"],
        ),
        Decimal("0"),
    )
    components["net_pnl"] = position.funding_pnl + market_pnl - total_cost
    _attribute_replay(attribution["strategy"], opportunity.strategy, components)
    _attribute_replay(attribution["asset"], opportunity.asset, components)
    venues = tuple(
        dict.fromkeys((opportunity.venue_a, opportunity.venue_b or opportunity.venue_a))
    )
    divisor = Decimal(len(venues))
    for venue in venues:
        _attribute_replay(
            attribution["exchange"],
            venue,
            {key: value / divisor for key, value in components.items()},
        )
    events.extend(
        [
            FillEvent(
                event_id=f"close:{position.position_id}",
                timestamp=timestamp,
                position_id=position.position_id,
                notional=position.capital * Decimal("2"),
                fee=quote.costs.exit_fees,
                spread=quote.costs.exit_spread,
                slippage=quote.costs.exit_slippage,
            ),
            PositionEvent(
                event_id=f"position-close:{position.position_id}",
                timestamp=timestamp,
                position_id=position.position_id,
                state="CLOSED",
                pnl=market_pnl - borrow_cost - quote.costs.network_cost,
            ),
        ]
    )
    exit_cost = (
        quote.costs.exit_fees
        + quote.costs.exit_spread
        + quote.costs.exit_slippage
        + borrow_cost
        + quote.costs.network_cost
    )
    return market_pnl - exit_cost


def _attribute_replay(
    target: dict[str, dict[str, Decimal]],
    key: str,
    components: dict[str, Decimal],
) -> None:
    bucket = target.setdefault(key, {})
    for component, value in components.items():
        bucket[component] = bucket.get(component, Decimal("0")) + value


def _leg_pnl(capital: Decimal, entry: Decimal, exit_price: Decimal, side: str) -> Decimal:
    quantity = capital / entry
    return (exit_price - entry) * quantity * (Decimal("1") if side == "BUY" else Decimal("-1"))


def _current_market_pnl(
    position: _ReplayPosition,
    prices: dict[tuple[str, str, str], Decimal],
) -> Decimal:
    opportunity = position.opportunity
    exit_a = prices.get(
        (opportunity.venue_a, opportunity.symbol_a or "", opportunity.leg_a_type),
        position.entry_a,
    )
    exit_b = prices.get(
        (
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b or "",
            opportunity.leg_b_type,
        ),
        position.entry_b,
    )
    return _leg_pnl(
        position.capital, position.entry_a, exit_a, opportunity.leg_a_side
    ) + _leg_pnl(position.capital, position.entry_b, exit_b, opportunity.leg_b_side)


def _opportunity_key(opportunity: Opportunity) -> str:
    return "|".join(
        (
            opportunity.strategy,
            opportunity.asset,
            opportunity.venue_a,
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_a or "",
            opportunity.symbol_b or "",
            opportunity.leg_a_side,
            opportunity.leg_b_side,
        )
    )


def _funding_sign_changed(original: Opportunity, current: Opportunity) -> bool:
    return (original.funding_a >= 0) != (current.funding_a >= 0) or (
        original.funding_b >= 0
    ) != (current.funding_b >= 0)


def _venue_count(opportunity: Opportunity) -> int:
    return 2


def _exchange_position_exposure(
    positions: dict[str, _ReplayPosition], venue: str
) -> Decimal:
    return sum(
        (
            position.capital
            * Decimal(
                (
                    position.opportunity.venue_a,
                    position.opportunity.venue_b or position.opportunity.venue_a,
                ).count(venue)
            )
            for position in positions.values()
        ),
        Decimal("0"),
    )


def _infer_interval(points: list[FundingHistoryPoint]) -> int:
    if len(points) < 2:
        return 8
    deltas = sorted(
        max(1, round((right.funding_timestamp - left.funding_timestamp).total_seconds() / 3600))
        for left, right in zip(points, points[1:], strict=False)
    )
    return deltas[len(deltas) // 2]


def _instrument(row: InstrumentRecord) -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange=row.exchange,
        exchange_symbol=row.exchange_symbol,
        base_asset=row.base_asset,
        quote_asset=row.quote_asset,
        instrument_type=InstrumentType(row.instrument_type),
        settlement_asset=row.settlement_asset,
        contract_size=row.contract_size,
        tick_size=row.tick_size,
        step_size=row.step_size,
        min_order_size=row.min_order_size,
        funding_interval=row.funding_interval,
        expiry=row.expiry,
        is_active=row.is_active,
    )


def _dataset_digest(
    candles: list[MarketCandleRecord], funding: list[FundingHistoryPoint]
) -> str:
    payload = [
        [
            "c",
            row.exchange,
            row.symbol,
            row.instrument_type,
            _utc(row.open_time).isoformat(),
            str(row.open),
            str(row.high),
            str(row.low),
            str(row.close),
            str(row.volume),
        ]
        for row in candles
    ]
    payload.extend(
        [
            "f",
            point.exchange,
            point.symbol,
            point.funding_timestamp.isoformat(),
            str(point.funding_rate),
            str(point.mark_price),
        ]
        for point in funding
    )
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _coverage(
    candles: list[MarketCandleRecord],
    funding: list[FundingHistoryPoint],
    start: datetime,
    end: datetime,
) -> dict[str, object]:
    grouped: dict[str, list[datetime]] = defaultdict(list)
    for row in candles:
        grouped[f"{row.exchange}:{row.symbol}:{row.instrument_type}"].append(_utc(row.close_time))
    expected = max(1, int((end - start).total_seconds() // 3600))
    series = {
        key: {
            "candles": len(values),
            "coverage_ratio": str(Decimal(len(set(values))) / Decimal(expected)),
            "largest_gap_hours": str(
                max(
                    (
                        Decimal(str((right - left).total_seconds())) / Decimal("3600")
                        for left, right in zip(
                            sorted(set(values)),
                            sorted(set(values))[1:],
                            strict=False,
                        )
                    ),
                    default=Decimal("0"),
                )
            ),
        }
        for key, values in sorted(grouped.items())
    }
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "candle_rows": len(candles),
        "funding_events": len(funding),
        "series": series,
    }


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


def _candle_timestamp(candle: MarketCandleRecord) -> datetime:
    """Canonical close boundary independent of venue inclusive-end conventions."""

    return _utc(candle.open_time) + timedelta(minutes=candle.interval_minutes)
