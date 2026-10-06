"""Replay series entry/exit rules over recorded opportunities and funding.

The paper runner answers "what happened"; this answers "what would these rules
have done" on the same recorded period, before a new series spends weeks finding
out. It reads three tables the runner already keeps:

* ``opportunities`` - confirmed signals with their stability, persistence,
  funding intervals, and current funding edge;
* ``funding_snapshots`` - the rate each venue showed for its next settlement,
  roughly every 15 minutes, which drives exits as the runner sees them;
* ``funding_history`` - settled rates; where it has a gap, the last snapshot
  before a settlement stands in (on live data that matched the settled sign
  98-100% of the time).

Simplifications, all stated in the report: one fixed round-trip cost per trade
(the runner prices every book), no capital limit beyond position counts, signals
only exist where the recording stack fetched books, and passive execution is a
cost discount rather than a fill model. Use it to rank rule sets, not to forecast
returns.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    FundingSnapshotRecord,
    OpportunityRecord,
)
from funding_arbitrage.services.series import SeriesConfig

_EIGHT = Decimal("8")
_ZERO = Decimal("0")
# Measured on the live candidate/baseline series: fees 0.205% + slippage 0.129%.
DEFAULT_ROUND_TRIP_COST = Decimal("0.00334")
# Taker 0.05% vs maker 0.02% on one leg of the entry and one leg of the exit.
DEFAULT_MAKER_SAVING = Decimal("0.0006")
# The live loop runs a pass of ~12s and then sleeps 15s.
DEFAULT_CYCLE_SECONDS = 27

LegKey = tuple[str, str]


@dataclass(frozen=True)
class ReplayLeg:
    exchange: str
    symbol: str
    # +1 long, -1 short: a long leg pays positive funding.
    direction: int
    interval_hours: Decimal

    @property
    def key(self) -> LegKey:
        return (self.exchange, self.symbol)


@dataclass(frozen=True)
class Signal:
    at: datetime
    key: str
    asset: str
    strategy: str
    legs: tuple[ReplayLeg, ...]
    funding_rate_8h: Decimal
    net_apr: Decimal
    stability: Decimal
    persistence: Decimal


@dataclass
class FundingTape:
    """Settled rates and shown (next-settlement) rates per perpetual market."""

    settled: dict[LegKey, dict[datetime, Decimal]] = field(default_factory=dict)
    shown: dict[LegKey, list[tuple[datetime, Decimal]]] = field(default_factory=dict)
    _settled_times: dict[LegKey, list[datetime]] = field(default_factory=dict)
    _shown_times: dict[LegKey, list[datetime]] = field(default_factory=dict)

    def add_snapshot(
        self,
        leg: LegKey,
        at: datetime,
        rate: Decimal,
        interval_hours: Decimal,
        next_funding_time: datetime | None,
    ) -> None:
        """Add snapshots in time order; settlements from history must come after."""

        if interval_hours > 0:
            self.shown.setdefault(leg, []).append((at, rate * _EIGHT / interval_hours))
        if next_funding_time is not None and at < next_funding_time:
            # Later snapshots overwrite earlier ones: the last one before settlement.
            self.settled.setdefault(leg, {})[_settlement_minute(next_funding_time)] = rate

    def add_settlement(self, leg: LegKey, at: datetime, rate: Decimal) -> None:
        self.settled.setdefault(leg, {})[_settlement_minute(at)] = rate

    def freeze(self) -> FundingTape:
        for points in self.shown.values():
            points.sort()
        self._shown_times = {leg: [at for at, _ in points] for leg, points in self.shown.items()}
        self._settled_times = {leg: sorted(points) for leg, points in self.settled.items()}
        return self

    def settled_between(self, leg: LegKey, start: datetime, end: datetime) -> list[Decimal]:
        times = self._settled_times.get(leg, [])
        rates = self.settled.get(leg, {})
        return [rates[at] for at in times[bisect_right(times, start) : bisect_right(times, end)]]

    def shown_at(self, leg: LegKey, at: datetime) -> Decimal | None:
        times = self._shown_times.get(leg, [])
        index = bisect_right(times, at) - 1
        return self.shown[leg][index][1] if index >= 0 else None


@dataclass(frozen=True)
class Trade:
    key: str
    asset: str
    opened_at: datetime
    closed_at: datetime
    funding: Decimal
    cost: Decimal
    open_at_end: bool

    @property
    def net(self) -> Decimal:
        return self.funding - self.cost

    @property
    def hold_hours(self) -> Decimal:
        return Decimal(str((self.closed_at - self.opened_at).total_seconds())) / Decimal("3600")


@dataclass
class ReplayResult:
    series: str
    trades: list[Trade]

    def summary(self, notional: Decimal) -> dict[str, Any]:
        count = len(self.trades)
        if not count:
            return {"series": self.series, "trades": 0}
        net = sum((trade.net for trade in self.trades), _ZERO)
        funding = sum((trade.funding for trade in self.trades), _ZERO)
        assets = Counter(trade.asset for trade in self.trades)
        top, top_count = assets.most_common(1)[0]
        without_top = sum((t.net for t in self.trades if t.asset != top), _ZERO)
        return {
            "series": self.series,
            "trades": count,
            "assets": len(assets),
            "open_at_end": sum(trade.open_at_end for trade in self.trades),
            "net_per_trade_percent": _pct(net / count),
            "funding_per_trade_percent": _pct(funding / count),
            "win_rate": round(sum(trade.net > 0 for trade in self.trades) / count, 3),
            "average_hold_hours": round(
                float(sum((t.hold_hours for t in self.trades), _ZERO) / count), 1
            ),
            "net_usdt": str((net * notional).quantize(Decimal("0.01"))),
            "top_asset": f"{top} x{top_count}",
            "net_usdt_without_top_asset": str((without_top * notional).quantize(Decimal("0.01"))),
        }


def _pct(value: Decimal) -> str:
    return f"{value * 100:.3f}"


def _settlement_minute(at: datetime) -> datetime:
    """Venues stamp one settlement a few milliseconds apart: key it by the minute."""

    return (at + timedelta(seconds=30)).replace(second=0, microsecond=0)


def settled_edge(tape: FundingTape, signal: Signal, start: datetime, end: datetime) -> Decimal:
    """Funding received per unit notional between ``start`` and ``end``."""

    received = _ZERO
    for leg in signal.legs:
        for rate in tape.settled_between(leg.key, start, end):
            received -= leg.direction * rate
    return received


def settled_mean_8h(
    tape: FundingTape, signal: Signal, at: datetime, lookback_hours: Decimal, min_points: int
) -> Decimal | None:
    """Same definition as the runner's forecast input (selection.settled_edge_8h)."""

    start = at - timedelta(hours=float(lookback_hours))
    edge = _ZERO
    for leg in signal.legs:
        rates = tape.settled_between(leg.key, start, at)
        if len(rates) < min_points:
            return None
        scale = _EIGHT / leg.interval_hours
        edge -= leg.direction * sum((rate * scale for rate in rates), _ZERO) / len(rates)
    return edge


def shown_edge_8h(tape: FundingTape, signal: Signal, at: datetime) -> Decimal | None:
    edge = _ZERO
    for leg in signal.legs:
        shown = tape.shown_at(leg.key, at)
        if shown is None:
            return None
        edge -= leg.direction * shown
    return edge


def _admits(
    config: SeriesConfig,
    signal: Signal,
    tape: FundingTape,
    last_closed: dict[str, datetime],
    cost: Decimal,
) -> bool:
    if signal.strategy not in {str(item) for item in config.strategies}:
        return False
    if signal.funding_rate_8h < config.entry.min_funding_rate_8h:
        return False
    if signal.net_apr < config.entry.min_net_apr:
        return False
    selection = config.selection
    if selection is None:
        return True
    if signal.stability < selection.min_stability_score:
        return False
    if signal.persistence < selection.min_persistence_score:
        return False
    intervals = [leg.interval_hours for leg in signal.legs]
    if (
        selection.max_interval_ratio is not None
        and len(intervals) == 2
        and max(intervals) / min(intervals) > selection.max_interval_ratio
    ):
        return False
    closed_at = last_closed.get(signal.asset)
    cooldown = timedelta(hours=float(selection.reentry_cooldown_hours))
    if closed_at is not None and signal.at - closed_at < cooldown:
        return False
    forecast = selection.forecast
    if forecast is None:
        return True
    settled = settled_mean_8h(
        tape, signal, signal.at, forecast.lookback_hours, forecast.min_history_points
    )
    if settled is None:
        return False
    blended = (
        forecast.current_weight * signal.funding_rate_8h
        + (Decimal("1") - forecast.current_weight) * settled
    )
    return blended * forecast.horizon_hours / _EIGHT - cost >= forecast.min_expected_net


def _exit_time(
    config: SeriesConfig,
    signal: Signal,
    tape: FundingTape,
    end: datetime,
    step: timedelta,
    cycle_seconds: int,
) -> datetime | None:
    """First time the series' exit rules fire, or None if still open at ``end``."""

    if config.holding is not None:
        confirm = timedelta(minutes=float(config.holding.exit_confirmation_minutes))
    else:
        confirm = timedelta(seconds=cycle_seconds * config.exit.exit_confirmations)
    min_hold = timedelta(hours=float(config.exit.min_hold_hours))
    max_hold = timedelta(hours=float(config.exit.max_hold_hours))
    low_since: datetime | None = None
    at = signal.at
    while True:
        at += step
        if at >= end:
            return None
        if at - signal.at >= max_hold:
            return at
        edge = shown_edge_8h(tape, signal, at)
        if edge is None:
            continue
        if edge < config.exit.exit_funding_rate_8h:
            low_since = low_since or at
            # A confirmation shorter than one step is met by a single low observation.
            confirmed = confirm <= step or at - low_since >= confirm
            if at - signal.at >= min_hold and confirmed:
                return at
        else:
            low_since = None


def replay_series(
    config: SeriesConfig,
    signals: Iterable[Signal],
    tape: FundingTape,
    *,
    end: datetime,
    cost: Decimal = DEFAULT_ROUND_TRIP_COST,
    maker_saving: Decimal = DEFAULT_MAKER_SAVING,
    step: timedelta = timedelta(minutes=15),
    cycle_seconds: int = DEFAULT_CYCLE_SECONDS,
) -> ReplayResult:
    execution = config.execution
    trade_cost = cost
    if execution is not None:
        trade_cost -= maker_saving * (
            Decimal(int(execution.maker_entry) + int(execution.maker_exit)) / 2
        )
    ordered = sorted(signals, key=lambda item: (item.at, -item.funding_rate_8h))
    open_until: dict[str, datetime] = {}  # key -> exit time (end for open trades)
    asset_until: dict[str, datetime] = {}
    last_closed: dict[str, datetime] = {}
    trades: list[Trade] = []
    for signal in ordered:
        busy = [until for until in open_until.values() if until > signal.at]
        if len(busy) >= config.max_open_positions:
            continue
        if open_until.get(signal.key, signal.at) > signal.at:
            continue
        if asset_until.get(signal.asset, signal.at) > signal.at:
            continue
        if not _admits(config, signal, tape, last_closed, trade_cost):
            continue
        exit_at = _exit_time(config, signal, tape, end, step, cycle_seconds)
        closed_at = exit_at or end
        trades.append(
            Trade(
                key=signal.key,
                asset=signal.asset,
                opened_at=signal.at,
                closed_at=closed_at,
                funding=settled_edge(tape, signal, signal.at, closed_at),
                cost=trade_cost,
                open_at_end=exit_at is None,
            )
        )
        open_until[signal.key] = closed_at
        asset_until[signal.asset] = closed_at
        if exit_at is not None:
            last_closed[signal.asset] = exit_at
    return ReplayResult(config.name, trades)


# ------------------------------------------------------------------ database
def signal_from_payload(at: datetime, payload: dict[str, Any]) -> Signal | None:
    legs: list[ReplayLeg] = []
    for side in ("a", "b"):
        if payload.get(f"leg_{side}_type") != "PERPETUAL":
            continue
        interval = payload.get(f"funding_interval_hours_{side}")
        venue = payload.get(f"venue_{side}") or payload.get("venue_a")
        symbol = payload.get(f"symbol_{side}")
        if interval is None or symbol is None or venue is None:
            return None
        legs.append(
            ReplayLeg(
                exchange=str(venue),
                symbol=str(symbol),
                direction=1 if str(payload.get(f"leg_{side}_side", "")).upper() == "BUY" else -1,
                interval_hours=Decimal(str(interval)),
            )
        )
    if not legs:
        return None
    key = ":".join(
        str(payload.get(name) or "")
        for name in (
            "strategy",
            "asset",
            "venue_a",
            "symbol_a",
            "leg_a_type",
            "leg_a_side",
            "venue_b",
            "symbol_b",
            "leg_b_type",
            "leg_b_side",
        )
    )
    return Signal(
        at=at,
        key=key,
        asset=str(payload["asset"]),
        strategy=str(payload["strategy"]),
        legs=tuple(legs),
        funding_rate_8h=Decimal(str(payload.get("funding_rate_8h", "0"))),
        net_apr=Decimal(str(payload.get("net_apr", "0"))),
        stability=Decimal(str(payload.get("funding_stability_score", "0"))),
        persistence=Decimal(str(payload.get("persistence_score", "0"))),
    )


async def load_inputs(
    session: AsyncSession, start: datetime, end: datetime, lookback: timedelta
) -> tuple[list[Signal], FundingTape]:
    rows = await session.execute(
        select(OpportunityRecord.created_at, OpportunityRecord.payload)
        .where(
            OpportunityRecord.status == "confirmed",
            OpportunityRecord.created_at >= start,
            OpportunityRecord.created_at <= end,
        )
        .order_by(OpportunityRecord.created_at)
    )
    signals: list[Signal] = []
    for row in rows.all():
        signal = signal_from_payload(cast(datetime, row[0]), cast(dict[str, Any], row[1]))
        if signal is not None:
            signals.append(signal)
    legs = sorted({leg.key for signal in signals for leg in signal.legs})
    tape = FundingTape()
    if not legs:
        return signals, tape.freeze()
    since = start - lookback
    for index in range(0, len(legs), 200):
        chunk = legs[index : index + 200]
        snapshots = await session.execute(
            select(
                FundingSnapshotRecord.exchange,
                FundingSnapshotRecord.symbol,
                FundingSnapshotRecord.timestamp,
                FundingSnapshotRecord.funding_rate,
                FundingSnapshotRecord.funding_interval_hours,
                FundingSnapshotRecord.next_funding_time,
            )
            .where(
                tuple_(FundingSnapshotRecord.exchange, FundingSnapshotRecord.symbol).in_(chunk),
                FundingSnapshotRecord.timestamp >= since,
            )
            .order_by(FundingSnapshotRecord.timestamp)
        )
        for exchange, symbol, at, rate, interval, next_time in snapshots.all():
            tape.add_snapshot(
                (exchange, symbol),
                at,
                Decimal(str(rate)),
                Decimal(str(interval)),
                next_time,
            )
        history = await session.execute(
            select(
                FundingHistoryRecord.exchange,
                FundingHistoryRecord.symbol,
                FundingHistoryRecord.funding_timestamp,
                FundingHistoryRecord.funding_rate,
            ).where(
                tuple_(FundingHistoryRecord.exchange, FundingHistoryRecord.symbol).in_(chunk),
                FundingHistoryRecord.funding_timestamp >= since,
            )
        )
        for exchange, symbol, at, rate in history.all():
            tape.add_settlement((exchange, symbol), at, Decimal(str(rate)))
    return signals, tape.freeze()
