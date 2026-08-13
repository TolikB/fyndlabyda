"""Event-driven backtest runner."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from .events import BacktestEvent, FillEvent, FundingEvent, OpportunityEvent, PositionEvent
from .metrics import BacktestMetrics, calculate_metrics
from .replay import EventReplay, config_hash


class BacktestResult:
    def __init__(
        self,
        metrics: BacktestMetrics,
        config_digest: str,
        dataset_version: str,
        git_commit: str | None,
    ) -> None:
        self.metrics = metrics
        self.config_hash = config_digest
        self.dataset_version = dataset_version
        self.git_commit = git_commit


class BacktestEngine:
    def run(
        self,
        events: list[BacktestEvent],
        initial_capital: Decimal,
        config: object,
        dataset_version: str,
        git_commit: str | None = None,
    ) -> BacktestResult:
        monthly: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        fees = Decimal("0")
        funding = Decimal("0")
        opportunities = 0
        slippage = Decimal("0")
        pnl_curve: list[Decimal] = []
        opened_at: dict[str, datetime] = {}
        durations: list[Decimal] = []
        pending_entry_notional: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        locked_by_position: dict[str, Decimal] = {}
        locked_notional = Decimal("0")
        capital_time_seconds = Decimal("0")
        first_timestamp: datetime | None = None
        last_timestamp: datetime | None = None

        def handle(event: BacktestEvent) -> None:
            nonlocal fees, funding, opportunities, slippage
            nonlocal locked_notional, capital_time_seconds
            nonlocal first_timestamp, last_timestamp
            if first_timestamp is None:
                first_timestamp = event.timestamp
            if last_timestamp is not None:
                elapsed = Decimal(
                    str((event.timestamp - last_timestamp).total_seconds())
                )
                capital_time_seconds += locked_notional * max(Decimal("0"), elapsed)
            last_timestamp = event.timestamp
            month = event.timestamp.strftime("%Y-%m")
            if isinstance(event, FundingEvent):
                funding_pnl = event.pnl if event.pnl is not None else event.rate * event.notional
                monthly[month] += funding_pnl
                funding += funding_pnl
                pnl_curve.append(funding_pnl)
            elif isinstance(event, FillEvent):
                if event.position_id not in opened_at:
                    pending_entry_notional[event.position_id] += event.notional
                event_pnl = -event.fee
                fees += event.fee
                execution_cost = event.spread + event.slippage
                event_pnl -= execution_cost
                monthly[month] += event_pnl
                slippage += execution_cost
                pnl_curve.append(event_pnl)
            elif isinstance(event, OpportunityEvent):
                opportunities += 1
            elif isinstance(event, PositionEvent):
                monthly[month] += event.pnl
                pnl_curve.append(event.pnl)
                if event.state.upper() == "OPEN":
                    opened_at[event.position_id] = event.timestamp
                    position_notional = pending_entry_notional.pop(
                        event.position_id, Decimal("0")
                    )
                    locked_by_position[event.position_id] = position_notional
                    locked_notional += position_notional
                elif event.state.upper() == "CLOSED" and event.position_id in opened_at:
                    start = opened_at.pop(event.position_id)
                    locked_notional -= locked_by_position.pop(
                        event.position_id, Decimal("0")
                    )
                    durations.append(
                        Decimal(str((event.timestamp - start).total_seconds())) / Decimal("3600")
                    )

        EventReplay(events).run(handle)
        observation_seconds = (
            Decimal(str((last_timestamp - first_timestamp).total_seconds()))
            if first_timestamp is not None
            and last_timestamp is not None
            and last_timestamp > first_timestamp
            else Decimal("0")
        )
        curve = list(monthly.items())
        metrics = calculate_metrics(
            curve,
            initial_capital,
            fees=fees,
            slippage=slippage,
            funding_income=funding,
            opportunities=opportunities,
            average_position_duration_hours=(
                sum(durations, Decimal("0")) / Decimal(len(durations))
                if durations
                else Decimal("0")
            ),
            capital_utilization=(
                capital_time_seconds / (initial_capital * observation_seconds)
                if observation_seconds > 0
                else Decimal("0")
            ),
            pnl_curve=pnl_curve,
        )
        return BacktestResult(metrics, config_hash(config), dataset_version, git_commit)
