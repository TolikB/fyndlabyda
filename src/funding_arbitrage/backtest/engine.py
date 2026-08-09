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
        opened_at: dict[str, datetime] = {}
        durations: list[Decimal] = []

        def handle(event: BacktestEvent) -> None:
            nonlocal fees, funding, opportunities, slippage
            month = event.timestamp.strftime("%Y-%m")
            if isinstance(event, FundingEvent):
                monthly[month] += event.rate * event.notional
                funding += event.rate * event.notional
            elif isinstance(event, FillEvent):
                monthly[month] -= event.fee
                fees += event.fee
                monthly[month] -= event.slippage
                slippage += event.slippage
            elif isinstance(event, OpportunityEvent):
                opportunities += 1
            elif isinstance(event, PositionEvent):
                monthly[month] += event.pnl
                if event.state.upper() == "OPEN":
                    opened_at[event.position_id] = event.timestamp
                elif event.state.upper() == "CLOSED" and event.position_id in opened_at:
                    start = opened_at.pop(event.position_id)
                    durations.append(
                        Decimal(str((event.timestamp - start).total_seconds())) / Decimal("3600")
                    )

        EventReplay(events).run(handle)
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
        )
        return BacktestResult(metrics, config_hash(config), dataset_version, git_commit)
