"""Hard live-capital limits and a persistent operator kill switch."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, tzinfo
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from funding_arbitrage.config import Settings

logger = logging.getLogger(__name__)


class LiveTradingPaused(RuntimeError):
    """Raised before submission whenever a live safety interlock is active."""


@dataclass
class LiveRiskState:
    starting_equity: Decimal | None = None
    high_water_equity: Decimal | None = None
    day_start_equity: Decimal | None = None
    equity_day: date | None = None
    current_equity: Decimal | None = None
    paused_reason: str | None = None


class LiveRiskController:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.state = LiveRiskState()
        self.kill_switch_path = Path(settings.live_kill_switch_file)
        try:
            self.timezone: tzinfo = ZoneInfo(settings.telegram_timezone)
        except ZoneInfoNotFoundError:
            self.timezone = UTC

    @property
    def paused(self) -> bool:
        return self.kill_switch_path.exists() or self.state.paused_reason is not None

    @property
    def paused_reason(self) -> str | None:
        if self.state.paused_reason:
            return self.state.paused_reason
        if self.kill_switch_path.exists():
            return "operator_kill_switch"
        return None

    def trip(self, reason: str) -> None:
        if self.paused:
            return
        self.state.paused_reason = reason
        try:
            self.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
            self.kill_switch_path.write_text(reason + "\n", encoding="utf-8")
        except OSError:
            self.state.paused_reason = reason + ";kill_switch_persistence_failed"
            logger.exception("live_kill_switch_persistence_failed")

    def verify_interlock_storage(self) -> None:
        self.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
        probe = self.kill_switch_path.parent / f".live-interlock-{uuid4().hex}"
        try:
            probe.write_text("probe\n", encoding="utf-8")
        finally:
            probe.unlink(missing_ok=True)

    def restore_baselines(
        self,
        *,
        starting_equity: Decimal,
        high_water_equity: Decimal,
        day_start_equity: Decimal,
        equity_day: date,
    ) -> None:
        self.state.starting_equity = starting_equity
        self.state.high_water_equity = high_water_equity
        self.state.day_start_equity = day_start_equity
        self.state.equity_day = equity_day

    def update_equity(self, equity: Decimal, now: datetime | None = None) -> None:
        observed_at = now or datetime.now(UTC)
        observed_day = observed_at.astimezone(self.timezone).date()
        if self.state.starting_equity is None:
            self.state.starting_equity = equity
            self.state.high_water_equity = equity
        if self.state.equity_day != observed_day:
            self.state.equity_day = observed_day
            self.state.day_start_equity = equity
        self.state.current_equity = equity
        self.state.high_water_equity = max(self.state.high_water_equity or equity, equity)
        if self.state.day_start_equity is not None:
            daily_loss = self.state.day_start_equity - equity
            if daily_loss >= self.settings.live_max_daily_loss_usd:
                self.trip("daily_loss_limit")
        if self.state.high_water_equity and self.state.high_water_equity > 0:
            drawdown = (self.state.high_water_equity - equity) / self.state.high_water_equity
            if drawdown >= self.settings.live_max_drawdown_percent:
                self.trip("drawdown_limit")

    def assert_can_open(
        self,
        *,
        order_notional: Decimal,
        open_notional: Decimal,
        free_collateral: Decimal,
    ) -> None:
        if self.paused:
            raise LiveTradingPaused(self.paused_reason or "live_trading_paused")
        if not self.settings.live_armed or not self.settings.live_autotrade:
            raise LiveTradingPaused("live_autotrade_not_armed")
        if order_notional > self.settings.live_max_order_notional_usd:
            raise LiveTradingPaused("single_order_notional_limit")
        if open_notional + order_notional > self.settings.live_max_total_notional_usd:
            raise LiveTradingPaused("total_notional_limit")
        reserve = free_collateral * self.settings.live_min_free_balance_percent / Decimal("100")
        if free_collateral - order_notional < reserve:
            raise LiveTradingPaused("free_collateral_reserve")

    def assert_can_reduce(self, *, order_notional: Decimal) -> None:
        """Allow risk-reducing orders even after the entry kill switch has tripped."""

        if not self.settings.live_armed:
            raise LiveTradingPaused("live_trading_not_armed")
        if order_notional <= 0:
            raise LiveTradingPaused("invalid_reduce_order_notional")
