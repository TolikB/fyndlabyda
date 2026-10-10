"""Paper series definitions (candidate, baseline, ...) and their identity."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from funding_arbitrage.opportunity.filters import OpportunityFilterConfig
from funding_arbitrage.opportunity.models import StrategyName
from funding_arbitrage.services.preflight import EXPECTED_MARKETS

if TYPE_CHECKING:
    from funding_arbitrage.config import Settings

TRADABLE_STRATEGIES = frozenset({StrategyName.SPOT_PERP, StrategyName.CROSS_EXCHANGE_FUNDING})


class EntryRules(BaseModel):
    # Funding per 8 hours in the position's favour (0.0002 = 0.02%).
    min_funding_rate_8h: Decimal = Field(default=Decimal("0"), ge=0)
    min_net_apr: Decimal = Decimal("0.10")
    min_liquidity_score: Decimal = Field(default=Decimal("70"), ge=0, le=100)
    # Percent units, round trip on the per-leg notional.
    max_slippage_percent: Decimal = Field(default=Decimal("0.15"), ge=0)
    max_spread_percent: Decimal = Field(default=Decimal("0.20"), ge=0)
    min_funding_samples: int = Field(default=20, ge=0)


class ExitRules(BaseModel):
    # Hold through at least one settlement before an edge-decay exit.
    min_hold_hours: Decimal = Field(default=Decimal("8"), ge=0)
    max_hold_hours: Decimal = Field(default=Decimal("72"), gt=0)
    exit_funding_rate_8h: Decimal = Decimal("0.00005")
    exit_confirmations: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def hold_window(self) -> ExitRules:
        if self.min_hold_hours > self.max_hold_hours:
            raise ValueError("min_hold_hours cannot exceed max_hold_hours")
        return self


# The blocks below are optional. A series without them behaves exactly like a series
# configured before they existed, and its identity (config hash) is unchanged.


class EdgeForecast(BaseModel):
    """Funding edge expected over the holding horizon, instead of the spot rate.

    The current rate of a funding spike says little about the next day; the edge
    the pair actually settled over the lookback says more. The forecast blends
    both (per 8 hours), and an entry must cover its full round-trip cost with it.
    """

    current_weight: Decimal = Field(default=Decimal("0.1"), ge=0, le=1)
    lookback_hours: Decimal = Field(default=Decimal("24"), gt=0)
    # Minimum settlements per perpetual leg inside the lookback window.
    min_history_points: int = Field(default=3, ge=1)
    horizon_hours: Decimal = Field(default=Decimal("48"), gt=0)
    # Forecast funding over the horizon minus round-trip costs, per unit of notional.
    min_expected_net: Decimal = Decimal("0")


class EntryGapRule(BaseModel):
    """Price the gap between the legs' entry fills into the entry decision.

    The scanner ignores what the two venues' prices are doing. In 234 closed paper
    positions (live and v2 stacks, 2026-10-03..10) the gap between the entry fills
    carried into the trade's price result (correlation 0.45), and the funding that
    actually settled was a flat 0.11-0.15% per trade whatever rate the entry saw.
    An entry has to pay its round-trip fees and its entry gap from the funding it is
    likely to receive; in that replay this halved the loss in both halves of the data.
    """

    # Share of the promised funding that settled on average (0.23 in the replay).
    funding_realisation: Decimal = Field(default=Decimal("0.23"), gt=0, le=1)
    horizon_hours: Decimal = Field(default=Decimal("24"), gt=0)
    gap_weight: Decimal = Field(default=Decimal("1"), ge=0)
    # Per unit of notional, like ``EdgeForecast.min_expected_net``.
    min_expected_value: Decimal = Decimal("0")


class SelectionRules(BaseModel):
    """Entry refinements on top of ``entry``; evaluated on the confirmed opportunity."""

    min_stability_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    min_persistence_score: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    # Longest/shortest funding interval of the two legs (1h against 8h is 8).
    max_interval_ratio: Decimal | None = Field(default=None, ge=1)
    # No new position in an asset this soon after a position in it closed.
    reentry_cooldown_hours: Decimal = Field(default=Decimal("0"), ge=0)
    forecast: EdgeForecast | None = None
    # "expected_net" needs a forecast; "score" keeps the scanner's ranking.
    rank_by: Literal["score", "expected_net"] = "score"
    entry_gap: EntryGapRule | None = None

    @model_validator(mode="after")
    def ranking_needs_forecast(self) -> SelectionRules:
        if self.rank_by == "expected_net" and self.forecast is None:
            raise ValueError("rank_by 'expected_net' requires a forecast")
        return self


class HoldingRules(BaseModel):
    """Exit refinements on top of ``exit``."""

    # The edge must stay below ``exit.exit_funding_rate_8h`` this long before an
    # edge-decay exit (time, not cycles, so the loop interval does not matter).
    exit_confirmation_minutes: Decimal = Field(gt=0)


class ExecutionRules(BaseModel):
    """Post one leg as a passive (maker) order and hedge the other when it fills.

    A resting order counts as filled only once a later book trades through its
    price, which ignores fills at the touch and so understates the fill rate.
    """

    maker_entry: bool = False
    maker_exit: bool = False
    maker_timeout_seconds: int = Field(default=180, ge=1)
    # An unfilled exit always falls back to taker fills; an entry only if this is set.
    entry_fallback_taker: bool = False

    @model_validator(mode="after")
    def something_enabled(self) -> ExecutionRules:
        if not (self.maker_entry or self.maker_exit):
            raise ValueError("execution needs maker_entry or maker_exit")
        return self


_OPTIONAL_BLOCKS = ("selection", "holding", "execution")


class SeriesConfig(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    # Durable identity; change it to start a fresh, separately accounted series.
    label: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$")
    initial_balance_usdt: Decimal = Field(gt=0)
    # Exposure is the hedged size: per-leg notional at entry.
    position_notional_usdt: Decimal = Field(gt=0)
    min_position_notional_usdt: Decimal | None = Field(default=None, gt=0)
    max_total_notional_usdt: Decimal = Field(gt=0)
    max_open_positions: int = Field(default=2, ge=1)
    max_positions_per_asset: int = Field(default=1, ge=1)
    # Derivative legs post notional / leverage as collateral; spot always pays in full.
    perp_leverage: Decimal = Field(default=Decimal("1"), ge=1, le=5)
    strategies: list[StrategyName] = Field(
        default_factory=lambda: [StrategyName.SPOT_PERP, StrategyName.CROSS_EXCHANGE_FUNDING]
    )
    entry: EntryRules = Field(default_factory=EntryRules)
    exit: ExitRules = Field(default_factory=ExitRules)
    selection: SelectionRules | None = None
    holding: HoldingRules | None = None
    execution: ExecutionRules | None = None

    @field_validator("strategies")
    @classmethod
    def only_funding_strategies(cls, value: list[StrategyName]) -> list[StrategyName]:
        unsupported = set(value) - TRADABLE_STRATEGIES
        if unsupported:
            raise ValueError(f"paper series cannot trade {sorted(unsupported)}")
        if not value:
            raise ValueError("at least one strategy is required")
        return value

    @model_validator(mode="after")
    def sizing(self) -> SeriesConfig:
        if self.position_notional_usdt > self.max_total_notional_usdt:
            raise ValueError("position_notional_usdt exceeds max_total_notional_usdt")
        # Worst case is spot/perp: full spot notional plus the perp margin.
        collateral_per_notional = Decimal("1") + Decimal("1") / self.perp_leverage
        if self.max_total_notional_usdt * collateral_per_notional > self.initial_balance_usdt:
            raise ValueError("max_total_notional_usdt cannot be funded by the initial balance")
        return self

    @property
    def minimum_notional(self) -> Decimal:
        return self.min_position_notional_usdt or self.position_notional_usdt / Decimal("2")

    def identity(self, simulator_version: str, context: dict[str, Any]) -> dict[str, Any]:
        """Everything that shapes this series' simulated results."""

        series = self.model_dump(mode="json")
        for block in _OPTIONAL_BLOCKS:
            # Absent blocks stay out, so older series keep their recorded hash.
            if series.get(block) is None:
                series.pop(block, None)
        selection = series.get("selection")
        if selection is not None and selection.get("entry_gap") is None:
            # Added after the round-2 series started; absent, it stays out too.
            selection.pop("entry_gap", None)
        return {
            "simulator_version": simulator_version,
            "series": series,
            "context": context,
        }

    def config_hash(self, simulator_version: str, context: dict[str, Any] | None = None) -> str:
        payload = json.dumps(
            self.identity(simulator_version, context or {}), sort_keys=True, default=str
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def filter_config(self, confirmation_seconds: int) -> OpportunityFilterConfig:
        return OpportunityFilterConfig(
            minimum_net_apr=self.entry.min_net_apr,
            minimum_liquidity_score=self.entry.min_liquidity_score,
            maximum_slippage_percent=self.entry.max_slippage_percent,
            maximum_spread_percent=self.entry.max_spread_percent,
            minimum_funding_samples=self.entry.min_funding_samples,
            minimum_opportunity_duration_seconds=confirmation_seconds,
            minimum_funding_rate_8h=self.entry.min_funding_rate_8h,
        )


class PaperSeriesFile(BaseModel):
    primary_series: str
    series: list[SeriesConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_series(self) -> PaperSeriesFile:
        names = [item.name for item in self.series]
        labels = [item.label for item in self.series]
        if len(set(names)) != len(names) or len(set(labels)) != len(labels):
            raise ValueError("series names and labels must be unique")
        if self.primary_series not in names:
            raise ValueError("primary_series must name one of the configured series")
        if "legacy" in labels:
            raise ValueError("the label 'legacy' is reserved for pre-series data")
        return self

    @property
    def primary(self) -> SeriesConfig:
        return next(item for item in self.series if item.name == self.primary_series)

    def loosest_filter(self, confirmation_seconds: int) -> OpportunityFilterConfig:
        """Scanner filter that admits everything any series could trade."""

        entries = [item.entry for item in self.series]
        return OpportunityFilterConfig(
            minimum_net_apr=min(entry.min_net_apr for entry in entries),
            minimum_liquidity_score=min(entry.min_liquidity_score for entry in entries),
            maximum_slippage_percent=max(entry.max_slippage_percent for entry in entries),
            maximum_spread_percent=max(entry.max_spread_percent for entry in entries),
            minimum_funding_samples=min(entry.min_funding_samples for entry in entries),
            minimum_opportunity_duration_seconds=confirmation_seconds,
            minimum_funding_rate_8h=min(entry.min_funding_rate_8h for entry in entries),
        )


def load_series_file(path: str | Path) -> PaperSeriesFile:
    raw: Any = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return PaperSeriesFile.model_validate(raw)


def simulation_context(settings: Settings) -> dict[str, Any]:
    """Global settings that change simulated results; part of every series identity.

    Fees, venues, confirmation, and fill rules affect PnL as much as the series'
    own thresholds, so changing any of them requires a new series label.
    """

    return {
        "market_data_mode": settings.market_data_mode,
        "enabled_venues": sorted(settings.enabled_venue_values),
        "expected_markets": {
            venue: sorted(market.value for market in EXPECTED_MARKETS[venue])
            for venue in settings.enabled_venue_values
        },
        "loop_interval_seconds": settings.paper_loop_interval_seconds,
        "funding_grace_seconds": settings.paper_funding_grace_seconds,
        "funding_poll_seconds": settings.paper_funding_poll_seconds,
        "book_candidates_per_cycle": settings.paper_book_candidates_per_cycle,
        "history_requests_per_cycle": settings.paper_history_requests_per_cycle,
        "okx_funding_symbol_limit": settings.okx_funding_symbol_limit,
        "history_ttl_seconds": settings.market_history_ttl_seconds,
        "instrument_refresh_seconds": settings.market_instrument_refresh_seconds,
        "mock_funding_interval_seconds": settings.mock_funding_interval_seconds,
        "fees": {
            venue: schedule.model_dump(mode="json")
            for venue, schedule in sorted(settings.fee_schedules.items())
        },
        "confirmation_seconds": settings.paper_confirmation_seconds,
        "expected_holding_hours": str(settings.scanner_expected_holding_hours),
        "book_depth": settings.paper_book_depth,
        "max_book_age_seconds": settings.paper_max_book_age_seconds,
        "max_fill_slippage_percent": str(settings.paper_max_fill_slippage_percent),
        "market_data_stale_seconds": settings.market_data_stale_seconds,
        "funding_stale_seconds": settings.market_funding_stale_seconds,
        "allow_short_spot": settings.scanner_allow_short_spot,
        "max_cross_price_deviation": str(settings.scanner_max_cross_price_deviation),
        "max_basis": str(settings.scanner_max_basis),
        "equivalent_quotes": sorted(settings.equivalent_quote_values),
    }


def changed_keys(old: Any, new: Any, prefix: str = "") -> list[str]:
    """Dotted paths whose values differ between two identity documents."""

    if isinstance(old, dict) and isinstance(new, dict):
        changes: list[str] = []
        for key in sorted(set(old) | set(new)):
            changes.extend(changed_keys(old.get(key), new.get(key), f"{prefix}{key}."))
        return changes
    return [] if old == new else [prefix.rstrip(".") or "<root>"]
