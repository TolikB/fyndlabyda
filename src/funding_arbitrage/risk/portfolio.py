"""Sole portfolio risk authority, hierarchy sizing, and scoped kill switches."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.decisions import RiskDecision, SignalIntent
from funding_arbitrage.risk.margin import PortfolioMarginAssessment

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class PortfolioRiskLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_order_notional_usd: Decimal = Field(default=Decimal("5000"), gt=0)
    maximum_position_fraction: Decimal = Field(default=Decimal("0.20"), gt=0, le=1)
    maximum_asset_fraction: Decimal = Field(default=Decimal("0.30"), gt=0, le=1)
    maximum_strategy_fraction: Decimal = Field(default=Decimal("0.40"), gt=0, le=1)
    maximum_venue_fraction: Decimal = Field(default=Decimal("0.40"), gt=0, le=1)
    maximum_correlation_group_fraction: Decimal = Field(
        default=Decimal("0.40"), gt=0, le=1
    )
    maximum_portfolio_gross_fraction: Decimal = Field(
        default=Decimal("2"), gt=0
    )
    maximum_portfolio_delta_fraction: Decimal = Field(
        default=Decimal("0.15"), gt=0, le=1
    )
    minimum_cash_reserve_fraction: Decimal = Field(
        default=Decimal("0.20"), ge=0, lt=1
    )
    risk_per_trade_fraction: Decimal = Field(default=Decimal("0.01"), gt=0, le=1)
    maximum_liquidity_participation: Decimal = Field(
        default=Decimal("0.10"), gt=0, le=1
    )
    target_volatility_bps: Decimal = Field(default=Decimal("50"), gt=0)
    minimum_approved_notional_usd: Decimal = Field(default=Decimal("25"), gt=0)
    maximum_slippage_bps: Decimal = Field(default=Decimal("20"), ge=0)
    maximum_execution_seconds: int = Field(default=10, gt=0)


class RiskAuthorizationContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent
    timestamp: datetime
    requested_notional_usd: Decimal = Field(gt=0)
    reference_price: Decimal = Field(gt=0)
    quantity_step: Decimal = Field(gt=0)
    stop_distance_bps: Decimal = Field(gt=0)
    expected_slippage_bps: Decimal = Field(ge=0)
    volatility_bps: Decimal = Field(gt=0)
    available_liquidity_usd: Decimal = Field(gt=0)
    incremental_margin_rate: Decimal = Field(gt=0, le=1)
    delta_per_primary_notional: Decimal
    correlation_multiplier: Decimal = Field(gt=0, le=1)
    drawdown_multiplier: Decimal = Field(gt=0, le=1)
    regime_multiplier: Decimal = Field(gt=0, le=1)
    decision_support_multiplier: Decimal = Field(default=ONE, gt=0, le=1)
    equity_usd: Decimal = Field(gt=0)
    cash_usd: Decimal = Field(ge=0)
    portfolio_gross_notional_usd: Decimal = Field(ge=0)
    portfolio_net_delta_usd: Decimal
    position_exposure_usd: Decimal = Field(default=ZERO, ge=0)
    asset_exposures_usd: dict[str, Decimal] = Field(default_factory=dict)
    strategy_exposures_usd: dict[str, Decimal] = Field(default_factory=dict)
    venue_exposures_usd: dict[str, Decimal] = Field(default_factory=dict)
    correlation_exposures_usd: dict[str, Decimal] = Field(default_factory=dict)
    correlation_group: str = Field(min_length=1)
    margin: PortfolioMarginAssessment
    data_fresh: bool
    reconciliation_healthy: bool
    operator_entries_enabled: bool

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class RiskHierarchyCaps(BaseModel):
    model_config = ConfigDict(frozen=True)

    caps_usd: dict[str, Decimal]
    pre_multiplier_notional_usd: Decimal = Field(ge=0)
    combined_multiplier: Decimal = Field(ge=0, le=1)
    sized_notional_usd: Decimal = Field(ge=0)
    binding_constraints: tuple[str, ...]


class PortfolioRiskAuthorization(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision: RiskDecision
    hierarchy: RiskHierarchyCaps
    rejection_reasons: tuple[str, ...] = ()


class PortfolioRiskAuthority:
    """The only component that converts a SignalIntent into executable quantity."""

    def __init__(
        self,
        limits: PortfolioRiskLimits | None = None,
        interlocks: RiskInterlockRegistry | None = None,
    ) -> None:
        self.limits = limits or PortfolioRiskLimits()
        self.interlocks = interlocks or RiskInterlockRegistry()

    def authorize(self, context: RiskAuthorizationContext) -> PortfolioRiskAuthorization:
        reasons = self._hard_rejections(context)
        ratios_by_venue: dict[str, Decimal] = {}
        for leg in context.intent.legs:
            venue = leg.instrument.venue
            ratios_by_venue[venue] = ratios_by_venue.get(venue, ZERO) + leg.hedge_ratio
        gross_ratio = sum(ratios_by_venue.values(), ZERO)
        equity = context.equity_usd
        limits = self.limits
        caps: dict[str, Decimal] = {
            "requested": context.requested_notional_usd,
            "order": limits.maximum_order_notional_usd,
            "position": max(
                ZERO,
                equity * limits.maximum_position_fraction
                - context.position_exposure_usd,
            ),
            "asset": max(
                ZERO,
                (
                    equity * limits.maximum_asset_fraction
                    - context.asset_exposures_usd.get(
                        context.intent.primary_instrument.base_asset,
                        ZERO,
                    )
                )
                / gross_ratio,
            ),
            "strategy": max(
                ZERO,
                (
                    equity * limits.maximum_strategy_fraction
                    - context.strategy_exposures_usd.get(
                        context.intent.strategy_id,
                        ZERO,
                    )
                )
                / gross_ratio,
            ),
            "correlation": max(
                ZERO,
                (
                    equity * limits.maximum_correlation_group_fraction
                    - context.correlation_exposures_usd.get(
                        context.correlation_group,
                        ZERO,
                    )
                )
                / gross_ratio,
            ),
            "portfolio_gross": max(
                ZERO,
                (
                    equity * limits.maximum_portfolio_gross_fraction
                    - context.portfolio_gross_notional_usd
                )
                / gross_ratio,
            ),
            "cash_reserve": max(
                ZERO,
                (
                    context.cash_usd
                    - equity * limits.minimum_cash_reserve_fraction
                )
                / (context.incremental_margin_rate * gross_ratio),
            ),
            "stop_risk": (
                equity
                * limits.risk_per_trade_fraction
                / (context.stop_distance_bps / BPS * gross_ratio)
            ),
            "liquidity": (
                context.available_liquidity_usd
                * limits.maximum_liquidity_participation
            ),
            "volatility": (
                context.requested_notional_usd
                * min(ONE, limits.target_volatility_bps / context.volatility_bps)
            ),
            "margin": max(
                ZERO,
                context.margin.total_available_initial_margin_usd
                / (context.incremental_margin_rate * gross_ratio),
            ),
        }
        for venue, ratio in ratios_by_venue.items():
            caps[f"venue:{venue}"] = max(
                ZERO,
                (
                    equity * limits.maximum_venue_fraction
                    - context.venue_exposures_usd.get(venue, ZERO)
                )
                / ratio,
            )
        if context.delta_per_primary_notional != 0:
            remaining_delta = max(
                ZERO,
                equity * limits.maximum_portfolio_delta_fraction
                - abs(context.portfolio_net_delta_usd),
            )
            caps["portfolio_delta"] = remaining_delta / abs(
                context.delta_per_primary_notional
            )
        else:
            caps["portfolio_delta"] = context.requested_notional_usd
        pre_multiplier = min(caps.values())
        combined_multiplier = (
            context.correlation_multiplier
            * context.drawdown_multiplier
            * context.regime_multiplier
            * context.decision_support_multiplier
        )
        unrounded = max(ZERO, pre_multiplier * combined_multiplier)
        quantity = (
            (unrounded / context.reference_price / context.quantity_step)
            .to_integral_value(rounding=ROUND_FLOOR)
            * context.quantity_step
        )
        sized_notional = quantity * context.reference_price
        minimum_cap = min(caps.values())
        binding = tuple(
            sorted(name for name, cap in caps.items() if cap == minimum_cap)
        )
        hierarchy = RiskHierarchyCaps(
            caps_usd=caps,
            pre_multiplier_notional_usd=pre_multiplier,
            combined_multiplier=combined_multiplier,
            sized_notional_usd=sized_notional,
            binding_constraints=binding,
        )
        if sized_notional < limits.minimum_approved_notional_usd:
            reasons.append("approved_size_below_minimum")
        if reasons:
            return PortfolioRiskAuthorization(
                decision=self._rejected(context, reasons),
                hierarchy=hierarchy,
                rejection_reasons=tuple(sorted(set(reasons))),
            )
        risk_usd = sized_notional * gross_ratio * context.stop_distance_bps / BPS
        decision_id = _decision_id(context, sized_notional, "approved")
        decision = RiskDecision(
            signal_id=context.intent.signal_id,
            decision_id=decision_id,
            decided_at=context.timestamp,
            approved=True,
            approved_risk_usdt=risk_usd,
            approved_quantity=quantity,
            approved_notional=sized_notional,
            max_slippage_bps=limits.maximum_slippage_bps,
            max_execution_seconds=limits.maximum_execution_seconds,
            correlation_multiplier=context.correlation_multiplier,
            drawdown_multiplier=context.drawdown_multiplier,
            regime_multiplier=context.regime_multiplier,
            decision_support_multiplier=context.decision_support_multiplier,
        )
        return PortfolioRiskAuthorization(decision=decision, hierarchy=hierarchy)

    def _hard_rejections(self, context: RiskAuthorizationContext) -> list[str]:
        reasons = list(self.interlocks.blocking_reasons(context.intent))
        if context.intent.expires_at <= context.timestamp:
            reasons.append("signal_expired_at_risk")
        if not context.data_fresh:
            reasons.append("risk_data_stale")
        if not context.reconciliation_healthy:
            reasons.append("risk_reconciliation_unhealthy")
        if not context.operator_entries_enabled:
            reasons.append("operator_entries_disabled")
        if not context.margin.approved:
            reasons.append("portfolio_margin_rejected")
        if context.expected_slippage_bps > self.limits.maximum_slippage_bps:
            reasons.append("slippage_limit")
        return reasons

    def _rejected(
        self,
        context: RiskAuthorizationContext,
        reasons: list[str],
    ) -> RiskDecision:
        normalized = tuple(sorted(set(reasons)))
        return RiskDecision(
            signal_id=context.intent.signal_id,
            decision_id=_decision_id(context, ZERO, ";".join(normalized)),
            decided_at=context.timestamp,
            approved=False,
            rejection_reason=";".join(normalized),
            approved_risk_usdt=ZERO,
            approved_quantity=ZERO,
            approved_notional=ZERO,
            max_slippage_bps=self.limits.maximum_slippage_bps,
            max_execution_seconds=self.limits.maximum_execution_seconds,
            correlation_multiplier=context.correlation_multiplier,
            drawdown_multiplier=context.drawdown_multiplier,
            regime_multiplier=context.regime_multiplier,
            decision_support_multiplier=context.decision_support_multiplier,
        )


class RiskInterlockScope(StrEnum):
    GLOBAL = "GLOBAL"
    STRATEGY = "STRATEGY"
    VENUE = "VENUE"


class RiskInterlockReason(StrEnum):
    OPERATOR = "OPERATOR"
    STALE_DATA = "STALE_DATA"
    STRATEGY_LOSS = "STRATEGY_LOSS"
    VENUE_HEALTH = "VENUE_HEALTH"
    PORTFOLIO_DRAWDOWN = "PORTFOLIO_DRAWDOWN"
    DAILY_LOSS = "DAILY_LOSS"
    RECONCILIATION = "RECONCILIATION"


class RiskInterlock(BaseModel):
    interlock_id: str
    scope: RiskInterlockScope
    scope_id: str
    reason: RiskInterlockReason
    details: str
    tripped_at: datetime
    active: bool = True
    cleared_at: datetime | None = None
    cleared_by: str | None = None
    approved_by: str | None = None

    @field_validator("tripped_at", "cleared_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None


class RiskHealthSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    stale_venues: tuple[str, ...] = ()
    unhealthy_venues: tuple[str, ...] = ()
    strategy_losses_usd: dict[str, Decimal] = Field(default_factory=dict)
    portfolio_drawdown_fraction: Decimal = Field(ge=0, le=1)
    daily_loss_usd: Decimal = Field(ge=0)
    reconciliation_healthy: bool
    operator_halt: bool = False

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class RiskKillSwitchConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_strategy_loss_usd: Decimal = Field(default=Decimal("250"), gt=0)
    maximum_daily_loss_usd: Decimal = Field(default=Decimal("500"), gt=0)
    maximum_portfolio_drawdown_fraction: Decimal = Field(
        default=Decimal("0.10"), gt=0, le=1
    )


class RiskInterlockRegistry:
    def __init__(self, config: RiskKillSwitchConfig | None = None) -> None:
        self.config = config or RiskKillSwitchConfig()
        self.interlocks: dict[str, RiskInterlock] = {}

    def trip(
        self,
        scope: RiskInterlockScope,
        scope_id: str,
        reason: RiskInterlockReason,
        details: str,
        timestamp: datetime,
    ) -> RiskInterlock:
        normalized_scope = scope_id.strip().upper()
        key = f"{scope.value}:{normalized_scope}:{reason.value}"
        existing = self.interlocks.get(key)
        if existing is not None and existing.active:
            return existing
        interlock = RiskInterlock(
            interlock_id="risklock_" + hashlib.sha256(
                f"{key}|{_utc(timestamp).isoformat()}".encode()
            ).hexdigest()[:32],
            scope=scope,
            scope_id=normalized_scope,
            reason=reason,
            details=details,
            tripped_at=timestamp,
        )
        self.interlocks[key] = interlock
        return interlock

    def evaluate_health(self, health: RiskHealthSnapshot) -> tuple[RiskInterlock, ...]:
        tripped: list[RiskInterlock] = []
        if health.operator_halt:
            tripped.append(
                self.trip(
                    RiskInterlockScope.GLOBAL,
                    "PORTFOLIO",
                    RiskInterlockReason.OPERATOR,
                    "operator halt",
                    health.timestamp,
                )
            )
        if not health.reconciliation_healthy:
            tripped.append(
                self.trip(
                    RiskInterlockScope.GLOBAL,
                    "PORTFOLIO",
                    RiskInterlockReason.RECONCILIATION,
                    "reconciliation failed",
                    health.timestamp,
                )
            )
        if health.portfolio_drawdown_fraction >= (
            self.config.maximum_portfolio_drawdown_fraction
        ):
            tripped.append(
                self.trip(
                    RiskInterlockScope.GLOBAL,
                    "PORTFOLIO",
                    RiskInterlockReason.PORTFOLIO_DRAWDOWN,
                    str(health.portfolio_drawdown_fraction),
                    health.timestamp,
                )
            )
        if health.daily_loss_usd >= self.config.maximum_daily_loss_usd:
            tripped.append(
                self.trip(
                    RiskInterlockScope.GLOBAL,
                    "PORTFOLIO",
                    RiskInterlockReason.DAILY_LOSS,
                    str(health.daily_loss_usd),
                    health.timestamp,
                )
            )
        for venue in sorted(set(health.stale_venues)):
            tripped.append(
                self.trip(
                    RiskInterlockScope.VENUE,
                    venue,
                    RiskInterlockReason.STALE_DATA,
                    "venue data stale",
                    health.timestamp,
                )
            )
        for venue in sorted(set(health.unhealthy_venues)):
            tripped.append(
                self.trip(
                    RiskInterlockScope.VENUE,
                    venue,
                    RiskInterlockReason.VENUE_HEALTH,
                    "venue unhealthy",
                    health.timestamp,
                )
            )
        for strategy, loss in sorted(health.strategy_losses_usd.items()):
            if loss >= self.config.maximum_strategy_loss_usd:
                tripped.append(
                    self.trip(
                        RiskInterlockScope.STRATEGY,
                        strategy,
                        RiskInterlockReason.STRATEGY_LOSS,
                        str(loss),
                        health.timestamp,
                    )
                )
        return tuple(tripped)

    def blocking_reasons(self, intent: SignalIntent) -> tuple[str, ...]:
        strategy = intent.strategy_id.upper()
        venues = {leg.instrument.venue.upper() for leg in intent.legs}
        reasons = []
        for interlock in self.interlocks.values():
            if not interlock.active:
                continue
            applies = (
                interlock.scope is RiskInterlockScope.GLOBAL
                or (
                    interlock.scope is RiskInterlockScope.STRATEGY
                    and interlock.scope_id == strategy
                )
                or (
                    interlock.scope is RiskInterlockScope.VENUE
                    and interlock.scope_id in venues
                )
            )
            if applies:
                reasons.append(
                    f"risk_interlock:{interlock.scope.value.lower()}:{interlock.reason.value.lower()}"
                )
        return tuple(sorted(set(reasons)))

    def clear(
        self,
        interlock_id: str,
        *,
        operator_id: str,
        approver_id: str,
        reconciliation_healthy: bool,
        timestamp: datetime,
    ) -> RiskInterlock:
        interlock = next(
            (
                item
                for item in self.interlocks.values()
                if item.interlock_id == interlock_id
            ),
            None,
        )
        if interlock is None:
            raise ValueError("unknown risk interlock")
        if not interlock.active:
            return interlock
        if not operator_id or not approver_id or operator_id == approver_id:
            raise ValueError("risk interlock clear requires two distinct approvers")
        if not reconciliation_healthy:
            raise ValueError("risk interlock clear requires healthy reconciliation")
        interlock.active = False
        interlock.cleared_at = _utc(timestamp)
        interlock.cleared_by = operator_id
        interlock.approved_by = approver_id
        return interlock


def _decision_id(
    context: RiskAuthorizationContext,
    size: Decimal,
    outcome: str,
) -> str:
    payload = (
        f"{context.intent.signal_id}|{context.timestamp.isoformat()}|"
        f"{size}|{outcome}"
    )
    return "risk_" + hashlib.sha256(payload.encode()).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
