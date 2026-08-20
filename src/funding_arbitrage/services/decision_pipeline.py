"""Side-effect boundary from declarative strategy intent to venue execution."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    ExecutionReport,
    LiveExecutionApproval,
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    InstrumentKey,
    OrderStatus,
    OrderType,
    Side,
)
from funding_arbitrage.domain.events import (
    InstrumentType as DomainInstrumentType,
)
from funding_arbitrage.domain.modes import ExecutionPath, mode_contract
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.oms import DurableOMS, OMSOrderSnapshot
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import Opportunity, SizeQuote
from funding_arbitrage.opportunity.settlement import target_settlements
from funding_arbitrage.risk.live import LiveRiskController


class SignalValidator(Protocol):
    def validate(self, intent: SignalIntent, now: datetime) -> None: ...


class RiskEvaluator(Protocol):
    def evaluate(self, intent: SignalIntent, now: datetime) -> RiskDecision: ...


class ExecutionPlanner(Protocol):
    def build(
        self, intent: SignalIntent, decision: RiskDecision, now: datetime
    ) -> ExecutionPlan: ...


class ExecutionAdapter(Protocol):
    path: ExecutionPath

    async def submit(self, order: OMSOrderSnapshot) -> ExecutionReport: ...


class CompensationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    completed: bool
    reason: str | None = None


class ExposureCompensator(Protocol):
    async def compensate(
        self,
        *,
        plan: ExecutionPlan,
        filled_orders: tuple[OMSOrderSnapshot, ...],
        reports: tuple[ExecutionReport, ...],
        now: datetime,
    ) -> CompensationResult: ...


class PipelineStatus(StrEnum):
    RISK_REJECTED = "RISK_REJECTED"
    PREPARED = "PREPARED"
    COMPLETED = "COMPLETED"
    COMPENSATED = "COMPENSATED"
    STOPPED = "STOPPED"
    AMBIGUOUS = "AMBIGUOUS"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"


class DecisionPipelineResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: PipelineStatus
    signal_id: str
    risk_decision: RiskDecision
    plan: ExecutionPlan | None = None
    orders: tuple[OMSOrderSnapshot, ...] = ()
    reports: tuple[ExecutionReport, ...] = ()
    reason: str | None = None


class StrictSignalValidator:
    """Reject temporal or evidence contracts before any sizing can occur."""

    def validate(self, intent: SignalIntent, now: datetime) -> None:
        current = _utc(now)
        if intent.created_at > current:
            raise ValueError("signal creation time is in the future")
        if current >= intent.expires_at:
            raise ValueError("signal intent has expired")
        if not intent.legs:
            raise ValueError("signal intent has no legs")
        if intent.primary_instrument != intent.legs[0].instrument:
            raise ValueError("primary instrument must be the first signal leg")


class FundingLiveDecisionService:
    """Turn a confirmed funding opportunity into bounded live authority."""

    def __init__(self, settings: Settings, risk: LiveRiskController) -> None:
        self.settings = settings
        self.risk = risk
        self.validator = StrictSignalValidator()

    def approve(
        self,
        opportunity: Opportunity,
        quote: SizeQuote,
        snapshot: MarketSnapshot,
        opportunity_key: str,
        *,
        now: datetime,
    ) -> LiveExecutionApproval:
        current = _utc(now)
        self.risk.assert_entry_enabled()
        if opportunity.status != "confirmed":
            raise ValueError("only confirmed opportunities may enter live decision pipeline")
        if opportunity.net_edge <= 0 or quote.net_profit <= 0:
            raise ValueError("live decision requires positive net edge after costs")
        if quote.capital <= 0 or not quote.fully_filled:
            raise ValueError("live decision requires a fully executable positive size")
        if opportunity.asset.upper() not in self.settings.live_allowed_asset_values:
            raise ValueError("asset is not live-allowlisted")
        strategy = str(opportunity.strategy).lower()
        if strategy not in self.settings.live_allowed_strategy_values:
            raise ValueError("strategy is not live-allowlisted")
        contract = mode_contract(self.settings.effective_trading_mode)
        if not contract.exchange_orders_enabled:
            raise ValueError("runtime mode does not authorize exchange orders")

        instruments = self._instruments(opportunity, snapshot)
        sides = (Side(opportunity.leg_a_side.upper()), Side(opportunity.leg_b_side.upper()))
        prices = (opportunity.price_a, opportunity.price_b)
        requested_quantity = min(
            quote.capital / prices[0],
            quote.capital / prices[1],
        )
        if requested_quantity <= 0:
            raise ValueError("risk-authorized quantity must be positive")

        execution_seconds = max(
            1,
            math.ceil(self.settings.live_order_timeout_seconds * len(instruments)),
        )
        horizon = current + timedelta(seconds=execution_seconds)
        if opportunity.expires_at is not None:
            expires_at = _utc(opportunity.expires_at)
            if expires_at <= current:
                raise ValueError("opportunity expired before live decision")
            expires_at = min(expires_at, horizon)
        else:
            expires_at = horizon
        signal_id = f"funding:{opportunity.id}"
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=strategy,
            mode=self.settings.effective_trading_mode,
            signal_type=SignalType.FUNDING_BASIS,
            primary_instrument=instruments[0],
            side=sides[0],
            legs=tuple(
                SignalLeg(instrument=instrument, side=side)
                for instrument, side in zip(instruments, sides, strict=True)
            ),
            regime=MarketRegime.UNKNOWN,
            quality_score=max(
                Decimal("0"),
                min(Decimal("100"), opportunity.opportunity_score),
            ),
            confidence=max(
                Decimal("0"),
                min(
                    Decimal("1"),
                    opportunity.funding_stability_score / Decimal("100"),
                ),
            ),
            expected_holding_seconds=max(
                1,
                math.ceil(opportunity.expected_holding_hours * Decimal("3600")),
            ),
            expected_move_bps=opportunity.net_edge * Decimal("10000"),
            estimated_cost_bps=(
                quote.costs.total
                / (quote.capital * Decimal("2"))
                * Decimal("10000")
            ),
            created_at=current,
            expires_at=expires_at,
            evidence={
                "opportunity_id": opportunity.id,
                "opportunity_key": opportunity_key,
                "snapshot_at": snapshot.captured_at.isoformat(),
            },
        )
        self.validator.validate(intent, current)

        decision_id = _stable_id("risk", signal_id, current.isoformat())
        notional = quote.capital * Decimal("2")
        estimated_risk = (
            quote.costs.total
            + notional * self.settings.live_max_slippage_percent
        )
        decision = RiskDecision(
            signal_id=signal_id,
            decision_id=decision_id,
            decided_at=current,
            approved=True,
            approved_risk_usdt=max(estimated_risk, Decimal("0.00000001")),
            approved_quantity=requested_quantity,
            approved_notional=notional,
            max_slippage_bps=(
                self.settings.live_max_slippage_percent * Decimal("10000")
            ),
            max_execution_seconds=execution_seconds,
            correlation_multiplier=Decimal("1"),
            drawdown_multiplier=Decimal("1"),
            regime_multiplier=Decimal("1"),
        )
        instructions = tuple(
            ExecutionInstruction(
                leg_index=index,
                instrument=instrument,
                side=side,
                order_type=OrderType.LIMIT,
                quantity=requested_quantity,
                limit_price=(
                    price * (Decimal("1") + self.settings.live_max_slippage_percent)
                    if side is Side.BUY
                    else price * (
                        Decimal("1") - self.settings.live_max_slippage_percent
                    )
                ),
            )
            for index, (instrument, side, price) in enumerate(
                zip(instruments, sides, prices, strict=True)
            )
        )
        plan = ExecutionPlan(
            plan_id=_stable_id("plan", signal_id, decision_id),
            signal_id=signal_id,
            risk_decision_id=decision_id,
            mode=intent.mode,
            created_at=current,
            expires_at=expires_at,
            instructions=instructions,
        )
        DecisionPipeline._validate_plan(intent, decision, plan, current)
        return LiveExecutionApproval(
            intent=intent,
            risk_decision=decision,
            plan=plan,
            opportunity_id=opportunity.id,
            opportunity_key=opportunity_key,
            strategy=strategy,
            asset=opportunity.asset,
            capital_per_leg=quote.capital,
            expected_net_profit=quote.net_profit,
            reference_prices=prices,
            market_snapshot_at=snapshot.captured_at,
            target_settlements=target_settlements(opportunity, snapshot, current),
        )

    @staticmethod
    def _instruments(
        opportunity: Opportunity,
        snapshot: MarketSnapshot,
    ) -> tuple[InstrumentKey, InstrumentKey]:
        raw_legs = (
            (
                opportunity.venue_a,
                opportunity.symbol_a,
                opportunity.leg_a_type,
            ),
            (
                opportunity.venue_b or opportunity.venue_a,
                opportunity.symbol_b,
                opportunity.leg_b_type,
            ),
        )
        result: list[InstrumentKey] = []
        for venue, symbol, raw_type in raw_legs:
            if not symbol:
                raise ValueError("live decision requires exact exchange symbols")
            instrument_type = InstrumentType(raw_type)
            normalized = snapshot.instrument(venue, symbol, instrument_type)
            if normalized is None or not normalized.is_active:
                raise ValueError("live decision requires active typed instrument metadata")
            result.append(
                InstrumentKey(
                    venue=normalized.exchange,
                    exchange_symbol=normalized.exchange_symbol,
                    base_asset=normalized.base_asset,
                    quote_asset=normalized.quote_asset,
                    instrument_type=DomainInstrumentType(normalized.instrument_type.value),
                    settlement_asset=normalized.settlement_asset,
                    expiry=normalized.expiry,
                )
            )
        return result[0], result[1]


class DecisionPipeline:
    """Ensure adapters receive only persisted OMS orders, never raw signals."""

    def __init__(
        self,
        *,
        validator: SignalValidator,
        risk: RiskEvaluator,
        planner: ExecutionPlanner,
        oms: DurableOMS,
        adapters: Mapping[str, ExecutionAdapter],
        compensator: ExposureCompensator | None = None,
    ) -> None:
        self.validator = validator
        self.risk = risk
        self.planner = planner
        self.oms = oms
        self.adapters = {venue.lower(): adapter for venue, adapter in adapters.items()}
        self.compensator = compensator

    def prepare(
        self, intent: SignalIntent, *, now: datetime
    ) -> DecisionPipelineResult:
        """Validate, size, plan, and persist OMS orders without venue side effects."""

        current = _utc(now)
        self.validator.validate(intent, current)
        decision = self.risk.evaluate(intent, current)
        if decision.signal_id != intent.signal_id:
            raise ValueError("risk decision signal identity mismatch")
        if decision.decided_at > current:
            raise ValueError("risk decision time is in the future")
        if not decision.approved:
            return DecisionPipelineResult(
                status=PipelineStatus.RISK_REJECTED,
                signal_id=intent.signal_id,
                risk_decision=decision,
                reason=decision.rejection_reason,
            )
        contract = mode_contract(intent.mode)
        if contract.execution_path is ExecutionPath.DISABLED:
            raise ValueError("current trading mode forbids execution planning")
        plan = self.planner.build(intent, decision, current)
        self._validate_plan(intent, decision, plan, current)
        orders = tuple(
            self.oms.create_order(
                decision,
                leg_index=instruction.leg_index,
                instrument=instruction.instrument,
                side=instruction.side,
                order_type=instruction.order_type,
                quantity=instruction.quantity,
                limit_price=instruction.limit_price,
                reduce_only=instruction.reduce_only,
                timestamp=current,
            )
            for instruction in plan.instructions
        )
        return DecisionPipelineResult(
            status=PipelineStatus.PREPARED,
            signal_id=intent.signal_id,
            risk_decision=decision,
            plan=plan,
            orders=orders,
        )

    async def execute(
        self, intent: SignalIntent, *, now: datetime
    ) -> DecisionPipelineResult:
        current = _utc(now)
        prepared_result = self.prepare(intent, now=current)
        if prepared_result.status is PipelineStatus.RISK_REJECTED:
            return prepared_result
        plan = prepared_result.plan
        if plan is None:
            raise RuntimeError("prepared pipeline result is missing an execution plan")
        decision = prepared_result.risk_decision
        orders = prepared_result.orders
        contract = mode_contract(intent.mode)
        reports: list[ExecutionReport] = []
        final_orders: list[OMSOrderSnapshot] = list(orders)
        for index, order in enumerate(orders):
            adapter = self._adapter(order, contract.execution_path)
            prepared = self.oms.prepare_submit(order.client_order_id, current)
            final_orders[index] = prepared
            try:
                report = await adapter.submit(prepared)
            except Exception as exc:
                unknown = self.oms.mark_unknown(
                    order.client_order_id,
                    current,
                    type(exc).__name__,
                )
                final_orders[index] = unknown
                return DecisionPipelineResult(
                    status=PipelineStatus.AMBIGUOUS,
                    signal_id=intent.signal_id,
                    risk_decision=decision,
                    plan=plan,
                    orders=tuple(final_orders),
                    reports=tuple(reports),
                    reason=f"ambiguous_submission:{order.instrument.venue.lower()}",
                )
            applied = self.oms.apply_report(report)
            final_orders[index] = applied
            reports.append(report)
            if applied.status is not OrderStatus.FILLED:
                reason = f"leg_not_filled:{applied.status.value}"
                filled_orders = tuple(
                    item
                    for item in final_orders[:index]
                    if item.status is OrderStatus.FILLED
                )
                if filled_orders:
                    return await self._compensate(
                        intent=intent,
                        decision=decision,
                        plan=plan,
                        orders=tuple(final_orders),
                        reports=tuple(reports),
                        filled_orders=filled_orders,
                        now=current,
                        reason=reason,
                    )
                return DecisionPipelineResult(
                    status=PipelineStatus.STOPPED,
                    signal_id=intent.signal_id,
                    risk_decision=decision,
                    plan=plan,
                    orders=tuple(final_orders),
                    reports=tuple(reports),
                    reason=reason,
                )
        return DecisionPipelineResult(
            status=PipelineStatus.COMPLETED,
            signal_id=intent.signal_id,
            risk_decision=decision,
            plan=plan,
            orders=tuple(final_orders),
            reports=tuple(reports),
        )

    async def _compensate(
        self,
        *,
        intent: SignalIntent,
        decision: RiskDecision,
        plan: ExecutionPlan,
        orders: tuple[OMSOrderSnapshot, ...],
        reports: tuple[ExecutionReport, ...],
        filled_orders: tuple[OMSOrderSnapshot, ...],
        now: datetime,
        reason: str,
    ) -> DecisionPipelineResult:
        if self.compensator is None:
            return DecisionPipelineResult(
                status=PipelineStatus.MANUAL_INTERVENTION,
                signal_id=intent.signal_id,
                risk_decision=decision,
                plan=plan,
                orders=orders,
                reports=reports,
                reason=f"{reason};compensator_unavailable",
            )
        try:
            result = await self.compensator.compensate(
                plan=plan,
                filled_orders=filled_orders,
                reports=reports,
                now=now,
            )
        except Exception as exc:
            return DecisionPipelineResult(
                status=PipelineStatus.MANUAL_INTERVENTION,
                signal_id=intent.signal_id,
                risk_decision=decision,
                plan=plan,
                orders=orders,
                reports=reports,
                reason=f"{reason};compensation_failed:{type(exc).__name__}",
            )
        return DecisionPipelineResult(
            status=(
                PipelineStatus.COMPENSATED
                if result.completed
                else PipelineStatus.MANUAL_INTERVENTION
            ),
            signal_id=intent.signal_id,
            risk_decision=decision,
            plan=plan,
            orders=orders,
            reports=reports,
            reason=(
                f"{reason};compensated"
                if result.completed
                else f"{reason};compensation_incomplete:{result.reason or 'unknown'}"
            ),
        )

    def _adapter(
        self, order: OMSOrderSnapshot, expected_path: ExecutionPath
    ) -> ExecutionAdapter:
        venue = order.instrument.venue.lower()
        try:
            adapter = self.adapters[venue]
        except KeyError as exc:
            raise ValueError(f"execution adapter is unavailable for {venue}") from exc
        if adapter.path is not expected_path:
            raise ValueError("execution adapter path does not match trading mode")
        return adapter

    @staticmethod
    def _validate_plan(
        intent: SignalIntent,
        decision: RiskDecision,
        plan: ExecutionPlan,
        now: datetime,
    ) -> None:
        if plan.signal_id != intent.signal_id:
            raise ValueError("execution plan signal identity mismatch")
        if plan.risk_decision_id != decision.decision_id:
            raise ValueError("execution plan risk identity mismatch")
        if plan.mode is not intent.mode:
            raise ValueError("execution plan mode mismatch")
        if plan.created_at > now or now >= plan.expires_at:
            raise ValueError("execution plan is not active")
        if plan.expires_at > intent.expires_at:
            raise ValueError("execution plan outlives signal intent")
        if len(plan.instructions) != len(intent.legs):
            raise ValueError("execution plan must cover every signal leg exactly once")
        by_index = {item.leg_index: item for item in plan.instructions}
        if set(by_index) != set(range(len(intent.legs))):
            raise ValueError("execution plan leg indexes are not contiguous")
        for index, leg in enumerate(intent.legs):
            instruction = by_index[index]
            if instruction.instrument != leg.instrument or instruction.side is not leg.side:
                raise ValueError("execution instruction changed strategy exposure")
            maximum = decision.approved_quantity * leg.hedge_ratio
            if instruction.quantity > maximum:
                raise ValueError("execution instruction exceeds risk-authorized quantity")


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]
    return f"{prefix}:{digest}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
