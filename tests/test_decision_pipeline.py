from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    ExecutionReport,
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    Side,
    TradingMode,
)
from funding_arbitrage.domain.modes import ExecutionPath
from funding_arbitrage.execution.oms import DurableOMS, JsonlOMSJournal, OMSOrderSnapshot
from funding_arbitrage.services.decision_pipeline import (
    CompensationResult,
    DecisionPipeline,
    DecisionPipelineResult,
    PipelineStatus,
    StrictSignalValidator,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
BTC = InstrumentKey(
    venue="bybit",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _intent(mode: TradingMode = TradingMode.PAPER) -> SignalIntent:
    return SignalIntent(
        signal_id="signal-1",
        strategy_id="funding-basis-v1",
        mode=mode,
        signal_type=SignalType.FUNDING_BASIS,
        primary_instrument=BTC,
        side=Side.BUY,
        legs=(SignalLeg(instrument=BTC, side=Side.BUY),),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.8"),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("10"),
        estimated_cost_bps=Decimal("4"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _decision(approved: bool = True) -> RiskDecision:
    return RiskDecision(
        signal_id="signal-1",
        decision_id="risk-1",
        decided_at=NOW,
        approved=approved,
        rejection_reason=None if approved else "daily_loss_limit",
        approved_risk_usdt=Decimal("10") if approved else Decimal("0"),
        approved_quantity=Decimal("0.01") if approved else Decimal("0"),
        approved_notional=Decimal("600") if approved else Decimal("0"),
        max_slippage_bps=Decimal("5"),
        max_execution_seconds=5,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _plan(intent: SignalIntent, decision: RiskDecision) -> ExecutionPlan:
    return ExecutionPlan(
        plan_id="plan-1",
        signal_id=intent.signal_id,
        risk_decision_id=decision.decision_id,
        mode=intent.mode,
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=10),
        instructions=(
            ExecutionInstruction(
                leg_index=0,
                instrument=BTC,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.01"),
                limit_price=Decimal("60000"),
            ),
        ),
    )


class Risk:
    def __init__(self, approved: bool = True) -> None:
        self.approved = approved

    def evaluate(self, intent: SignalIntent, now: datetime) -> RiskDecision:
        assert intent.signal_id == "signal-1"
        assert now == NOW
        return _decision(self.approved)


class Planner:
    def __init__(self, *, wrong_side: bool = False) -> None:
        self.wrong_side = wrong_side

    def build(
        self, intent: SignalIntent, decision: RiskDecision, now: datetime
    ) -> ExecutionPlan:
        assert now == NOW
        plan = _plan(intent, decision)
        if not self.wrong_side:
            return plan
        instruction = plan.instructions[0].model_copy(update={"side": Side.SELL})
        return plan.model_copy(update={"instructions": (instruction,)})


class Adapter:
    path = ExecutionPath.SIMULATED

    def __init__(self, journal_path: Path, *, fail: bool = False) -> None:
        self.journal_path = journal_path
        self.fail = fail
        self.orders: list[OMSOrderSnapshot] = []

    async def submit(self, order: OMSOrderSnapshot) -> ExecutionReport:
        events = [
            json.loads(line)["event_type"]
            for line in self.journal_path.read_text(encoding="utf-8").splitlines()
        ]
        assert events == ["CREATED", "SUBMIT_PREPARED"]
        assert order.status is OrderStatus.SUBMITTING
        self.orders.append(order)
        if self.fail:
            raise TimeoutError("synthetic ambiguous submit")
        return ExecutionReport(
            client_order_id=order.client_order_id,
            exchange_order_id="venue-1",
            status=OrderStatus.FILLED,
            requested_quantity=order.requested_quantity,
            filled_quantity=order.requested_quantity,
            average_fill_price=Decimal("60000"),
            fee=Decimal("0.30"),
            fee_asset="USDT",
            liquidity_role=LiquidityRole.TAKER,
            exchange_timestamp=NOW + timedelta(milliseconds=20),
            receive_timestamp=NOW + timedelta(milliseconds=25),
        )


def _pipeline(
    tmp_path: Path,
    *,
    approved: bool = True,
    wrong_side: bool = False,
    fail: bool = False,
) -> tuple[DecisionPipeline, Adapter, Path]:
    path = tmp_path / "oms.jsonl"
    adapter = Adapter(path, fail=fail)
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(approved),
        planner=Planner(wrong_side=wrong_side),
        oms=DurableOMS(JsonlOMSJournal(path)),
        adapters={"bybit": adapter},
    )
    return pipeline, adapter, path


async def test_pipeline_separates_intent_risk_plan_oms_and_adapter(tmp_path: Path) -> None:
    pipeline, adapter, path = _pipeline(tmp_path)

    result = await pipeline.execute(_intent(), now=NOW)

    assert result.status is PipelineStatus.COMPLETED
    assert result.orders[0].status is OrderStatus.FILLED
    assert len(adapter.orders) == 1
    assert isinstance(adapter.orders[0], OMSOrderSnapshot)
    assert [
        json.loads(line)["event_type"]
        for line in path.read_text(encoding="utf-8").splitlines()
    ] == ["CREATED", "SUBMIT_PREPARED", "EXECUTION_REPORT"]


async def test_risk_rejection_never_builds_oms_order_or_calls_adapter(tmp_path: Path) -> None:
    pipeline, adapter, path = _pipeline(tmp_path, approved=False)

    result = await pipeline.execute(_intent(), now=NOW)

    assert result.status is PipelineStatus.RISK_REJECTED
    assert result.reason == "daily_loss_limit"
    assert adapter.orders == []
    assert not path.exists()


async def test_planner_cannot_change_strategy_exposure(tmp_path: Path) -> None:
    pipeline, adapter, path = _pipeline(tmp_path, wrong_side=True)

    with pytest.raises(ValueError, match="changed strategy exposure"):
        await pipeline.execute(_intent(), now=NOW)

    assert adapter.orders == []
    assert not path.exists()


async def test_ambiguous_submit_is_persisted_unknown_and_never_retried(tmp_path: Path) -> None:
    pipeline, adapter, path = _pipeline(tmp_path, fail=True)

    result = await pipeline.execute(_intent(), now=NOW)

    assert result.status is PipelineStatus.AMBIGUOUS
    assert result.orders[0].status is OrderStatus.UNKNOWN
    assert len(adapter.orders) == 1
    assert [
        json.loads(line)["event_type"]
        for line in path.read_text(encoding="utf-8").splitlines()
    ] == ["CREATED", "SUBMIT_PREPARED", "UNKNOWN_MARKED"]


async def test_mode_requires_matching_simulated_or_exchange_adapter(tmp_path: Path) -> None:
    pipeline, _, _ = _pipeline(tmp_path)
    pipeline.adapters["bybit"].path = ExecutionPath.LIVE
    with pytest.raises(ValueError, match="path does not match"):
        await pipeline.execute(_intent(TradingMode.PAPER), now=NOW)

    safe_pipeline, _, _ = _pipeline(tmp_path / "safe")
    with pytest.raises(ValueError, match="forbids execution planning"):
        await safe_pipeline.execute(_intent(TradingMode.SAFE_MODE), now=NOW)


class TwoLegPlanner:
    def build(
        self, intent: SignalIntent, decision: RiskDecision, now: datetime
    ) -> ExecutionPlan:
        return ExecutionPlan(
            plan_id="plan-2",
            signal_id=intent.signal_id,
            risk_decision_id=decision.decision_id,
            mode=intent.mode,
            created_at=now,
            expires_at=now + timedelta(seconds=10),
            instructions=(
                ExecutionInstruction(
                    leg_index=0,
                    instrument=BTC,
                    side=Side.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.01"),
                    limit_price=Decimal("60000"),
                ),
                ExecutionInstruction(
                    leg_index=1,
                    instrument=BTC,
                    side=Side.SELL,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.01"),
                    limit_price=Decimal("60000"),
                ),
            ),
        )


class SecondLegRejectedAdapter:
    path = ExecutionPath.SIMULATED

    def __init__(self) -> None:
        self.orders: list[OMSOrderSnapshot] = []

    async def submit(self, order: OMSOrderSnapshot) -> ExecutionReport:
        self.orders.append(order)
        rejected = len(self.orders) == 2
        return ExecutionReport(
            client_order_id=order.client_order_id,
            exchange_order_id=f"venue-{len(self.orders)}",
            status=OrderStatus.REJECTED if rejected else OrderStatus.FILLED,
            requested_quantity=order.requested_quantity,
            filled_quantity=Decimal("0") if rejected else order.requested_quantity,
            average_fill_price=None if rejected else Decimal("60000"),
            fee=Decimal("0") if rejected else Decimal("0.30"),
            fee_asset="USDT",
            liquidity_role=LiquidityRole.TAKER,
            exchange_timestamp=NOW + timedelta(milliseconds=20),
            receive_timestamp=NOW + timedelta(milliseconds=25),
            reject_code="synthetic_reject" if rejected else None,
        )


class RecordingCompensator:
    def __init__(self, *, completed: bool = True) -> None:
        self.completed = completed
        self.filled_orders: tuple[OMSOrderSnapshot, ...] = ()

    async def compensate(
        self,
        *,
        plan: ExecutionPlan,
        filled_orders: tuple[OMSOrderSnapshot, ...],
        reports: tuple[ExecutionReport, ...],
        now: datetime,
    ) -> CompensationResult:
        assert plan.plan_id == "plan-2"
        assert now == NOW
        assert len(reports) == 2
        self.filled_orders = filled_orders
        return CompensationResult(
            completed=self.completed,
            reason=None if self.completed else "synthetic_incomplete",
        )


def _two_leg_intent() -> SignalIntent:
    base = _intent()
    return SignalIntent.model_validate(
        base.model_dump()
        | {
            "legs": (
                SignalLeg(instrument=BTC, side=Side.BUY),
                SignalLeg(instrument=BTC, side=Side.SELL),
            )
        }
    )


async def test_second_leg_rejection_compensates_known_first_leg_fill(
    tmp_path: Path,
) -> None:
    adapter = SecondLegRejectedAdapter()
    compensator = RecordingCompensator()
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(),
        planner=TwoLegPlanner(),
        oms=DurableOMS(JsonlOMSJournal(tmp_path / "oms.jsonl")),
        adapters={"bybit": adapter},
        compensator=compensator,
    )

    result = await pipeline.execute(_two_leg_intent(), now=NOW)

    assert result.status is PipelineStatus.COMPENSATED
    assert result.reason == "leg_not_filled:REJECTED;compensated"
    assert len(compensator.filled_orders) == 1
    assert compensator.filled_orders[0].status is OrderStatus.FILLED


async def test_known_open_exposure_without_compensator_requires_manual_intervention(
    tmp_path: Path,
) -> None:
    adapter = SecondLegRejectedAdapter()
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(),
        planner=TwoLegPlanner(),
        oms=DurableOMS(JsonlOMSJournal(tmp_path / "oms.jsonl")),
        adapters={"bybit": adapter},
    )

    result = await pipeline.execute(_two_leg_intent(), now=NOW)

    assert result.status is PipelineStatus.MANUAL_INTERVENTION
    assert result.reason == "leg_not_filled:REJECTED;compensator_unavailable"

def test_prepare_persists_oms_contract_without_calling_adapter(
    tmp_path: Path,
) -> None:
    pipeline, adapter, path = _pipeline(tmp_path)

    result = pipeline.prepare(_intent(), now=NOW)

    assert result.status is PipelineStatus.PREPARED
    assert result.plan is not None
    assert len(result.orders) == 1
    assert result.orders[0].status is OrderStatus.NEW
    assert adapter.orders == []
    assert [
        json.loads(line)["event_type"]
        for line in path.read_text(encoding="utf-8").splitlines()
    ] == ["CREATED"]

def test_strict_signal_validator_rejects_temporal_and_identity_corruption() -> None:
    validator = StrictSignalValidator()
    intent = _intent()

    with pytest.raises(ValueError, match="creation time is in the future"):
        validator.validate(
            intent.model_copy(update={"created_at": NOW + timedelta(microseconds=1)}),
            NOW,
        )
    with pytest.raises(ValueError, match="has expired"):
        validator.validate(
            intent.model_copy(update={"expires_at": NOW}),
            NOW.replace(tzinfo=None),
        )
    with pytest.raises(ValueError, match="has no legs"):
        validator.validate(intent.model_copy(update={"legs": ()}), NOW)
    with pytest.raises(ValueError, match="must be the first signal leg"):
        validator.validate(
            intent.model_copy(
                update={
                    "primary_instrument": BTC.model_copy(
                        update={"venue": "gate", "exchange_symbol": "BTC_USDT"}
                    )
                }
            ),
            NOW,
        )


class RiskVariant:
    def __init__(self, **updates: object) -> None:
        self.updates = updates

    def evaluate(self, intent: SignalIntent, now: datetime) -> RiskDecision:
        return _decision().model_copy(update=self.updates)


async def test_pipeline_rejects_risk_identity_and_future_decision(
    tmp_path: Path,
) -> None:
    for name, updates, message in (
        (
            "identity",
            {"signal_id": "different-signal"},
            "risk decision signal identity mismatch",
        ),
        (
            "future",
            {"decided_at": NOW + timedelta(microseconds=1)},
            "risk decision time is in the future",
        ),
    ):
        pipeline = DecisionPipeline(
            validator=StrictSignalValidator(),
            risk=RiskVariant(**updates),
            planner=Planner(),
            oms=DurableOMS(JsonlOMSJournal(tmp_path / f"{name}.jsonl")),
            adapters={},
        )
        with pytest.raises(ValueError, match=message):
            await pipeline.execute(_intent(), now=NOW)


def test_execution_plan_validation_rejects_every_scope_escalation() -> None:
    intent = _two_leg_intent()
    decision = _decision()
    plan = TwoLegPlanner().build(intent, decision, NOW)

    cases: tuple[tuple[str, ExecutionPlan], ...] = (
        (
            "signal identity mismatch",
            plan.model_copy(update={"signal_id": "different"}),
        ),
        (
            "risk identity mismatch",
            plan.model_copy(update={"risk_decision_id": "different"}),
        ),
        (
            "mode mismatch",
            plan.model_copy(update={"mode": TradingMode.SHADOW}),
        ),
        (
            "is not active",
            plan.model_copy(update={"created_at": NOW + timedelta(microseconds=1)}),
        ),
        (
            "outlives signal intent",
            plan.model_copy(update={"expires_at": intent.expires_at + timedelta(seconds=1)}),
        ),
        (
            "cover every signal leg",
            plan.model_copy(update={"instructions": plan.instructions[:1]}),
        ),
        (
            "indexes are not contiguous",
            plan.model_copy(
                update={
                    "instructions": (
                        plan.instructions[0],
                        plan.instructions[1].model_copy(update={"leg_index": 2}),
                    )
                }
            ),
        ),
        (
            "exceeds risk-authorized quantity",
            plan.model_copy(
                update={
                    "instructions": (
                        plan.instructions[0].model_copy(
                            update={"quantity": Decimal("0.011")}
                        ),
                        plan.instructions[1],
                    )
                }
            ),
        ),
    )
    for message, candidate in cases:
        with pytest.raises(ValueError, match=message):
            DecisionPipeline._validate_plan(intent, decision, candidate, NOW)


async def test_execute_rejects_missing_prepared_plan(tmp_path: Path) -> None:
    pipeline, _, _ = _pipeline(tmp_path)
    missing = DecisionPipelineResult(
        status=PipelineStatus.PREPARED,
        signal_id="signal-1",
        risk_decision=_decision(),
        plan=None,
    )
    pipeline.prepare = lambda intent, *, now: missing  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="missing an execution plan"):
        await pipeline.execute(_intent(), now=NOW)


class FirstLegRejectedAdapter(SecondLegRejectedAdapter):
    async def submit(self, order: OMSOrderSnapshot) -> ExecutionReport:
        self.orders.append(order)
        return ExecutionReport(
            client_order_id=order.client_order_id,
            exchange_order_id="venue-rejected",
            status=OrderStatus.REJECTED,
            requested_quantity=order.requested_quantity,
            filled_quantity=Decimal("0"),
            fee=Decimal("0"),
            liquidity_role=LiquidityRole.TAKER,
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            reject_code="synthetic_first_leg_reject",
        )


class ExplodingCompensator(RecordingCompensator):
    async def compensate(
        self,
        *,
        plan: ExecutionPlan,
        filled_orders: tuple[OMSOrderSnapshot, ...],
        reports: tuple[ExecutionReport, ...],
        now: datetime,
    ) -> CompensationResult:
        raise RuntimeError("synthetic compensation failure")


async def test_first_leg_rejection_stops_without_compensation(tmp_path: Path) -> None:
    adapter = FirstLegRejectedAdapter()
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(),
        planner=TwoLegPlanner(),
        oms=DurableOMS(JsonlOMSJournal(tmp_path / "first-reject.jsonl")),
        adapters={"bybit": adapter},
        compensator=RecordingCompensator(),
    )

    result = await pipeline.execute(_two_leg_intent(), now=NOW)

    assert result.status is PipelineStatus.STOPPED
    assert result.reason == "leg_not_filled:REJECTED"


@pytest.mark.parametrize(
    ("compensator", "reason"),
    [
        (
            RecordingCompensator(completed=False),
            "compensation_incomplete:synthetic_incomplete",
        ),
        (
            ExplodingCompensator(),
            "compensation_failed:RuntimeError",
        ),
    ],
)
async def test_compensation_failure_requires_manual_intervention(
    tmp_path: Path,
    compensator: RecordingCompensator,
    reason: str,
) -> None:
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(),
        planner=TwoLegPlanner(),
        oms=DurableOMS(JsonlOMSJournal(tmp_path / f"{reason}.jsonl")),
        adapters={"bybit": SecondLegRejectedAdapter()},
        compensator=compensator,
    )

    result = await pipeline.execute(_two_leg_intent(), now=NOW)

    assert result.status is PipelineStatus.MANUAL_INTERVENTION
    assert reason in (result.reason or "")


async def test_missing_execution_adapter_fails_before_exchange_action(
    tmp_path: Path,
) -> None:
    pipeline = DecisionPipeline(
        validator=StrictSignalValidator(),
        risk=Risk(),
        planner=Planner(),
        oms=DurableOMS(JsonlOMSJournal(tmp_path / "missing-adapter.jsonl")),
        adapters={},
    )

    with pytest.raises(ValueError, match="adapter is unavailable for bybit"):
        await pipeline.execute(_intent(), now=NOW)

