from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

from pydantic import BaseModel, ConfigDict

from funding_arbitrage.backtest.fills import FillModelPolicy, SimulatedOrderState
from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    OrderType,
    Side,
    TradingMode,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalExitReason,
    DirectionalPaperBroker,
    DirectionalPaperStatus,
)
from funding_arbitrage.risk.portfolio import (
    PortfolioRiskAuthorization,
    RiskHierarchyCaps,
)
from funding_arbitrage.strategies.directional import DirectionalStrategyEvaluation

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


class BatchStub(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: TradingMode
    evaluations: tuple[DirectionalStrategyEvaluation, ...]
    risk_authorizations: tuple[PortfolioRiskAuthorization, ...]
    execution_plans: tuple[ExecutionPlan, ...]


def _batch(*, quantity: Decimal = Decimal("1"), ttl_seconds: int = 15) -> BatchStub:
    intent = SignalIntent(
        signal_id="signal-1",
        strategy_id="orderflow-breakout-v1",
        mode=TradingMode.PAPER,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=INSTRUMENT,
        side=Side.BUY,
        legs=(SignalLeg(instrument=INSTRUMENT, side=Side.BUY),),
        regime=MarketRegime.TREND_UP,
        quality_score=Decimal("80"),
        confidence=Decimal("0.8"),
        entry_zone_low=Decimal("100"),
        entry_zone_high=Decimal("101"),
        structural_stop=Decimal("98"),
        targets=(Decimal("102"),),
        expected_holding_seconds=60,
        expected_move_bps=Decimal("200"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=ttl_seconds),
    )
    decision = RiskDecision(
        signal_id=intent.signal_id,
        decision_id="risk-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("3"),
        approved_quantity=quantity,
        approved_notional=quantity * Decimal("101"),
        max_slippage_bps=Decimal("20"),
        max_execution_seconds=ttl_seconds,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    authorization = PortfolioRiskAuthorization(
        decision=decision,
        hierarchy=RiskHierarchyCaps(
            caps_usd={"requested": decision.approved_notional},
            pre_multiplier_notional_usd=decision.approved_notional,
            combined_multiplier=Decimal("1"),
            sized_notional_usd=decision.approved_notional,
            binding_constraints=("requested",),
        ),
    )
    plan = ExecutionPlan(
        plan_id="plan-1",
        signal_id=intent.signal_id,
        risk_decision_id=decision.decision_id,
        mode=TradingMode.PAPER,
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=ttl_seconds),
        instructions=(
            ExecutionInstruction(
                leg_index=0,
                instrument=INSTRUMENT,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                limit_price=Decimal("101"),
            ),
        ),
    )
    return BatchStub(
        mode=TradingMode.PAPER,
        evaluations=(
            DirectionalStrategyEvaluation(
                strategy_id=intent.strategy_id,
                intent=intent,
                score=Decimal("0.8"),
            ),
        ),
        risk_authorizations=(authorization,),
        execution_plans=(plan,),
    )


def _event(
    event_id: str,
    seconds: int,
    *,
    bid: str,
    ask: str,
    depth: str = "10",
    quality: DataQuality = DataQuality.VALID,
) -> EventEnvelope[BookSnapshot]:
    timestamp = NOW + timedelta(seconds=seconds)
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id=event_id,
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp,
            monotonic_ns=seconds,
            sequence_id=str(seconds),
            native_sequence=seconds,
            source="BYBIT:BOOK",
            correlation_id="BTCUSDT",
            payload_version=1,
            quality=quality,
        ),
        payload=BookSnapshot(
            instrument=INSTRUMENT,
            bids=(BookLevel(price=Decimal(bid), quantity=Decimal(depth)),),
            asks=(BookLevel(price=Decimal(ask), quantity=Decimal(depth)),),
            sequence=seconds,
            exchange_timestamp=timestamp,
        ),
    )


def _broker(*, participation: str = "1") -> DirectionalPaperBroker:
    return DirectionalPaperBroker(
        {
            "BYBIT": FillModelPolicy(
                maker_fee_bps=Decimal("2"),
                taker_fee_bps=Decimal("5"),
                order_latency_ms=0,
                maximum_participation_rate=Decimal(participation),
                impact_coefficient_bps=Decimal("1"),
            )
        }
    )


def test_simulation_versions_produce_isolated_position_and_order_ids() -> None:
    candidate = DirectionalPaperBroker(
        {
            "BYBIT": FillModelPolicy(
                order_latency_ms=0,
                maximum_participation_rate=Decimal("1"),
            )
        },
        simulation_version="candidate-v1",
    )
    baseline = DirectionalPaperBroker(
        {
            "BYBIT": FillModelPolicy(
                order_latency_ms=0,
                maximum_participation_rate=Decimal("1"),
            )
        },
        simulation_version="baseline-v1",
    )

    candidate_position = candidate.submit(cast(Any, _batch()))[0].position
    baseline_position = baseline.submit(cast(Any, _batch()))[0].position

    assert candidate_position.simulation_version == "candidate-v1"
    assert baseline_position.simulation_version == "baseline-v1"
    assert candidate_position.position_id != baseline_position.position_id
    assert (
        candidate_position.entry_order.client_order_id
        != baseline_position.entry_order.client_order_id
    )


def test_paper_broker_opens_targets_and_closes_with_net_pnl() -> None:
    broker = _broker()
    created = broker.submit(cast(Any, _batch()))
    assert len(created) == 1
    assert created[0].position.status is DirectionalPaperStatus.PENDING_ENTRY

    opened = broker.advance(_event("book-1", 1, bid="100", ask="100.5"))
    assert opened[0].position.status is DirectionalPaperStatus.OPEN

    triggered = broker.advance(_event("book-2", 2, bid="103", ask="103.5"))
    assert triggered[0].position.status is DirectionalPaperStatus.PENDING_EXIT
    assert triggered[0].position.exit_reason is DirectionalExitReason.TARGET

    closed = broker.advance(_event("book-3", 3, bid="102.5", ask="103"))
    position = closed[0].position
    assert position.status is DirectionalPaperStatus.CLOSED
    assert position.signed_quantity == 0
    assert position.realized_gross_pnl > 0
    assert position.net_pnl > 0
    assert position.total_fee > 0


def test_paper_broker_duplicate_book_cannot_double_fill_partial_entry() -> None:
    broker = _broker(participation="0.1")
    broker.submit(cast(Any, _batch(quantity=Decimal("1"))))
    event = _event("book-partial", 1, bid="100", ask="100.5", depth="0.4")

    first = broker.advance(event)
    assert first[0].position.entry_order.filled_quantity == Decimal("0.04")
    assert broker.advance(event) == ()
    assert broker.positions[0].entry_order.filled_quantity == Decimal("0.04")


def test_partial_entry_expiry_becomes_mandatory_exit() -> None:
    broker = _broker(participation="0.1")
    broker.submit(cast(Any, _batch(quantity=Decimal("1"), ttl_seconds=2)))
    broker.advance(_event("book-partial", 1, bid="100", ask="100.5", depth="0.4"))

    expired = broker.advance(_event("book-expire", 2, bid="100", ask="100.5", depth="0.4"))
    position = expired[0].position
    assert position.status is DirectionalPaperStatus.PENDING_EXIT
    assert position.exit_reason is DirectionalExitReason.ENTRY_PARTIAL
    assert position.exit_order is not None
    assert position.exit_order.requested_quantity == Decimal("0.04")


def test_stale_book_rejects_unfilled_entry() -> None:
    broker = _broker()
    broker.submit(cast(Any, _batch()))

    updates = broker.advance(
        _event("book-stale", 1, bid="100", ask="100.5", quality=DataQuality.STALE)
    )
    assert updates[0].position.status is DirectionalPaperStatus.REJECTED
    assert updates[0].position.entry_order.filled_quantity == 0

def test_active_instrument_conflict_is_persistable_rejection_not_silent_drop() -> None:
    broker = _broker()
    first = _batch()
    broker.submit(cast(Any, first))
    first_intent = first.evaluations[0].intent
    assert first_intent is not None
    second_intent = first_intent.model_copy(update={"signal_id": "signal-2"})
    second_decision = first.risk_authorizations[0].decision.model_copy(
        update={"signal_id": "signal-2", "decision_id": "risk-2"}
    )
    second = first.model_copy(
        update={
            "evaluations": (
                first.evaluations[0].model_copy(update={"intent": second_intent}),
            ),
            "risk_authorizations": (
                first.risk_authorizations[0].model_copy(
                    update={"decision": second_decision}
                ),
            ),
            "execution_plans": (
                first.execution_plans[0].model_copy(
                    update={
                        "plan_id": "plan-2",
                        "signal_id": "signal-2",
                        "risk_decision_id": "risk-2",
                    }
                ),
            ),
        }
    )

    updates = broker.submit(cast(Any, second))

    assert len(updates) == 1
    assert updates[0].position.status is DirectionalPaperStatus.REJECTED
    assert updates[0].position.rejection_reason == "active_instrument_conflict"
    assert (
        updates[0].position.entry_order.state
        is SimulatedOrderState.REJECTED
    )
    assert (
        updates[0].position.entry_order.rejection_reason
        == "active_instrument_conflict"
    )
    assert len(broker.active_positions) == 1


def test_partial_entry_stop_cancels_remainder_and_starts_reduce_only_exit() -> None:
    broker = _broker(participation="0.1")
    broker.submit(cast(Any, _batch(quantity=Decimal("1"))))

    stopped = broker.advance(
        _event("partial-stop", 1, bid="97", ask="97.5", depth="0.4")
    )
    position = stopped[0].position

    assert position.entry_order.filled_quantity == Decimal("0.04")
    assert position.entry_order.state is SimulatedOrderState.CANCELLED
    assert position.status is DirectionalPaperStatus.PENDING_EXIT
    assert position.exit_reason is DirectionalExitReason.STOP
    assert position.exit_order is not None
    assert position.exit_order.requested_quantity == Decimal("0.04")
    assert abs(position.signed_quantity) == Decimal("0.04")


def test_partial_market_exit_uses_child_ioc_orders_and_incremental_realized_pnl() -> None:
    broker = _broker(participation="0.1")
    broker.submit(cast(Any, _batch(quantity=Decimal("1"))))
    broker.advance(_event("retry-open", 1, bid="100", ask="100.5", depth="10"))
    broker.advance(_event("retry-trigger", 2, bid="103", ask="103.5", depth="10"))

    partial = broker.advance(
        _event("retry-partial", 3, bid="102.5", ask="103", depth="0.4")
    )[0].position

    assert partial.status is DirectionalPaperStatus.PENDING_EXIT
    assert partial.realized_gross_pnl > 0
    assert len(partial.exit_order_history) == 1
    first_child = partial.exit_order_history[0]
    assert first_child.state is SimulatedOrderState.CANCELLED
    assert first_child.filled_quantity == Decimal("0.04")
    assert partial.exit_order is not None
    assert partial.exit_order.client_order_id != first_child.client_order_id
    assert abs(partial.signed_quantity) == Decimal("0.96")

    closed = broker.advance(
        _event("retry-close", 4, bid="102.5", ask="103", depth="10")
    )[0].position

    assert closed.status is DirectionalPaperStatus.CLOSED
    assert closed.realized_gross_pnl > partial.realized_gross_pnl
    assert len({order.client_order_id for order in closed.exit_orders}) == 2
    assert closed.signed_quantity == 0


def test_short_paper_position_uses_ask_for_target_and_realizes_profit() -> None:
    base = _batch()
    base_intent = base.evaluations[0].intent
    assert base_intent is not None
    short_intent = base_intent.model_copy(
        update={
            "side": Side.SELL,
            "legs": (SignalLeg(instrument=INSTRUMENT, side=Side.SELL),),
            "entry_zone_low": Decimal("99.5"),
            "entry_zone_high": Decimal("100.5"),
            "structural_stop": Decimal("102"),
            "targets": (Decimal("98"),),
        }
    )
    short = base.model_copy(
        update={
            "evaluations": (
                base.evaluations[0].model_copy(update={"intent": short_intent}),
            ),
            "execution_plans": (
                base.execution_plans[0].model_copy(
                    update={
                        "instructions": (
                            base.execution_plans[0].instructions[0].model_copy(
                                update={
                                    "side": Side.SELL,
                                    "limit_price": Decimal("99.5"),
                                }
                            ),
                        )
                    }
                ),
            ),
        }
    )
    broker = _broker()
    broker.submit(cast(Any, short))

    opened = broker.advance(_event("short-open", 1, bid="100", ask="100.5"))
    assert opened[0].position.status is DirectionalPaperStatus.OPEN
    triggered = broker.advance(_event("short-target", 2, bid="97.5", ask="98"))
    assert triggered[0].position.exit_reason is DirectionalExitReason.TARGET
    closed = broker.advance(_event("short-close", 3, bid="98", ask="98.5"))

    assert closed[0].position.status is DirectionalPaperStatus.CLOSED
    assert closed[0].position.realized_gross_pnl > 0
    assert closed[0].position.net_pnl > 0
