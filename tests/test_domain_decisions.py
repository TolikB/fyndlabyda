from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

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
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    Side,
    TradingMode,
)

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
BTC_PERP = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
GATE_PERP = BTC_PERP.model_copy(
    update={"venue": "GATE", "exchange_symbol": "BTC_USDT"}
)


def _breakout_intent() -> SignalIntent:
    return SignalIntent(
        signal_id="signal-1",
        strategy_id="orderflow-breakout-v1",
        mode=TradingMode.PAPER,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=BTC_PERP,
        side=Side.BUY,
        legs=(SignalLeg(instrument=BTC_PERP, side=Side.BUY),),
        regime=MarketRegime.TREND_UP,
        quality_score=Decimal("92"),
        confidence=Decimal("0.84"),
        entry_zone_low=Decimal("62000"),
        entry_zone_high=Decimal("62010"),
        structural_stop=Decimal("61800"),
        targets=(Decimal("62500"), Decimal("63000")),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("80"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2.5"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
        evidence={"ofi_5s": "2.1", "book_sequence": 123},
    )


def test_directional_signal_is_declarative_and_expiring() -> None:
    intent = _breakout_intent()

    assert intent.legs[0].hedge_ratio == Decimal("1")
    assert intent.expires_at > intent.created_at
    assert "quantity" not in SignalIntent.model_fields
    assert "order_type" not in SignalIntent.model_fields


def test_directional_signal_rejects_missing_stop_or_target_contract() -> None:
    with pytest.raises(ValidationError, match="directional signals require"):
        _breakout_intent().model_copy(update={"structural_stop": None}).model_validate(
            _breakout_intent().model_dump() | {"structural_stop": None}
        )


def test_meta_filter_cannot_be_misused_as_a_live_execution_signal() -> None:
    payload = _breakout_intent().model_dump()
    payload.update(
        signal_type=SignalType.LEAD_LAG_FILTER,
        mode=TradingMode.LIVE,
        structural_stop=None,
        targets=(),
        expected_rr=None,
    )
    with pytest.raises(ValidationError, match="cannot directly request live execution"):
        SignalIntent.model_validate(payload)


def test_risk_decision_is_the_only_positive_size_authorization() -> None:
    decision = RiskDecision(
        signal_id="signal-1",
        decision_id="risk-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("25"),
        approved_quantity=Decimal("0.01"),
        approved_notional=Decimal("620"),
        max_slippage_bps=Decimal("4"),
        max_execution_seconds=2,
        correlation_multiplier=Decimal("0.5"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    assert decision.approved_notional == Decimal("620")

    with pytest.raises(ValidationError, match="requires a reason and zero size"):
        RiskDecision.model_validate(
            decision.model_dump()
            | {
                "approved": False,
                "rejection_reason": "stale_book",
            }
        )

def _live_authority() -> LiveExecutionApproval:
    intent = SignalIntent(
        signal_id="signal-live-1",
        strategy_id="funding-basis-v1",
        mode=TradingMode.LIVE,
        signal_type=SignalType.FUNDING_BASIS,
        primary_instrument=BTC_PERP,
        side=Side.BUY,
        legs=(
            SignalLeg(instrument=BTC_PERP, side=Side.BUY),
            SignalLeg(instrument=GATE_PERP, side=Side.SELL),
        ),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.8"),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("12"),
        estimated_cost_bps=Decimal("4"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )
    decision = RiskDecision(
        signal_id=intent.signal_id,
        decision_id="risk-live-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("20"),
        approved_quantity=Decimal("0.01"),
        approved_notional=Decimal("620"),
        max_slippage_bps=Decimal("5"),
        max_execution_seconds=5,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    plan = ExecutionPlan(
        plan_id="plan-live-1",
        signal_id=intent.signal_id,
        risk_decision_id=decision.decision_id,
        mode=TradingMode.LIVE,
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=10),
        instructions=(
            ExecutionInstruction(
                leg_index=0,
                instrument=BTC_PERP,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.01"),
                limit_price=Decimal("62010"),
            ),
            ExecutionInstruction(
                leg_index=1,
                instrument=GATE_PERP,
                side=Side.SELL,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.01"),
                limit_price=Decimal("61990"),
            ),
        ),
    )
    return LiveExecutionApproval(
        intent=intent,
        risk_decision=decision,
        plan=plan,
        opportunity_id="opportunity-1",
        opportunity_key="key-1",
        strategy="funding-basis-v1",
        asset=" btc ",
        capital_per_leg=Decimal("620"),
        expected_net_profit=Decimal("1.5"),
        reference_prices=(Decimal("62000"), Decimal("62000")),
        market_snapshot_at=NOW,
        target_settlements=(
            NOW + timedelta(hours=8),
            NOW + timedelta(hours=4),
            NOW + timedelta(hours=8),
        ),
    )


def _validate_authority_update(**updates: object) -> None:
    authority = _live_authority()
    LiveExecutionApproval.model_validate(authority.model_dump() | updates)


def test_signal_report_instruction_and_plan_fail_closed_boundaries() -> None:
    intent = _breakout_intent()
    invalid_intents = (
        {"targets": (Decimal("0"),)},
        {"expires_at": intent.created_at},
        {"entry_zone_high": None},
        {"entry_zone_low": Decimal("62011")},
    )
    for update in invalid_intents:
        with pytest.raises(ValidationError):
            SignalIntent.model_validate(intent.model_dump() | update)

    decision = _live_authority().risk_decision
    with pytest.raises(ValidationError, match="positive size and no rejection"):
        RiskDecision.model_validate(
            decision.model_dump() | {"rejection_reason": "unexpected"}
        )
    with pytest.raises(ValidationError, match="positive size and no rejection"):
        RiskDecision.model_validate(
            decision.model_dump() | {"approved_quantity": Decimal("0")}
        )

    report = {
        "client_order_id": "order-1",
        "status": OrderStatus.FILLED,
        "requested_quantity": Decimal("1"),
        "filled_quantity": Decimal("2"),
        "average_fill_price": Decimal("100"),
        "fee": Decimal("0"),
        "liquidity_role": LiquidityRole.TAKER,
        "exchange_timestamp": NOW,
        "receive_timestamp": NOW,
    }
    with pytest.raises(ValidationError, match="cannot exceed"):
        ExecutionReport.model_validate(report)
    with pytest.raises(ValidationError, match="requires a code or message"):
        ExecutionReport.model_validate(
            report
            | {
                "status": OrderStatus.REJECTED,
                "filled_quantity": Decimal("0"),
                "average_fill_price": None,
            }
        )

    with pytest.raises(ValidationError, match="requires limit price"):
        ExecutionInstruction(
            leg_index=0,
            instrument=BTC_PERP,
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.01"),
        )

    authority = _live_authority()
    plan = authority.plan
    with pytest.raises(ValidationError, match="expire after creation"):
        ExecutionPlan.model_validate(
            plan.model_dump() | {"expires_at": plan.created_at}
        )
    duplicate = plan.instructions[1].model_copy(update={"leg_index": 0})
    with pytest.raises(ValidationError, match="indexes must be unique"):
        ExecutionPlan.model_validate(
            plan.model_dump()
            | {"instructions": (plan.instructions[0], duplicate)}
        )


def test_live_authority_normalizes_metadata_and_settlements() -> None:
    authority = _live_authority()

    assert authority.asset == "BTC"
    assert authority.target_settlements == (
        NOW + timedelta(hours=4),
        NOW + timedelta(hours=8),
    )
    with pytest.raises(ValidationError, match="asset cannot be blank"):
        _validate_authority_update(asset="   ")


def test_live_authority_rejects_broken_identity_and_temporal_chain() -> None:
    authority = _live_authority()
    rejected_risk = authority.risk_decision.model_copy(
        update={
            "approved": False,
            "rejection_reason": "risk_limit",
            "approved_risk_usdt": Decimal("0"),
            "approved_quantity": Decimal("0"),
            "approved_notional": Decimal("0"),
        }
    )
    cases: tuple[tuple[str, dict[str, object]], ...] = (
        ("requires approved risk", {"risk_decision": rejected_risk}),
        (
            "signal/risk identity mismatch",
            {
                "risk_decision": authority.risk_decision.model_copy(
                    update={"signal_id": "other"}
                )
            },
        ),
        (
            "signal/plan identity mismatch",
            {"plan": authority.plan.model_copy(update={"signal_id": "other"})},
        ),
        (
            "risk/plan identity mismatch",
            {
                "plan": authority.plan.model_copy(
                    update={"risk_decision_id": "other"}
                )
            },
        ),
        (
            "mode identity mismatch",
            {
                "plan": authority.plan.model_copy(
                    update={"mode": TradingMode.LIMITED_LIVE}
                )
            },
        ),
        (
            "predates its risk authorization",
            {
                "plan": authority.plan.model_copy(
                    update={"created_at": NOW - timedelta(seconds=1)}
                )
            },
        ),
        (
            "outlives signal intent",
            {
                "plan": authority.plan.model_copy(
                    update={
                        "expires_at": authority.intent.expires_at
                        + timedelta(seconds=1)
                    }
                )
            },
        ),
    )
    for message, update in cases:
        with pytest.raises(ValidationError, match=message):
            _validate_authority_update(**update)

    paper_intent = authority.intent.model_copy(update={"mode": TradingMode.PAPER})
    paper_plan = authority.plan.model_copy(update={"mode": TradingMode.PAPER})
    with pytest.raises(ValidationError, match="requires an exchange-order mode"):
        _validate_authority_update(intent=paper_intent, plan=paper_plan)


def test_live_authority_rejects_exposure_and_execution_scope_escalation() -> None:
    authority = _live_authority()
    first, second = authority.plan.instructions
    one_leg_intent = authority.intent.model_copy(update={"legs": authority.intent.legs[:1]})
    one_leg_plan = authority.plan.model_copy(update={"instructions": (first,)})
    with pytest.raises(ValidationError, match="requires exactly two legs"):
        _validate_authority_update(intent=one_leg_intent, plan=one_leg_plan)

    bad_indexes = (
        first.model_copy(update={"leg_index": 1}),
        second.model_copy(update={"leg_index": 2}),
    )
    with pytest.raises(ValidationError, match="indexes must be zero and one"):
        _validate_authority_update(
            plan=authority.plan.model_copy(update={"instructions": bad_indexes})
        )

    bad_side = second.model_copy(update={"side": Side.BUY})
    with pytest.raises(ValidationError, match="changed approved exposure"):
        _validate_authority_update(
            plan=authority.plan.model_copy(update={"instructions": (first, bad_side)})
        )

    oversized = first.model_copy(update={"quantity": Decimal("0.011")})
    with pytest.raises(ValidationError, match="exceeds approved quantity"):
        _validate_authority_update(
            plan=authority.plan.model_copy(update={"instructions": (oversized, second)})
        )

    market = first.model_copy(update={"order_type": OrderType.MARKET})
    with pytest.raises(ValidationError, match="bounded limit orders"):
        _validate_authority_update(
            plan=authority.plan.model_copy(update={"instructions": (market, second)})
        )

    with pytest.raises(ValidationError, match="prices must be positive"):
        _validate_authority_update(
            reference_prices=(Decimal("0"), Decimal("62000"))
        )
