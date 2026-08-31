from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.ai import (
    LLMAction,
    LLMAuditRecord,
    LLMBudgetSnapshot,
    LLMDecision,
    LLMDecisionRequest,
    LLMGatewayResult,
    MetaLabelDecision,
    RLAction,
    RLDecision,
)
from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.services.decision_support import (
    BoundDecisionSupport,
    DecisionSupportGate,
    llm_request_fingerprint,
)

NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _intent() -> SignalIntent:
    return SignalIntent(
        signal_id="decision-support-signal",
        strategy_id="decision-support-strategy",
        mode=TradingMode.REPLAY,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=INSTRUMENT,
        side=Side.BUY,
        legs=(SignalLeg(instrument=INSTRUMENT, side=Side.BUY),),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        entry_zone_low=Decimal("100"),
        entry_zone_high=Decimal("101"),
        structural_stop=Decimal("98"),
        targets=(Decimal("105"),),
        expected_holding_seconds=900,
        expected_move_bps=Decimal("100"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _meta(*, accepted: bool = True) -> MetaLabelDecision:
    return MetaLabelDecision(
        decision_id="meta-1",
        accepted=accepted,
        probability=Decimal("0.8"),
        used_fallback=False,
        reason="meta_label_pass" if accepted else "meta_label_reject",
        model_version="meta-v1",
    )


def _rl(action: RLAction) -> RLDecision:
    return RLDecision(
        decision_id=f"rl-{action.value}",
        action=action,
        requested_position_fraction_change=action.position_fraction_change,
        used_fallback=False,
        reason="rl_policy_action",
        policy_version="rl-v1",
    )


def _llm(
    intent: SignalIntent,
    action: LLMAction,
) -> tuple[LLMDecisionRequest, LLMGatewayResult]:
    request = LLMDecisionRequest(
        request_id=f"llm-{action.value}",
        prompt_template_version="decision-support-v1",
        timestamp=NOW,
        signal_id=intent.signal_id,
        strategy_id=intent.strategy_id,
        regime=intent.regime,
        expected_move_bps=intent.expected_move_bps,
        estimated_cost_bps=intent.estimated_cost_bps,
        quality_score=intent.quality_score,
    )
    decision = LLMDecision(
        action=action,
        confidence=Decimal("0.9"),
        used_fallback=False,
        reason="llm_structured_decision",
        model_id="allowlisted-model",
    )
    result = LLMGatewayResult(
        decision=decision,
        audit=LLMAuditRecord(
            audit_id=f"audit-{action.value}",
            timestamp=NOW,
            request_id=request.request_id,
            request_hash=llm_request_fingerprint(request),
            response_hash="b" * 64,
            prompt_template_version=request.prompt_template_version,
            request_schema_version=request.request_schema_version,
            response_schema_version="llm-decision-response-v1",
            model_id="allowlisted-model",
            action=action,
            reason=decision.reason,
            used_fallback=False,
            latency_ms=10,
            prompt_tokens=10,
            completion_tokens=5,
            charged_cost_usd=Decimal("0.001"),
            budget=LLMBudgetSnapshot(
                utc_date=NOW.date(),
                calls=1,
                charged_usd=Decimal("0.001"),
            ),
        ),
    )
    return request, result


def test_support_reductions_are_bounded_and_risk_increase_is_ignored() -> None:
    intent = _intent()
    llm_request, llm_result = _llm(intent, LLMAction.REDUCE)
    support = BoundDecisionSupport.bind(
        intent,
        NOW,
        meta_label=_meta(),
        rl=_rl(RLAction.REDUCE_25),
        llm_request=llm_request,
        llm_result=llm_result,
    )

    assessment = DecisionSupportGate().assess(intent, support, NOW)
    increase = DecisionSupportGate().assess(
        intent,
        BoundDecisionSupport.bind(
            intent,
            NOW,
            rl=_rl(RLAction.INCREASE_10),
        ),
        NOW,
    )

    assert assessment.accepted is True
    assert assessment.risk_multiplier == Decimal("0.50")
    assert assessment.support.support_id == support.support_id
    assert increase.accepted is True
    assert increase.risk_multiplier == Decimal("1")
    assert "rl:risk_increase_ignored" in increase.reasons


@pytest.mark.parametrize("source", ["meta", "rl", "llm"])
def test_any_ai_veto_rejects_without_execution_authority(source: str) -> None:
    intent = _intent()
    kwargs: dict[str, object]
    if source == "meta":
        kwargs = {"meta_label": _meta(accepted=False)}
    elif source == "rl":
        kwargs = {"rl": _rl(RLAction.CLOSE)}
    else:
        request, result = _llm(intent, LLMAction.REJECT)
        kwargs = {"llm_request": request, "llm_result": result}
    support = BoundDecisionSupport.bind(intent, NOW, **kwargs)  # type: ignore[arg-type]

    assessment = DecisionSupportGate().assess(intent, support, NOW)

    assert assessment.accepted is False
    assert assessment.risk_multiplier == 0


def test_support_identity_time_and_llm_binding_fail_closed() -> None:
    intent = _intent()
    support = BoundDecisionSupport.bind(intent, NOW, meta_label=_meta())
    corrupted = support.model_dump(mode="json")
    corrupted["support_id"] = "support_corrupted"

    with pytest.raises(ValidationError, match="identity checksum mismatch"):
        BoundDecisionSupport.model_validate(corrupted)
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        DecisionSupportGate().assess(
            intent.model_copy(update={"confidence": Decimal("0.8")}),
            support,
            NOW,
        )
    with pytest.raises(ValueError, match="in the future"):
        DecisionSupportGate().assess(
            intent,
            BoundDecisionSupport.bind(
                intent,
                NOW + timedelta(seconds=1),
                meta_label=_meta(),
            ),
            NOW,
        )

    request, result = _llm(intent, LLMAction.PASS)
    mismatched_request = request.model_copy(update={"strategy_id": "other"})
    mismatched_result = result.model_copy(
        update={
            "audit": result.audit.model_copy(
                update={
                    "request_hash": llm_request_fingerprint(mismatched_request)
                }
            )
        }
    )
    mismatched = BoundDecisionSupport.bind(
        intent,
        NOW,
        llm_request=mismatched_request,
        llm_result=mismatched_result,
    )
    with pytest.raises(ValueError, match="strategy identity mismatch"):
        DecisionSupportGate().assess(intent, mismatched, NOW)


@pytest.mark.parametrize(
    ("audit_update", "message"),
    [
        ({"request_hash": "a" * 64}, "fingerprint mismatch"),
        ({"action": LLMAction.REJECT}, "outcome mismatch"),
        (
            {"timestamp": NOW + timedelta(microseconds=1)},
            "metadata mismatch",
        ),
    ],
)
def test_llm_audit_must_match_request_and_decision(
    audit_update: dict[str, object],
    message: str,
) -> None:
    intent = _intent()
    request, result = _llm(intent, LLMAction.PASS)
    corrupted_result = result.model_copy(
        update={"audit": result.audit.model_copy(update=audit_update)}
    )

    with pytest.raises(ValidationError, match=message):
        BoundDecisionSupport.bind(
            intent,
            NOW,
            llm_request=request,
            llm_result=corrupted_result,
        )
