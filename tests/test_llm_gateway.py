from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.ai import (
    GuardedLLMGateway,
    LLMAction,
    LLMDecisionRequest,
    LLMGatewayConfig,
    LLMRationaleCode,
    LLMStructuredResponse,
)
from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import TradingMode

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


class MockProvider:
    def __init__(
        self,
        response: LLMStructuredResponse,
        *,
        delay_seconds: float = 0,
        fail: bool = False,
    ) -> None:
        self.response = response
        self.delay_seconds = delay_seconds
        self.fail = fail
        self.calls = 0

    async def decide(
        self,
        request: LLMDecisionRequest,
        *,
        timeout_seconds: float,
    ) -> LLMStructuredResponse:
        del request, timeout_seconds
        self.calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.fail:
            raise RuntimeError("provider failed")
        return self.response


def _request(**updates: object) -> LLMDecisionRequest:
    values: dict[str, object] = {
        "request_id": "request-1",
        "prompt_template_version": "trade-review-v3",
        "timestamp": NOW,
        "signal_id": "signal-1",
        "strategy_id": "funding-basis-v1",
        "regime": MarketRegime.RANGE,
        "expected_move_bps": Decimal("40"),
        "estimated_cost_bps": Decimal("10"),
        "quality_score": Decimal("90"),
        "features": {"funding_persistence": Decimal("0.8")},
    }
    values.update(updates)
    return LLMDecisionRequest(**values)


def _response(**updates: object) -> LLMStructuredResponse:
    values: dict[str, object] = {
        "response_schema_version": "llm-decision-response-v1",
        "model_id": "approved-model-v1",
        "action": LLMAction.PASS,
        "confidence": Decimal("0.9"),
        "rationale_code": LLMRationaleCode.EDGE_CONFIRMED,
        "prompt_tokens": 100,
        "completion_tokens": 20,
        "cost_usd": Decimal("0.01"),
        "latency_ms": 100,
        "received_at": NOW + timedelta(milliseconds=100),
    }
    values.update(updates)
    return LLMStructuredResponse(**values)


@pytest.mark.asyncio
async def test_llm_gateway_is_disabled_by_default_without_calling_provider() -> None:
    provider = MockProvider(_response())
    result = await GuardedLLMGateway(provider).decide(
        _request(), TradingMode.PAPER
    )

    assert provider.calls == 0
    assert result.decision.action is LLMAction.REJECT
    assert result.decision.reason == "llm_gateway_disabled"
    assert result.decision.execution_authorized is False
    assert result.audit.used_fallback is True
    assert result.audit.budget.calls == 0


@pytest.mark.asyncio
async def test_structured_response_is_schema_allowlist_and_budget_audited() -> None:
    provider = MockProvider(_response())
    gateway = GuardedLLMGateway(
        provider,
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
        ),
    )

    result = await gateway.decide(_request(), TradingMode.PAPER)

    assert result.decision.action is LLMAction.PASS
    assert result.decision.used_fallback is False
    assert result.decision.confidence == Decimal("0.9")
    assert result.audit.request_schema_version == "llm-decision-request-v1"
    assert result.audit.response_schema_version == "llm-decision-response-v1"
    assert result.audit.prompt_template_version == "trade-review-v3"
    assert len(result.audit.request_hash) == 64
    assert result.audit.response_hash is not None
    assert result.audit.charged_cost_usd == Decimal("0.01")
    assert result.audit.budget.calls == 1
    assert result.audit.budget.charged_usd == Decimal("0.01")


@pytest.mark.asyncio
async def test_timeout_schema_token_and_model_failures_use_deterministic_fallback() -> None:
    timeout_gateway = GuardedLLMGateway(
        MockProvider(_response(), delay_seconds=0.02),
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
            maximum_latency_ms=5,
        ),
    )
    timeout = await timeout_gateway.decide(_request(), TradingMode.PAPER)

    bad_schema = await GuardedLLMGateway(
        MockProvider(_response(response_schema_version="v999")),
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
        ),
    ).decide(_request(), TradingMode.PAPER)
    bad_tokens = await GuardedLLMGateway(
        MockProvider(_response(prompt_tokens=2001)),
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
        ),
    ).decide(_request(), TradingMode.PAPER)
    bad_model = await GuardedLLMGateway(
        MockProvider(_response(model_id="unapproved-model")),
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
        ),
    ).decide(_request(), TradingMode.PAPER)

    assert timeout.decision.reason == "llm_timeout"
    assert timeout.audit.charged_cost_usd == Decimal("0.02")
    assert bad_schema.decision.reason == "llm_response_schema_mismatch"
    assert bad_tokens.decision.reason == "llm_prompt_token_budget_exceeded"
    assert bad_model.decision.reason == "llm_model_not_allowed"
    assert all(
        result.decision.action is LLMAction.REJECT
        for result in (timeout, bad_schema, bad_tokens, bad_model)
    )


@pytest.mark.asyncio
async def test_daily_spend_and_live_authorization_are_hard_limits() -> None:
    provider = MockProvider(_response())
    gateway = GuardedLLMGateway(
        provider,
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
            maximum_daily_cost_usd=Decimal("0.03"),
        ),
    )

    first = await gateway.decide(_request(request_id="r1"), TradingMode.PAPER)
    second = await gateway.decide(_request(request_id="r2"), TradingMode.PAPER)
    exhausted = await gateway.decide(_request(request_id="r3"), TradingMode.PAPER)
    live = await GuardedLLMGateway(
        MockProvider(_response()),
        LLMGatewayConfig(
            enabled=True,
            allowed_models=frozenset({"approved-model-v1"}),
        ),
    ).decide(_request(), TradingMode.LIVE, operator_authorized=True)

    assert first.decision.used_fallback is False
    assert second.decision.used_fallback is False
    assert provider.calls == 2
    assert exhausted.decision.reason == "llm_daily_spend_budget_exhausted"
    assert live.decision.reason == "llm_live_not_authorized"


def test_llm_request_rejects_secret_like_or_nonfinite_context() -> None:
    with pytest.raises(ValidationError, match="forbidden feature"):
        _request(features={"api_key": Decimal("1")})
    with pytest.raises(ValidationError, match="finite"):
        _request(features={"edge": Decimal("NaN")})
