"""The LLM decision component is callable, budgeted, and advisory only."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from funding_arbitrage.ai.llm_gateway import (
    RESPONSE_SCHEMA_VERSION,
    LLMAction,
    LLMDecisionRequest,
    LLMFallback,
    LLMRationaleCode,
)
from funding_arbitrage.ai.llm_providers import (
    AnthropicMessagesProvider,
    LLMProviderError,
    build_prompt,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.services.runtime_llm import (
    build_llm_gateway,
    build_llm_gateway_config,
    build_llm_provider,
    llm_decision_support_enabled,
    llm_live_authorized,
)

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)


def _request() -> LLMDecisionRequest:
    return LLMDecisionRequest(
        request_id="llm-req-1",
        prompt_template_version="anthropic-decision-v1",
        timestamp=NOW,
        signal_id="signal-1",
        strategy_id="orderflow-breakout-v1",
        regime=MarketRegime.TREND_UP,
        expected_move_bps=Decimal("35"),
        estimated_cost_bps=Decimal("8"),
        quality_score=Decimal("74"),
        features={"ofi_zscore_5s": Decimal("1.4")},
    )


def _enabled_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "DECISION_SUPPORT_ENABLED": True,
        "DECISION_SUPPORT_ARTIFACT_SHA256": "a" * 64,
        "DECISION_SUPPORT_META_LABEL_ENABLED": True,
        "DECISION_SUPPORT_LLM_ENABLED": True,
        "DECISION_SUPPORT_LLM_API_KEY": "test-api-key",
        "DECISION_SUPPORT_LLM_DAILY_BUDGET_USD": "2.50",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def _anthropic_payload(
    *,
    action: str = "PASS",
    confidence: str = "0.8",
    rationale: str = "EDGE_CONFIRMED",
    input_tokens: int = 400,
    output_tokens: int = 20,
) -> dict[str, Any]:
    return {
        "model": "claude-sonnet-5",
        "content": [
            {
                "type": "text",
                "text": json.dumps(
                    {
                        "action": action,
                        "confidence": confidence,
                        "rationale_code": rationale,
                    }
                ),
            }
        ],
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _provider(client: httpx.AsyncClient) -> AnthropicMessagesProvider:
    return AnthropicMessagesProvider(
        base_url="https://api.anthropic.com",
        model="claude-sonnet-5",
        api_key="test-api-key",
        maximum_output_tokens=512,
        input_usd_per_mtok=Decimal("3"),
        output_usd_per_mtok=Decimal("15"),
        client=client,
    )


def test_the_default_runtime_holds_no_llm_gateway() -> None:
    settings = Settings(_env_file=None)
    assert llm_decision_support_enabled(settings) is False
    assert build_llm_provider(settings) is None
    assert build_llm_gateway(settings) is None


def test_live_llm_authority_needs_the_capability_name() -> None:
    without = _enabled_settings()
    assert llm_live_authorized(without) is False
    assert build_llm_gateway_config(without).live_enabled is False

    with_authorization = _enabled_settings(
        DANGEROUS_CAPABILITY_AUTHORIZATION="live_llm_decisions"
    )
    assert llm_live_authorized(with_authorization) is True
    assert build_llm_gateway_config(with_authorization).live_enabled is True


def test_the_gateway_config_binds_the_configured_budget_and_model() -> None:
    config = build_llm_gateway_config(
        _enabled_settings(
            DECISION_SUPPORT_LLM_MODEL="claude-sonnet-5",
            DECISION_SUPPORT_LLM_TIMEOUT_SECONDS=3,
            DECISION_SUPPORT_LLM_MAXIMUM_DAILY_CALLS=7,
        )
    )
    assert config.enabled is True
    assert config.allowed_models == frozenset({"claude-sonnet-5"})
    assert config.maximum_latency_ms == 3000
    assert config.maximum_daily_cost_usd == Decimal("2.50")
    assert config.maximum_daily_calls == 7
    # A provider failure must reject, never pass a signal through untouched.
    assert config.fallback is LLMFallback.REJECT


def test_the_prompt_is_deterministic_and_carries_no_secrets() -> None:
    first = build_prompt(_request())
    assert first == build_prompt(_request())
    decoded = json.loads(first)
    assert decoded["signal_id"] == "signal-1"
    assert decoded["features"] == {"ofi_zscore_5s": "1.4"}
    assert "api_key" not in first.lower()


async def test_a_structured_reply_is_parsed_with_measured_cost() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-api-key"] == "test-api-key"
        assert request.url.path == "/v1/messages"
        return httpx.Response(200, json=_anthropic_payload())

    async with _client(handler) as client:
        response = await _provider(client).decide(_request(), timeout_seconds=5)

    assert response.response_schema_version == RESPONSE_SCHEMA_VERSION
    assert response.action is LLMAction.PASS
    assert response.confidence == Decimal("0.8")
    assert response.rationale_code is LLMRationaleCode.EDGE_CONFIRMED
    assert response.prompt_tokens == 400
    assert response.completion_tokens == 20
    # 400 input at $3/Mtok plus 20 output at $15/Mtok.
    assert response.cost_usd == Decimal("0.0015")


@pytest.mark.parametrize(
    "payload",
    [
        {"model": "claude-sonnet-5", "content": [], "usage": {}},
        {
            "model": "claude-sonnet-5",
            "content": [{"type": "text", "text": "not json"}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
        },
        {
            "model": "claude-sonnet-5",
            "content": [{"type": "text", "text": '{"action": "BUY_EVERYTHING"}'}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
        },
        _anthropic_payload(confidence="1.5"),
        _anthropic_payload(input_tokens=-1),
    ],
)
async def test_a_malformed_reply_is_rejected(payload: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with _client(handler) as client:
        with pytest.raises(LLMProviderError):
            await _provider(client).decide(_request(), timeout_seconds=5)


async def test_a_provider_error_never_leaks_the_response_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "invalid x-api-key sk-secret"})

    async with _client(handler) as client:
        with pytest.raises(LLMProviderError) as excinfo:
            await _provider(client).decide(_request(), timeout_seconds=5)
    assert "sk-secret" not in str(excinfo.value)
    assert "401" in str(excinfo.value)


async def test_the_gateway_never_grants_execution_authority() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_anthropic_payload())

    async with _client(handler) as client:
        gateway = build_llm_gateway(_enabled_settings(), client=client)
        assert gateway is not None
        result = await gateway.decide(_request(), TradingMode.PAPER)

    assert result.decision.execution_authorized is False
    assert result.decision.action is LLMAction.PASS
    assert result.decision.used_fallback is False


async def test_an_unauthorized_live_call_falls_back_deterministically() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("the gateway must not call the provider")

    async with _client(handler) as client:
        gateway = build_llm_gateway(_enabled_settings(), client=client)
        assert gateway is not None
        result = await gateway.decide(_request(), TradingMode.LIVE)

    assert result.decision.used_fallback is True
    assert result.decision.action is LLMAction.REJECT
    assert result.decision.execution_authorized is False
