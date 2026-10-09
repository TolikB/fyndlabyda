"""Construct the guarded LLM decision gateway from the operator's explicit policy.

The gateway's safety envelope already existed; what was missing was any concrete
provider and any code that built either. This module supplies both, and returns
``None`` unless `DECISION_SUPPORT_LLM_ENABLED` is set — so the default runtime
holds no object that can reach an external model.

Live authority is separate again: `live_enabled` additionally requires
`live_llm_decisions` in `DANGEROUS_CAPABILITY_AUTHORIZATION`. Even then the
gateway's own contract forbids execution authority, so an LLM can only ever
narrow a signal the strategies and risk authority already produced.
"""

from __future__ import annotations

from decimal import Decimal

import httpx

from funding_arbitrage.ai.llm_gateway import (
    GuardedLLMGateway,
    LLMFallback,
    LLMGatewayConfig,
    LLMProvider,
)
from funding_arbitrage.ai.llm_providers import AnthropicMessagesProvider
from funding_arbitrage.config import Settings

MILLISECONDS_PER_SECOND = 1000


def llm_decision_support_enabled(settings: Settings) -> bool:
    return settings.decision_support_llm_enabled


def llm_live_authorized(settings: Settings) -> bool:
    return (
        llm_decision_support_enabled(settings)
        and "live_llm_decisions" in settings.authorized_dangerous_capabilities
    )


def build_llm_gateway_config(settings: Settings) -> LLMGatewayConfig:
    return LLMGatewayConfig(
        enabled=llm_decision_support_enabled(settings),
        live_enabled=llm_live_authorized(settings),
        allowed_models=frozenset({settings.decision_support_llm_model}),
        maximum_latency_ms=max(
            1,
            int(settings.decision_support_llm_timeout_seconds * MILLISECONDS_PER_SECOND),
        ),
        maximum_completion_tokens=settings.decision_support_llm_maximum_output_tokens,
        maximum_daily_cost_usd=settings.decision_support_llm_daily_budget_usd,
        maximum_daily_calls=settings.decision_support_llm_maximum_daily_calls,
        # A provider failure must never silently pass a signal through.
        fallback=LLMFallback.REJECT,
    )


def build_llm_provider(
    settings: Settings,
    *,
    client: httpx.AsyncClient | None = None,
) -> LLMProvider | None:
    if not llm_decision_support_enabled(settings):
        return None
    return AnthropicMessagesProvider(
        base_url=settings.decision_support_llm_base_url,
        model=settings.decision_support_llm_model,
        api_key=settings.decision_support_llm_api_key.get_secret_value(),
        maximum_output_tokens=settings.decision_support_llm_maximum_output_tokens,
        input_usd_per_mtok=settings.decision_support_llm_input_usd_per_mtok,
        output_usd_per_mtok=settings.decision_support_llm_output_usd_per_mtok,
        client=client,
    )


def build_llm_gateway(
    settings: Settings,
    *,
    provider: LLMProvider | None = None,
    client: httpx.AsyncClient | None = None,
) -> GuardedLLMGateway | None:
    """Return a configured gateway, or nothing when LLM support is disabled."""

    if not llm_decision_support_enabled(settings):
        return None
    resolved = provider or build_llm_provider(settings, client=client)
    if resolved is None:
        return None
    return GuardedLLMGateway(resolved, build_llm_gateway_config(settings))


def maximum_request_cost_usd(settings: Settings) -> Decimal:
    """The most one call can cost at the configured prices and token bound."""

    return (
        Decimal(settings.decision_support_llm_maximum_output_tokens)
        * settings.decision_support_llm_output_usd_per_mtok
    ) / Decimal("1000000")
