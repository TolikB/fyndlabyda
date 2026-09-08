"""A concrete `LLMProvider` over the Anthropic Messages API.

`GuardedLLMGateway` carried the whole safety envelope — budget ledger, timeout,
audit record, structured-response validation, deterministic fallback — but the
`LLMProvider` Protocol had no implementation anywhere, so AI-003 could not make
a call at all.

This provider is the missing half and nothing more. It sends only the fields the
already-validated `LLMDecisionRequest` carries (which forbids secret-named
features), demands a strict JSON object back, and reports real token usage so the
gateway's budget ledger charges measured cost rather than an estimate. It has no
execution authority: its output is advisory input to the gateway, which is itself
advisory input to the risk authority.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from funding_arbitrage.ai.llm_gateway import (
    RESPONSE_SCHEMA_VERSION,
    LLMAction,
    LLMDecisionRequest,
    LLMRationaleCode,
    LLMStructuredResponse,
)

ANTHROPIC_VERSION = "2023-06-01"
PROMPT_TEMPLATE_VERSION = "anthropic-decision-v1"
TOKENS_PER_MILLION = Decimal("1000000")

_SYSTEM_PROMPT = (
    "You review one already-generated trading signal and reply with strict JSON "
    "only. Reply with exactly this object and nothing else: "
    '{"action": "PASS"|"REJECT"|"REDUCE", "confidence": <number 0..1>, '
    '"rationale_code": "EDGE_CONFIRMED"|"COST_TOO_HIGH"|"RISK_CONFLICT"'
    '|"INSUFFICIENT_DATA"}. '
    "You never place, size, or authorize an order; your reply is advisory only. "
    "If the evidence is incomplete, answer INSUFFICIENT_DATA."
)


class LLMProviderError(RuntimeError):
    """The provider could not produce a valid structured decision."""


def build_prompt(request: LLMDecisionRequest) -> str:
    """Render the request deterministically so identical input hashes identically."""

    features = {name: str(value) for name, value in sorted(request.features.items())}
    payload = {
        "request_schema_version": request.request_schema_version,
        "prompt_template_version": request.prompt_template_version,
        "signal_id": request.signal_id,
        "strategy_id": request.strategy_id,
        "regime": request.regime.value,
        "expected_move_bps": str(request.expected_move_bps),
        "estimated_cost_bps": str(request.estimated_cost_bps),
        "quality_score": str(request.quality_score),
        "features": features,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class AnthropicMessagesProvider:
    """Advisory decision provider backed by the Anthropic Messages API."""

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str,
        maximum_output_tokens: int,
        input_usd_per_mtok: Decimal,
        output_usd_per_mtok: Decimal,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("LLM provider requires an API key")
        if not model.strip():
            raise ValueError("LLM provider requires a model identifier")
        if maximum_output_tokens < 1:
            raise ValueError("LLM provider requires a positive output token bound")
        if input_usd_per_mtok < 0 or output_usd_per_mtok < 0:
            raise ValueError("LLM provider prices cannot be negative")
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._maximum_output_tokens = maximum_output_tokens
        self._input_usd_per_mtok = input_usd_per_mtok
        self._output_usd_per_mtok = output_usd_per_mtok
        self._client = client

    def cost_usd(self, prompt_tokens: int, completion_tokens: int) -> Decimal:
        return (
            Decimal(prompt_tokens) * self._input_usd_per_mtok
            + Decimal(completion_tokens) * self._output_usd_per_mtok
        ) / TOKENS_PER_MILLION

    async def decide(
        self,
        request: LLMDecisionRequest,
        *,
        timeout_seconds: float,
    ) -> LLMStructuredResponse:
        body = {
            "model": self._model,
            "max_tokens": self._maximum_output_tokens,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": build_prompt(request)}],
        }
        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        }
        started = datetime.now(UTC)
        client = self._client
        if client is None:
            async with httpx.AsyncClient(timeout=timeout_seconds) as owned:
                payload = await self._post(owned, body, headers, timeout_seconds)
        else:
            payload = await self._post(client, body, headers, timeout_seconds)
        received = datetime.now(UTC)
        latency_ms = int((received - started).total_seconds() * 1000)
        return self._structured_response(payload, latency_ms=latency_ms, received=received)

    async def _post(
        self,
        client: httpx.AsyncClient,
        body: dict[str, Any],
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        try:
            response = await client.post(
                f"{self._base_url}/v1/messages",
                json=body,
                headers=headers,
                timeout=timeout_seconds,
            )
        except httpx.HTTPError as exc:
            raise LLMProviderError("LLM provider request failed") from exc
        if response.status_code != 200:
            # The body may echo request content; the status alone is enough.
            raise LLMProviderError(
                f"LLM provider returned status {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise LLMProviderError("LLM provider returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise LLMProviderError("LLM provider response must be an object")
        return payload

    def _structured_response(
        self,
        payload: dict[str, Any],
        *,
        latency_ms: int,
        received: datetime,
    ) -> LLMStructuredResponse:
        decision = _decision_object(payload)
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            raise LLMProviderError("LLM provider response is missing token usage")
        prompt_tokens = _non_negative_int(usage.get("input_tokens"), "input_tokens")
        completion_tokens = _non_negative_int(usage.get("output_tokens"), "output_tokens")
        try:
            action = LLMAction(str(decision["action"]))
            rationale = LLMRationaleCode(str(decision["rationale_code"]))
        except (KeyError, ValueError) as exc:
            raise LLMProviderError("LLM provider returned an unknown decision") from exc
        try:
            confidence = Decimal(str(decision["confidence"]))
        except (KeyError, InvalidOperation) as exc:
            raise LLMProviderError("LLM provider returned invalid confidence") from exc
        if not confidence.is_finite() or not Decimal("0") <= confidence <= Decimal("1"):
            raise LLMProviderError("LLM provider confidence is outside [0, 1]")
        model_id = payload.get("model")
        if not isinstance(model_id, str) or not model_id.strip():
            raise LLMProviderError("LLM provider response is missing its model id")
        return LLMStructuredResponse(
            response_schema_version=RESPONSE_SCHEMA_VERSION,
            model_id=model_id,
            action=action,
            confidence=confidence,
            rationale_code=rationale,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cost_usd=self.cost_usd(prompt_tokens, completion_tokens),
            latency_ms=latency_ms,
            received_at=received,
        )


def _decision_object(payload: dict[str, Any]) -> dict[str, Any]:
    content = payload.get("content")
    if not isinstance(content, list) or not content:
        raise LLMProviderError("LLM provider response has no content")
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "text":
            continue
        text = block.get("text")
        if not isinstance(text, str):
            continue
        try:
            decoded = json.loads(text)
        except ValueError:
            continue
        if isinstance(decoded, dict):
            return decoded
    raise LLMProviderError("LLM provider returned no structured decision")


def _non_negative_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise LLMProviderError(f"LLM provider {label} must be a non-negative integer")
    return value
