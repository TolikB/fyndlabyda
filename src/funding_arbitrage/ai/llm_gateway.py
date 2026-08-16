"""Typed, budgeted LLM decision support with immutable audit and safe fallback."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import TradingMode

ZERO = Decimal("0")
LIVE_MODES = frozenset({TradingMode.LIMITED_LIVE, TradingMode.LIVE})
REQUEST_SCHEMA_VERSION = "llm-decision-request-v1"
RESPONSE_SCHEMA_VERSION = "llm-decision-response-v1"


class LLMAction(StrEnum):
    PASS = "PASS"
    REJECT = "REJECT"
    REDUCE = "REDUCE"


class LLMRationaleCode(StrEnum):
    EDGE_CONFIRMED = "EDGE_CONFIRMED"
    COST_TOO_HIGH = "COST_TOO_HIGH"
    RISK_CONFLICT = "RISK_CONFLICT"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class LLMFallback(StrEnum):
    REJECT = "REJECT"
    PASS_THROUGH = "PASS_THROUGH"


class LLMDecisionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str = Field(min_length=1)
    request_schema_version: str = REQUEST_SCHEMA_VERSION
    prompt_template_version: str = Field(min_length=1)
    timestamp: datetime
    signal_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    regime: MarketRegime
    expected_move_bps: Decimal
    estimated_cost_bps: Decimal = Field(ge=0)
    quality_score: Decimal = Field(ge=0, le=100)
    features: dict[str, Decimal] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("features")
    @classmethod
    def validate_safe_features(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        normalized: dict[str, Decimal] = {}
        forbidden = ("secret", "token", "password", "private", "api_key", "apikey")
        for raw_name, feature in value.items():
            name = raw_name.strip().lower()
            if not name or any(fragment in name for fragment in forbidden):
                raise ValueError("LLM request contains a forbidden feature name")
            if not feature.is_finite():
                raise ValueError("LLM request features must be finite")
            normalized[name] = feature
        if len(normalized) != len(value):
            raise ValueError("LLM request feature names must be unique")
        return normalized

    @model_validator(mode="after")
    def require_schema_version(self) -> LLMDecisionRequest:
        if self.request_schema_version != REQUEST_SCHEMA_VERSION:
            raise ValueError("unsupported LLM request schema version")
        return self


class LLMStructuredResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    response_schema_version: str
    model_id: str = Field(min_length=1)
    action: LLMAction
    confidence: Decimal = Field(ge=0, le=1)
    rationale_code: LLMRationaleCode
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    cost_usd: Decimal = Field(ge=0)
    latency_ms: int = Field(ge=0)
    received_at: datetime

    @field_validator("received_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class LLMProvider(Protocol):
    async def decide(
        self,
        request: LLMDecisionRequest,
        *,
        timeout_seconds: float,
    ) -> LLMStructuredResponse: ...


class LLMGatewayConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    live_enabled: bool = False
    allowed_models: frozenset[str] = frozenset()
    maximum_latency_ms: int = Field(default=500, gt=0)
    maximum_prompt_tokens: int = Field(default=2000, gt=0)
    maximum_completion_tokens: int = Field(default=300, gt=0)
    maximum_request_cost_usd: Decimal = Field(default=Decimal("0.02"), gt=0)
    maximum_daily_cost_usd: Decimal = Field(default=Decimal("1"), gt=0)
    maximum_daily_calls: int = Field(default=100, gt=0)
    minimum_confidence: Decimal = Field(default=Decimal("0.65"), ge=0, le=1)
    fallback: LLMFallback = LLMFallback.REJECT


class LLMBudgetSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    utc_date: date
    calls: int = Field(ge=0)
    charged_usd: Decimal = Field(ge=0)


class LLMAuditRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    audit_id: str
    timestamp: datetime
    request_id: str
    request_hash: str
    response_hash: str | None = None
    prompt_template_version: str
    request_schema_version: str
    response_schema_version: str | None = None
    model_id: str | None = None
    action: LLMAction
    reason: str
    used_fallback: bool
    latency_ms: int = Field(ge=0)
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    charged_cost_usd: Decimal = Field(ge=0)
    budget: LLMBudgetSnapshot

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class LLMDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: LLMAction
    confidence: Decimal = Field(ge=0, le=1)
    used_fallback: bool
    reason: str
    model_id: str | None = None
    execution_authorized: bool = False

    @model_validator(mode="after")
    def forbid_execution_authority(self) -> LLMDecision:
        if self.execution_authorized:
            raise ValueError("LLM decisions cannot authorize execution")
        return self


class LLMGatewayResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision: LLMDecision
    audit: LLMAuditRecord


class _BudgetLedger:
    def __init__(self) -> None:
        self._date: date | None = None
        self._calls = 0
        self._charged = ZERO
        self._lock = asyncio.Lock()

    async def reserve(
        self,
        timestamp: datetime,
        config: LLMGatewayConfig,
    ) -> tuple[bool, str | None]:
        async with self._lock:
            self._roll(timestamp.date())
            if self._calls >= config.maximum_daily_calls:
                return False, "llm_daily_call_budget_exhausted"
            if self._charged + config.maximum_request_cost_usd > config.maximum_daily_cost_usd:
                return False, "llm_daily_spend_budget_exhausted"
            self._calls += 1
            self._charged += config.maximum_request_cost_usd
            return True, None

    async def settle(
        self,
        timestamp: datetime,
        config: LLMGatewayConfig,
        actual_cost: Decimal,
    ) -> None:
        async with self._lock:
            self._roll(timestamp.date())
            self._charged += actual_cost - config.maximum_request_cost_usd

    async def snapshot(self, timestamp: datetime) -> LLMBudgetSnapshot:
        async with self._lock:
            self._roll(timestamp.date())
            return LLMBudgetSnapshot(
                utc_date=timestamp.date(),
                calls=self._calls,
                charged_usd=self._charged,
            )

    def _roll(self, current: date) -> None:
        if self._date != current:
            self._date = current
            self._calls = 0
            self._charged = ZERO


class GuardedLLMGateway:
    def __init__(
        self,
        provider: LLMProvider,
        config: LLMGatewayConfig | None = None,
    ) -> None:
        self.provider = provider
        self.config = config or LLMGatewayConfig()
        self._budget = _BudgetLedger()

    async def decide(
        self,
        request: LLMDecisionRequest,
        mode: TradingMode,
        *,
        operator_authorized: bool = False,
    ) -> LLMGatewayResult:
        preflight = self._preflight(mode, operator_authorized)
        if preflight is not None:
            return await self._fallback(request, preflight)
        reserved, budget_reason = await self._budget.reserve(request.timestamp, self.config)
        if not reserved:
            assert budget_reason is not None
            return await self._fallback(request, budget_reason)
        timeout_seconds = self.config.maximum_latency_ms / 1000
        try:
            async with asyncio.timeout(timeout_seconds):
                response = await self.provider.decide(
                    request,
                    timeout_seconds=timeout_seconds,
                )
        except TimeoutError:
            return await self._fallback(
                request,
                "llm_timeout",
                charged_cost=self.config.maximum_request_cost_usd,
                latency_ms=self.config.maximum_latency_ms,
            )
        except Exception:
            return await self._fallback(
                request,
                "llm_provider_error",
                charged_cost=self.config.maximum_request_cost_usd,
            )
        await self._budget.settle(request.timestamp, self.config, response.cost_usd)
        rejection = self._response_rejection(request, response)
        if rejection is not None:
            return await self._fallback_from_response(request, response, rejection)
        decision = LLMDecision(
            action=response.action,
            confidence=response.confidence,
            used_fallback=False,
            reason="llm_structured_decision",
            model_id=response.model_id,
        )
        return LLMGatewayResult(
            decision=decision,
            audit=await self._audit(
                request,
                decision,
                response=response,
                charged_cost=response.cost_usd,
            ),
        )

    def _preflight(
        self,
        mode: TradingMode,
        operator_authorized: bool,
    ) -> str | None:
        if not self.config.enabled:
            return "llm_gateway_disabled"
        if mode in LIVE_MODES and not (
            self.config.live_enabled and operator_authorized
        ):
            return "llm_live_not_authorized"
        if not self.config.allowed_models:
            return "llm_model_allowlist_empty"
        return None

    def _response_rejection(
        self,
        request: LLMDecisionRequest,
        response: LLMStructuredResponse,
    ) -> str | None:
        if response.response_schema_version != RESPONSE_SCHEMA_VERSION:
            return "llm_response_schema_mismatch"
        if response.model_id not in self.config.allowed_models:
            return "llm_model_not_allowed"
        if response.received_at < request.timestamp:
            return "llm_response_timestamp_invalid"
        if response.latency_ms > self.config.maximum_latency_ms:
            return "llm_latency_budget_exceeded"
        if response.prompt_tokens > self.config.maximum_prompt_tokens:
            return "llm_prompt_token_budget_exceeded"
        if response.completion_tokens > self.config.maximum_completion_tokens:
            return "llm_completion_token_budget_exceeded"
        if response.cost_usd > self.config.maximum_request_cost_usd:
            return "llm_request_cost_budget_exceeded"
        if response.confidence < self.config.minimum_confidence:
            return "llm_confidence_below_threshold"
        return None

    async def _fallback_from_response(
        self,
        request: LLMDecisionRequest,
        response: LLMStructuredResponse,
        reason: str,
    ) -> LLMGatewayResult:
        decision = self._fallback_decision(reason)
        return LLMGatewayResult(
            decision=decision,
            audit=await self._audit(
                request,
                decision,
                response=response,
                charged_cost=response.cost_usd,
            ),
        )

    async def _fallback(
        self,
        request: LLMDecisionRequest,
        reason: str,
        *,
        charged_cost: Decimal = ZERO,
        latency_ms: int = 0,
    ) -> LLMGatewayResult:
        decision = self._fallback_decision(reason)
        return LLMGatewayResult(
            decision=decision,
            audit=await self._audit(
                request,
                decision,
                charged_cost=charged_cost,
                latency_ms=latency_ms,
            ),
        )

    def _fallback_decision(self, reason: str) -> LLMDecision:
        action = (
            LLMAction.PASS
            if self.config.fallback is LLMFallback.PASS_THROUGH
            else LLMAction.REJECT
        )
        return LLMDecision(
            action=action,
            confidence=ZERO,
            used_fallback=True,
            reason=reason,
        )

    async def _audit(
        self,
        request: LLMDecisionRequest,
        decision: LLMDecision,
        *,
        response: LLMStructuredResponse | None = None,
        charged_cost: Decimal,
        latency_ms: int = 0,
    ) -> LLMAuditRecord:
        request_hash = _hash_json(request.model_dump(mode="json"))
        response_hash = (
            _hash_json(response.model_dump(mode="json")) if response is not None else None
        )
        budget = await self._budget.snapshot(request.timestamp)
        audit_payload = {
            "action": decision.action,
            "budget": budget,
            "reason": decision.reason,
            "request_hash": request_hash,
            "response_hash": response_hash,
        }
        return LLMAuditRecord(
            audit_id="llmaudit_" + _hash_json(audit_payload)[:32],
            timestamp=request.timestamp,
            request_id=request.request_id,
            request_hash=request_hash,
            response_hash=response_hash,
            prompt_template_version=request.prompt_template_version,
            request_schema_version=request.request_schema_version,
            response_schema_version=(
                response.response_schema_version if response is not None else None
            ),
            model_id=response.model_id if response is not None else None,
            action=decision.action,
            reason=decision.reason,
            used_fallback=decision.used_fallback,
            latency_ms=response.latency_ms if response is not None else latency_ms,
            prompt_tokens=response.prompt_tokens if response is not None else 0,
            completion_tokens=response.completion_tokens if response is not None else 0,
            charged_cost_usd=charged_cost,
            budget=budget,
        )


def _hash_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
