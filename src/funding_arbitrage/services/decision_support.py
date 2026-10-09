"""Fail-closed ML/RL/LLM decision support with no execution authority."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.ai import (
    LLMAction,
    LLMDecisionRequest,
    LLMGatewayResult,
    MetaLabelDecision,
    RLAction,
    RLDecision,
)
from funding_arbitrage.domain.decisions import SignalIntent

ZERO = Decimal("0")
ONE = Decimal("1")


class BoundDecisionSupport(BaseModel):
    """Precomputed AI decisions deterministically bound to one signal intent."""

    model_config = ConfigDict(frozen=True)

    support_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    intent_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_bundle_checksum: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    evaluated_at: datetime
    meta_label: MetaLabelDecision | None = None
    rl: RLDecision | None = None
    llm_request: LLMDecisionRequest | None = None
    llm_result: LLMGatewayResult | None = None

    @field_validator("evaluated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_support(self) -> BoundDecisionSupport:
        if self.meta_label is None and self.rl is None and self.llm_result is None:
            raise ValueError("decision support requires at least one decision")
        if (self.llm_request is None) is not (self.llm_result is None):
            raise ValueError("LLM support requires both request and audited result")
        if self.llm_request is not None and self.llm_result is not None:
            if self.llm_request.signal_id != self.signal_id:
                raise ValueError("LLM support signal identity mismatch")
            audit = self.llm_result.audit
            decision = self.llm_result.decision
            if audit.request_id != self.llm_request.request_id:
                raise ValueError("LLM support request/audit identity mismatch")
            if audit.request_hash != llm_request_fingerprint(self.llm_request):
                raise ValueError("LLM support request/audit fingerprint mismatch")
            if (
                audit.action is not decision.action
                or audit.reason != decision.reason
                or audit.used_fallback is not decision.used_fallback
            ):
                raise ValueError("LLM support decision/audit outcome mismatch")
            if (
                audit.timestamp != self.llm_request.timestamp
                or audit.prompt_template_version
                != self.llm_request.prompt_template_version
                or audit.request_schema_version
                != self.llm_request.request_schema_version
            ):
                raise ValueError("LLM support request/audit metadata mismatch")
            if self.llm_request.timestamp > self.evaluated_at:
                raise ValueError("LLM support request is newer than evaluation")
        expected = _support_id(
            signal_id=self.signal_id,
            intent_fingerprint=self.intent_fingerprint,
            artifact_bundle_checksum=self.artifact_bundle_checksum,
            evaluated_at=self.evaluated_at,
            meta_label=self.meta_label,
            rl=self.rl,
            llm_request=self.llm_request,
            llm_result=self.llm_result,
        )
        if self.support_id != expected:
            raise ValueError("decision support identity checksum mismatch")
        return self

    @classmethod
    def bind(
        cls,
        intent: SignalIntent,
        evaluated_at: datetime,
        *,
        artifact_bundle_checksum: str | None = None,
        meta_label: MetaLabelDecision | None = None,
        rl: RLDecision | None = None,
        llm_request: LLMDecisionRequest | None = None,
        llm_result: LLMGatewayResult | None = None,
    ) -> BoundDecisionSupport:
        timestamp = _utc(evaluated_at)
        fingerprint = intent_fingerprint(intent)
        return cls(
            support_id=_support_id(
                signal_id=intent.signal_id,
                intent_fingerprint=fingerprint,
                artifact_bundle_checksum=artifact_bundle_checksum,
                evaluated_at=timestamp,
                meta_label=meta_label,
                rl=rl,
                llm_request=llm_request,
                llm_result=llm_result,
            ),
            signal_id=intent.signal_id,
            intent_fingerprint=fingerprint,
            artifact_bundle_checksum=artifact_bundle_checksum,
            evaluated_at=timestamp,
            meta_label=meta_label,
            rl=rl,
            llm_request=llm_request,
            llm_result=llm_result,
        )


class DecisionSupportGateConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    llm_reduce_multiplier: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)


class DecisionSupportAssessment(BaseModel):
    """Auditable veto/reduction result consumed by portfolio risk."""

    model_config = ConfigDict(frozen=True)

    assessment_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    evaluated_at: datetime
    accepted: bool
    risk_multiplier: Decimal = Field(ge=0, le=1)
    reasons: tuple[str, ...] = Field(min_length=1)
    support: BoundDecisionSupport

    @field_validator("evaluated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_outcome(self) -> DecisionSupportAssessment:
        if self.signal_id != self.support.signal_id:
            raise ValueError("decision support assessment identity mismatch")
        if self.accepted and self.risk_multiplier <= 0:
            raise ValueError("accepted decision support requires positive risk multiplier")
        if not self.accepted and self.risk_multiplier != 0:
            raise ValueError("rejected decision support requires zero risk multiplier")
        expected = _assessment_id(
            self.support.support_id,
            self.accepted,
            self.risk_multiplier,
            self.reasons,
        )
        if self.assessment_id != expected:
            raise ValueError("decision support assessment checksum mismatch")
        return self


class DecisionSupportGate:
    """AI may veto or reduce; it can never create or increase exposure."""

    def __init__(self, config: DecisionSupportGateConfig | None = None) -> None:
        self.config = config or DecisionSupportGateConfig()

    def assess(
        self,
        intent: SignalIntent,
        support: BoundDecisionSupport,
        timestamp: datetime,
    ) -> DecisionSupportAssessment:
        now = _utc(timestamp)
        if support.signal_id != intent.signal_id:
            raise ValueError("decision support signal identity mismatch")
        if support.intent_fingerprint != intent_fingerprint(intent):
            raise ValueError("decision support intent fingerprint mismatch")
        if support.evaluated_at < intent.created_at:
            raise ValueError("decision support predates signal intent")
        if support.evaluated_at > now:
            raise ValueError("decision support evaluation is in the future")
        if now >= intent.expires_at:
            raise ValueError("decision support cannot evaluate an expired signal")
        if support.llm_request is not None:
            if support.llm_request.strategy_id != intent.strategy_id:
                raise ValueError("LLM support strategy identity mismatch")
            if support.llm_request.timestamp < intent.created_at:
                raise ValueError("LLM support request predates signal intent")

        multiplier = ONE
        reasons: list[str] = []
        vetoes: list[str] = []
        meta_label = support.meta_label
        if meta_label is not None:
            reason = f"meta_label:{meta_label.reason}"
            reasons.append(reason)
            if not meta_label.accepted:
                vetoes.append(reason)

        rl = support.rl
        if rl is not None:
            reason = f"rl:{rl.reason}:{rl.action.value}"
            reasons.append(reason)
            if rl.action is RLAction.CLOSE:
                vetoes.append(reason)
            elif rl.requested_position_fraction_change < 0:
                multiplier = min(
                    multiplier,
                    ONE + rl.requested_position_fraction_change,
                )
            elif rl.requested_position_fraction_change > 0:
                reasons.append("rl:risk_increase_ignored")

        llm_result = support.llm_result
        if llm_result is not None:
            decision = llm_result.decision
            reason = f"llm:{decision.reason}:{decision.action.value}"
            reasons.append(reason)
            if decision.action is LLMAction.REJECT:
                vetoes.append(reason)
            elif decision.action is LLMAction.REDUCE:
                multiplier = min(multiplier, self.config.llm_reduce_multiplier)

        if not reasons:
            raise ValueError("decision support has no usable decisions")
        accepted = not vetoes
        if not accepted:
            multiplier = ZERO
        normalized_reasons = tuple(dict.fromkeys((*reasons, *vetoes)))
        return DecisionSupportAssessment(
            assessment_id=_assessment_id(
                support.support_id,
                accepted,
                multiplier,
                normalized_reasons,
            ),
            signal_id=intent.signal_id,
            evaluated_at=now,
            accepted=accepted,
            risk_multiplier=multiplier,
            reasons=normalized_reasons,
            support=support,
        )


def intent_fingerprint(intent: SignalIntent) -> str:
    payload = json.dumps(
        intent.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def llm_request_fingerprint(request: LLMDecisionRequest) -> str:
    """Match the canonical request hash recorded by the audited LLM gateway."""

    payload = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _support_id(
    *,
    signal_id: str,
    intent_fingerprint: str,
    artifact_bundle_checksum: str | None,
    evaluated_at: datetime,
    meta_label: MetaLabelDecision | None,
    rl: RLDecision | None,
    llm_request: LLMDecisionRequest | None,
    llm_result: LLMGatewayResult | None,
) -> str:
    payload: dict[str, object] = {
        "signal_id": signal_id,
        "intent_fingerprint": intent_fingerprint,
        "evaluated_at": _utc(evaluated_at).isoformat(),
        "meta_label": _dump(meta_label),
        "rl": _dump(rl),
        "llm_request": _dump(llm_request),
        "llm_result": _dump(llm_result),
    }
    if artifact_bundle_checksum is not None:
        payload["artifact_bundle_checksum"] = artifact_bundle_checksum
    return _stable_id("support", payload)


def _assessment_id(
    support_id: str,
    accepted: bool,
    risk_multiplier: Decimal,
    reasons: tuple[str, ...],
) -> str:
    return _stable_id(
        "support_assessment",
        {
            "support_id": support_id,
            "accepted": accepted,
            "risk_multiplier": str(risk_multiplier),
            "reasons": reasons,
        },
    )


def _dump(model: BaseModel | None) -> dict[str, object] | None:
    if model is None:
        return None
    payload: dict[str, object] = model.model_dump(mode="json")
    if (
        isinstance(model, MetaLabelDecision)
        and model.maximum_feature_age_seconds is None
    ):
        payload.pop("maximum_feature_age_seconds", None)
    return payload


def _stable_id(prefix: str, payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
