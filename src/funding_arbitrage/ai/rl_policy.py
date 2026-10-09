"""Constrained RL policy inference with offline evidence and deterministic fallback."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import DataQuality, TradingMode

ZERO = Decimal("0")
ONE = Decimal("1")
LIVE_MODES = frozenset({TradingMode.LIMITED_LIVE, TradingMode.LIVE})


class RLAction(StrEnum):
    HOLD = "HOLD"
    REDUCE_25 = "REDUCE_25"
    REDUCE_50 = "REDUCE_50"
    CLOSE = "CLOSE"
    INCREASE_10 = "INCREASE_10"

    @property
    def position_fraction_change(self) -> Decimal:
        return {
            RLAction.HOLD: ZERO,
            RLAction.REDUCE_25: Decimal("-0.25"),
            RLAction.REDUCE_50: Decimal("-0.50"),
            RLAction.CLOSE: Decimal("-1"),
            RLAction.INCREASE_10: Decimal("0.10"),
        }[self]


class RLState(BaseModel):
    model_config = ConfigDict(frozen=True)

    state_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    timestamp: datetime
    features: dict[str, Decimal] = Field(min_length=1)
    data_quality: DataQuality
    regime: MarketRegime
    portfolio_drawdown_fraction: Decimal = Field(ge=0, le=1)
    reconciliation_healthy: bool

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("features")
    @classmethod
    def normalize_features(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        normalized = {name.strip().lower(): feature for name, feature in value.items()}
        if any(not name for name in normalized) or len(normalized) != len(value):
            raise ValueError("RL feature names must be unique and nonblank")
        return normalized


class RLPolicyArtifact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    policy_version: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_schema_version: str = Field(min_length=1)
    feature_names: tuple[str, ...] = Field(min_length=1)
    action_space: tuple[RLAction, ...] = Field(min_length=1)
    action_weights: dict[RLAction, dict[str, Decimal]]
    action_intercepts: dict[RLAction, Decimal]
    trained_at: datetime
    valid_until: datetime
    artifact_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("feature_names")
    @classmethod
    def normalize_feature_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(name.strip().lower() for name in value)
        if any(not name for name in normalized) or len(normalized) != len(set(normalized)):
            raise ValueError("RL feature names must be unique and nonblank")
        return normalized

    @field_validator("trained_at", "valid_until")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_artifact(self) -> RLPolicyArtifact:
        actions = set(self.action_space)
        if len(actions) != len(self.action_space):
            raise ValueError("RL action space cannot contain duplicates")
        if set(self.action_weights) != actions or set(self.action_intercepts) != actions:
            raise ValueError("RL policy parameters must match action space")
        expected_features = set(self.feature_names)
        if any(set(weights) != expected_features for weights in self.action_weights.values()):
            raise ValueError("RL action weights must match feature schema")
        parameters = (
            *self.action_intercepts.values(),
            *(
                value
                for weights in self.action_weights.values()
                for value in weights.values()
            ),
        )
        if any(not value.is_finite() for value in parameters):
            raise ValueError("RL policy artifact contains non-finite parameters")
        if self.valid_until <= self.trained_at:
            raise ValueError("RL policy validity must end after training")
        expected_checksum = _hash_json(_rl_artifact_payload(self))
        if self.artifact_checksum != expected_checksum:
            raise ValueError("RL policy artifact checksum mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        policy_version: str,
        dataset_id: str,
        dataset_checksum: str,
        state_schema_version: str,
        feature_names: tuple[str, ...],
        action_space: tuple[RLAction, ...],
        action_weights: dict[RLAction, dict[str, Decimal]],
        action_intercepts: dict[RLAction, Decimal],
        trained_at: datetime,
        valid_until: datetime,
    ) -> RLPolicyArtifact:
        normalized_trained_at = _utc(trained_at)
        normalized_valid_until = _utc(valid_until)
        provisional = cls.model_construct(
            policy_version=policy_version,
            dataset_id=dataset_id,
            dataset_checksum=dataset_checksum,
            state_schema_version=state_schema_version,
            feature_names=feature_names,
            action_space=action_space,
            action_weights=action_weights,
            action_intercepts=action_intercepts,
            trained_at=normalized_trained_at,
            valid_until=normalized_valid_until,
            artifact_checksum="",
        )
        return cls(
            policy_version=policy_version,
            dataset_id=dataset_id,
            dataset_checksum=dataset_checksum,
            state_schema_version=state_schema_version,
            feature_names=feature_names,
            action_space=action_space,
            action_weights=action_weights,
            action_intercepts=action_intercepts,
            trained_at=normalized_trained_at,
            valid_until=normalized_valid_until,
            artifact_checksum=_hash_json(_rl_artifact_payload(provisional)),
        )


class RLPolicyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    live_enabled: bool = False
    maximum_state_age_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    maximum_drawdown_fraction: Decimal = Field(default=Decimal("0.10"), gt=0, le=1)
    permitted_actions: frozenset[RLAction] = frozenset(
        {RLAction.HOLD, RLAction.REDUCE_25, RLAction.REDUCE_50, RLAction.CLOSE}
    )
    fallback_action: RLAction = RLAction.CLOSE

    @model_validator(mode="after")
    def validate_actions(self) -> RLPolicyConfig:
        if not self.permitted_actions:
            raise ValueError("RL permitted action space cannot be empty")
        if self.fallback_action not in self.permitted_actions:
            raise ValueError("RL fallback action must be permitted")
        return self


class RLDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision_id: str
    action: RLAction
    requested_position_fraction_change: Decimal = Field(ge=-1, le=1)
    used_fallback: bool
    reason: str
    policy_version: str | None = None
    action_scores: dict[RLAction, Decimal] = Field(default_factory=dict)
    execution_authorized: bool = False

    @model_validator(mode="after")
    def forbid_execution_authority(self) -> RLDecision:
        if self.execution_authorized:
            raise ValueError("RL decisions cannot authorize execution")
        if self.requested_position_fraction_change != self.action.position_fraction_change:
            raise ValueError("RL action and fraction change do not match")
        return self


class GuardedRLPolicy:
    def __init__(self, config: RLPolicyConfig | None = None) -> None:
        self.config = config or RLPolicyConfig()

    def decide(
        self,
        state: RLState,
        timestamp: datetime,
        mode: TradingMode,
        artifact: RLPolicyArtifact | None,
        *,
        operator_authorized: bool = False,
    ) -> RLDecision:
        now = _utc(timestamp)
        reason = self._fallback_reason(
            state,
            now,
            mode,
            artifact,
            operator_authorized,
        )
        if reason is not None:
            return self._fallback(state, now, artifact, reason)
        assert artifact is not None
        scores = {
            action: artifact.action_intercepts[action]
            + sum(
                (
                    artifact.action_weights[action][name] * state.features[name]
                    for name in artifact.feature_names
                ),
                ZERO,
            )
            for action in artifact.action_space
        }
        selected = min(
            artifact.action_space,
            key=lambda action: (-scores[action], action.value),
        )
        if selected not in self.config.permitted_actions:
            return self._fallback(state, now, artifact, "rl_action_not_permitted")
        if state.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN} and (
            selected.position_fraction_change > 0
        ):
            return self._fallback(state, now, artifact, "rl_risk_increase_blocked")
        return RLDecision(
            decision_id=_decision_id(state, now, artifact.policy_version, selected.value),
            action=selected,
            requested_position_fraction_change=selected.position_fraction_change,
            used_fallback=False,
            reason="rl_policy_action",
            policy_version=artifact.policy_version,
            action_scores=scores,
        )

    def _fallback_reason(
        self,
        state: RLState,
        timestamp: datetime,
        mode: TradingMode,
        artifact: RLPolicyArtifact | None,
        operator_authorized: bool,
    ) -> str | None:
        if not self.config.enabled:
            return "rl_policy_disabled"
        if mode in LIVE_MODES and not (
            self.config.live_enabled and operator_authorized
        ):
            return "rl_live_not_authorized"
        if state.data_quality is not DataQuality.VALID:
            return "rl_state_quality_not_valid"
        age = Decimal(str((timestamp - state.timestamp).total_seconds()))
        if age < 0:
            return "rl_state_from_future"
        if age > self.config.maximum_state_age_seconds:
            return "rl_state_stale"
        if not state.reconciliation_healthy:
            return "rl_reconciliation_unhealthy"
        if state.portfolio_drawdown_fraction >= self.config.maximum_drawdown_fraction:
            return "rl_drawdown_guardrail"
        if artifact is None:
            return "rl_artifact_missing"
        if artifact.trained_at > timestamp or artifact.valid_until <= timestamp:
            return "rl_artifact_stale"
        if state.schema_version != artifact.state_schema_version:
            return "rl_state_schema_mismatch"
        if set(state.features) != set(artifact.feature_names):
            return "rl_feature_schema_mismatch"
        return None

    def _fallback(
        self,
        state: RLState,
        timestamp: datetime,
        artifact: RLPolicyArtifact | None,
        reason: str,
    ) -> RLDecision:
        action = self.config.fallback_action
        version = artifact.policy_version if artifact is not None else None
        return RLDecision(
            decision_id=_decision_id(state, timestamp, version or "none", reason),
            action=action,
            requested_position_fraction_change=action.position_fraction_change,
            used_fallback=True,
            reason=reason,
            policy_version=version,
        )


class RLOfflineTransition(BaseModel):
    model_config = ConfigDict(frozen=True)

    transition_id: str
    state: RLState
    logged_action: RLAction
    behavior_probability: Decimal = Field(gt=0, le=1)
    reward: Decimal
    episode_id: str


class RLOfflineEvaluationConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    minimum_transitions: int = Field(default=20, gt=0)
    minimum_matched_actions: int = Field(default=5, gt=0)
    minimum_effective_sample_size: Decimal = Field(default=Decimal("5"), gt=0)
    minimum_lower_confidence_reward: Decimal = ZERO
    maximum_drawdown: Decimal = Field(default=Decimal("5"), ge=0)


class RLOfflineEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    policy_version: str
    transition_count: int = Field(ge=0)
    matched_action_count: int = Field(ge=0)
    effective_sample_size: Decimal = Field(ge=0)
    weighted_mean_reward: Decimal
    lower_confidence_reward: Decimal
    maximum_drawdown: Decimal = Field(ge=0)
    passed: bool
    reason: str
    evidence_checksum: str


class RLOfflineEvaluator:
    def __init__(self, config: RLOfflineEvaluationConfig | None = None) -> None:
        self.config = config or RLOfflineEvaluationConfig()

    def evaluate(
        self,
        artifact: RLPolicyArtifact,
        transitions: tuple[RLOfflineTransition, ...],
    ) -> RLOfflineEvaluation:
        weighted_rewards: list[Decimal] = []
        weights: list[Decimal] = []
        cumulative = ZERO
        peak = ZERO
        max_drawdown = ZERO
        ordered_transitions = tuple(
            sorted(transitions, key=lambda item: item.transition_id)
        )
        for transition in ordered_transitions:
            target = _artifact_action(artifact, transition.state)
            weight = (
                ONE / transition.behavior_probability
                if target is transition.logged_action
                else ZERO
            )
            if weight > 0:
                weights.append(weight)
                weighted_reward = weight * transition.reward
                weighted_rewards.append(weighted_reward)
                cumulative += transition.reward
                peak = max(peak, cumulative)
                max_drawdown = max(max_drawdown, peak - cumulative)
        total_weight = sum(weights, ZERO)
        matched = len(weights)
        mean = (
            sum(weighted_rewards, ZERO) / total_weight
            if total_weight > 0
            else ZERO
        )
        effective_sample_size = (
            total_weight**2 / sum((weight**2 for weight in weights), ZERO)
            if weights
            else ZERO
        )
        rewards = [
            weighted / weight
            for weighted, weight in zip(weighted_rewards, weights, strict=True)
        ]
        if len(rewards) > 1:
            variance = sum(((reward - mean) ** 2 for reward in rewards), ZERO) / Decimal(
                len(rewards) - 1
            )
            standard_error = variance.sqrt() / Decimal(str(math.sqrt(len(rewards))))
            lower = mean - Decimal("1.96") * standard_error
        else:
            lower = mean
        reason = self._reason(
            len(transitions),
            matched,
            effective_sample_size,
            lower,
            max_drawdown,
        )
        evidence = {
            "artifact": artifact.artifact_checksum,
            "transitions": [
                item.model_dump(mode="json") for item in ordered_transitions
            ],
        }
        return RLOfflineEvaluation(
            policy_version=artifact.policy_version,
            transition_count=len(transitions),
            matched_action_count=matched,
            effective_sample_size=effective_sample_size,
            weighted_mean_reward=mean,
            lower_confidence_reward=lower,
            maximum_drawdown=max_drawdown,
            passed=reason == "offline_acceptance_passed",
            reason=reason,
            evidence_checksum=_hash_json(evidence),
        )

    def _reason(
        self,
        transitions: int,
        matched: int,
        effective_sample_size: Decimal,
        lower_reward: Decimal,
        max_drawdown: Decimal,
    ) -> str:
        if transitions < self.config.minimum_transitions:
            return "insufficient_offline_transitions"
        if matched < self.config.minimum_matched_actions:
            return "insufficient_matched_actions"
        if effective_sample_size < self.config.minimum_effective_sample_size:
            return "insufficient_effective_sample_size"
        if lower_reward < self.config.minimum_lower_confidence_reward:
            return "offline_reward_guardrail_failed"
        if max_drawdown > self.config.maximum_drawdown:
            return "offline_drawdown_guardrail_failed"
        return "offline_acceptance_passed"


def _artifact_action(artifact: RLPolicyArtifact, state: RLState) -> RLAction:
    if state.schema_version != artifact.state_schema_version:
        raise ValueError("offline state schema mismatch")
    if set(state.features) != set(artifact.feature_names):
        raise ValueError("offline feature schema mismatch")
    scores = {
        action: artifact.action_intercepts[action]
        + sum(
            (
                artifact.action_weights[action][name] * state.features[name]
                for name in artifact.feature_names
            ),
            ZERO,
        )
        for action in artifact.action_space
    }
    return min(artifact.action_space, key=lambda action: (-scores[action], action.value))


def _rl_artifact_payload(artifact: RLPolicyArtifact) -> dict[str, object]:
    return artifact.model_dump(
        mode="json",
        exclude={"artifact_checksum"},
    )


def _decision_id(state: RLState, timestamp: datetime, version: str, reason: str) -> str:
    return "rldec_" + _hash_json(
        {
            "reason": reason,
            "state": state.model_dump(mode="json"),
            "timestamp": timestamp.isoformat(),
            "version": version,
        }
    )[:32]


def _hash_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
