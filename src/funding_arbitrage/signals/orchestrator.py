"""Fail-closed signal TTL, deduplication, conflict, and allocation policy."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalType,
)
from funding_arbitrage.domain.events import TradingMode

ZERO = Decimal("0")
ONE = Decimal("1")


class SignalDecisionStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    DUPLICATE = "DUPLICATE"


class SignalOrchestratorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_ttl_seconds: int = Field(default=3600, gt=0)
    future_clock_skew_seconds: int = Field(default=2, ge=0)
    minimum_edge_to_cost_ratio: Decimal = Field(default=Decimal("2.5"), gt=0)
    max_active_per_strategy: int = Field(default=3, gt=0)
    max_active_per_asset: int = Field(default=2, gt=0)
    max_active_per_correlation_group: int = Field(default=3, gt=0)
    max_allocation_weight: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)
    seen_signal_limit: int = Field(default=10000, gt=0)
    correlation_groups: tuple[frozenset[str], ...] = ()
    enabled_dangerous_signal_types: frozenset[SignalType] = frozenset()
    dangerous_operator_authorized: bool = False

    @field_validator("correlation_groups")
    @classmethod
    def normalize_correlation_groups(
        cls, value: tuple[frozenset[str], ...]
    ) -> tuple[frozenset[str], ...]:
        normalized = tuple(
            frozenset(asset.strip().upper() for asset in group if asset.strip())
            for group in value
        )
        flattened = [asset for group in normalized for asset in group]
        if len(flattened) != len(set(flattened)):
            raise ValueError("correlation groups must not overlap")
        return normalized


class SignalOrchestrationDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    signal_id: str
    status: SignalDecisionStatus
    reason: str | None = None
    priority_score: Decimal = Field(ge=0)


class ActiveSignal(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent
    priority_score: Decimal = Field(ge=0)
    allocation_weight: Decimal = Field(ge=0, le=1)
    correlation_group: str


class SignalOrchestrationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    active: tuple[ActiveSignal, ...]
    decisions: tuple[SignalOrchestrationDecision, ...]
    expired_signal_ids: tuple[str, ...] = ()
    replaced_signal_ids: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class _Candidate(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent: SignalIntent
    priority_score: Decimal
    correlation_group: str


class SignalOrchestrator:
    """Select intents for risk evaluation without ever authorizing size."""

    def __init__(
        self,
        mode: TradingMode,
        config: SignalOrchestratorConfig | None = None,
    ) -> None:
        self.mode = mode
        self.config = config or SignalOrchestratorConfig()
        self._active: dict[str, _Candidate] = {}
        self._seen: OrderedDict[str, str] = OrderedDict()
        self._last_timestamp: datetime | None = None

    def orchestrate(
        self, intents: tuple[SignalIntent, ...], timestamp: datetime
    ) -> SignalOrchestrationResult:
        now = _utc(timestamp)
        if self._last_timestamp is not None and now < self._last_timestamp:
            raise ValueError("signal orchestration time cannot move backwards")
        self._last_timestamp = now
        expired = self._expire(now)
        decisions: list[SignalOrchestrationDecision] = []
        candidates: list[_Candidate] = []
        for intent in intents:
            priority = self._priority(intent)
            fingerprint = _fingerprint(intent)
            previous_fingerprint = self._seen.get(intent.signal_id)
            if previous_fingerprint is not None:
                decisions.append(
                    SignalOrchestrationDecision(
                        signal_id=intent.signal_id,
                        status=(
                            SignalDecisionStatus.DUPLICATE
                            if previous_fingerprint == fingerprint
                            else SignalDecisionStatus.REJECTED
                        ),
                        reason=(
                            "idempotent_replay"
                            if previous_fingerprint == fingerprint
                            else "signal_id_collision"
                        ),
                        priority_score=priority,
                    )
                )
                continue
            self._remember(intent.signal_id, fingerprint)
            rejection = self._validate(intent, now)
            if rejection is not None:
                decisions.append(
                    SignalOrchestrationDecision(
                        signal_id=intent.signal_id,
                        status=SignalDecisionStatus.REJECTED,
                        reason=rejection,
                        priority_score=priority,
                    )
                )
                continue
            candidates.append(
                _Candidate(
                    intent=intent,
                    priority_score=priority,
                    correlation_group=self._correlation_group(
                        intent.primary_instrument.base_asset
                    ),
                )
            )
        replaced: list[str] = []
        for candidate in sorted(
            candidates,
            key=lambda item: (-item.priority_score, item.intent.signal_id),
        ):
            conflicts = self._conflicts(candidate)
            if conflicts:
                strongest = max(
                    conflicts,
                    key=lambda item: (item.priority_score, -len(item.intent.signal_id)),
                )
                if candidate.priority_score <= strongest.priority_score:
                    decisions.append(
                        SignalOrchestrationDecision(
                            signal_id=candidate.intent.signal_id,
                            status=SignalDecisionStatus.REJECTED,
                            reason=self._conflict_reason(candidate, strongest),
                            priority_score=candidate.priority_score,
                        )
                    )
                    continue
                removed: dict[str, _Candidate] = {}
                for conflict in conflicts:
                    removed[conflict.intent.signal_id] = conflict
                    self._active.pop(conflict.intent.signal_id, None)
            else:
                removed = {}
            limit_rejection = self._limit_rejection(candidate)
            if limit_rejection is not None:
                self._active.update(removed)
                decisions.append(
                    SignalOrchestrationDecision(
                        signal_id=candidate.intent.signal_id,
                        status=SignalDecisionStatus.REJECTED,
                        reason=limit_rejection,
                        priority_score=candidate.priority_score,
                    )
                )
                continue
            replaced.extend(removed)
            self._active[candidate.intent.signal_id] = candidate
            decisions.append(
                SignalOrchestrationDecision(
                    signal_id=candidate.intent.signal_id,
                    status=SignalDecisionStatus.ACCEPTED,
                    priority_score=candidate.priority_score,
                )
            )
        active = self._weighted_active()
        return SignalOrchestrationResult(
            timestamp=now,
            active=active,
            decisions=tuple(sorted(decisions, key=lambda item: item.signal_id)),
            expired_signal_ids=tuple(sorted(expired)),
            replaced_signal_ids=tuple(sorted(set(replaced))),
        )

    def cancel(self, signal_id: str) -> bool:
        return self._active.pop(signal_id, None) is not None

    def restore(
        self,
        result: SignalOrchestrationResult,
        submitted_intents: tuple[SignalIntent, ...],
    ) -> None:
        """Restore persisted orchestration state without re-running policy inputs."""

        now = _utc(result.timestamp)
        if self._last_timestamp is not None and now < self._last_timestamp:
            raise ValueError("persisted signal orchestration time moved backwards")
        submitted = {intent.signal_id: intent for intent in submitted_intents}
        if len(submitted) != len(submitted_intents):
            raise ValueError("persisted orchestration has duplicate submitted signals")
        decision_ids = tuple(decision.signal_id for decision in result.decisions)
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("persisted orchestration has duplicate decisions")
        if set(decision_ids) != set(submitted):
            raise ValueError("persisted orchestration decisions do not match inputs")

        restored_active: dict[str, _Candidate] = {}
        for active in result.active:
            intent = active.intent
            if intent.signal_id in restored_active:
                raise ValueError("persisted orchestration has duplicate active signals")
            if intent.mode is not self.mode:
                raise ValueError("persisted orchestration trading mode mismatch")
            if intent.expires_at <= now:
                raise ValueError("persisted orchestration contains an expired signal")
            priority = self._priority(intent)
            correlation_group = self._correlation_group(
                intent.primary_instrument.base_asset
            )
            if (
                priority != active.priority_score
                or correlation_group != active.correlation_group
            ):
                raise ValueError("persisted orchestration policy projection mismatch")
            restored_active[intent.signal_id] = _Candidate(
                intent=intent,
                priority_score=priority,
                correlation_group=correlation_group,
            )

        previous_active = self._active
        previous_seen = self._seen.copy()
        previous_timestamp = self._last_timestamp
        try:
            for intent in submitted_intents:
                fingerprint = _fingerprint(intent)
                previous = self._seen.get(intent.signal_id)
                if previous is not None and previous != fingerprint:
                    raise ValueError("persisted orchestration signal ID collision")
                self._remember(intent.signal_id, fingerprint)
            self._active = restored_active
            self._last_timestamp = now
            if self._weighted_active() != result.active:
                raise ValueError("persisted orchestration allocation mismatch")
        except Exception:
            self._active = previous_active
            self._seen = previous_seen
            self._last_timestamp = previous_timestamp
            raise

    def _validate(self, intent: SignalIntent, now: datetime) -> str | None:
        config = self.config
        if intent.mode is not self.mode:
            return "trading_mode_mismatch"
        if self.mode is TradingMode.SAFE_MODE:
            return "safe_mode_suppressed"
        if intent.created_at > now + timedelta(seconds=config.future_clock_skew_seconds):
            return "created_in_future"
        if intent.expires_at <= now:
            return "signal_expired"
        if (intent.expires_at - intent.created_at).total_seconds() > config.maximum_ttl_seconds:
            return "ttl_exceeds_limit"
        if intent.regime in {MarketRegime.STRESS, MarketRegime.UNKNOWN}:
            return "unsafe_regime"
        dangerous = {
            SignalType.DEX,
            SignalType.GRID,
            SignalType.LLM_DECISION,
            SignalType.LOSS_AVERAGING,
            SignalType.MARTINGALE,
            SignalType.MEV,
            SignalType.RL_POLICY,
        }
        if intent.signal_type in dangerous:
            if intent.signal_type not in config.enabled_dangerous_signal_types:
                return "dangerous_signal_disabled"
            if self.mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE} and not (
                config.dangerous_operator_authorized
            ):
                return "dangerous_signal_not_operator_authorized"
        if intent.signal_type is SignalType.LEAD_LAG_FILTER:
            return None
        if intent.expected_move_bps <= 0:
            return "nonpositive_expected_move"
        required_edge = intent.estimated_cost_bps * config.minimum_edge_to_cost_ratio
        if intent.expected_move_bps < required_edge:
            return "insufficient_edge_to_cost"
        return None

    def _priority(self, intent: SignalIntent) -> Decimal:
        quality = intent.quality_score / Decimal("100")
        edge_ratio = (
            intent.expected_move_bps / intent.estimated_cost_bps
            if intent.estimated_cost_bps > 0
            else self.config.minimum_edge_to_cost_ratio * Decimal("2")
        )
        edge_component = min(
            Decimal("2"), edge_ratio / self.config.minimum_edge_to_cost_ratio
        )
        return max(ZERO, quality * intent.confidence * edge_component)

    def _conflicts(self, candidate: _Candidate) -> list[_Candidate]:
        return [
            active
            for active in self._active.values()
            if self._is_duplicate_thesis(candidate.intent, active.intent)
            or self._is_opposing_direction(candidate.intent, active.intent)
        ]

    @staticmethod
    def _is_duplicate_thesis(left: SignalIntent, right: SignalIntent) -> bool:
        return (
            left.signal_type is right.signal_type
            and left.primary_instrument.canonical_id
            == right.primary_instrument.canonical_id
            and left.side is right.side
        )

    @staticmethod
    def _is_opposing_direction(left: SignalIntent, right: SignalIntent) -> bool:
        directional = {
            SignalType.ORDERFLOW_BREAKOUT,
            SignalType.LIQUIDITY_SWEEP_REVERSION,
        }
        return (
            left.signal_type in directional
            and right.signal_type in directional
            and left.primary_instrument.base_asset
            == right.primary_instrument.base_asset
            and left.side is not right.side
        )

    def _conflict_reason(self, left: _Candidate, right: _Candidate) -> str:
        return (
            "correlated_duplicate"
            if self._is_duplicate_thesis(left.intent, right.intent)
            else "opposing_signal_conflict"
        )

    def _limit_rejection(self, candidate: _Candidate) -> str | None:
        active = tuple(self._active.values())
        if (
            sum(item.intent.strategy_id == candidate.intent.strategy_id for item in active)
            >= self.config.max_active_per_strategy
        ):
            return "strategy_allocation_limit"
        if (
            sum(
                item.intent.primary_instrument.base_asset
                == candidate.intent.primary_instrument.base_asset
                for item in active
            )
            >= self.config.max_active_per_asset
        ):
            return "asset_allocation_limit"
        if (
            sum(
                item.correlation_group == candidate.correlation_group for item in active
            )
            >= self.config.max_active_per_correlation_group
        ):
            return "correlation_allocation_limit"
        return None

    def _weighted_active(self) -> tuple[ActiveSignal, ...]:
        ordered = sorted(
            self._active.values(),
            key=lambda item: (-item.priority_score, item.intent.signal_id),
        )
        total = sum((item.priority_score for item in ordered), ZERO)
        return tuple(
            ActiveSignal(
                intent=item.intent,
                priority_score=item.priority_score,
                allocation_weight=(
                    min(
                        self.config.max_allocation_weight,
                        item.priority_score / total,
                    )
                    if total > 0
                    else ZERO
                ),
                correlation_group=item.correlation_group,
            )
            for item in ordered
        )

    def _correlation_group(self, asset: str) -> str:
        normalized = asset.upper()
        for index, group in enumerate(self.config.correlation_groups):
            if normalized in group:
                return f"group:{index}"
        return f"asset:{normalized}"

    def _expire(self, now: datetime) -> list[str]:
        expired = [
            signal_id
            for signal_id, candidate in self._active.items()
            if candidate.intent.expires_at <= now
        ]
        for signal_id in expired:
            self._active.pop(signal_id, None)
        return expired

    def _remember(self, signal_id: str, fingerprint: str) -> None:
        self._seen[signal_id] = fingerprint
        self._seen.move_to_end(signal_id)
        while len(self._seen) > self.config.seen_signal_limit:
            self._seen.popitem(last=False)


def _fingerprint(intent: SignalIntent) -> str:
    payload = json.dumps(
        intent.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
