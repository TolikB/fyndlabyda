"""Versioned local ML/RL support projected from canonical runtime state."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter

from pydantic import BaseModel, ConfigDict, Field, model_validator

from funding_arbitrage.ai import (
    DecisionSupportArtifactBundle,
    GuardedRLPolicy,
    MetaLabelFallback,
    MetaLabelInferenceConfig,
    MetaLabelPolicy,
    RLAction,
    RLPolicyConfig,
    RLState,
    TimedFeature,
)
from funding_arbitrage.domain.decisions import SignalIntent
from funding_arbitrage.domain.events import DataQuality
from funding_arbitrage.monitoring.metrics import (
    decision_support_artifact_loaded,
    decision_support_decisions_total,
    decision_support_inference_duration_seconds,
    decision_support_projection_failures_total,
)
from funding_arbitrage.services.decision_support import BoundDecisionSupport
from funding_arbitrage.services.multi_regime import MultiRegimeStrategySnapshot
from funding_arbitrage.services.strategy_suite import StrategySuiteResult

BPS = Decimal("10000")
ZERO = Decimal("0")
RUNTIME_RL_STATE_SCHEMA_VERSION = "runtime-decision-support-state-v1"

logger = logging.getLogger(__name__)

DrawdownProvider = Callable[[datetime], Decimal]
ReconciliationHealthProvider = Callable[[], bool]


class EquityHighWaterDrawdown:
    """Restart-restorable drawdown measured from peak observed equity."""

    def __init__(self, initial_equity: Decimal) -> None:
        if not initial_equity.is_finite() or initial_equity <= ZERO:
            raise ValueError("initial equity must be finite and positive")
        self._high_water_equity = initial_equity

    @property
    def high_water_equity(self) -> Decimal:
        return self._high_water_equity

    def restore(self, high_water_equity: Decimal) -> None:
        if not high_water_equity.is_finite() or high_water_equity <= ZERO:
            raise ValueError("restored high-water equity must be finite and positive")
        self._high_water_equity = max(
            self._high_water_equity,
            high_water_equity,
        )

    def observe(self, equity: Decimal) -> Decimal:
        if not equity.is_finite() or equity <= ZERO:
            return Decimal("1")
        self._high_water_equity = max(self._high_water_equity, equity)
        return max(
            ZERO,
            min(
                Decimal("1"),
                (self._high_water_equity - equity) / self._high_water_equity,
            ),
        )


def fresh_equity_drawdown(
    *,
    current_equity: Decimal | None,
    high_water_equity: Decimal | None,
    observed_at: datetime | None,
    evaluated_at: datetime,
    maximum_age_seconds: Decimal,
) -> Decimal:
    """Calculate drawdown only from a complete, fresh, causal equity observation."""

    if maximum_age_seconds <= ZERO or not maximum_age_seconds.is_finite():
        raise ValueError("maximum equity age must be finite and positive")
    if current_equity is None or high_water_equity is None or observed_at is None:
        raise ValueError("live equity observation is incomplete")
    observation = _utc(observed_at)
    evaluation = _utc(evaluated_at)
    age_seconds = Decimal(str((evaluation - observation).total_seconds()))
    if age_seconds < ZERO:
        raise ValueError("live equity observation comes from the future")
    if age_seconds > maximum_age_seconds:
        raise ValueError("live equity observation is stale")
    return EquityHighWaterDrawdown(high_water_equity).observe(current_equity)


class RuntimeDecisionSupportConfig(BaseModel):
    """Safe runtime activation policy; every component is opt-in."""

    model_config = ConfigDict(frozen=True)

    meta_label_enabled: bool = False
    rl_enabled: bool = False
    meta_label_maximum_feature_zscore: Decimal = Field(
        default=Decimal("6"), gt=0
    )
    intent_feature_maximum_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    technical_feature_maximum_age_seconds: Decimal = Field(
        default=Decimal("960"), gt=0
    )
    orderflow_feature_maximum_age_seconds: Decimal = Field(
        default=Decimal("5"), gt=0
    )
    regime_feature_maximum_age_seconds: Decimal = Field(
        default=Decimal("3660"), gt=0
    )
    derivatives_feature_maximum_age_seconds: Decimal = Field(
        default=Decimal("180"), gt=0
    )
    rl_maximum_state_age_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    rl_maximum_drawdown_fraction: Decimal = Field(
        default=Decimal("0.10"), gt=0, le=1
    )
    rl_live_enabled: bool = False
    operator_authorized: bool = False

    @model_validator(mode="after")
    def require_component(self) -> RuntimeDecisionSupportConfig:
        if not self.meta_label_enabled and not self.rl_enabled:
            raise ValueError("runtime decision support has no enabled component")
        if self.operator_authorized and not self.rl_live_enabled:
            raise ValueError("RL operator authorization requires live enablement")
        return self


class _ProjectedFeature(BaseModel):
    model_config = ConfigDict(frozen=True)

    value: Decimal
    available_at: datetime


class RuntimeDecisionSupportProvider:
    """Bind local model decisions to exact intents without execution authority."""

    def __init__(
        self,
        artifacts: DecisionSupportArtifactBundle,
        config: RuntimeDecisionSupportConfig,
        *,
        drawdown_provider: DrawdownProvider,
        reconciliation_health_provider: ReconciliationHealthProvider,
    ) -> None:
        if config.meta_label_enabled and artifacts.meta_label is None:
            raise ValueError("enabled meta-label policy has no artifact")
        if config.rl_enabled and artifacts.rl_policy is None:
            raise ValueError("enabled RL policy has no artifact")
        feature_names: set[str] = set()
        if config.meta_label_enabled:
            assert artifacts.meta_label is not None
            feature_names.update(artifacts.meta_label.feature_names)
        if config.rl_enabled:
            assert artifacts.rl_policy is not None
            if (
                artifacts.rl_policy.state_schema_version
                != RUNTIME_RL_STATE_SCHEMA_VERSION
            ):
                raise ValueError("RL artifact uses an unsupported runtime state schema")
            feature_names.update(artifacts.rl_policy.feature_names)
        unsupported = feature_names - SUPPORTED_RUNTIME_FEATURES
        if unsupported:
            raise ValueError("decision-support artifact requests unsupported features")
        self.artifacts = artifacts
        self.config = config
        self.drawdown_provider = drawdown_provider
        self.reconciliation_health_provider = reconciliation_health_provider
        self.meta_label_policy = MetaLabelPolicy(
            MetaLabelInferenceConfig(
                enabled=config.meta_label_enabled,
                maximum_feature_zscore=(
                    config.meta_label_maximum_feature_zscore
                ),
                maximum_feature_age_seconds=(
                    max(
                        config.intent_feature_maximum_age_seconds,
                        config.technical_feature_maximum_age_seconds,
                        config.orderflow_feature_maximum_age_seconds,
                        config.regime_feature_maximum_age_seconds,
                        config.derivatives_feature_maximum_age_seconds,
                    )
                ),
                fallback=MetaLabelFallback.REJECT,
            )
        )
        self.rl_policy = GuardedRLPolicy(
            RLPolicyConfig(
                enabled=config.rl_enabled,
                live_enabled=config.rl_live_enabled,
                maximum_state_age_seconds=config.rl_maximum_state_age_seconds,
                maximum_drawdown_fraction=config.rl_maximum_drawdown_fraction,
                permitted_actions=frozenset(
                    {
                        RLAction.HOLD,
                        RLAction.REDUCE_25,
                        RLAction.REDUCE_50,
                        RLAction.CLOSE,
                    }
                ),
                fallback_action=RLAction.CLOSE,
            )
        )
        decision_support_artifact_loaded.labels("meta_label").set(
            int(config.meta_label_enabled)
        )
        decision_support_artifact_loaded.labels("rl").set(int(config.rl_enabled))
        logger.info(
            "decision_support_artifacts_activated",
            extra={
                "event": "decision_support_artifacts_activated",
                "correlation_id": artifacts.bundle_checksum[:16],
                "bundle_version": artifacts.bundle_version,
                "meta_label_enabled": config.meta_label_enabled,
                "rl_enabled": config.rl_enabled,
            },
        )

    def __call__(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        suite: StrategySuiteResult,
    ) -> tuple[BoundDecisionSupport, ...]:
        self._validate_boundary(snapshot, suite)
        return tuple(
            self._support(snapshot, intent, suite.timestamp)
            for intent in sorted(suite.intents, key=lambda item: item.signal_id)
        )

    def _support(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        intent: SignalIntent,
        evaluated_at: datetime,
    ) -> BoundDecisionSupport:
        projected = _project_features(snapshot, intent)
        meta_decision = None
        rl_decision = None
        if self.config.meta_label_enabled:
            meta_artifact = self.artifacts.meta_label
            assert meta_artifact is not None
            features = _timed_features(
                projected,
                meta_artifact.feature_names,
                evaluated_at,
                self.config,
            )
            started = perf_counter()
            meta_decision = self.meta_label_policy.decide(
                features,
                evaluated_at,
                meta_artifact,
            )
            decision_support_inference_duration_seconds.labels(
                "meta_label"
            ).observe(perf_counter() - started)
            complete = len(features) == len(meta_artifact.feature_names)
            if not complete:
                decision_support_projection_failures_total.labels("meta_label").inc()
            decision_support_decisions_total.labels(
                "meta_label",
                "accept" if meta_decision.accepted else "reject",
                "true" if meta_decision.used_fallback else "false",
            ).inc()
        if self.config.rl_enabled:
            rl_artifact = self.artifacts.rl_policy
            assert rl_artifact is not None
            rl_features = {
                name: projected[name].value
                for name in rl_artifact.feature_names
                if name in projected
                and _feature_is_usable(
                    name,
                    projected[name],
                    evaluated_at,
                    self.config,
                )
            }
            complete = len(rl_features) == len(rl_artifact.feature_names)
            if not rl_features:
                rl_features = {"__projection_unavailable__": ZERO}
            quality = _projection_quality(
                snapshot,
                projected,
                rl_artifact.feature_names,
                evaluated_at,
                self.config,
            )
            if not complete:
                decision_support_projection_failures_total.labels("rl").inc()
            state = RLState(
                state_id=_state_id(intent, evaluated_at, rl_features),
                schema_version=RUNTIME_RL_STATE_SCHEMA_VERSION,
                timestamp=_utc(evaluated_at),
                features=rl_features,
                data_quality=quality,
                regime=snapshot.regime.regime,
                portfolio_drawdown_fraction=self._drawdown(evaluated_at),
                reconciliation_healthy=self._reconciliation_healthy(),
            )
            started = perf_counter()
            rl_decision = self.rl_policy.decide(
                state,
                evaluated_at,
                snapshot.mode,
                rl_artifact,
                operator_authorized=self.config.operator_authorized,
            )
            decision_support_inference_duration_seconds.labels("rl").observe(
                perf_counter() - started
            )
            outcome = (
                "close"
                if rl_decision.action is RLAction.CLOSE
                else "reduce"
                if rl_decision.requested_position_fraction_change < 0
                else "hold"
            )
            decision_support_decisions_total.labels(
                "rl",
                outcome,
                "true" if rl_decision.used_fallback else "false",
            ).inc()
        return BoundDecisionSupport.bind(
            intent,
            evaluated_at,
            artifact_bundle_checksum=self.artifacts.bundle_checksum,
            meta_label=meta_decision,
            rl=rl_decision,
        )

    def _drawdown(self, timestamp: datetime) -> Decimal:
        try:
            value = self.drawdown_provider(timestamp)
        except Exception:
            logger.warning(
                "decision_support_drawdown_unavailable",
                extra={
                    "event": "decision_support_drawdown_unavailable",
                    "correlation_id": self.artifacts.bundle_checksum[:16],
                },
                exc_info=True,
            )
            return Decimal("1")
        if not value.is_finite() or not ZERO <= value <= Decimal("1"):
            return Decimal("1")
        return value

    def _reconciliation_healthy(self) -> bool:
        try:
            return bool(self.reconciliation_health_provider())
        except Exception:
            logger.warning(
                "decision_support_reconciliation_unavailable",
                extra={
                    "event": "decision_support_reconciliation_unavailable",
                    "correlation_id": self.artifacts.bundle_checksum[:16],
                },
                exc_info=True,
            )
            return False

    @staticmethod
    def _validate_boundary(
        snapshot: MultiRegimeStrategySnapshot,
        suite: StrategySuiteResult,
    ) -> None:
        if suite.source_event_id != snapshot.source_event_id:
            raise ValueError("decision-support source event mismatch")
        if suite.mode is not snapshot.mode:
            raise ValueError("decision-support trading mode mismatch")
        if suite.timestamp != snapshot.timestamp:
            raise ValueError("decision-support timestamp mismatch")
        if any(intent.created_at > suite.timestamp for intent in suite.intents):
            raise ValueError("decision-support intent comes from the future")


def _project_features(
    snapshot: MultiRegimeStrategySnapshot,
    intent: SignalIntent,
) -> dict[str, _ProjectedFeature]:
    values: dict[str, _ProjectedFeature] = {}

    def add(name: str, value: Decimal | None, available_at: datetime) -> None:
        if value is not None and value.is_finite():
            values[name] = _ProjectedFeature(
                value=value,
                available_at=_utc(available_at),
            )

    add("expected_move_bps", intent.expected_move_bps, intent.created_at)
    add("estimated_cost_bps", intent.estimated_cost_bps, intent.created_at)
    add(
        "net_expected_edge_bps",
        intent.expected_move_bps - intent.estimated_cost_bps,
        intent.created_at,
    )
    add("quality_score", intent.quality_score, intent.created_at)
    add("signal_confidence", intent.confidence, intent.created_at)
    add(
        "expected_holding_seconds",
        Decimal(intent.expected_holding_seconds),
        intent.created_at,
    )
    add("expected_rr", intent.expected_rr, intent.created_at)
    technical = snapshot.technical
    technical_valid = technical.data_quality is DataQuality.VALID
    add("close_price", technical.close if technical_valid else None, technical.timestamp)
    add(
        "ema_spread_bps",
        (
            (technical.ema_fast - technical.ema_slow) / technical.close * BPS
            if technical_valid
            else None
        ),
        technical.timestamp,
    )
    add(
        "atr_bps",
        (
            technical.atr / technical.close * BPS
            if technical_valid and technical.atr is not None
            else None
        ),
        technical.timestamp,
    )
    add("adx", technical.adx if technical_valid else None, technical.timestamp)
    add(
        "efficiency_ratio",
        technical.efficiency_ratio if technical_valid else None,
        technical.timestamp,
    )
    orderflow = snapshot.orderflow
    orderflow_valid = orderflow.data_quality is DataQuality.VALID
    add(
        "spread_bps",
        orderflow.spread_bps if orderflow_valid else None,
        orderflow.timestamp,
    )
    add(
        "ofi_zscore_5s",
        orderflow.ofi_zscore_5s if orderflow_valid else None,
        orderflow.timestamp,
    )
    add(
        "book_imbalance_l5",
        orderflow.book_imbalance_l5 if orderflow_valid else None,
        orderflow.timestamp,
    )
    add(
        "trade_imbalance_5s",
        orderflow.trade_imbalance_5s if orderflow_valid else None,
        orderflow.timestamp,
    )
    add("cvd", orderflow.cvd if orderflow_valid else None, orderflow.timestamp)
    regime = snapshot.regime
    regime_valid = regime.data_quality is DataQuality.VALID
    add(
        "regime_confidence",
        regime.confidence if regime_valid else None,
        regime.timestamp,
    )
    add(
        "regime_dwell_seconds",
        regime.dwell_seconds if regime_valid else None,
        regime.timestamp,
    )
    derivatives = snapshot.derivatives
    if derivatives is not None:
        derivatives_valid = derivatives.data_quality is DataQuality.VALID
        add(
            "funding_rate_bps",
            derivatives.funding_rate * BPS
            if derivatives_valid and derivatives.funding_rate is not None
            else None,
            derivatives.timestamp,
        )
        add(
            "basis_bps",
            derivatives.mark_index_basis_bps if derivatives_valid else None,
            derivatives.timestamp,
        )
        add(
            "open_interest_change_percent",
            (
                derivatives.open_interest_change_percent
                if derivatives_valid
                else None
            ),
            derivatives.timestamp,
        )
        add(
            "crowding_score",
            derivatives.crowding_score if derivatives_valid else None,
            derivatives.timestamp,
        )
    return values


INTENT_RUNTIME_FEATURES = frozenset(
    {
        "expected_move_bps",
        "estimated_cost_bps",
        "net_expected_edge_bps",
        "quality_score",
        "signal_confidence",
        "expected_holding_seconds",
        "expected_rr",
    }
)
TECHNICAL_RUNTIME_FEATURES = frozenset(
    {"close_price", "ema_spread_bps", "atr_bps", "adx", "efficiency_ratio"}
)
ORDERFLOW_RUNTIME_FEATURES = frozenset(
    {
        "spread_bps",
        "ofi_zscore_5s",
        "book_imbalance_l5",
        "trade_imbalance_5s",
        "cvd",
    }
)
REGIME_RUNTIME_FEATURES = frozenset(
    {"regime_confidence", "regime_dwell_seconds"}
)
DERIVATIVES_RUNTIME_FEATURES = frozenset(
    {
        "funding_rate_bps",
        "basis_bps",
        "open_interest_change_percent",
        "crowding_score",
    }
)
SUPPORTED_RUNTIME_FEATURES = frozenset(
    INTENT_RUNTIME_FEATURES
    | TECHNICAL_RUNTIME_FEATURES
    | ORDERFLOW_RUNTIME_FEATURES
    | REGIME_RUNTIME_FEATURES
    | DERIVATIVES_RUNTIME_FEATURES
)


def _timed_features(
    projected: dict[str, _ProjectedFeature],
    names: tuple[str, ...],
    evaluated_at: datetime,
    config: RuntimeDecisionSupportConfig,
) -> tuple[TimedFeature, ...]:
    return tuple(
        TimedFeature(
            name=name,
            value=projected[name].value,
            available_at=projected[name].available_at,
        )
        for name in names
        if name in projected
        and _feature_is_usable(name, projected[name], evaluated_at, config)
    )


def _projection_quality(
    snapshot: MultiRegimeStrategySnapshot,
    projected: dict[str, _ProjectedFeature],
    names: tuple[str, ...],
    evaluated_at: datetime,
    config: RuntimeDecisionSupportConfig,
) -> DataQuality:
    if any(name not in projected for name in names):
        return DataQuality.UNAVAILABLE
    if any(projected[name].available_at > snapshot.timestamp for name in names):
        return DataQuality.INVALID
    if any(
        not _feature_is_usable(name, projected[name], evaluated_at, config)
        for name in names
    ):
        return DataQuality.STALE
    return DataQuality.VALID


def _feature_is_usable(
    name: str,
    feature: _ProjectedFeature,
    evaluated_at: datetime,
    config: RuntimeDecisionSupportConfig,
) -> bool:
    now = _utc(evaluated_at)
    if feature.available_at > now:
        return True
    age = Decimal(str((now - feature.available_at).total_seconds()))
    return age <= _maximum_feature_age(name, config)


def _maximum_feature_age(
    name: str,
    config: RuntimeDecisionSupportConfig,
) -> Decimal:
    if name in TECHNICAL_RUNTIME_FEATURES:
        return config.technical_feature_maximum_age_seconds
    if name in ORDERFLOW_RUNTIME_FEATURES:
        return config.orderflow_feature_maximum_age_seconds
    if name in REGIME_RUNTIME_FEATURES:
        return config.regime_feature_maximum_age_seconds
    if name in DERIVATIVES_RUNTIME_FEATURES:
        return config.derivatives_feature_maximum_age_seconds
    return config.intent_feature_maximum_age_seconds


def _state_id(
    intent: SignalIntent,
    timestamp: datetime,
    features: dict[str, Decimal],
) -> str:
    payload = {
        "features": features,
        "signal_id": intent.signal_id,
        "timestamp": _utc(timestamp),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return "rlstate_" + hashlib.sha256(encoded).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
