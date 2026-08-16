from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.ai import (
    GuardedRLPolicy,
    RLAction,
    RLOfflineEvaluationConfig,
    RLOfflineEvaluator,
    RLOfflineTransition,
    RLPolicyArtifact,
    RLPolicyConfig,
    RLState,
)
from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import DataQuality, TradingMode

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _state(**updates: object) -> RLState:
    values: dict[str, object] = {
        "state_id": "state-1",
        "schema_version": "risk-state-v1",
        "timestamp": NOW,
        "features": {"risk": Decimal("1"), "edge": Decimal("0.2")},
        "data_quality": DataQuality.VALID,
        "regime": MarketRegime.RANGE,
        "portfolio_drawdown_fraction": Decimal("0.02"),
        "reconciliation_healthy": True,
    }
    values.update(updates)
    return RLState(**values)


def _artifact(
    *,
    include_increase: bool = False,
    increase_weight: str = "0",
) -> RLPolicyArtifact:
    actions = [RLAction.HOLD, RLAction.REDUCE_25]
    if include_increase:
        actions.append(RLAction.INCREASE_10)
    weights = {
        RLAction.HOLD: {"edge": Decimal("0"), "risk": Decimal("0")},
        RLAction.REDUCE_25: {"edge": Decimal("0"), "risk": Decimal("1")},
    }
    if include_increase:
        weights[RLAction.INCREASE_10] = {
            "edge": Decimal("0"),
            "risk": Decimal(increase_weight),
        }
    return RLPolicyArtifact.create(
        policy_version="rl-v4",
        dataset_id="offline-v2",
        dataset_checksum="a" * 64,
        state_schema_version="risk-state-v1",
        feature_names=("edge", "risk"),
        action_space=tuple(actions),
        action_weights=weights,
        action_intercepts={action: Decimal("0") for action in actions},
        trained_at=NOW - timedelta(days=1),
        valid_until=NOW + timedelta(days=7),
    )


def test_rl_policy_is_disabled_by_default_and_has_no_execution_authority() -> None:
    decision = GuardedRLPolicy().decide(
        _state(), NOW, TradingMode.PAPER, _artifact()
    )

    assert decision.action is RLAction.HOLD
    assert decision.requested_position_fraction_change == Decimal("0")
    assert decision.used_fallback is True
    assert decision.reason == "rl_policy_disabled"
    assert decision.execution_authorized is False


def test_enabled_policy_selects_only_a_constrained_deterministic_action() -> None:
    policy = GuardedRLPolicy(RLPolicyConfig(enabled=True))
    artifact = _artifact()

    first = policy.decide(_state(), NOW, TradingMode.PAPER, artifact)
    second = policy.decide(_state(), NOW, TradingMode.PAPER, artifact)

    assert first.model_dump() == second.model_dump()
    assert first.action is RLAction.REDUCE_25
    assert first.requested_position_fraction_change == Decimal("-0.25")
    assert first.used_fallback is False
    assert first.action_scores[RLAction.REDUCE_25] == Decimal("1.0")


def test_rl_guardrails_block_unpermitted_risk_increase_and_live_use() -> None:
    increasing = _artifact(include_increase=True, increase_weight="2")
    policy = GuardedRLPolicy(RLPolicyConfig(enabled=True))

    unpermitted = policy.decide(_state(), NOW, TradingMode.PAPER, increasing)
    stress = GuardedRLPolicy(
        RLPolicyConfig(
            enabled=True,
            permitted_actions=frozenset({RLAction.HOLD, RLAction.INCREASE_10}),
        )
    ).decide(
        _state(regime=MarketRegime.STRESS),
        NOW,
        TradingMode.PAPER,
        increasing,
    )
    live = policy.decide(_state(), NOW, TradingMode.LIVE, _artifact())
    live_authorized = GuardedRLPolicy(
        RLPolicyConfig(enabled=True, live_enabled=True)
    ).decide(
        _state(),
        NOW,
        TradingMode.LIVE,
        _artifact(),
        operator_authorized=True,
    )

    assert unpermitted.reason == "rl_action_not_permitted"
    assert unpermitted.action is RLAction.HOLD
    assert stress.reason == "rl_risk_increase_blocked"
    assert live.reason == "rl_live_not_authorized"
    assert live_authorized.action is RLAction.REDUCE_25


def test_rl_state_artifact_and_reconciliation_fail_closed() -> None:
    policy = GuardedRLPolicy(RLPolicyConfig(enabled=True))
    artifact = _artifact()

    assert policy.decide(
        _state(timestamp=NOW - timedelta(seconds=3)),
        NOW,
        TradingMode.PAPER,
        artifact,
    ).reason == "rl_state_stale"
    assert policy.decide(
        _state(reconciliation_healthy=False),
        NOW,
        TradingMode.PAPER,
        artifact,
    ).reason == "rl_reconciliation_unhealthy"
    assert policy.decide(
        _state(schema_version="risk-state-v2"),
        NOW,
        TradingMode.PAPER,
        artifact,
    ).reason == "rl_state_schema_mismatch"
    assert policy.decide(
        _state(), NOW + timedelta(days=8), TradingMode.PAPER, artifact
    ).reason == "rl_state_stale"


def test_offline_evaluation_requires_coverage_reward_and_drawdown_evidence() -> None:
    artifact = _artifact()
    transitions = tuple(
        RLOfflineTransition(
            transition_id=f"transition-{index:02d}",
            state=_state(state_id=f"state-{index:02d}"),
            logged_action=RLAction.REDUCE_25,
            behavior_probability=Decimal("0.5"),
            reward=Decimal("1"),
            episode_id="episode-1",
        )
        for index in range(20)
    )
    evaluator = RLOfflineEvaluator()

    passed = evaluator.evaluate(artifact, transitions)
    repeated = evaluator.evaluate(artifact, tuple(reversed(transitions)))
    insufficient = evaluator.evaluate(artifact, transitions[:1])
    negative = RLOfflineEvaluator(
        RLOfflineEvaluationConfig(
            minimum_transitions=2,
            minimum_matched_actions=2,
            minimum_effective_sample_size=Decimal("2"),
        )
    ).evaluate(
        artifact,
        tuple(
            transition.model_copy(update={"reward": Decimal("-1")})
            for transition in transitions[:2]
        ),
    )

    assert passed.passed is True
    assert passed.reason == "offline_acceptance_passed"
    assert passed.effective_sample_size == Decimal("20")
    assert passed.weighted_mean_reward == Decimal("1")
    assert passed.model_dump() == repeated.model_dump()
    assert insufficient.reason == "insufficient_offline_transitions"
    assert negative.reason == "offline_reward_guardrail_failed"
