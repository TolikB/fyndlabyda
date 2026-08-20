from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.backtest.research import (
    MonteCarloConfig,
    ResearchTrade,
    StressScenario,
    WalkForwardConfig,
    build_walk_forward_report,
    run_monte_carlo,
    run_research_suite,
    run_stress_suite,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def _trade(
    index: int,
    *,
    pnl: str = "10",
    decision_day: int | None = None,
    holding_days: int = 1,
    feature_lag_hours: int = 1,
) -> ResearchTrade:
    decision = START + timedelta(days=decision_day if decision_day is not None else index)
    return ResearchTrade(
        trade_id=f"trade-{index:03d}",
        asset="SOL",
        strategy="funding_basis",
        decision_at=decision,
        features_available_at=decision - timedelta(hours=feature_lag_hours),
        outcome_at=decision + timedelta(days=holding_days),
        listed_at=START - timedelta(days=365),
        delisted_at=None,
        universe_selection_id=f"universe-{decision.date().isoformat()}",
        universe_selected_at=decision - timedelta(hours=2),
        initial_risk=Decimal("10"),
        gross_pnl=Decimal(pnl),
        funding_pnl=Decimal("1"),
        fees=Decimal("1"),
        spread=Decimal("0.5"),
        slippage=Decimal("0.5"),
    )


def test_research_trade_rejects_lookahead_and_invalid_universe_membership() -> None:
    base = _trade(1).model_dump()

    with pytest.raises(ValidationError, match="features cannot"):
        ResearchTrade(
            **{
                **base,
                "features_available_at": base["decision_at"] + timedelta(seconds=1),
            }
        )
    with pytest.raises(ValidationError, match="not listed"):
        ResearchTrade(
            **{
                **base,
                "listed_at": base["decision_at"] + timedelta(seconds=1),
            }
        )
    with pytest.raises(ValidationError, match="delisted"):
        ResearchTrade(
            **{
                **base,
                "delisted_at": base["decision_at"],
            }
        )
    with pytest.raises(ValidationError, match="universe cannot"):
        ResearchTrade(
            **{
                **base,
                "universe_selected_at": base["decision_at"] + timedelta(seconds=1),
            }
        )
    with pytest.raises(ValidationError, match="outcome must"):
        ResearchTrade(
            **{
                **base,
                "outcome_at": base["decision_at"],
            }
        )


def test_walk_forward_rejects_overlapping_validation_windows() -> None:
    with pytest.raises(ValidationError, match="shorter than validation_window"):
        WalkForwardConfig(
            validation_window=timedelta(days=5),
            step=timedelta(days=4),
        )


def test_walk_forward_purges_unknown_training_outcomes_and_embargoes_validation() -> None:
    trades = [_trade(index) for index in range(18)]
    trades.append(_trade(100, decision_day=4, holding_days=5))
    config = WalkForwardConfig(
        training_window=timedelta(days=6),
        validation_window=timedelta(days=4),
        step=timedelta(days=4),
        embargo=timedelta(days=1),
        minimum_training_trades=4,
        minimum_validation_trades=2,
    )

    report = build_walk_forward_report(trades, config)

    assert report.folds
    first = report.folds[0]
    assert "trade-100" in first.purged_training_trade_ids
    assert all(
        trade_id not in first.validation_trade_ids
        for trade_id in first.training_trade_ids
    )
    assert first.validation_start == first.training_end + timedelta(days=1)
    assert all(
        next(item for item in trades if item.trade_id == trade_id).outcome_at
        <= first.training_end
        for trade_id in first.training_trade_ids
    )


def test_walk_forward_is_deterministic_and_rejects_duplicate_trade_ids() -> None:
    trades = [_trade(index, pnl="5" if index % 2 == 0 else "-2") for index in range(20)]
    config = WalkForwardConfig(
        training_window=timedelta(days=6),
        validation_window=timedelta(days=4),
        step=timedelta(days=4),
        embargo=timedelta(0),
        minimum_training_trades=4,
        minimum_validation_trades=2,
    )

    first = build_walk_forward_report(trades, config)
    second = build_walk_forward_report(list(reversed(trades)), config)

    assert first == second
    assert first.profitable_validation_folds <= len(first.folds)
    with pytest.raises(ValueError, match="unique"):
        build_walk_forward_report([trades[0], trades[0]], config)


def test_seeded_block_bootstrap_monte_carlo_is_reproducible() -> None:
    trades = [_trade(index, pnl=value) for index, value in enumerate(("8", "-4", "5", "-2"))]
    config = MonteCarloConfig(
        iterations=200,
        block_size=2,
        horizon_trades=8,
        seed=42,
    )

    first = run_monte_carlo(trades, Decimal("1000"), config)
    second = run_monte_carlo(list(reversed(trades)), Decimal("1000"), config)

    assert first == second
    assert len(first.path_digest) == 64
    assert first.p05_net_pnl <= first.p50_net_pnl <= first.p95_net_pnl
    assert Decimal("0") <= first.probability_profitable_percent <= Decimal("100")


def test_monte_carlo_rejects_missing_data_and_invalid_capital() -> None:
    with pytest.raises(ValueError, match="at least one"):
        run_monte_carlo([], Decimal("1000"))
    with pytest.raises(ValueError, match="positive"):
        run_monte_carlo([_trade(1)], Decimal("0"))


def test_stress_suite_reprices_each_cost_component_and_funding_sign() -> None:
    trades = [_trade(1, pnl="10"), _trade(2, pnl="-2")]
    scenarios = (
        StressScenario(name="double_slippage", slippage_multiplier=Decimal("2")),
        StressScenario(name="funding_reversal", funding_pnl_multiplier=Decimal("-1")),
        StressScenario(
            name="outage_gap",
            fixed_loss_per_trade=Decimal("3"),
            fee_multiplier=Decimal("1.5"),
        ),
    )

    results = {item.scenario: item for item in run_stress_suite(trades, scenarios)}

    baseline = sum((trade.net_pnl for trade in trades), Decimal("0"))
    assert results["double_slippage"].baseline_net_pnl == baseline
    assert results["double_slippage"].pnl_delta == -sum(
        (trade.slippage for trade in trades), Decimal("0")
    )
    assert results["funding_reversal"].pnl_delta == -Decimal("4")
    assert results["outage_gap"].pnl_delta < Decimal("0")

    with pytest.raises(ValueError, match="unique"):
        run_stress_suite(trades, (scenarios[0], scenarios[0]))


def test_full_research_suite_has_stable_dataset_fingerprint() -> None:
    trades = tuple(_trade(index, pnl="4") for index in range(20))
    walk = WalkForwardConfig(
        training_window=timedelta(days=6),
        validation_window=timedelta(days=4),
        step=timedelta(days=4),
        embargo=timedelta(0),
        minimum_training_trades=4,
        minimum_validation_trades=2,
    )
    monte = MonteCarloConfig(iterations=100, block_size=2, seed=7)
    scenarios = (StressScenario(name="double_slippage", slippage_multiplier=Decimal("2")),)

    first = run_research_suite(trades, Decimal("1000"), walk, monte, scenarios)
    second = run_research_suite(
        tuple(reversed(trades)),
        Decimal("1000"),
        walk,
        monte,
        scenarios,
    )

    assert first == second
    assert first.trade_count == len(trades)
    assert first.walk_forward.folds
    assert first.stress[0].scenario == "double_slippage"