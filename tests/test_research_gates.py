from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.backtest.gates import (
    ResearchGateConfig,
    evaluate_research_gates,
)
from funding_arbitrage.backtest.research import (
    ResearchTrade,
    WalkForwardConfig,
    build_walk_forward_report,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def _trade(
    index: int,
    *,
    strategy: str,
    gross_pnl: str = "5",
    slippage: str = "0.1",
) -> ResearchTrade:
    decision = START + timedelta(days=index)
    return ResearchTrade(
        trade_id=f"gate-{index:03d}",
        asset="SOL" if index % 2 == 0 else "DOGE",
        strategy=strategy,
        decision_at=decision,
        features_available_at=decision - timedelta(minutes=5),
        outcome_at=decision + timedelta(hours=1),
        listed_at=START - timedelta(days=365),
        universe_selection_id=f"universe-{index:03d}",
        universe_selected_at=decision - timedelta(hours=1),
        initial_risk=Decimal("10"),
        gross_pnl=Decimal(gross_pnl),
        funding_pnl=Decimal("0.5"),
        fees=Decimal("0.1"),
        spread=Decimal("0.1"),
        slippage=Decimal(slippage),
        borrow_cost=Decimal("0.05"),
        other_costs=Decimal("0.05"),
    )


def _walk_forward(trades: list[ResearchTrade]):
    return build_walk_forward_report(
        trades,
        WalkForwardConfig(
            training_window=timedelta(days=8),
            validation_window=timedelta(days=5),
            step=timedelta(days=5),
            embargo=timedelta(0),
            minimum_training_trades=5,
            minimum_validation_trades=3,
        ),
    )


def test_profitable_diversified_evidence_passes_all_research_gates() -> None:
    trades = [
        _trade(
            index,
            strategy="funding_basis" if index % 2 == 0 else "orderflow_breakout",
        )
        for index in range(30)
    ]

    report = evaluate_research_gates(
        trades,
        _walk_forward(trades),
        Decimal("1000"),
    )

    assert report.accepted is True
    assert all(report.checks.values())
    assert report.metrics.profit_factor is None
    assert report.metrics.expectancy_r >= Decimal("0.15")
    assert (
        report.metrics.maximum_single_strategy_pnl_share_percent
        == Decimal("50")
    )
    assert report.metrics.doubled_slippage_net_pnl > 0


def test_concentration_and_doubled_slippage_fail_closed() -> None:
    trades = [
        _trade(index, strategy="only_strategy", gross_pnl="0.5", slippage="0.8")
        for index in range(30)
    ]

    report = evaluate_research_gates(
        trades,
        _walk_forward(trades),
        Decimal("1000"),
        ResearchGateConfig(
            minimum_expectancy_r=Decimal("-1"),
            minimum_sharpe=Decimal("-100"),
            maximum_cost_share_of_gross_alpha_percent=Decimal("1000"),
        ),
    )

    assert report.accepted is False
    assert report.checks["strategy_diversification"] is False
    assert report.checks["doubled_slippage_profitable"] is False


def test_losing_out_of_sample_evidence_fails_profit_and_expectancy() -> None:
    trades = [
        _trade(
            index,
            strategy="a" if index % 2 == 0 else "b",
            gross_pnl="-2",
        )
        for index in range(30)
    ]

    report = evaluate_research_gates(
        trades,
        _walk_forward(trades),
        Decimal("1000"),
    )

    assert report.accepted is False
    assert report.checks["out_of_sample_profitable"] is False
    assert report.checks["profit_factor"] is False
    assert report.checks["expectancy_r"] is False


def test_gates_require_positive_capital_trades_and_completed_folds() -> None:
    trades = [_trade(index, strategy="a") for index in range(10)]
    empty_walk = build_walk_forward_report(
        trades,
        WalkForwardConfig(
            training_window=timedelta(days=30),
            validation_window=timedelta(days=10),
            step=timedelta(days=10),
        ),
    )

    with pytest.raises(ValueError, match="positive"):
        evaluate_research_gates(trades, empty_walk, Decimal("0"))
    with pytest.raises(ValueError, match="at least one"):
        evaluate_research_gates([], empty_walk, Decimal("1000"))
    with pytest.raises(ValueError, match="completed walk-forward"):
        evaluate_research_gates(trades, empty_walk, Decimal("1000"))