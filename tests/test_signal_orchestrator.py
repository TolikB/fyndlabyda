from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, Side, TradingMode
from funding_arbitrage.signals import (
    SignalDecisionStatus,
    SignalOrchestrator,
    SignalOrchestratorConfig,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(asset: str, venue: str = "BYBIT") -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol=f"{asset}USDT",
        base_asset=asset,
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        settlement_asset="USDT",
    )


def _intent(
    signal_id: str,
    *,
    asset: str = "BTC",
    side: Side = Side.BUY,
    signal_type: SignalType = SignalType.ORDERFLOW_BREAKOUT,
    strategy_id: str = "breakout-v1",
    mode: TradingMode = TradingMode.PAPER,
    created_at: datetime = NOW,
    ttl_seconds: int = 30,
    quality: str = "90",
    confidence: str = "0.8",
    expected_move_bps: str = "20",
    estimated_cost_bps: str = "5",
) -> SignalIntent:
    instrument = _instrument(asset)
    return SignalIntent(
        signal_id=signal_id,
        strategy_id=strategy_id,
        mode=mode,
        signal_type=signal_type,
        primary_instrument=instrument,
        side=side,
        legs=(SignalLeg(instrument=instrument, side=side),),
        regime=MarketRegime.TREND_UP,
        quality_score=Decimal(quality),
        confidence=Decimal(confidence),
        entry_zone_low=Decimal("100"),
        entry_zone_high=Decimal("101"),
        structural_stop=Decimal("98"),
        targets=(Decimal("105"),),
        expected_holding_seconds=300,
        expected_move_bps=Decimal(expected_move_bps),
        estimated_cost_bps=Decimal(estimated_cost_bps),
        expected_rr=Decimal("2"),
        created_at=created_at,
        expires_at=created_at + timedelta(seconds=ttl_seconds),
    )


def _decision(result: object, signal_id: str) -> object:
    return next(item for item in result.decisions if item.signal_id == signal_id)


def test_ttl_mode_edge_and_regime_validation_fail_closed() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    expired = _intent("expired", created_at=NOW - timedelta(seconds=60))
    wrong_mode = _intent("wrong-mode", mode=TradingMode.LIVE)
    low_edge = _intent("low-edge", expected_move_bps="10", estimated_cost_bps="5")
    unsafe = _intent("unsafe").model_copy(update={"regime": MarketRegime.STRESS})

    result = orchestrator.orchestrate(
        (expired, wrong_mode, low_edge, unsafe), NOW
    )
    reasons = {item.signal_id: item.reason for item in result.decisions}

    assert reasons == {
        "expired": "signal_expired",
        "low-edge": "insufficient_edge_to_cost",
        "unsafe": "unsafe_regime",
        "wrong-mode": "trading_mode_mismatch",
    }
    assert not result.active


def test_signal_ids_are_idempotent_and_collisions_are_rejected() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    intent = _intent("same")
    accepted = orchestrator.orchestrate((intent,), NOW)
    replay = orchestrator.orchestrate((intent,), NOW)
    collision = orchestrator.orchestrate(
        (intent.model_copy(update={"confidence": Decimal("0.7")}),), NOW
    )

    assert accepted.decisions[0].status is SignalDecisionStatus.ACCEPTED
    assert replay.decisions[0].status is SignalDecisionStatus.DUPLICATE
    assert replay.decisions[0].reason == "idempotent_replay"
    assert collision.decisions[0].status is SignalDecisionStatus.REJECTED
    assert collision.decisions[0].reason == "signal_id_collision"
    assert len(collision.active) == 1


def test_stronger_opposing_signal_replaces_weaker_active_thesis() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    weak = _intent("weak", quality="50", confidence="0.5")
    strong = _intent(
        "strong",
        side=Side.SELL,
        quality="100",
        confidence="1",
        created_at=NOW + timedelta(seconds=1),
    )
    orchestrator.orchestrate((weak,), NOW)
    result = orchestrator.orchestrate((strong,), NOW + timedelta(seconds=1))

    assert result.replaced_signal_ids == ("weak",)
    assert [item.intent.signal_id for item in result.active] == ["strong"]
    assert result.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_lower_duplicate_thesis_is_rejected_without_removing_active_signal() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    strong = _intent("strong", quality="100", confidence="1")
    weak = _intent(
        "weak",
        quality="50",
        confidence="0.5",
        created_at=NOW + timedelta(seconds=1),
    )
    orchestrator.orchestrate((strong,), NOW)
    result = orchestrator.orchestrate((weak,), NOW + timedelta(seconds=1))

    assert result.decisions[0].reason == "correlated_duplicate"
    assert [item.intent.signal_id for item in result.active] == ["strong"]


def test_allocation_limits_correlation_groups_and_weights_are_deterministic() -> None:
    config = SignalOrchestratorConfig(
        max_active_per_strategy=3,
        max_active_per_asset=2,
        max_active_per_correlation_group=1,
        correlation_groups=(frozenset({"btc", "eth"}),),
        max_allocation_weight=Decimal("0.6"),
    )
    btc = _intent("btc", strategy_id="btc-strategy")
    eth = _intent("eth", asset="ETH", strategy_id="eth-strategy")

    first = SignalOrchestrator(TradingMode.PAPER, config).orchestrate(
        (eth, btc), NOW
    )
    second = SignalOrchestrator(TradingMode.PAPER, config).orchestrate(
        (btc, eth), NOW
    )

    assert first.model_dump() == second.model_dump()
    assert len(first.active) == 1
    assert first.active[0].allocation_weight == Decimal("0.6")
    rejected = next(
        item for item in first.decisions if item.status is SignalDecisionStatus.REJECTED
    )
    assert rejected.reason == "correlation_allocation_limit"


def test_expiration_is_reported_and_dangerous_signals_are_disabled_by_default() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    short = _intent("short", ttl_seconds=10)
    orchestrator.orchestrate((short,), NOW)
    expired = orchestrator.orchestrate((), NOW + timedelta(seconds=11))
    dangerous = orchestrator.orchestrate(
        (
            _intent(
                "martingale",
                signal_type=SignalType.MARTINGALE,
                created_at=NOW + timedelta(seconds=11),
            ),
        ),
        NOW + timedelta(seconds=11),
    )

    assert expired.expired_signal_ids == ("short",)
    assert dangerous.decisions[0].reason == "dangerous_signal_disabled"
    assert not dangerous.active


def test_orchestration_clock_cannot_move_backwards() -> None:
    orchestrator = SignalOrchestrator(TradingMode.PAPER)
    orchestrator.orchestrate((), NOW)

    with pytest.raises(ValueError, match="cannot move backwards"):
        orchestrator.orchestrate((), NOW - timedelta(seconds=1))
