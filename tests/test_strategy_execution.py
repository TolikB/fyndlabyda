from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.services.strategy_execution import (
    AdvancedStrategyExecutionPlanner,
    InstrumentExecutionQuote,
    StrategyExecutionPlanningError,
    StrategyExecutionSnapshot,
    StrategyPlanningBlockCode,
    build_strategy_execution_snapshot,
)

NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)


def _instrument(venue: str) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _book(
    instrument: InstrumentKey,
    *,
    bid: Decimal,
    ask: Decimal,
    timestamp: datetime = NOW,
    quantity: Decimal = Decimal("10"),
) -> BookSnapshot:
    return BookSnapshot(
        instrument=instrument,
        bids=(BookLevel(price=bid, quantity=quantity),),
        asks=(BookLevel(price=ask, quantity=quantity),),
        sequence=1,
        exchange_timestamp=timestamp,
    )


def _quote(
    instrument: InstrumentKey,
    *,
    bid: str,
    ask: str,
    timestamp: datetime = NOW,
    quantity: str = "10",
) -> InstrumentExecutionQuote:
    return InstrumentExecutionQuote(
        instrument=instrument,
        book=_book(
            instrument,
            bid=Decimal(bid),
            ask=Decimal(ask),
            timestamp=timestamp,
            quantity=Decimal(quantity),
        ),
        data_quality=DataQuality.VALID,
        quantity_step=Decimal("0.1"),
        price_tick=Decimal("0.1"),
        minimum_quantity=Decimal("0.1"),
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("5"),
    )


def _intent(*, market_making: bool = False) -> SignalIntent:
    primary = _instrument("BYBIT")
    hedge = primary if market_making else _instrument("GATE")
    legs = (
        SignalLeg(
            instrument=primary,
            side=Side.BUY if market_making else Side.SELL,
            preferred_limit_price=Decimal("99") if market_making else None,
            post_only=market_making,
        ),
        SignalLeg(
            instrument=hedge,
            side=Side.SELL if market_making else Side.BUY,
            hedge_ratio=Decimal("0.5") if not market_making else Decimal("1"),
            execution_priority=1,
            preferred_limit_price=Decimal("101") if market_making else None,
            post_only=market_making,
        ),
    )
    return SignalIntent(
        signal_id="signal-mm" if market_making else "signal-stat-arb",
        strategy_id=(
            "passive-market-making-v1"
            if market_making
            else "cross-exchange-lead-lag-v1"
        ),
        mode=TradingMode.PAPER,
        signal_type=(
            SignalType.PASSIVE_MARKET_MAKING
            if market_making
            else SignalType.CROSS_EXCHANGE_STAT_ARB
        ),
        primary_instrument=primary,
        side=legs[0].side,
        legs=legs,
        regime=MarketRegime.RANGE,
        quality_score=Decimal("80"),
        confidence=Decimal("0.8"),
        expected_holding_seconds=30,
        expected_move_bps=Decimal("20"),
        estimated_cost_bps=Decimal("5"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=10),
    )


def _decision(intent: SignalIntent, *, quantity: str = "2") -> RiskDecision:
    approved_quantity = Decimal(quantity)
    return RiskDecision(
        signal_id=intent.signal_id,
        decision_id="risk-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("10"),
        approved_quantity=approved_quantity,
        approved_notional=approved_quantity * Decimal("100"),
        max_slippage_bps=Decimal("20"),
        max_execution_seconds=5,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _snapshot(
    intent: SignalIntent,
    *quotes: InstrumentExecutionQuote,
) -> StrategyExecutionSnapshot:
    return build_strategy_execution_snapshot(
        intent=intent,
        source_event_id="event-1",
        captured_at=NOW,
        quotes=tuple(quotes),
    )


def test_planner_builds_content_bound_multi_leg_plan_deterministically() -> None:
    intent = _intent()
    snapshot = _snapshot(
        intent,
        _quote(intent.legs[0].instrument, bid="101", ask="102"),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    planner = AdvancedStrategyExecutionPlanner()

    first = planner.build(intent, _decision(intent), snapshot, NOW)
    second = planner.build(intent, _decision(intent), snapshot, NOW)

    assert first == second
    assert first.market_snapshot_id == snapshot.snapshot_id
    assert first.intent_fingerprint == snapshot.intent_fingerprint
    assert tuple(instruction.leg_index for instruction in first.instructions) == (0, 1)
    assert first.instructions[0].quantity == Decimal("2")
    assert first.instructions[0].limit_price == Decimal("100.7")
    assert first.instructions[1].quantity == Decimal("1.0")
    assert first.instructions[1].limit_price == Decimal("100.2")
    assert not any(instruction.post_only for instruction in first.instructions)


def test_planner_rejects_stale_and_future_books() -> None:
    intent = _intent()
    planner = AdvancedStrategyExecutionPlanner()
    stale = _snapshot(
        intent,
        _quote(
            intent.legs[0].instrument,
            bid="101",
            ask="102",
            timestamp=NOW - timedelta(seconds=6),
        ),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    with pytest.raises(StrategyExecutionPlanningError) as stale_error:
        planner.build(intent, _decision(intent), stale, NOW)
    assert stale_error.value.code is StrategyPlanningBlockCode.BOOK_STALE

    future = _snapshot(
        intent,
        _quote(
            intent.legs[0].instrument,
            bid="101",
            ask="102",
            timestamp=NOW + timedelta(milliseconds=1),
        ),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    with pytest.raises(StrategyExecutionPlanningError) as future_error:
        planner.build(intent, _decision(intent), future, NOW)
    assert future_error.value.code is StrategyPlanningBlockCode.BOOK_IN_FUTURE


def test_planner_rejects_tampered_intent_and_insufficient_depth() -> None:
    intent = _intent()
    snapshot = _snapshot(
        intent,
        _quote(intent.legs[0].instrument, bid="101", ask="102"),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    changed = intent.model_copy(update={"confidence": Decimal("0.7")})
    planner = AdvancedStrategyExecutionPlanner()
    with pytest.raises(StrategyExecutionPlanningError) as mismatch_error:
        planner.build(changed, _decision(changed), snapshot, NOW)
    assert mismatch_error.value.code is StrategyPlanningBlockCode.SNAPSHOT_MISMATCH

    shallow = _snapshot(
        intent,
        _quote(
            intent.legs[0].instrument,
            bid="101",
            ask="102",
            quantity="0.2",
        ),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    with pytest.raises(StrategyExecutionPlanningError) as depth_error:
        planner.build(intent, _decision(intent), shallow, NOW)
    assert depth_error.value.code is StrategyPlanningBlockCode.INSUFFICIENT_DEPTH


def test_planner_preserves_valid_post_only_quotes_and_rejects_crossing_price() -> None:
    intent = _intent(market_making=True)
    quote = _quote(intent.primary_instrument, bid="99", ask="101")
    snapshot = _snapshot(intent, quote)
    planner = AdvancedStrategyExecutionPlanner()

    plan = planner.build(intent, _decision(intent, quantity="1"), snapshot, NOW)

    assert tuple(instruction.limit_price for instruction in plan.instructions) == (
        Decimal("99"),
        Decimal("101"),
    )
    assert all(instruction.post_only for instruction in plan.instructions)

    crossing_leg = intent.legs[0].model_copy(
        update={"preferred_limit_price": Decimal("101")}
    )
    crossing = intent.model_copy(update={"legs": (crossing_leg, intent.legs[1])})
    crossing_snapshot = _snapshot(crossing, quote)
    with pytest.raises(StrategyExecutionPlanningError) as crossing_error:
        planner.build(
            crossing,
            _decision(crossing, quantity="1"),
            crossing_snapshot,
            NOW,
        )
    assert crossing_error.value.code is StrategyPlanningBlockCode.POST_ONLY_WOULD_CROSS


def test_post_only_signal_leg_requires_a_typed_price_preference() -> None:
    with pytest.raises(ValueError, match="preferred limit price"):
        SignalLeg(
            instrument=_instrument("BYBIT"),
            side=Side.BUY,
            post_only=True,
        )
