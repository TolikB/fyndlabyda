from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, Side, TradingMode

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
BTC_PERP = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _breakout_intent() -> SignalIntent:
    return SignalIntent(
        signal_id="signal-1",
        strategy_id="orderflow-breakout-v1",
        mode=TradingMode.PAPER,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=BTC_PERP,
        side=Side.BUY,
        legs=(SignalLeg(instrument=BTC_PERP, side=Side.BUY),),
        regime=MarketRegime.TREND_UP,
        quality_score=Decimal("92"),
        confidence=Decimal("0.84"),
        entry_zone_low=Decimal("62000"),
        entry_zone_high=Decimal("62010"),
        structural_stop=Decimal("61800"),
        targets=(Decimal("62500"), Decimal("63000")),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("80"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2.5"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
        evidence={"ofi_5s": "2.1", "book_sequence": 123},
    )


def test_directional_signal_is_declarative_and_expiring() -> None:
    intent = _breakout_intent()

    assert intent.legs[0].hedge_ratio == Decimal("1")
    assert intent.expires_at > intent.created_at
    assert "quantity" not in SignalIntent.model_fields
    assert "order_type" not in SignalIntent.model_fields


def test_directional_signal_rejects_missing_stop_or_target_contract() -> None:
    with pytest.raises(ValidationError, match="directional signals require"):
        _breakout_intent().model_copy(update={"structural_stop": None}).model_validate(
            _breakout_intent().model_dump() | {"structural_stop": None}
        )


def test_meta_filter_cannot_be_misused_as_a_live_execution_signal() -> None:
    payload = _breakout_intent().model_dump()
    payload.update(
        signal_type=SignalType.LEAD_LAG_FILTER,
        mode=TradingMode.LIVE,
        structural_stop=None,
        targets=(),
        expected_rr=None,
    )
    with pytest.raises(ValidationError, match="cannot directly request live execution"):
        SignalIntent.model_validate(payload)


def test_risk_decision_is_the_only_positive_size_authorization() -> None:
    decision = RiskDecision(
        signal_id="signal-1",
        decision_id="risk-1",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("25"),
        approved_quantity=Decimal("0.01"),
        approved_notional=Decimal("620"),
        max_slippage_bps=Decimal("4"),
        max_execution_seconds=2,
        correlation_multiplier=Decimal("0.5"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    assert decision.approved_notional == Decimal("620")

    with pytest.raises(ValidationError, match="requires a reason and zero size"):
        RiskDecision.model_validate(
            decision.model_dump()
            | {
                "approved": False,
                "rejection_reason": "stale_book",
            }
        )
