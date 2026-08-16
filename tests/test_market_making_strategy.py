from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.decisions import MarketRegime, SignalType
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.signals import SignalDecisionStatus, SignalOrchestrator
from funding_arbitrage.strategies import (
    MarketMakingContext,
    MarketMakingCosts,
    MarketMakingInventory,
    PassiveMarketMakingStrategy,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BINANCE",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _book(*, age_seconds: int = 0) -> BookSnapshot:
    return BookSnapshot(
        instrument=INSTRUMENT,
        bids=(
            BookLevel(price=Decimal("99.90"), quantity=Decimal("10")),
            BookLevel(price=Decimal("99.80"), quantity=Decimal("20")),
        ),
        asks=(
            BookLevel(price=Decimal("100.10"), quantity=Decimal("10")),
            BookLevel(price=Decimal("100.20"), quantity=Decimal("20")),
        ),
        sequence=10,
        exchange_timestamp=NOW - timedelta(seconds=age_seconds),
    )


def _orderflow(*, age_seconds: int = 0) -> OrderFlowFeatureSnapshot:
    return OrderFlowFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW - timedelta(seconds=age_seconds),
        data_quality=DataQuality.VALID,
        mid_price=Decimal("100"),
        microprice=Decimal("100"),
        spread_bps=Decimal("20"),
        ofi_zscore_5s=Decimal("0.5"),
        book_imbalance_l5=Decimal("0"),
        trade_imbalance_5s=Decimal("0.1"),
        cvd=Decimal("0"),
    )


def _costs(*, maker_fee: str = "1", adverse: str = "2") -> MarketMakingCosts:
    return MarketMakingCosts(
        maker_fee_bps_per_fill=Decimal(maker_fee),
        expected_adverse_selection_bps=Decimal(adverse),
        expected_hedging_bps=Decimal("1"),
    )


def _context(
    *,
    inventory: str = "0",
    book: BookSnapshot | None = None,
    orderflow: OrderFlowFeatureSnapshot | None = None,
    book_quality: DataQuality = DataQuality.VALID,
    costs: MarketMakingCosts | None = None,
    volatility_bps: str = "4",
    mode: TradingMode = TradingMode.PAPER,
    regime: MarketRegime = MarketRegime.RANGE,
    live_authorized: bool = False,
) -> MarketMakingContext:
    return MarketMakingContext(
        instrument=INSTRUMENT,
        book=book or _book(),
        book_quality=book_quality,
        orderflow=orderflow or _orderflow(),
        inventory=MarketMakingInventory(
            signed_quantity=Decimal(inventory),
            maximum_abs_quantity=Decimal("10"),
        ),
        costs=costs or _costs(),
        short_horizon_volatility_bps=Decimal(volatility_bps),
        timestamp=NOW,
        mode=mode,
        regime=regime,
        live_operator_authorized=live_authorized,
    )


def test_passive_quotes_capture_spread_after_fees_and_adverse_selection() -> None:
    result = PassiveMarketMakingStrategy().evaluate(_context())

    assert result.intent is not None
    assert result.proposal is not None
    assert result.proposal.bid_price == Decimal("99.90")
    assert result.proposal.ask_price == Decimal("100.10")
    assert result.proposal.gross_capture_bps == Decimal("20.00")
    assert result.proposal.adverse_selection_bps == Decimal("3.70")
    assert result.proposal.estimated_cost_bps == Decimal("6.70")
    assert result.proposal.expected_net_edge_bps == Decimal("13.30")
    assert result.intent.signal_type is SignalType.PASSIVE_MARKET_MAKING
    assert [leg.side for leg in result.intent.legs] == [Side.BUY, Side.SELL]
    assert result.intent.evidence["post_only_required"] is True

    decision = SignalOrchestrator(TradingMode.PAPER).orchestrate((result.intent,), NOW)
    assert decision.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_positive_inventory_skews_price_and_size_toward_reduction() -> None:
    result = PassiveMarketMakingStrategy().evaluate(_context(inventory="5"))

    assert result.proposal is not None
    assert result.proposal.inventory_deviation == Decimal("0.5")
    assert result.proposal.reservation_price == Decimal("99.9500")
    assert result.proposal.bid_price == Decimal("99.85")
    assert result.proposal.ask_price == Decimal("100.10")
    assert result.proposal.bid_size_multiplier == Decimal("0.625")
    assert result.proposal.ask_size_multiplier == Decimal("1.375")
    assert result.intent is not None and result.intent.side is Side.SELL


def test_market_making_rejects_fee_and_adverse_selection_traps() -> None:
    strategy = PassiveMarketMakingStrategy()

    fee_trap = strategy.evaluate(_context(costs=_costs(maker_fee="30")))
    toxic_flow = strategy.evaluate(_context(volatility_bps="100"))

    assert fee_trap.rejection_reason == "required_quote_width_exceeds_limit"
    assert toxic_flow.rejection_reason == "adverse_selection_too_high"


def test_market_making_fails_closed_on_quality_age_inventory_regime_and_live() -> None:
    strategy = PassiveMarketMakingStrategy()

    assert strategy.evaluate(
        _context(book_quality=DataQuality.GAP)
    ).rejection_reason == "market_making_data_quality_not_valid"
    assert strategy.evaluate(_context(book=_book(age_seconds=3))).rejection_reason == (
        "market_making_data_stale"
    )
    assert strategy.evaluate(_context(inventory="11")).rejection_reason == (
        "inventory_limit_breached"
    )
    assert strategy.evaluate(_context(regime=MarketRegime.TREND_UP)).rejection_reason == (
        "regime_not_market_making"
    )
    assert strategy.evaluate(
        _context(mode=TradingMode.LIVE, live_authorized=True)
    ).rejection_reason == "live_market_making_disabled"


def test_market_making_is_deterministic() -> None:
    strategy = PassiveMarketMakingStrategy()
    context = _context(inventory="2")

    assert strategy.evaluate(context).model_dump() == strategy.evaluate(context).model_dump()
