from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.decisions import MarketRegime, SignalType
from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.signals import SignalDecisionStatus, SignalOrchestrator
from funding_arbitrage.strategies import (
    BasisMarketLeg,
    DatedBasisContext,
    DatedBasisCosts,
    DatedFuturesBasisStrategy,
    ForecastFundingEvent,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
EXPIRY = NOW + timedelta(days=30)


def _instrument(
    instrument_type: InstrumentType,
    *,
    expiry: datetime | None = None,
) -> InstrumentKey:
    return InstrumentKey(
        venue="BYBIT",
        exchange_symbol="BTCUSDT" if expiry is None else "BTC-16SEP26",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=instrument_type,
        settlement_asset="USDT",
        expiry=expiry,
    )


def _leg(
    instrument_type: InstrumentType,
    price: str,
    *,
    expiry: datetime | None = None,
    age_seconds: int = 0,
    quality: DataQuality = DataQuality.VALID,
) -> BasisMarketLeg:
    return BasisMarketLeg(
        instrument=_instrument(instrument_type, expiry=expiry),
        price=Decimal(price),
        timestamp=NOW - timedelta(seconds=age_seconds),
        data_quality=quality,
        liquidity_score=Decimal("0.9"),
    )


def _costs(total: str = "20") -> DatedBasisCosts:
    return DatedBasisCosts(
        entry_exit_fees_bps=Decimal(total),
        spread_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
    )


def _context(
    *,
    perpetual_price: str = "100",
    future_price: str = "102",
    funding_events: tuple[ForecastFundingEvent, ...] | None = None,
    costs: DatedBasisCosts | None = None,
    regime: MarketRegime = MarketRegime.RANGE,
    margin_available: bool = True,
    perpetual_age_seconds: int = 0,
    perpetual_quality: DataQuality = DataQuality.VALID,
    expiry: datetime = EXPIRY,
) -> DatedBasisContext:
    return DatedBasisContext(
        perpetual=_leg(
            InstrumentType.PERPETUAL,
            perpetual_price,
            age_seconds=perpetual_age_seconds,
            quality=perpetual_quality,
        ),
        future=_leg(InstrumentType.FUTURE, future_price, expiry=expiry),
        funding_events=funding_events
        or (
            ForecastFundingEvent(
                settlement_time=NOW + timedelta(hours=8),
                funding_rate=Decimal("0.0001"),
            ),
            ForecastFundingEvent(
                settlement_time=NOW + timedelta(hours=16),
                funding_rate=Decimal("0.0001"),
            ),
            ForecastFundingEvent(
                settlement_time=expiry + timedelta(hours=1),
                funding_rate=Decimal("0.5"),
            ),
        ),
        costs=costs or _costs(),
        timestamp=NOW,
        mode=TradingMode.PAPER,
        regime=regime,
        margin_available=margin_available,
    )


def test_premium_basis_emits_hedged_intent_with_exact_funding_window() -> None:
    result = DatedFuturesBasisStrategy().evaluate(_context())

    assert result.intent is not None
    assert result.basis_bps == Decimal("200")
    assert result.expected_funding_bps == Decimal("-2")
    assert result.expected_gross_carry_bps == Decimal("198")
    assert result.expected_net_carry_bps == Decimal("178")
    assert len(result.included_settlements) == 2
    assert result.annualized_basis_percent == Decimal("24.33333333333333333333333333")
    assert result.intent.signal_type is SignalType.DATED_FUTURES_BASIS
    assert [leg.side for leg in result.intent.legs] == [Side.BUY, Side.SELL]
    assert result.intent.expected_move_bps == Decimal("198")
    assert result.intent.estimated_cost_bps == Decimal("20")

    orchestrated = SignalOrchestrator(TradingMode.PAPER).orchestrate((result.intent,), NOW)
    assert orchestrated.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_discount_basis_shorts_perpetual_and_earns_positive_funding() -> None:
    result = DatedFuturesBasisStrategy().evaluate(
        _context(perpetual_price="100", future_price="98")
    )

    assert result.intent is not None
    assert [leg.side for leg in result.intent.legs] == [Side.SELL, Side.BUY]
    assert result.expected_funding_bps == Decimal("2")
    assert result.expected_gross_carry_bps == Decimal("202")


def test_settlement_boundaries_are_exact_and_deterministic() -> None:
    events = (
        ForecastFundingEvent(settlement_time=NOW, funding_rate=Decimal("0.5")),
        ForecastFundingEvent(settlement_time=EXPIRY, funding_rate=Decimal("0.0001")),
        ForecastFundingEvent(
            settlement_time=EXPIRY + timedelta(microseconds=1),
            funding_rate=Decimal("0.5"),
        ),
    )
    strategy = DatedFuturesBasisStrategy()
    context = _context(funding_events=events)

    first = strategy.evaluate(context)
    second = strategy.evaluate(context)

    assert first.model_dump() == second.model_dump()
    assert first.included_settlements == (EXPIRY,)
    assert first.expected_funding_bps == Decimal("-1")


def test_basis_strategy_fails_closed_on_market_and_risk_gates() -> None:
    strategy = DatedFuturesBasisStrategy()

    assert strategy.evaluate(_context(margin_available=False)).rejection_reason == (
        "margin_unavailable"
    )
    assert strategy.evaluate(_context(regime=MarketRegime.STRESS)).rejection_reason == (
        "unsafe_regime"
    )
    assert strategy.evaluate(
        _context(perpetual_quality=DataQuality.INVALID)
    ).rejection_reason == "market_quality_not_valid"
    assert strategy.evaluate(
        _context(perpetual_age_seconds=6)
    ).rejection_reason == "market_data_stale"
    assert strategy.evaluate(
        _context(perpetual_age_seconds=-1)
    ).rejection_reason == "market_timestamp_in_future"
    assert strategy.evaluate(_context(future_price="100.1")).rejection_reason == (
        "basis_below_threshold"
    )
    assert strategy.evaluate(_context(costs=_costs("100"))).rejection_reason == (
        "insufficient_carry_to_cost"
    )
    assert strategy.evaluate(_context(expiry=NOW)).rejection_reason == "future_expired"
    assert strategy.evaluate(
        _context(expiry=NOW + timedelta(days=121))
    ).rejection_reason == "expiry_too_distant"
