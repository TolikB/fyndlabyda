from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import MarketRegime, SignalType
from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
    OptionRight,
    Side,
    TradingMode,
)
from funding_arbitrage.signals import SignalDecisionStatus, SignalOrchestrator
from funding_arbitrage.strategies import (
    BlackScholesModel,
    OptionQuote,
    OptionsRiskLimits,
    OptionsVolatilityConfig,
    OptionsVolatilityContext,
    OptionsVolatilityStrategy,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
EXPIRY = NOW + timedelta(days=30)


def _option(right: OptionRight, *, expiry: datetime = EXPIRY) -> InstrumentKey:
    suffix = "C" if right is OptionRight.CALL else "P"
    return InstrumentKey(
        venue="DERIBIT",
        exchange_symbol=f"BTC-15SEP26-100-{suffix}",
        base_asset="BTC",
        quote_asset="USD",
        settlement_asset="BTC",
        instrument_type=InstrumentType.OPTION,
        expiry=expiry,
        strike_price=Decimal("100"),
        option_right=right,
    )


def _quote(
    right: OptionRight,
    *,
    implied_volatility: str = "0.50",
    underlying_price: str = "100",
    age_seconds: int = 0,
    liquidity_score: str = "0.9",
    quality: DataQuality = DataQuality.VALID,
    expiry: datetime = EXPIRY,
) -> OptionQuote:
    return OptionQuote(
        instrument=_option(right, expiry=expiry),
        underlying_price=Decimal(underlying_price),
        bid_price=Decimal("5.4"),
        ask_price=Decimal("5.6"),
        market_implied_volatility=Decimal(implied_volatility),
        contract_multiplier=Decimal("1"),
        open_interest_contracts=Decimal("10000"),
        volume_contracts=Decimal("1000"),
        liquidity_score=Decimal(liquidity_score),
        timestamp=NOW - timedelta(seconds=age_seconds),
        data_quality=quality,
    )


def _hedge(quote_asset: str = "USD") -> InstrumentKey:
    return InstrumentKey(
        venue="DERIBIT",
        exchange_symbol="BTC-PERPETUAL",
        base_asset="BTC",
        quote_asset=quote_asset,
        settlement_asset=quote_asset,
        instrument_type=InstrumentType.PERPETUAL,
    )


def _context(
    *,
    forecast: str = "0.70",
    call: OptionQuote | None = None,
    put: OptionQuote | None = None,
    costs: str = "100",
    mode: TradingMode = TradingMode.PAPER,
    regime: MarketRegime = MarketRegime.VOLATILITY_EXPANSION,
    margin_available: bool = True,
    short_authorized: bool = False,
    live_authorized: bool = False,
    hedge_quote_asset: str = "USD",
    hedge_quote_conversion_rate: Decimal | None = None,
    risk_limits: OptionsRiskLimits | None = None,
) -> OptionsVolatilityContext:
    return OptionsVolatilityContext(
        call=call or _quote(OptionRight.CALL),
        put=put or _quote(OptionRight.PUT),
        hedge_instrument=_hedge(hedge_quote_asset),
        forecast_realized_volatility=Decimal(forecast),
        estimated_cost_bps=Decimal(costs),
        timestamp=NOW,
        mode=mode,
        regime=regime,
        margin_available=margin_available,
        short_volatility_operator_authorized=short_authorized,
        live_operator_authorized=live_authorized,
        hedge_quote_conversion_rate=hedge_quote_conversion_rate,
        risk_limits=risk_limits or OptionsRiskLimits(),
    )


def test_black_scholes_put_call_parity_greeks_and_implied_volatility() -> None:
    model = BlackScholesModel()
    call = model.value(
        spot=Decimal("100"),
        strike=Decimal("100"),
        time_years=Decimal("1"),
        volatility=Decimal("0.2"),
        right=OptionRight.CALL,
    )
    put = model.value(
        spot=Decimal("100"),
        strike=Decimal("100"),
        time_years=Decimal("1"),
        volatility=Decimal("0.2"),
        right=OptionRight.PUT,
    )
    recovered = model.implied_volatility(
        option_price=call.price,
        spot=Decimal("100"),
        strike=Decimal("100"),
        time_years=Decimal("1"),
        right=OptionRight.CALL,
    )

    assert float(call.price) == pytest.approx(7.965567, rel=1e-6)
    assert float(call.price - put.price) == pytest.approx(0, abs=1e-12)
    assert float(call.delta - put.delta) == pytest.approx(1, abs=1e-12)
    assert call.gamma == put.gamma
    assert call.vega == put.vega
    assert float(recovered) == pytest.approx(0.2, abs=1e-8)

    with pytest.raises(ValueError, match="no-arbitrage"):
        model.implied_volatility(
            option_price=Decimal("101"),
            spot=Decimal("100"),
            strike=Decimal("100"),
            time_years=Decimal("1"),
            right=OptionRight.CALL,
        )


def test_long_volatility_signal_is_delta_hedged_and_risk_gated() -> None:
    result = OptionsVolatilityStrategy().evaluate(_context())

    assert result.intent is not None
    assert result.market_implied_volatility == Decimal("0.50")
    assert result.volatility_edge == Decimal("0.20")
    assert result.intent.signal_type is SignalType.OPTIONS_VOLATILITY
    assert result.intent.expected_move_bps == Decimal("2000.00")
    assert [leg.side for leg in result.intent.legs[:2]] == [Side.BUY, Side.BUY]
    assert result.intent.legs[2].instrument == _hedge()
    assert result.intent.legs[2].side is Side.SELL
    assert result.risk is not None and result.risk.approved is True
    assert abs(result.risk.snapshot.delta_notional_usd) < Decimal("0.000000001")

    decision = SignalOrchestrator(TradingMode.PAPER).orchestrate((result.intent,), NOW)
    assert decision.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_short_volatility_requires_explicit_authorization_and_stress_limits() -> None:
    disabled = OptionsVolatilityStrategy().evaluate(_context(forecast="0.30"))
    assert disabled.rejection_reason == "short_volatility_disabled"

    enabled = OptionsVolatilityStrategy(
        OptionsVolatilityConfig(short_volatility_enabled=True)
    ).evaluate(_context(forecast="0.30", short_authorized=True))
    assert enabled.intent is not None
    assert [leg.side for leg in enabled.intent.legs[:2]] == [Side.SELL, Side.SELL]
    assert enabled.intent.legs[2].side is Side.BUY

    constrained = OptionsVolatilityStrategy().evaluate(
        _context(
            risk_limits=OptionsRiskLimits(maximum_abs_gamma_cash_usd=Decimal("1"))
        )
    )
    assert constrained.rejection_reason == "options_gamma_limit"


def test_options_strategy_fails_closed_on_data_regime_cost_margin_and_live_mode() -> None:
    strategy = OptionsVolatilityStrategy()

    assert strategy.evaluate(_context(regime=MarketRegime.STRESS)).rejection_reason == (
        "unsafe_regime"
    )
    assert strategy.evaluate(
        _context(call=_quote(OptionRight.CALL, age_seconds=6))
    ).rejection_reason == "option_market_data_stale"
    assert strategy.evaluate(
        _context(call=_quote(OptionRight.CALL, age_seconds=-1))
    ).rejection_reason == "option_timestamp_in_future"
    assert strategy.evaluate(
        _context(call=_quote(OptionRight.CALL, quality=DataQuality.GAP))
    ).rejection_reason == "option_market_quality_not_valid"
    assert strategy.evaluate(
        _context(call=_quote(OptionRight.CALL, liquidity_score="0.5"))
    ).rejection_reason == "option_liquidity_below_threshold"
    assert strategy.evaluate(_context(forecast="0.52")).rejection_reason == (
        "volatility_edge_below_threshold"
    )
    assert strategy.evaluate(_context(costs="1000")).rejection_reason == (
        "insufficient_volatility_edge_to_cost"
    )
    assert strategy.evaluate(_context(margin_available=False)).rejection_reason == (
        "options_margin_unavailable"
    )
    assert strategy.evaluate(
        _context(mode=TradingMode.LIVE, live_authorized=True)
    ).rejection_reason == "live_options_disabled"


def test_options_strategy_exits_before_expiry_delivery_window() -> None:
    strategy = OptionsVolatilityStrategy()
    too_close = NOW + timedelta(seconds=900)
    rejected = strategy.evaluate(
        _context(
            call=_quote(OptionRight.CALL, expiry=too_close),
            put=_quote(OptionRight.PUT, expiry=too_close),
        )
    )

    assert rejected.rejection_reason == "option_expiry_exit_window"

    tradable_expiry = NOW + timedelta(hours=1)
    accepted = strategy.evaluate(
        _context(
            call=_quote(OptionRight.CALL, expiry=tradable_expiry),
            put=_quote(OptionRight.PUT, expiry=tradable_expiry),
        )
    )

    assert accepted.intent is not None
    assert accepted.intent.expected_holding_seconds == 2700
    assert accepted.intent.evidence["expiry_exit_buffer_seconds"] == "900"


def test_options_context_requires_explicit_compatible_quote_conversion() -> None:
    with pytest.raises(ValueError, match="explicit conversion"):
        _context(hedge_quote_asset="USDT")
    with pytest.raises(ValueError, match="incompatible"):
        _context(
            hedge_quote_asset="EUR",
            hedge_quote_conversion_rate=Decimal("1"),
        )
    with pytest.raises(ValueError, match="parity"):
        _context(
            hedge_quote_asset="USDT",
            hedge_quote_conversion_rate=Decimal("0.99"),
        )

    accepted = OptionsVolatilityStrategy().evaluate(
        _context(
            hedge_quote_asset="USDT",
            hedge_quote_conversion_rate=Decimal("1"),
        )
    )

    assert accepted.intent is not None
    assert accepted.intent.evidence["hedge_quote_conversion_rate"] == "1"


def test_options_strategy_is_deterministic() -> None:
    strategy = OptionsVolatilityStrategy()
    context = _context()

    assert strategy.evaluate(context).model_dump() == strategy.evaluate(context).model_dump()
