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
    FundingBasisContext,
    FundingBasisHarvestStrategy,
    FundingForecastEvent,
    FundingHarvestCosts,
    FundingLegForecast,
    FundingMarketLeg,
    SpotBorrowQuote,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
HORIZON = 8 * 3600


def _instrument(
    venue: str,
    instrument_type: InstrumentType,
) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol=f"BTCUSDT-{instrument_type.value}",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=instrument_type,
    )


def _leg(
    venue: str,
    instrument_type: InstrumentType,
    price: str,
    *,
    age_seconds: int = 0,
) -> FundingMarketLeg:
    return FundingMarketLeg(
        instrument=_instrument(venue, instrument_type),
        price=Decimal(price),
        timestamp=NOW - timedelta(seconds=age_seconds),
        data_quality=DataQuality.VALID,
        liquidity_score=Decimal("0.9"),
    )


def _forecast(
    instrument: InstrumentKey,
    rates: tuple[tuple[timedelta, str], ...],
    *,
    persistence: str = "0.8",
    sample_count: int = 30,
    sign_changes: int = 2,
    outlier: str = "1",
) -> FundingLegForecast:
    return FundingLegForecast(
        instrument=instrument,
        generated_at=NOW,
        events=tuple(
            FundingForecastEvent(
                settlement_time=NOW + offset,
                predicted_rate=Decimal(rate),
                source="median-ewma-v1",
            )
            for offset, rate in rates
        ),
        median_rate=Decimal(rates[0][1]) if rates else Decimal("0"),
        ewma_rate=Decimal(rates[0][1]) if rates else Decimal("0"),
        persistence_score=Decimal(persistence),
        sign_change_count=sign_changes,
        two_sided_outlier_score=Decimal(outlier),
        sample_count=sample_count,
        data_quality=DataQuality.VALID,
    )


def _costs(**updates: str) -> FundingHarvestCosts:
    values = {
        "leg_a_entry_fee_bps": "1",
        "leg_a_exit_fee_bps": "1",
        "leg_b_entry_fee_bps": "1",
        "leg_b_exit_fee_bps": "1",
        "spread_bps": "2",
        "slippage_bps": "2",
        "legging_risk_bps": "1",
    }
    values.update(updates)
    return FundingHarvestCosts(
        **{name: Decimal(value) for name, value in values.items()}
    )


def _spot_perp_context(
    *,
    rates: tuple[tuple[timedelta, str], ...] | None = None,
    costs: FundingHarvestCosts | None = None,
    borrow_quote: SpotBorrowQuote | None = None,
    mode: TradingMode = TradingMode.PAPER,
    live_authorized: bool = False,
    forecast_updates: dict[str, object] | None = None,
) -> FundingBasisContext:
    spot = _leg("BYBIT", InstrumentType.SPOT, "100")
    perp = _leg("BYBIT", InstrumentType.PERPETUAL, "101")
    forecast = _forecast(
        perp.instrument,
        rates
        or (
            (timedelta(hours=1), "0.001"),
            (timedelta(hours=8), "0.001"),
            (timedelta(hours=9), "0.5"),
        ),
    )
    if forecast_updates:
        forecast = forecast.model_copy(update=forecast_updates)
    return FundingBasisContext(
        leg_a=spot,
        leg_b=perp,
        forecasts=(forecast,),
        costs=costs or _costs(),
        requested_notional_usd=Decimal("1000"),
        expected_basis_convergence_bps=Decimal("10"),
        holding_horizon_seconds=HORIZON,
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
        margin_available=True,
        borrow_quote=borrow_quote,
        live_operator_authorized=live_authorized,
    )


def test_spot_perp_uses_exact_settlements_and_venue_leg_costs() -> None:
    result = FundingBasisHarvestStrategy().evaluate(_spot_perp_context())

    assert result.intent is not None
    assert result.expected_funding_bps == Decimal("20.000")
    assert result.expected_basis_bps == Decimal("10")
    assert result.expected_gross_bps == Decimal("30.000")
    assert result.expected_cost_bps == Decimal("9")
    assert result.expected_net_bps == Decimal("21.000")
    assert result.target_settlements == (
        NOW + timedelta(hours=1),
        NOW + timedelta(hours=8),
    )
    assert [leg.side for leg in result.intent.legs] == [Side.BUY, Side.SELL]
    assert result.intent.signal_type is SignalType.FUNDING_BASIS
    assert result.intent.evidence["venue_specific_fees_bps"] == {
        "leg_a_entry": "1",
        "leg_a_exit": "1",
        "leg_b_entry": "1",
        "leg_b_exit": "1",
    }
    decision = SignalOrchestrator(TradingMode.PAPER).orchestrate((result.intent,), NOW)
    assert decision.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_negative_funding_requires_fresh_sized_spot_borrow_quote() -> None:
    rates = (
        (timedelta(hours=1), "-0.001"),
        (timedelta(hours=8), "-0.001"),
    )
    unavailable = FundingBasisHarvestStrategy().evaluate(
        _spot_perp_context(rates=rates)
    )
    borrow = SpotBorrowQuote(
        venue="BYBIT",
        asset="BTC",
        available=True,
        daily_rate=Decimal("0.002"),
        maximum_notional_usd=Decimal("2000"),
        quoted_at=NOW - timedelta(seconds=1),
        valid_until=NOW + timedelta(minutes=5),
    )
    low_costs = _costs(
        leg_a_entry_fee_bps="0.1",
        leg_a_exit_fee_bps="0.1",
        leg_b_entry_fee_bps="0.1",
        leg_b_exit_fee_bps="0.1",
        spread_bps="0",
        slippage_bps="0",
        legging_risk_bps="0",
    )
    enabled = FundingBasisHarvestStrategy().evaluate(
        _spot_perp_context(rates=rates, costs=low_costs, borrow_quote=borrow)
    )

    assert unavailable.rejection_reason == "spot_borrow_unavailable"
    assert enabled.intent is not None
    assert [leg.side for leg in enabled.intent.legs] == [Side.SELL, Side.BUY]
    assert enabled.expected_borrow_bps == Decimal("6.666666666666666666666666666")


def test_perp_perp_uses_synchronized_signed_funding_cashflows() -> None:
    leg_a = _leg("BYBIT", InstrumentType.PERPETUAL, "100")
    leg_b = _leg("GATE", InstrumentType.PERPETUAL, "100.1")
    context = FundingBasisContext(
        leg_a=leg_a,
        leg_b=leg_b,
        forecasts=(
            _forecast(leg_a.instrument, ((timedelta(hours=1), "0.001"),)),
            _forecast(leg_b.instrument, ((timedelta(hours=4), "-0.0005"),)),
        ),
        costs=_costs(
            leg_a_entry_fee_bps="0.5",
            leg_a_exit_fee_bps="0.5",
            leg_b_entry_fee_bps="0.5",
            leg_b_exit_fee_bps="0.5",
            spread_bps="1",
            slippage_bps="1",
            legging_risk_bps="1",
        ),
        requested_notional_usd=Decimal("1000"),
        expected_basis_convergence_bps=Decimal("5"),
        holding_horizon_seconds=HORIZON,
        timestamp=NOW,
        mode=TradingMode.PAPER,
        regime=MarketRegime.RANGE,
        margin_available=True,
    )

    result = FundingBasisHarvestStrategy().evaluate(context)

    assert result.intent is not None
    assert [leg.side for leg in result.intent.legs] == [Side.SELL, Side.BUY]
    assert result.expected_funding_bps == Decimal("15.0000")
    assert result.expected_gross_bps == Decimal("20.0000")


def test_settlement_boundaries_forecast_quality_and_live_mode_fail_closed() -> None:
    strategy = FundingBasisHarvestStrategy()
    no_due = strategy.evaluate(
        _spot_perp_context(
            rates=(
                (timedelta(0), "0.1"),
                (timedelta(hours=8, microseconds=1), "0.1"),
            )
        )
    )
    low_persistence = strategy.evaluate(
        _spot_perp_context(forecast_updates={"persistence_score": Decimal("0.2")})
    )
    outlier = strategy.evaluate(
        _spot_perp_context(
            forecast_updates={"two_sided_outlier_score": Decimal("5")}
        )
    )
    live = strategy.evaluate(
        _spot_perp_context(mode=TradingMode.LIVE, live_authorized=True)
    )

    assert no_due.rejection_reason == "no_funding_settlement_in_horizon"
    assert low_persistence.rejection_reason == "funding_forecast_persistence_low"
    assert outlier.rejection_reason == "funding_forecast_outlier"
    assert live.rejection_reason == "live_funding_harvest_disabled"


def test_funding_basis_strategy_is_deterministic() -> None:
    strategy = FundingBasisHarvestStrategy()
    context = _spot_perp_context()

    assert strategy.evaluate(context).model_dump() == strategy.evaluate(context).model_dump()
