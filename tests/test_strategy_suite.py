from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import (
    MarketRegime,
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
    OptionRight,
    Side,
    TradingMode,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.services.strategy_suite import (
    LeadLagStrategyContext,
    StrategyFamily,
    StrategySuite,
    StrategySuiteRequest,
    StrategySuiteResult,
    SupplementalStrategyContexts,
)
from funding_arbitrage.strategies import (
    BasisMarketLeg,
    DangerousResearchContext,
    DatedBasisContext,
    DatedBasisCosts,
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
    ForecastFundingEvent,
    FundingBasisContext,
    FundingForecastEvent,
    FundingHarvestCosts,
    FundingLegForecast,
    FundingMarketLeg,
    LeadLagCostModel,
    MarketMakingContext,
    MarketMakingCosts,
    MarketMakingInventory,
    OptionQuote,
    OptionsVolatilityContext,
    VenueFairValueInput,
)

NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
PERPETUAL = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


class AcceptDirectional:
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        created_at = max(
            context.technical.timestamp,
            context.orderflow.timestamp,
            context.structure.timestamp,
            context.regime.timestamp,
        )
        strategy_id = "suite-directional-accepted"
        signal_id = "sig_" + hashlib.sha256(
            f"{strategy_id}|{context.instrument.canonical_id}|{created_at.isoformat()}".encode()
        ).hexdigest()[:32]
        price = context.technical.close
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id=strategy_id,
            mode=context.mode,
            signal_type=SignalType.ORDERFLOW_BREAKOUT,
            primary_instrument=context.instrument,
            side=Side.BUY,
            legs=(SignalLeg(instrument=context.instrument, side=Side.BUY),),
            regime=context.regime.regime,
            quality_score=Decimal("90"),
            confidence=Decimal("0.9"),
            entry_zone_low=price,
            entry_zone_high=price + Decimal("0.01"),
            structural_stop=price - Decimal("1"),
            targets=(price + Decimal("3"),),
            expected_holding_seconds=900,
            expected_move_bps=Decimal("100"),
            estimated_cost_bps=context.estimated_cost_bps,
            expected_rr=Decimal("3"),
            created_at=created_at,
            expires_at=created_at + timedelta(seconds=15),
        )
        return DirectionalStrategyEvaluation(
            strategy_id=strategy_id,
            intent=intent,
            score=Decimal("0.9"),
        )


class RejectDirectional:
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        del context
        return DirectionalStrategyEvaluation(
            strategy_id="suite-directional-rejected",
            rejection_reason="synthetic_rejection",
            score=Decimal("0"),
        )


class ScoredDirectional(AcceptDirectional):
    def __init__(self, score: Decimal) -> None:
        self.score = score

    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        return super().evaluate(context).model_copy(update={"score": self.score})


def _instrument(
    venue: str,
    instrument_type: InstrumentType = InstrumentType.PERPETUAL,
    *,
    exchange_symbol: str = "BTCUSDT",
    expiry: datetime | None = None,
) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol=exchange_symbol,
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=instrument_type,
        expiry=expiry,
    )


def _regime(timestamp: datetime = NOW) -> RegimeSnapshot:
    return RegimeSnapshot(
        instrument=PERPETUAL,
        timestamp=timestamp,
        regime=MarketRegime.RANGE,
        candidate=MarketRegime.RANGE,
        confidence=Decimal("0.9"),
        regime_since=timestamp - timedelta(hours=1),
        dwell_seconds=Decimal("3600"),
        pending_confirmations=0,
        data_quality=DataQuality.VALID,
    )


def _orderflow(timestamp: datetime = NOW) -> OrderFlowFeatureSnapshot:
    return OrderFlowFeatureSnapshot(
        instrument=PERPETUAL,
        timestamp=timestamp,
        data_quality=DataQuality.VALID,
        mid_price=Decimal("100"),
        microprice=Decimal("100"),
        spread_bps=Decimal("20"),
        ofi_zscore_5s=Decimal("0.5"),
        book_imbalance_l5=Decimal("0"),
        trade_imbalance_5s=Decimal("0.1"),
        cvd=Decimal("0"),
    )


def _directional(
    mode: TradingMode,
    *,
    timestamp: datetime = NOW,
) -> DirectionalStrategyContext:
    return DirectionalStrategyContext(
        instrument=PERPETUAL,
        mode=mode,
        technical=TechnicalFeatureSnapshot(
            instrument=PERPETUAL,
            timestamp=timestamp,
            data_quality=DataQuality.VALID,
            sample_count=100,
            close=Decimal("100"),
            ema_fast=Decimal("101"),
            ema_slow=Decimal("99"),
            atr=Decimal("1"),
        ),
        orderflow=_orderflow(timestamp),
        structure=MarketStructureSnapshot(
            instrument=PERPETUAL,
            timestamp=timestamp,
            data_quality=DataQuality.VALID,
            trend=StructureDirection.NEUTRAL,
        ),
        regime=_regime(timestamp),
        estimated_cost_bps=Decimal("5"),
    )


def _funding(mode: TradingMode) -> FundingBasisContext:
    spot = FundingMarketLeg(
        instrument=_instrument("BYBIT", InstrumentType.SPOT),
        price=Decimal("100"),
        timestamp=NOW,
        data_quality=DataQuality.VALID,
        liquidity_score=Decimal("0.9"),
    )
    perpetual = FundingMarketLeg(
        instrument=PERPETUAL,
        price=Decimal("101"),
        timestamp=NOW,
        data_quality=DataQuality.VALID,
        liquidity_score=Decimal("0.9"),
    )
    forecast = FundingLegForecast(
        instrument=PERPETUAL,
        generated_at=NOW,
        events=(
            FundingForecastEvent(
                settlement_time=NOW + timedelta(hours=1),
                predicted_rate=Decimal("0.001"),
                source="suite-test",
            ),
            FundingForecastEvent(
                settlement_time=NOW + timedelta(hours=8),
                predicted_rate=Decimal("0.001"),
                source="suite-test",
            ),
        ),
        median_rate=Decimal("0.001"),
        ewma_rate=Decimal("0.001"),
        persistence_score=Decimal("0.9"),
        sign_change_count=0,
        two_sided_outlier_score=Decimal("1"),
        sample_count=30,
        data_quality=DataQuality.VALID,
    )
    return FundingBasisContext(
        leg_a=spot,
        leg_b=perpetual,
        forecasts=(forecast,),
        costs=FundingHarvestCosts(
            leg_a_entry_fee_bps=Decimal("1"),
            leg_a_exit_fee_bps=Decimal("1"),
            leg_b_entry_fee_bps=Decimal("1"),
            leg_b_exit_fee_bps=Decimal("1"),
            spread_bps=Decimal("1"),
            slippage_bps=Decimal("1"),
        ),
        requested_notional_usd=Decimal("1000"),
        expected_basis_convergence_bps=Decimal("10"),
        holding_horizon_seconds=8 * 3600,
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
        margin_available=True,
    )


def _lead_lag(mode: TradingMode) -> LeadLagStrategyContext:
    def venue(name: str, price: str) -> VenueFairValueInput:
        return VenueFairValueInput(
            instrument=_instrument(name),
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            mid_price=Decimal(price),
            microprice=Decimal(price),
            liquidity_score=Decimal("0.9"),
        )

    return LeadLagStrategyContext(
        primary=venue("BYBIT", "101"),
        references=(venue("BINANCE", "100"), venue("OKX", "100")),
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
        costs=LeadLagCostModel(
            fees_bps=Decimal("2"),
            spread_bps=Decimal("1"),
            slippage_bps=Decimal("1"),
            adverse_selection_bps=Decimal("1"),
        ),
        inventory_available=True,
        transfer_ready=True,
    )


def _dated(mode: TradingMode) -> DatedBasisContext:
    expiry = NOW + timedelta(days=30)
    return DatedBasisContext(
        perpetual=BasisMarketLeg(
            instrument=PERPETUAL,
            price=Decimal("100"),
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            liquidity_score=Decimal("0.9"),
        ),
        future=BasisMarketLeg(
            instrument=_instrument(
                "BYBIT",
                InstrumentType.FUTURE,
                exchange_symbol="BTC-30SEP26",
                expiry=expiry,
            ),
            price=Decimal("102"),
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            liquidity_score=Decimal("0.9"),
        ),
        funding_events=(
            ForecastFundingEvent(
                settlement_time=NOW + timedelta(hours=8),
                funding_rate=Decimal("0.0001"),
            ),
        ),
        costs=DatedBasisCosts(
            entry_exit_fees_bps=Decimal("10"),
            spread_bps=Decimal("2"),
            slippage_bps=Decimal("2"),
        ),
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
        margin_available=True,
    )


def _options(mode: TradingMode) -> OptionsVolatilityContext:
    expiry = NOW + timedelta(days=30)

    def quote(right: OptionRight) -> OptionQuote:
        suffix = "C" if right is OptionRight.CALL else "P"
        return OptionQuote(
            instrument=InstrumentKey(
                venue="DERIBIT",
                exchange_symbol=f"BTC-30SEP26-100-{suffix}",
                base_asset="BTC",
                quote_asset="USD",
                settlement_asset="BTC",
                instrument_type=InstrumentType.OPTION,
                expiry=expiry,
                strike_price=Decimal("100"),
                option_right=right,
            ),
            underlying_price=Decimal("100"),
            bid_price=Decimal("5.4"),
            ask_price=Decimal("5.6"),
            market_implied_volatility=Decimal("0.50"),
            open_interest_contracts=Decimal("10000"),
            volume_contracts=Decimal("1000"),
            liquidity_score=Decimal("0.9"),
            timestamp=NOW,
            data_quality=DataQuality.VALID,
        )

    hedge = InstrumentKey(
        venue="DERIBIT",
        exchange_symbol="BTC-PERPETUAL",
        base_asset="BTC",
        quote_asset="USD",
        settlement_asset="BTC",
        instrument_type=InstrumentType.PERPETUAL,
    )
    return OptionsVolatilityContext(
        call=quote(OptionRight.CALL),
        put=quote(OptionRight.PUT),
        hedge_instrument=hedge,
        forecast_realized_volatility=Decimal("0.70"),
        estimated_cost_bps=Decimal("100"),
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.VOLATILITY_EXPANSION,
        margin_available=True,
    )


def _market_making(mode: TradingMode) -> MarketMakingContext:
    book = BookSnapshot(
        instrument=PERPETUAL,
        bids=(BookLevel(price=Decimal("99.90"), quantity=Decimal("10")),),
        asks=(BookLevel(price=Decimal("100.10"), quantity=Decimal("10")),),
        sequence=1,
        exchange_timestamp=NOW,
    )
    return MarketMakingContext(
        instrument=PERPETUAL,
        book=book,
        book_quality=DataQuality.VALID,
        orderflow=_orderflow(),
        inventory=MarketMakingInventory(
            signed_quantity=Decimal("0"),
            maximum_abs_quantity=Decimal("10"),
        ),
        costs=MarketMakingCosts(
            maker_fee_bps_per_fill=Decimal("1"),
            expected_adverse_selection_bps=Decimal("2"),
            expected_hedging_bps=Decimal("1"),
        ),
        short_horizon_volatility_bps=Decimal("4"),
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
    )


def _dangerous(mode: TradingMode) -> DangerousResearchContext:
    return DangerousResearchContext(
        instrument=PERPETUAL,
        price=Decimal("98"),
        market_timestamp=NOW,
        timestamp=NOW,
        mode=mode,
        regime=MarketRegime.RANGE,
        data_quality=DataQuality.VALID,
        margin_available=True,
        portfolio_drawdown_fraction=Decimal("0.01"),
        estimated_cost_bps=Decimal("10"),
        reference_side=Side.BUY,
        latest_closed_trade_pnl_bps=Decimal("-60"),
        consecutive_losses=2,
        anchor_price=Decimal("100"),
        current_signed_quantity=Decimal("5"),
        average_entry_price=Decimal("100"),
        prior_additions=1,
    )


def _request(mode: TradingMode) -> StrategySuiteRequest:
    return StrategySuiteRequest(
        request_id="request-1",
        source_event_id="event-1",
        mode=mode,
        timestamp=NOW,
        directional=(_directional(mode),),
        supplemental=SupplementalStrategyContexts(
            funding_basis=(_funding(mode),),
            lead_lag=(_lead_lag(mode),),
            dated_basis=(_dated(mode),),
            options_volatility=(_options(mode),),
            passive_market_making=(_market_making(mode),),
            dangerous_research=(_dangerous(mode),),
        ),
    )


def _suite() -> StrategySuite:
    return StrategySuite(
        directional_strategies=(AcceptDirectional(), RejectDirectional())
    )


def test_strategy_suite_evaluates_every_family_deterministically() -> None:
    request = _request(TradingMode.SHADOW)

    first = _suite().evaluate(request)
    second = _suite().evaluate(request)

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
    restored = StrategySuiteResult.model_validate(first.model_dump(mode="json"))
    assert restored.model_dump(mode="json") == first.model_dump(mode="json")
    assert len(first.evaluations) == 10
    assert len(first.directional_evaluations) == 2
    assert {item.family for item in first.evaluations} == set(StrategyFamily)
    assert {intent.signal_type for intent in first.intents} == {
        SignalType.ORDERFLOW_BREAKOUT,
        SignalType.FUNDING_BASIS,
        SignalType.CROSS_EXCHANGE_STAT_ARB,
        SignalType.DATED_FUTURES_BASIS,
        SignalType.OPTIONS_VOLATILITY,
        SignalType.PASSIVE_MARKET_MAKING,
    }
    assert [
        item.rejection_reason
        for item in first.evaluations
        if item.family
        in {
            StrategyFamily.MARTINGALE_RESEARCH,
            StrategyFamily.GRID_RESEARCH,
            StrategyFamily.LOSS_AVERAGING_RESEARCH,
        }
    ] == [
        "grid-research-v1_disabled",
        "loss-averaging-research-v1_disabled",
        "martingale-research-v1_disabled",
    ]


def test_execution_modes_suppress_unplanned_advanced_intents_but_keep_audit() -> None:
    paper = _suite().evaluate(_request(TradingMode.PAPER))
    safe = _suite().evaluate(_request(TradingMode.SAFE_MODE))

    assert [intent.signal_type for intent in paper.intents] == [
        SignalType.ORDERFLOW_BREAKOUT
    ]
    funding = next(
        item
        for item in paper.evaluations
        if item.family is StrategyFamily.FUNDING_BASIS
    )
    assert funding.intent is None
    assert funding.rejection_reason == "execution_planner_unavailable"
    assert funding.evaluation_payload["intent"] is not None
    assert safe.intents == ()
    accepted_directional = next(
        item
        for item in safe.evaluations
        if item.strategy_id == "suite-directional-accepted"
    )
    assert accepted_directional.rejection_reason == "safe_mode_suppressed"


def test_strategy_suite_request_rejects_empty_mismatched_future_and_duplicate_contexts() -> None:
    with pytest.raises(ValueError, match="at least one context"):
        StrategySuiteRequest(
            request_id="empty",
            source_event_id="event",
            mode=TradingMode.SHADOW,
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="trading mode mismatch"):
        StrategySuiteRequest(
            request_id="mode",
            source_event_id="event",
            mode=TradingMode.SHADOW,
            timestamp=NOW,
            directional=(_directional(TradingMode.PAPER),),
        )
    with pytest.raises(ValueError, match="timestamp is in the future"):
        StrategySuiteRequest(
            request_id="future",
            source_event_id="event",
            mode=TradingMode.SHADOW,
            timestamp=NOW,
            directional=(
                _directional(TradingMode.SHADOW, timestamp=NOW + timedelta(seconds=1)),
            ),
        )
    funding = _funding(TradingMode.SHADOW)
    with pytest.raises(ValueError, match="duplicate funding_basis"):
        StrategySuiteRequest(
            request_id="duplicate",
            source_event_id="event",
            mode=TradingMode.SHADOW,
            timestamp=NOW,
            supplemental=SupplementalStrategyContexts(
                funding_basis=(funding, funding)
            ),
        )


def test_duplicate_strategy_signal_identity_fails_closed() -> None:
    suite = StrategySuite(
        directional_strategies=(AcceptDirectional(), AcceptDirectional())
    )
    request = StrategySuiteRequest(
        request_id="collision",
        source_event_id="event",
        mode=TradingMode.SHADOW,
        timestamp=NOW,
        directional=(_directional(TradingMode.SHADOW),),
    )

    with pytest.raises(ValueError, match="duplicate signal identities"):
        suite.evaluate(request)


def test_evaluation_identity_changes_when_raw_metrics_change() -> None:
    request = StrategySuiteRequest(
        request_id="metric-identity",
        source_event_id="event",
        mode=TradingMode.SHADOW,
        timestamp=NOW,
        directional=(_directional(TradingMode.SHADOW),),
    )

    high = StrategySuite(
        directional_strategies=(ScoredDirectional(Decimal("0.9")),)
    ).evaluate(request)
    low = StrategySuite(
        directional_strategies=(ScoredDirectional(Decimal("0.8")),)
    ).evaluate(request)

    assert high.intents == low.intents
    assert high.evaluations[0].evaluation_id != low.evaluations[0].evaluation_id
    assert high.suite_id != low.suite_id
