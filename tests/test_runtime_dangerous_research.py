"""The research strategies must be reachable when authorized and inert otherwise."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import MarketRegime
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
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.services.multi_regime import (
    MultiRegimeEngine,
    MultiRegimeStrategySnapshot,
)
from funding_arbitrage.services.runtime_dangerous_research import (
    build_dangerous_research_contexts,
    build_dangerous_research_strategies,
    dangerous_research_enabled,
)
from funding_arbitrage.services.strategy_suite import StrategySuite

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)

RESEARCH_FLAGS = {
    "martingale": "MARTINGALE_RESEARCH_ENABLED",
    "grid_averaging": "GRID_RESEARCH_ENABLED",
    "loss_averaging": "LOSS_AVERAGING_RESEARCH_ENABLED",
}


def _snapshot(regime: MarketRegime = MarketRegime.RANGE) -> MultiRegimeStrategySnapshot:
    book = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("99.99"), quantity=Decimal("100")),),
        asks=(BookLevel(price=Decimal("100.01"), quantity=Decimal("100")),),
        sequence=1,
        exchange_timestamp=NOW,
    )
    return MultiRegimeStrategySnapshot(
        source_event_id="dangerous-research-event",
        mode=TradingMode.PAPER,
        timestamp=NOW,
        instrument=INSTRUMENT,
        book=book,
        technical=TechnicalFeatureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            sample_count=100,
            close=Decimal("100"),
            ema_fast=Decimal("101"),
            ema_slow=Decimal("100"),
            atr=Decimal("1"),
            adx=Decimal("30"),
            efficiency_ratio=Decimal("0.7"),
        ),
        orderflow=OrderFlowFeatureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            mid_price=Decimal("100"),
            spread_bps=Decimal("2"),
            ofi_zscore_5s=Decimal("1.2"),
            book_imbalance_l5=Decimal("0.2"),
            trade_imbalance_5s=Decimal("0.1"),
            cvd=Decimal("10"),
        ),
        structure=MarketStructureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            trend=StructureDirection.NEUTRAL,
        ),
        regime=RegimeSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            regime=regime,
            candidate=regime,
            confidence=Decimal("0.9"),
            regime_since=NOW - timedelta(hours=1),
            dwell_seconds=Decimal("3600"),
            pending_confirmations=0,
            data_quality=DataQuality.VALID,
        ),
    )


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def _contexts(settings: Settings) -> tuple:
    return build_dangerous_research_contexts(
        _snapshot(),
        settings=settings,
        positions=(),
        signed_quantity=Decimal("0"),
        margin_available=True,
        portfolio_drawdown_fraction=Decimal("0"),
    )


def test_default_settings_build_no_research_context_at_all() -> None:
    settings = _settings()
    assert dangerous_research_enabled(settings) is False
    assert _contexts(settings) == ()


@pytest.mark.parametrize("capability", sorted(RESEARCH_FLAGS))
def test_enabling_one_capability_makes_the_context_reachable(capability: str) -> None:
    settings = _settings(
        DANGEROUS_CAPABILITY_AUTHORIZATION=capability,
        **{RESEARCH_FLAGS[capability]: True},
    )
    contexts = _contexts(settings)
    assert len(contexts) == 1
    context = contexts[0]
    assert context.instrument == INSTRUMENT
    assert context.price == Decimal("100")
    assert context.market_timestamp == NOW
    assert context.regime is MarketRegime.RANGE
    assert context.data_quality is DataQuality.VALID
    # A positive book imbalance with no open position implies the long side.
    assert context.reference_side is Side.BUY
    assert context.operator_authorized is False


@pytest.mark.parametrize("capability", sorted(RESEARCH_FLAGS))
def test_strategies_are_disabled_until_their_flag_is_set(capability: str) -> None:
    disabled = build_dangerous_research_strategies(_settings())
    assert [strategy.config.enabled for strategy in disabled] == [False, False, False]
    assert [strategy.config.live_enabled for strategy in disabled] == [
        False,
        False,
        False,
    ]

    enabled = build_dangerous_research_strategies(
        _settings(
            DANGEROUS_CAPABILITY_AUTHORIZATION=capability,
            **{RESEARCH_FLAGS[capability]: True},
        )
    )
    matching = [
        strategy for strategy in enabled if strategy.config.enabled
    ]
    assert len(matching) == 1
    assert matching[0].config.live_enabled is True


def test_a_disabled_strategy_rejects_every_context_it_receives() -> None:
    # Grid needs a RANGE regime, so it is the one that could otherwise fire.
    settings = _settings(
        DANGEROUS_CAPABILITY_AUTHORIZATION="grid_averaging",
        GRID_RESEARCH_ENABLED=True,
    )
    context = _contexts(settings)[0]
    _, enabled_grid, _ = build_dangerous_research_strategies(settings)
    _, disabled_grid, _ = build_dangerous_research_strategies(_settings())

    assert disabled_grid.evaluate(context).rejection_reason == (
        f"{disabled_grid.config.strategy_id}_disabled"
    )
    # Enabling it makes the same context produce a real evaluation instead.
    assert enabled_grid.evaluate(context).rejection_reason != (
        f"{enabled_grid.config.strategy_id}_disabled"
    )


def test_engine_defaults_keep_research_strategies_off() -> None:
    engine = MultiRegimeEngine()
    suite = engine.strategy_suite
    assert isinstance(suite, StrategySuite)
    assert suite.martingale.config.enabled is False
    assert suite.grid.config.enabled is False
    assert suite.loss_averaging.config.enabled is False


def test_engine_accepts_configured_research_strategies() -> None:
    settings = _settings(
        DANGEROUS_CAPABILITY_AUTHORIZATION="martingale",
        MARTINGALE_RESEARCH_ENABLED=True,
    )
    engine = MultiRegimeEngine(
        dangerous_research_strategies=build_dangerous_research_strategies(settings)
    )
    assert engine.strategy_suite.martingale.config.enabled is True
    assert engine.strategy_suite.grid.config.enabled is False


def test_engine_rejects_combining_a_suite_with_research_overrides() -> None:
    suite = MultiRegimeEngine().strategy_suite
    with pytest.raises(ValueError, match="cannot be combined with strategy policy"):
        MultiRegimeEngine(
            strategy_suite=suite,
            dangerous_research_strategies=build_dangerous_research_strategies(
                _settings()
            ),
        )
