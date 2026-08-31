from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import MarketRegime
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    TradingMode,
)
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.services.multi_regime import MultiRegimeStrategySnapshot
from funding_arbitrage.services.multi_regime_runtime import (
    RuntimeAdvancedRiskContextProvider,
    RuntimeStrategyExecutionSnapshotProvider,
    RuntimeSupplementalStrategyContextProvider,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.runtime_strategy_contexts import (
    RuntimeSynchronizedContextBuilder,
)
from funding_arbitrage.strategies import (
    CrossExchangeLeadLagStrategy,
    DatedFuturesBasisStrategy,
    FundingBasisHarvestStrategy,
)

NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
ZERO = Decimal("0")


def _settings(venues: str) -> Settings:
    return Settings(
        _env_file=None,
        run_mode="paper_test",
        market_data_mode="mock",
        execution_mode="paper",
        paper_initial_balance_usd=Decimal("1000"),
        paper_venues=venues,
        paper_reserve_percent=ZERO,
        paper_position_size_usd=Decimal("50"),
        paper_size_grid_usd="50,100",
        paper_max_funding_capital_usd=Decimal("100"),
        paper_minimum_funding_rate=Decimal("0.0001"),
        scanner_minimum_net_apr=ZERO,
        scanner_minimum_liquidity_score=ZERO,
        scanner_minimum_funding_samples=20,
        scanner_maximum_spread_percent=Decimal("0.01"),
        scanner_maximum_slippage_percent=Decimal("0.01"),
        bybit_maker_fee=ZERO,
        bybit_taker_fee=ZERO,
        gate_maker_fee=ZERO,
        gate_taker_fee=ZERO,
        okx_maker_fee=ZERO,
        okx_taker_fee=ZERO,
    )


def _instrument(
    venue: str,
    *,
    kind: LegacyInstrumentType = LegacyInstrumentType.PERPETUAL,
    symbol: str = "BTCUSDT",
    expiry: datetime | None = None,
) -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange=venue,
        exchange_symbol=symbol,
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=kind,
        settlement_asset="USDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
        expiry=expiry,
    )


def _canonical(metadata: NormalizedInstrument) -> InstrumentKey:
    return InstrumentKey(
        venue=metadata.exchange,
        exchange_symbol=metadata.exchange_symbol,
        base_asset=metadata.base_asset,
        quote_asset=metadata.quote_asset,
        instrument_type=InstrumentType(metadata.instrument_type.value),
        settlement_asset=metadata.settlement_asset,
        expiry=metadata.expiry,
    )


def _legacy_book(
    metadata: NormalizedInstrument,
    timestamp: datetime,
    *,
    mid: Decimal = Decimal("100"),
) -> OrderBook:
    return OrderBook(
        exchange=metadata.exchange,
        symbol=metadata.exchange_symbol,
        instrument_type=metadata.instrument_type,
        bids=(
            OrderBookLevel(price=mid - Decimal("0.01"), quantity=Decimal("100")),
        ),
        asks=(
            OrderBookLevel(price=mid + Decimal("0.01"), quantity=Decimal("100")),
        ),
        timestamp=timestamp,
        sequence=1,
    )


def _canonical_book(
    metadata: NormalizedInstrument,
    timestamp: datetime,
    *,
    mid: Decimal = Decimal("100"),
) -> BookSnapshot:
    return BookSnapshot(
        instrument=_canonical(metadata),
        bids=(
            BookLevel(price=mid - Decimal("0.01"), quantity=Decimal("100")),
        ),
        asks=(
            BookLevel(price=mid + Decimal("0.01"), quantity=Decimal("100")),
        ),
        sequence=1,
        exchange_timestamp=timestamp,
    )


def _strategy_snapshot(
    metadata: NormalizedInstrument,
    timestamp: datetime = NOW,
    *,
    mid: Decimal = Decimal("100"),
    atr: Decimal | None = Decimal("1"),
) -> MultiRegimeStrategySnapshot:
    instrument = _canonical(metadata)
    book = _canonical_book(metadata, timestamp, mid=mid)
    return MultiRegimeStrategySnapshot(
        source_event_id=f"event-{metadata.exchange}-{metadata.exchange_symbol}",
        mode=TradingMode.PAPER,
        timestamp=timestamp,
        instrument=instrument,
        book=book,
        technical=TechnicalFeatureSnapshot(
            instrument=instrument,
            timestamp=timestamp,
            data_quality=DataQuality.VALID,
            sample_count=100,
            close=mid,
            ema_fast=mid,
            ema_slow=mid,
            atr=atr,
        ),
        orderflow=OrderFlowFeatureSnapshot(
            instrument=instrument,
            timestamp=timestamp,
            data_quality=DataQuality.VALID,
            mid_price=mid,
            microprice=mid,
            spread_bps=Decimal("2"),
            cvd=ZERO,
        ),
        structure=MarketStructureSnapshot(
            instrument=instrument,
            timestamp=timestamp,
            data_quality=DataQuality.VALID,
            trend=StructureDirection.NEUTRAL,
        ),
        regime=RegimeSnapshot(
            instrument=instrument,
            timestamp=timestamp,
            regime=MarketRegime.RANGE,
            candidate=MarketRegime.RANGE,
            confidence=Decimal("0.9"),
            regime_since=timestamp - timedelta(hours=1),
            dwell_seconds=Decimal("3600"),
            pending_confirmations=0,
            data_quality=DataQuality.VALID,
        ),
    )


def _ticker(metadata: NormalizedInstrument, timestamp: datetime) -> Ticker:
    return Ticker(
        exchange=metadata.exchange,
        symbol=metadata.exchange_symbol,
        instrument_type=metadata.instrument_type,
        last_price=Decimal("100"),
        volume_24h=Decimal("10000000"),
        timestamp=timestamp,
    )


def _history(
    venue: str,
    rate: Decimal,
    *,
    symbol: str = "BTCUSDT",
) -> list[FundingHistoryPoint]:
    return [
        FundingHistoryPoint(
            exchange=venue,
            symbol=symbol,
            funding_rate=rate,
            funding_timestamp=NOW - timedelta(hours=8 * offset),
        )
        for offset in range(24, 0, -1)
    ]


def test_lead_lag_is_as_of_deterministic_and_survives_missing_atr() -> None:
    instruments = [_instrument(venue) for venue in ("bybit", "gate", "okx")]
    runtime = RuntimeState(_settings("bybit,gate,okx"), {}, emit_metrics=False)
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=instruments,
        tickers=[],
        funding=[],
        orderbooks={
            (
                item.exchange,
                item.exchange_symbol,
                item.instrument_type,
            ): _legacy_book(item, NOW)
            for item in instruments
        },
        captured_at=NOW,
        stale_after_seconds=5,
    )
    snapshot = _strategy_snapshot(
        instruments[0],
        mid=Decimal("100.20"),
        atr=None,
    )
    provider = RuntimeSupplementalStrategyContextProvider(runtime)

    first = provider(snapshot)
    second = provider(snapshot)

    assert first == second
    assert first.passive_market_making == ()
    assert len(first.lead_lag) == 1
    assert {item.instrument.venue for item in first.lead_lag[0].references} == {
        "GATE",
        "OKX",
    }
    assert first.lead_lag[0].inventory_available is True
    assert first.lead_lag[0].transfer_ready is True
    evaluation = CrossExchangeLeadLagStrategy().evaluate(
        primary=first.lead_lag[0].primary,
        references=first.lead_lag[0].references,
        timestamp=first.lead_lag[0].timestamp,
        mode=first.lead_lag[0].mode,
        regime=first.lead_lag[0].regime,
        costs=first.lead_lag[0].costs,
        inventory_available=first.lead_lag[0].inventory_available,
        transfer_ready=first.lead_lag[0].transfer_ready,
    )
    assert evaluation.intent is not None

    future_gate_book = _legacy_book(instruments[1], NOW + timedelta(seconds=1))
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=instruments,
        tickers=[],
        funding=[],
        orderbooks={
            (instruments[0].exchange, "BTCUSDT", LegacyInstrumentType.PERPETUAL): (
                _legacy_book(instruments[0], NOW)
            ),
            (instruments[1].exchange, "BTCUSDT", LegacyInstrumentType.PERPETUAL): (
                future_gate_book
            ),
            (instruments[2].exchange, "BTCUSDT", LegacyInstrumentType.PERPETUAL): (
                _legacy_book(instruments[2], NOW)
            ),
        },
        captured_at=NOW + timedelta(seconds=1),
        stale_after_seconds=5,
    )

    assert provider(snapshot).lead_lag == ()


def test_funding_context_uses_exact_intervals_cap_and_as_of_history() -> None:
    bybit = _instrument("bybit")
    gate = _instrument("gate")
    runtime = RuntimeState(_settings("bybit,gate"), {}, emit_metrics=False)
    funding = [
        FundingSnapshot(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.001"),
            funding_interval_hours=Decimal("8"),
            next_funding_time=NOW + timedelta(hours=1),
            timestamp=NOW,
        ),
        FundingSnapshot(
            exchange="gate",
            symbol="BTCUSDT",
            funding_rate=Decimal("-0.001"),
            funding_interval_hours=Decimal("4"),
            next_funding_time=NOW + timedelta(hours=2),
            timestamp=NOW,
        ),
    ]
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[bybit, gate],
        tickers=[_ticker(bybit, NOW), _ticker(gate, NOW)],
        funding=funding,
        orderbooks={
            ("bybit", "BTCUSDT", LegacyInstrumentType.PERPETUAL): _legacy_book(
                bybit, NOW - timedelta(seconds=1)
            ),
            ("gate", "BTCUSDT", LegacyInstrumentType.PERPETUAL): _legacy_book(
                gate, NOW
            ),
        },
        captured_at=NOW,
        funding_history={
            ("bybit", "BTCUSDT"): _history("bybit", Decimal("0.001")),
            ("gate", "BTCUSDT"): _history("gate", Decimal("-0.001")),
        },
        stale_after_seconds=5,
        funding_history_refreshed={
            ("bybit", "BTCUSDT"): NOW,
            ("gate", "BTCUSDT"): NOW,
        },
    )
    snapshot = _strategy_snapshot(gate)
    builder = RuntimeSynchronizedContextBuilder(runtime)

    contexts = builder.build(snapshot)
    repeated = builder.build(snapshot)

    assert repeated == contexts
    assert len(contexts.funding_basis) == 1
    context = contexts.funding_basis[0]
    assert context.requested_notional_usd * Decimal("2") <= Decimal("100")
    schedules = {
        forecast.instrument.venue: tuple(
            event.settlement_time for event in forecast.events
        )
        for forecast in context.forecasts
    }
    assert schedules["BYBIT"] == (
        NOW + timedelta(hours=1),
        NOW + timedelta(hours=9),
        NOW + timedelta(hours=17),
    )
    assert schedules["GATE"] == tuple(
        NOW + timedelta(hours=hours) for hours in (2, 6, 10, 14, 18, 22)
    )
    assert all(forecast.sample_count == 24 for forecast in context.forecasts)
    assert all(forecast.generated_at == NOW for forecast in context.forecasts)
    evaluation = FundingBasisHarvestStrategy().evaluate(context)
    assert evaluation.intent is not None
    assert evaluation.target_settlements == tuple(
        sorted(set((*schedules["BYBIT"], *schedules["GATE"])))
    )
    execution_snapshot = RuntimeStrategyExecutionSnapshotProvider(runtime)(
        evaluation.intent,
        snapshot.source_event_id,
        NOW,
        snapshot.book,
    )
    assert execution_snapshot is not None
    risk_context = RuntimeAdvancedRiskContextProvider(runtime)(
        evaluation.intent,
        execution_snapshot,
        NOW,
    )
    assert risk_context is not None
    assert risk_context.requested_notional_usd == Decimal("50")

    assert runtime.last_completed_snapshot is not None
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[bybit, gate],
        tickers=[_ticker(bybit, NOW), _ticker(gate, NOW)],
        funding=funding,
        orderbooks=runtime.last_completed_snapshot.orderbooks,
        captured_at=NOW + timedelta(seconds=1),
        funding_history=runtime.last_completed_snapshot.funding_history,
        stale_after_seconds=5,
        funding_history_refreshed={
            ("bybit", "BTCUSDT"): NOW + timedelta(seconds=1),
            ("gate", "BTCUSDT"): NOW + timedelta(seconds=1),
        },
    )

    assert builder.build(snapshot).funding_basis == ()


def test_dated_basis_uses_exact_expiry_and_perpetual_settlements() -> None:
    expiry = NOW + timedelta(days=30)
    future = _instrument(
        "bybit",
        kind=LegacyInstrumentType.FUTURE,
        symbol="BTCUSDT-20260930",
        expiry=expiry,
    )
    perpetual = _instrument("bybit")
    runtime = RuntimeState(_settings("bybit"), {}, emit_metrics=False)
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[future, perpetual],
        tickers=[],
        funding=[
            FundingSnapshot(
                exchange="bybit",
                symbol="BTCUSDT",
                funding_rate=Decimal("0.0002"),
                funding_interval_hours=Decimal("8"),
                next_funding_time=NOW + timedelta(hours=1),
                timestamp=NOW,
            )
        ],
        orderbooks={
            ("bybit", "BTCUSDT-20260930", LegacyInstrumentType.FUTURE): (
                _legacy_book(future, NOW, mid=Decimal("102"))
            ),
            ("bybit", "BTCUSDT", LegacyInstrumentType.PERPETUAL): (
                _legacy_book(perpetual, NOW)
            ),
        },
        captured_at=NOW,
        funding_history={
            ("bybit", "BTCUSDT"): _history("bybit", Decimal("0.0002"))
        },
        stale_after_seconds=5,
        funding_history_refreshed={("bybit", "BTCUSDT"): NOW},
    )
    snapshot = _strategy_snapshot(future, mid=Decimal("102"))
    builder = RuntimeSynchronizedContextBuilder(runtime)

    contexts = builder.build(snapshot)

    assert len(contexts.dated_basis) == 1
    context = contexts.dated_basis[0]
    assert context.future.instrument.expiry == expiry
    assert context.perpetual.instrument.instrument_type is InstrumentType.PERPETUAL
    assert context.future.instrument.instrument_type is InstrumentType.FUTURE
    assert context.funding_events[0].settlement_time == NOW + timedelta(hours=1)
    assert context.funding_events[-1].settlement_time < expiry
    assert all(
        later.settlement_time - earlier.settlement_time == timedelta(hours=8)
        for earlier, later in zip(
            context.funding_events,
            context.funding_events[1:],
            strict=False,
        )
    )
    assert DatedFuturesBasisStrategy().evaluate(context).intent is not None

    assert runtime.last_completed_snapshot is not None
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[future, perpetual],
        tickers=[],
        funding=[
            runtime.last_completed_snapshot.funding[0].model_copy(
                update={"next_funding_time": None}
            )
        ],
        orderbooks=runtime.last_completed_snapshot.orderbooks,
        captured_at=NOW,
        funding_history=runtime.last_completed_snapshot.funding_history,
        stale_after_seconds=5,
        funding_history_refreshed={("bybit", "BTCUSDT"): NOW},
    )

    assert builder.build(snapshot).dated_basis == ()

    distant_future = future.model_copy(update={"expiry": NOW + timedelta(days=121)})
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[distant_future, perpetual],
        tickers=[],
        funding=[],
        orderbooks={},
        captured_at=NOW,
        stale_after_seconds=5,
    )
    assert (
        builder.build(
            _strategy_snapshot(
                distant_future,
                mid=Decimal("102"),
            )
        ).dated_basis
        == ()
    )
