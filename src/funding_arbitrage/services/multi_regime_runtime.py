"""Durable async consumer for the canonical multi-regime decision engine."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.domain.decisions import MarketRegime, SignalIntent, SignalType
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    Side,
    TradingMode,
)
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot
from funding_arbitrage.risk.margin import PortfolioMarginAssessment
from funding_arbitrage.risk.portfolio import RiskAuthorizationContext

if TYPE_CHECKING:
    from funding_arbitrage.services.runtime import RuntimeState

from funding_arbitrage.database.repositories.directional_paper import (
    DirectionalPaperEventProjection,
    load_advanced_paper_positions,
    load_directional_paper_checkpoint,
    load_directional_paper_positions,
    save_directional_paper_page,
)
from funding_arbitrage.database.repositories.events import (
    latest_event_row_id,
    load_ingestion_events,
)
from funding_arbitrage.database.repositories.multi_regime import (
    load_multi_regime_batches,
    save_multi_regime_batch,
)
from funding_arbitrage.domain.events import EventEnvelope
from funding_arbitrage.execution.advanced_paper import AdvancedStrategyPaperBroker
from funding_arbitrage.execution.directional_paper import DirectionalPaperBroker
from funding_arbitrage.services.multi_regime import (
    MultiRegimeDecisionBatch,
    MultiRegimeEngine,
    MultiRegimeStrategySnapshot,
)
from funding_arbitrage.services.strategy_execution import (
    InstrumentExecutionQuote,
    StrategyExecutionSnapshot,
    build_strategy_execution_snapshot,
)
from funding_arbitrage.services.strategy_suite import SupplementalStrategyContexts
from funding_arbitrage.strategies import (
    MarketMakingContext,
    MarketMakingCosts,
    MarketMakingInventory,
)

logger = logging.getLogger(__name__)

BPS = Decimal("10000")
ZERO = Decimal("0")


class RuntimeSupplementalStrategyContextProvider:
    """Project canonical state into conservative, non-executable strategy inputs."""

    def __init__(
        self,
        runtime: RuntimeState,
        paper_broker: DirectionalPaperBroker | None = None,
        advanced_paper_broker: AdvancedStrategyPaperBroker | None = None,
    ) -> None:
        self.runtime = runtime
        self.paper_broker = paper_broker
        self.advanced_paper_broker = advanced_paper_broker

    def __call__(
        self, snapshot: MultiRegimeStrategySnapshot
    ) -> SupplementalStrategyContexts:
        atr = snapshot.technical.atr
        price = snapshot.technical.close
        fee_schedule = self.runtime.settings.fee_schedules.get(
            snapshot.instrument.venue.lower()
        )
        if atr is None or atr <= 0 or price <= 0 or fee_schedule is None:
            return SupplementalStrategyContexts()
        maker_fee, _ = fee_schedule
        maximum_abs_quantity = (
            self.runtime.settings.paper_position_size_usd / price
        )
        if maximum_abs_quantity <= 0:
            return SupplementalStrategyContexts()
        broker = self.paper_broker
        directional_quantity = (
            sum(
                (
                    position.signed_quantity
                    for position in broker.active_positions
                    if position.instrument == snapshot.instrument
                ),
                Decimal("0"),
            )
            if broker is not None
            else Decimal("0")
        )
        signed_quantity = directional_quantity + (
            self.advanced_paper_broker.instrument_signed_quantity(
                snapshot.instrument
            )
            if self.advanced_paper_broker is not None
            else ZERO
        )
        context = MarketMakingContext(
            instrument=snapshot.instrument,
            book=snapshot.book,
            book_quality=snapshot.orderflow.data_quality,
            orderflow=snapshot.orderflow,
            inventory=MarketMakingInventory(
                signed_quantity=signed_quantity,
                maximum_abs_quantity=maximum_abs_quantity,
            ),
            costs=MarketMakingCosts(
                maker_fee_bps_per_fill=maker_fee * BPS,
                expected_adverse_selection_bps=Decimal("0"),
                expected_hedging_bps=(
                    self.runtime.settings.multi_regime_estimated_cost_bps
                ),
            ),
            short_horizon_volatility_bps=atr / price * BPS,
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            regime=snapshot.regime.regime,
            live_operator_authorized=False,
        )
        return SupplementalStrategyContexts(passive_market_making=(context,))


class RuntimeStrategyExecutionSnapshotProvider:
    """Bind advanced intents to exact fresh books and venue trading rules."""

    def __init__(self, runtime: RuntimeState) -> None:
        self.runtime = runtime

    def __call__(
        self,
        intent: SignalIntent,
        source_event_id: str,
        timestamp: datetime,
        primary_book: BookSnapshot,
    ) -> StrategyExecutionSnapshot | None:
        market = self.runtime.last_completed_snapshot or self.runtime.latest_snapshot
        if market is None:
            return None
        unique_instruments = {
            leg.instrument.canonical_id: leg.instrument for leg in intent.legs
        }
        quotes: list[InstrumentExecutionQuote] = []
        for instrument in unique_instruments.values():
            try:
                legacy_type = LegacyInstrumentType(instrument.instrument_type.value)
            except ValueError:
                return None
            metadata = next(
                (
                    item
                    for item in market.instruments
                    if item.exchange.lower() == instrument.venue.lower()
                    and item.exchange_symbol.upper()
                    == instrument.exchange_symbol.upper()
                    and item.instrument_type is legacy_type
                    and item.is_active
                ),
                None,
            )
            fee_schedule = self.runtime.settings.fee_schedules.get(
                instrument.venue.lower()
            )
            if metadata is None or fee_schedule is None:
                return None
            if instrument == primary_book.instrument:
                canonical_book = primary_book
            else:
                legacy_book = next(
                    (
                        book
                        for (venue, symbol, kind), book in market.orderbooks.items()
                        if venue.lower() == instrument.venue.lower()
                        and symbol.upper() == instrument.exchange_symbol.upper()
                        and kind is legacy_type
                    ),
                    None,
                )
                if legacy_book is None:
                    return None
                canonical_book = BookSnapshot(
                    instrument=instrument,
                    bids=tuple(
                        BookLevel(price=level.price, quantity=level.quantity)
                        for level in legacy_book.bids
                        if level.quantity > ZERO
                    ),
                    asks=tuple(
                        BookLevel(price=level.price, quantity=level.quantity)
                        for level in legacy_book.asks
                        if level.quantity > ZERO
                    ),
                    sequence=legacy_book.sequence or 0,
                    exchange_timestamp=legacy_book.timestamp,
                )
            book_age = (timestamp - canonical_book.exchange_timestamp).total_seconds()
            if (
                book_age < 0
                or book_age > self.runtime.settings.multi_regime_stale_after_seconds
            ):
                return None
            maker_fee, taker_fee = fee_schedule
            quotes.append(
                InstrumentExecutionQuote(
                    instrument=instrument,
                    book=canonical_book,
                    data_quality=DataQuality.VALID,
                    quantity_step=metadata.step_size,
                    price_tick=metadata.tick_size,
                    minimum_quantity=metadata.min_order_size,
                    maker_fee_bps=maker_fee * BPS,
                    taker_fee_bps=taker_fee * BPS,
                )
            )
        return build_strategy_execution_snapshot(
            intent=intent,
            source_event_id=source_event_id,
            captured_at=timestamp,
            quotes=tuple(quotes),
        )


class RuntimeAdvancedRiskContextProvider:
    """Size synchronized multi-leg PAPER/SHADOW intents conservatively."""

    def __init__(
        self,
        runtime: RuntimeState,
        paper_broker: DirectionalPaperBroker | None = None,
        advanced_paper_broker: AdvancedStrategyPaperBroker | None = None,
    ) -> None:
        self.runtime = runtime
        self.paper_broker = paper_broker
        self.advanced_paper_broker = advanced_paper_broker

    def __call__(
        self,
        intent: SignalIntent,
        snapshot: StrategyExecutionSnapshot,
        timestamp: datetime,
    ) -> RiskAuthorizationContext | None:
        quotes = {
            quote.instrument.canonical_id: quote for quote in snapshot.quotes
        }
        primary = quotes.get(intent.primary_instrument.canonical_id)
        if primary is None or primary.best_bid is None or primary.best_ask is None:
            return None
        reference_price = (primary.best_bid + primary.best_ask) / Decimal("2")
        liquidity_caps: list[Decimal] = []
        spread_bps: list[Decimal] = []
        signed_delta = ZERO
        for leg in intent.legs:
            quote = quotes.get(leg.instrument.canonical_id)
            if quote is None or quote.best_bid is None or quote.best_ask is None:
                return None
            levels = quote.book.asks if leg.side is Side.BUY else quote.book.bids
            visible_notional = sum(
                (level.price * level.quantity for level in levels[:20]),
                ZERO,
            )
            if visible_notional <= ZERO:
                return None
            liquidity_caps.append(visible_notional / leg.hedge_ratio)
            midpoint = (quote.best_bid + quote.best_ask) / Decimal("2")
            spread_bps.append((quote.best_ask - quote.best_bid) / midpoint * BPS)
            direction = Decimal("1") if leg.side is Side.BUY else Decimal("-1")
            signed_delta += (
                direction * leg.hedge_ratio * midpoint / reference_price
            )
        available_liquidity = min(liquidity_caps)
        if available_liquidity <= ZERO:
            return None

        account = self.runtime.portfolio.snapshot(timestamp)
        broker = self.paper_broker
        advanced = self.advanced_paper_broker
        directional_reserved = broker.reserved_notional if broker else ZERO
        advanced_reserved = advanced.reserved_notional if advanced else ZERO
        directional_total_pnl = broker.total_net_pnl if broker else ZERO
        advanced_total_pnl = advanced.total_net_pnl if advanced else ZERO
        directional_gross = broker.gross_exposure if broker else ZERO
        advanced_gross = advanced.gross_exposure if advanced else ZERO
        directional_net_delta = (
            sum(
                (
                    position.signed_quantity
                    * (
                        position.mark_price
                        or position.entry_order.average_fill_price
                        or position.entry_order.limit_price
                        or ZERO
                    )
                    for position in broker.active_positions
                ),
                ZERO,
            )
            if broker
            else ZERO
        )
        advanced_net_delta = advanced.net_delta() if advanced else ZERO
        available_cash = max(
            ZERO,
            account.cash - directional_reserved - advanced_reserved,
        )
        asset = intent.primary_instrument.base_asset
        group = next(
            (
                candidate
                for candidate in self.runtime.settings.paper_correlation_group_values
                if asset in candidate
            ),
            frozenset({asset}),
        )
        directional_asset_exposure = broker.asset_exposure(asset) if broker else ZERO
        advanced_asset_exposure = advanced.asset_exposure(asset) if advanced else ZERO
        directional_strategy_exposure = (
            broker.strategy_exposure(intent.strategy_id) if broker else ZERO
        )
        advanced_strategy_exposure = (
            advanced.strategy_exposure(intent.strategy_id) if advanced else ZERO
        )
        venues = {leg.instrument.venue.upper() for leg in intent.legs}
        venue_exposures = {
            venue: self.runtime.portfolio.exchange_exposure(venue.lower())
            + (broker.venue_exposure(venue) if broker else ZERO)
            + (advanced.venue_exposure(venue) if advanced else ZERO)
            for venue in venues
        }
        requested_notional = self.runtime.settings.paper_position_size_usd
        if intent.signal_type is SignalType.FUNDING_BASIS:
            requested_notional = min(
                requested_notional,
                self.runtime.settings.paper_max_funding_capital_usd,
            )
        operator_entries_enabled = (
            intent.mode
            in {
                TradingMode.BACKTEST,
                TradingMode.REPLAY,
                TradingMode.SHADOW,
            }
            or (
                intent.mode is TradingMode.PAPER
                and self.runtime.settings.paper_autotrade
                and self.runtime.entries_allowed()
            )
        )
        volatility_bps = max(
            Decimal("100"),
            abs(intent.expected_move_bps),
            intent.estimated_cost_bps * Decimal("2"),
        )
        delta_per_primary_notional = (
            max(leg.hedge_ratio for leg in intent.legs)
            if intent.signal_type is SignalType.PASSIVE_MARKET_MAKING
            else signed_delta
        )
        available_margin = available_cash
        correlation_key = "runtime:" + ",".join(sorted(group))
        return RiskAuthorizationContext(
            intent=intent,
            timestamp=timestamp,
            requested_notional_usd=requested_notional,
            reference_price=reference_price,
            quantity_step=primary.quantity_step,
            stop_distance_bps=volatility_bps,
            expected_slippage_bps=max(spread_bps) / Decimal("2"),
            volatility_bps=volatility_bps,
            available_liquidity_usd=available_liquidity,
            incremental_margin_rate=Decimal("1"),
            delta_per_primary_notional=delta_per_primary_notional,
            correlation_multiplier=Decimal("1"),
            drawdown_multiplier=Decimal("1"),
            regime_multiplier=(
                Decimal("0.5")
                if intent.regime is MarketRegime.TRANSITION
                else Decimal("1")
            ),
            equity_usd=(
                account.equity + directional_total_pnl + advanced_total_pnl
            ),
            cash_usd=available_cash,
            portfolio_gross_notional_usd=(
                account.locked_capital + directional_gross + advanced_gross
            ),
            portfolio_net_delta_usd=directional_net_delta + advanced_net_delta,
            position_exposure_usd=(
                self.runtime.portfolio.asset_exposure(asset)
                + directional_asset_exposure
                + advanced_asset_exposure
            ),
            asset_exposures_usd={
                asset: (
                    self.runtime.portfolio.asset_exposure(asset)
                    + directional_asset_exposure
                    + advanced_asset_exposure
                )
            },
            strategy_exposures_usd={
                intent.strategy_id: (
                    self.runtime.portfolio.strategy_exposure(intent.strategy_id)
                    + directional_strategy_exposure
                    + advanced_strategy_exposure
                )
            },
            venue_exposures_usd=venue_exposures,
            correlation_exposures_usd={
                correlation_key: (
                    self.runtime.portfolio.correlated_exposure(
                        asset,
                        self.runtime.settings.paper_correlation_group_values,
                    )
                    + (
                        sum((broker.asset_exposure(item) for item in group), ZERO)
                        if broker
                        else ZERO
                    )
                    + (
                        sum(
                            (advanced.asset_exposure(item) for item in group),
                            ZERO,
                        )
                        if advanced
                        else ZERO
                    )
                )
            },
            correlation_group=correlation_key,
            margin=PortfolioMarginAssessment(
                approved=available_margin > ZERO,
                venues=(),
                total_initial_margin_required_usd=ZERO,
                total_maintenance_margin_required_usd=ZERO,
                total_available_initial_margin_usd=available_margin,
                worst_liquidation_buffer_usd=available_margin,
                reasons=() if available_margin > ZERO else ("paper_cash_unavailable",),
            ),
            data_fresh=True,
            reconciliation_healthy=True,
            operator_entries_enabled=operator_entries_enabled,
        )


class RuntimePortfolioRiskContextProvider:
    """Build paper/shadow risk context from current typed market and portfolio state."""

    def __init__(
        self,
        runtime: RuntimeState,
        paper_broker: DirectionalPaperBroker | None = None,
        advanced_paper_broker: AdvancedStrategyPaperBroker | None = None,
    ) -> None:
        self.runtime = runtime
        self.paper_broker = paper_broker
        self.advanced_paper_broker = advanced_paper_broker

    def __call__(
        self,
        intent: SignalIntent,
        technical: TechnicalFeatureSnapshot,
        orderflow: OrderFlowFeatureSnapshot,
        book: BookSnapshot,
        timestamp: datetime,
    ) -> RiskAuthorizationContext | None:
        snapshot = self.runtime.last_completed_snapshot or self.runtime.latest_snapshot
        if snapshot is None:
            return None
        try:
            legacy_type = LegacyInstrumentType(
                intent.primary_instrument.instrument_type.value
            )
        except ValueError:
            return None
        metadata = next(
            (
                item
                for item in snapshot.instruments
                if item.exchange.lower()
                == intent.primary_instrument.venue.lower()
                and item.exchange_symbol.upper()
                == intent.primary_instrument.exchange_symbol.upper()
                and item.instrument_type is legacy_type
                and item.is_active
            ),
            None,
        )
        if metadata is None or technical.atr is None or technical.atr <= 0:
            return None
        if intent.structural_stop is None:
            return None
        price = technical.close
        side_levels = book.asks if intent.side is Side.BUY else book.bids
        available_liquidity = sum(
            (level.price * level.quantity for level in side_levels[:20]),
            Decimal("0"),
        )
        if available_liquidity <= 0:
            return None
        portfolio = self.runtime.portfolio
        account = portfolio.snapshot(timestamp)
        asset = intent.primary_instrument.base_asset
        venue = intent.primary_instrument.venue.upper()
        group = next(
            (
                candidate
                for candidate in self.runtime.settings.paper_correlation_group_values
                if asset in candidate
            ),
            frozenset({asset}),
        )
        broker = self.paper_broker
        advanced = self.advanced_paper_broker
        directional_asset_exposure = broker.asset_exposure(asset) if broker else Decimal("0")
        advanced_asset_exposure = advanced.asset_exposure(asset) if advanced else ZERO
        directional_strategy_exposure = (
            broker.strategy_exposure(intent.strategy_id) if broker else Decimal("0")
        )
        advanced_strategy_exposure = (
            advanced.strategy_exposure(intent.strategy_id) if advanced else ZERO
        )
        directional_venue_exposure = broker.venue_exposure(venue) if broker else Decimal("0")
        advanced_venue_exposure = advanced.venue_exposure(venue) if advanced else ZERO
        directional_correlation_exposure = (
            sum((broker.asset_exposure(item) for item in group), Decimal("0"))
            if broker
            else Decimal("0")
        )
        advanced_correlation_exposure = (
            sum((advanced.asset_exposure(item) for item in group), ZERO)
            if advanced
            else ZERO
        )
        directional_reserved = broker.reserved_notional if broker else ZERO
        advanced_reserved = advanced.reserved_notional if advanced else ZERO
        directional_total_pnl = broker.total_net_pnl if broker else ZERO
        advanced_total_pnl = advanced.total_net_pnl if advanced else ZERO
        directional_gross = broker.gross_exposure if broker else ZERO
        advanced_gross = advanced.gross_exposure if advanced else ZERO
        directional_net_delta = (
            sum(
                (
                    position.signed_quantity
                    * (
                        position.mark_price
                        or position.entry_order.average_fill_price
                        or position.entry_order.limit_price
                        or Decimal("0")
                    )
                    for position in broker.active_positions
                ),
                Decimal("0"),
            )
            if broker
            else Decimal("0")
        )
        advanced_net_delta = advanced.net_delta() if advanced else ZERO
        available_cash = max(
            Decimal("0"),
            account.cash - directional_reserved - advanced_reserved,
        )
        operator_entries_enabled = (
            True
            if intent.mode in {
                TradingMode.BACKTEST,
                TradingMode.REPLAY,
                TradingMode.SHADOW,
            }
            else (
                intent.mode is TradingMode.PAPER
                and self.runtime.settings.paper_autotrade
                and self.runtime.entries_allowed()
            )
        )
        spread_bps = orderflow.spread_bps or Decimal("0")
        stop_distance_bps = (
            abs(price - intent.structural_stop) / price * Decimal("10000")
        )
        available_margin = available_cash
        return RiskAuthorizationContext(
            intent=intent,
            timestamp=timestamp,
            requested_notional_usd=self.runtime.settings.paper_position_size_usd,
            reference_price=price,
            quantity_step=metadata.step_size,
            stop_distance_bps=stop_distance_bps,
            expected_slippage_bps=spread_bps / Decimal("2"),
            volatility_bps=max(
                Decimal("0.00000001"),
                technical.atr / price * Decimal("10000"),
            ),
            available_liquidity_usd=available_liquidity,
            incremental_margin_rate=Decimal("1"),
            delta_per_primary_notional=Decimal("1"),
            correlation_multiplier=Decimal("1"),
            drawdown_multiplier=Decimal("1"),
            regime_multiplier=(
                Decimal("0.5")
                if intent.regime is MarketRegime.TRANSITION
                else Decimal("1")
            ),
            equity_usd=(
                account.equity + directional_total_pnl + advanced_total_pnl
            ),
            cash_usd=available_cash,
            portfolio_gross_notional_usd=(
                account.locked_capital + directional_gross + advanced_gross
            ),
            portfolio_net_delta_usd=directional_net_delta + advanced_net_delta,
            position_exposure_usd=(
                portfolio.asset_exposure(asset)
                + directional_asset_exposure
                + advanced_asset_exposure
            ),
            asset_exposures_usd={
                asset: (
                    portfolio.asset_exposure(asset)
                    + directional_asset_exposure
                    + advanced_asset_exposure
                )
            },
            strategy_exposures_usd={
                intent.strategy_id: (
                    portfolio.strategy_exposure(intent.strategy_id)
                    + directional_strategy_exposure
                    + advanced_strategy_exposure
                )
            },
            venue_exposures_usd={
                venue: (
                    portfolio.exchange_exposure(venue.lower())
                    + directional_venue_exposure
                    + advanced_venue_exposure
                )
            },
            correlation_exposures_usd={
                "runtime:" + ",".join(sorted(group)): (
                    portfolio.correlated_exposure(
                        asset,
                        self.runtime.settings.paper_correlation_group_values,
                    )
                    + directional_correlation_exposure
                    + advanced_correlation_exposure
                )
            },
            correlation_group="runtime:" + ",".join(sorted(group)),
            margin=PortfolioMarginAssessment(
                approved=available_margin > 0,
                venues=(),
                total_initial_margin_required_usd=Decimal("0"),
                total_maintenance_margin_required_usd=Decimal("0"),
                total_available_initial_margin_usd=available_margin,
                worst_liquidation_buffer_usd=available_margin,
                reasons=() if available_margin > 0 else ("paper_cash_unavailable",),
            ),
            data_fresh=orderflow.data_quality is DataQuality.VALID,
            reconciliation_healthy=True,
            operator_entries_enabled=operator_entries_enabled,
        )


class DurableMultiRegimeRuntime:
    def __init__(
        self,
        engine: MultiRegimeEngine,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        paper_broker: DirectionalPaperBroker | None = None,
        advanced_paper_broker: AdvancedStrategyPaperBroker | None = None,
        runtime_state: RuntimeState | None = None,
    ) -> None:
        if (
            paper_broker is not None or advanced_paper_broker is not None
        ) and engine.config.mode is not TradingMode.PAPER:
            raise ValueError("paper brokers require PAPER engine mode")
        if (
            paper_broker is not None
            and advanced_paper_broker is not None
            and paper_broker.simulation_version
            != advanced_paper_broker.simulation_version
        ):
            raise ValueError("paper brokers must share one simulation version")
        self.engine = engine
        self.session_factory = session_factory
        self.paper_broker = paper_broker
        self.advanced_paper_broker = advanced_paper_broker
        self.runtime_state = runtime_state
        self.latest_by_instrument: dict[str, MultiRegimeDecisionBatch] = {}
        self.persisted_batches = 0
        self.restored_events = 0
        self.paper_replayed_events = 0
        self.failure_reason: str | None = None
        self._processed_event_row_id = 0
        simulation_version = (
            paper_broker.simulation_version
            if paper_broker is not None
            else (
                advanced_paper_broker.simulation_version
                if advanced_paper_broker is not None
                else None
            )
        )
        self._paper_consumer_name = (
            (
                "multi_regime_paper_v1"
                if simulation_version == "v1-legacy"
                else "multi_regime_paper_"
                + hashlib.sha256(
                    simulation_version.encode()
                ).hexdigest()[:32]
            )
            if simulation_version is not None
            else "multi_regime_paper_disabled"
        )
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._stop_requested = False

    async def restore_features(self, *, start: datetime) -> int:
        """Restore features and paper state with bounded keyset replay."""

        async with self._lock:
            if self.failure_reason is not None:
                raise RuntimeError("multi-regime runtime is failed")
            provider = self.engine.risk_context_provider
            self.engine.risk_context_provider = None
            try:
                async with self.session_factory() as session:
                    journal_tip = await latest_event_row_id(session)
                    positions = (
                        await load_directional_paper_positions(
                            session,
                            simulation_version=self.paper_broker.simulation_version,
                        )
                        if self.paper_broker is not None
                        else ()
                    )
                    advanced_positions = (
                        await load_advanced_paper_positions(
                            session,
                            simulation_version=(
                                self.advanced_paper_broker.simulation_version
                            ),
                        )
                        if self.advanced_paper_broker is not None
                        else ()
                    )
                    checkpoint = (
                        await load_directional_paper_checkpoint(
                            session,
                            consumer_name=self._paper_consumer_name,
                        )
                        if self._paper_execution_enabled
                        else None
                    )
                if (
                    checkpoint is not None
                    and checkpoint.event_row_id > journal_tip
                ):
                    raise RuntimeError("paper checkpoint exceeds canonical journal tip")
                restored = await self._restore_feature_pages(
                    start=start,
                    journal_tip=journal_tip,
                )
                if isinstance(self.engine, MultiRegimeEngine):
                    async with self.session_factory() as session:
                        persisted_batches = await load_multi_regime_batches(
                            session,
                            start=start,
                            mode=self.engine.config.mode,
                        )
                    self.engine.restore_orchestration(persisted_batches)
                    for batch in persisted_batches:
                        self.latest_by_instrument[
                            batch.instrument.canonical_id
                        ] = batch
                if self.paper_broker is not None:
                    self.paper_broker.restore(positions)
                if self.advanced_paper_broker is not None:
                    self.advanced_paper_broker.restore(advanced_positions)
                if self._paper_execution_enabled:
                    await self._restore_paper_pages(
                        after_row_id=(
                            checkpoint.event_row_id
                            if checkpoint is not None
                            else 0
                        ),
                        start=(start if checkpoint is None else None),
                        journal_tip=journal_tip,
                    )
                self._processed_event_row_id = journal_tip
                self.restored_events += restored
                return restored
            except Exception as error:
                self.failure_reason = type(error).__name__
                raise
            finally:
                self.engine.risk_context_provider = provider

    async def _restore_feature_pages(
        self,
        *,
        start: datetime,
        journal_tip: int,
    ) -> int:
        cursor = 0
        restored = 0
        while True:
            async with self.session_factory() as session:
                page = await load_ingestion_events(
                    session,
                    after_row_id=cursor,
                    up_to_row_id=journal_tip,
                    start=start,
                )
            if not page:
                return restored
            for row_id, event in page:
                if isinstance(self.engine, MultiRegimeEngine):
                    self.engine.restore_event(event)
                else:
                    self.engine.process(event)
                cursor = row_id
                restored += 1

    async def _restore_paper_pages(
        self,
        *,
        after_row_id: int,
        start: datetime | None,
        journal_tip: int,
    ) -> None:
        cursor = after_row_id
        while True:
            async with self.session_factory() as session:
                page = await load_ingestion_events(
                    session,
                    after_row_id=cursor,
                    up_to_row_id=journal_tip,
                    start=start,
                )
                batches = (
                    await load_multi_regime_batches(
                        session,
                        mode=TradingMode.PAPER,
                        source_event_ids=tuple(
                            event.metadata.event_id for _, event in page
                        ),
                    )
                    if page
                    else ()
                )
            if not page:
                return
            await self._restore_paper(page, batches)
            cursor = page[-1][0]

    async def _restore_paper(
        self,
        events: list[tuple[int, EventEnvelope[Any]]],
        batches: tuple[MultiRegimeDecisionBatch, ...],
    ) -> None:
        if not self._paper_execution_enabled:
            raise RuntimeError("paper restore requested without a broker")
        batches_by_event: dict[str, list[MultiRegimeDecisionBatch]] = {}
        for batch in batches:
            batches_by_event.setdefault(batch.source_event_id, []).append(batch)
        projections: list[DirectionalPaperEventProjection] = []
        for row_id, event in events:
            updates = (
                list(self.paper_broker.advance(event))
                if self.paper_broker is not None
                else []
            )
            advanced_updates = (
                list(self.advanced_paper_broker.advance(event))
                if self.advanced_paper_broker is not None
                else []
            )
            for batch in batches_by_event.get(event.metadata.event_id, ()):
                if self.paper_broker is not None:
                    updates.extend(self.paper_broker.submit(batch))
                if self.advanced_paper_broker is not None:
                    advanced_updates.extend(
                        self.advanced_paper_broker.submit(batch)
                    )
                self.latest_by_instrument[batch.instrument.canonical_id] = batch
            projections.append(
                DirectionalPaperEventProjection(
                    event=event,
                    updates=tuple(updates),
                    event_row_id=row_id,
                    advanced_updates=tuple(advanced_updates),
                    portfolio_snapshot=(
                        self.combined_portfolio_snapshot(
                            event.metadata.exchange_timestamp
                        )
                        if updates or advanced_updates
                        else None
                    ),
                )
            )
        async with self.session_factory() as session:
            await save_directional_paper_page(
                session,
                projections,
                consumer_name=self._paper_consumer_name,
            )
        self.paper_replayed_events += len(events)

    async def _catch_up(self) -> None:
        async with self.session_factory() as session:
            journal_tip = await latest_event_row_id(session)
        while self._processed_event_row_id < journal_tip:
            async with self.session_factory() as session:
                pending = await load_ingestion_events(
                    session,
                    after_row_id=self._processed_event_row_id,
                    up_to_row_id=journal_tip,
                )
            if not pending:
                return
            projections: list[DirectionalPaperEventProjection] = []
            for row_id, event in pending:
                batch = self.engine.process(event)
                if batch is not None:
                    async with self.session_factory() as session:
                        inserted = await save_multi_regime_batch(session, batch)
                    self.latest_by_instrument[batch.instrument.canonical_id] = batch
                    self.persisted_batches += int(inserted)
                if self._paper_execution_enabled:
                    updates = (
                        list(self.paper_broker.advance(event))
                        if self.paper_broker is not None
                        else []
                    )
                    advanced_updates = (
                        list(self.advanced_paper_broker.advance(event))
                        if self.advanced_paper_broker is not None
                        else []
                    )
                    if batch is not None:
                        if self.paper_broker is not None:
                            updates.extend(self.paper_broker.submit(batch))
                        if self.advanced_paper_broker is not None:
                            advanced_updates.extend(
                                self.advanced_paper_broker.submit(batch)
                            )
                    projections.append(
                        DirectionalPaperEventProjection(
                            event=event,
                            updates=tuple(updates),
                            event_row_id=row_id,
                            advanced_updates=tuple(advanced_updates),
                            portfolio_snapshot=(
                                self.combined_portfolio_snapshot(
                                    event.metadata.exchange_timestamp
                                )
                                if updates or advanced_updates
                                else None
                            ),
                        )
                    )
            if projections:
                async with self.session_factory() as session:
                    await save_directional_paper_page(
                        session,
                        projections,
                        consumer_name=self._paper_consumer_name,
                    )
            self._processed_event_row_id = pending[-1][0]

    def combined_portfolio_snapshot(
        self,
        timestamp: datetime,
    ) -> PortfolioSnapshot | None:
        if not self._paper_execution_enabled or self.runtime_state is None:
            return None
        legacy = self.runtime_state.portfolio.snapshot(timestamp)
        reserved = (
            self.paper_broker.reserved_notional
            if self.paper_broker is not None
            else ZERO
        ) + (
            self.advanced_paper_broker.reserved_notional
            if self.advanced_paper_broker is not None
            else ZERO
        )
        cash = legacy.cash - reserved
        if cash < 0:
            raise RuntimeError("multi-regime paper reserve exceeds virtual cash")
        locked = legacy.locked_capital + reserved
        total_pnl = (
            legacy.total_pnl
            + (
                self.paper_broker.total_net_pnl
                if self.paper_broker is not None
                else ZERO
            )
            + (
                self.advanced_paper_broker.total_net_pnl
                if self.advanced_paper_broker is not None
                else ZERO
            )
        )
        fees = legacy.fees
        if self.paper_broker is not None:
            fees += sum(
                (position.total_fee for position in self.paper_broker.positions),
                ZERO,
            )
        if self.advanced_paper_broker is not None:
            fees += sum(
                (
                    position.total_fee
                    for position in self.advanced_paper_broker.positions
                ),
                ZERO,
            )
        equity = cash + locked + total_pnl
        return PortfolioSnapshot(
            timestamp=timestamp,
            simulation_version=legacy.simulation_version,
            equity=equity,
            cash=cash,
            locked_capital=locked,
            total_pnl=total_pnl,
            funding_pnl=legacy.funding_pnl,
            fees=fees,
            balances=dict(legacy.balances),
        )

    def _combined_portfolio_snapshot(
        self,
        timestamp: datetime,
    ) -> PortfolioSnapshot | None:
        """Backward-compatible alias used by focused accounting tests."""

        return self.combined_portfolio_snapshot(timestamp)

    @property
    def healthy(self) -> bool:
        return self.failure_reason is None

    @property
    def _paper_execution_enabled(self) -> bool:
        return (
            self.paper_broker is not None
            or self.advanced_paper_broker is not None
        )

    def start(self) -> None:
        """Start a coalescing worker after startup replay is complete."""

        if self.failure_reason is not None:
            raise RuntimeError("multi-regime runtime is failed")
        if self._worker_task is not None:
            raise RuntimeError("multi-regime runtime worker already started")
        self._stop_requested = False
        self._worker_task = asyncio.create_task(
            self._run_worker(),
            name="multi-regime-canonical-consumer",
        )

    async def stop(self, *, timeout_seconds: float = 10.0) -> None:
        """Drain the durable journal and stop the background worker."""

        if timeout_seconds <= 0:
            raise ValueError("multi-regime shutdown timeout must be positive")
        task = self._worker_task
        if task is None:
            return
        self._stop_requested = True
        self._wake_event.set()
        done, pending = await asyncio.wait((task,), timeout=timeout_seconds)
        if pending:
            task.cancel()
            await asyncio.wait((task,), timeout=min(timeout_seconds, 1.0))
            self.failure_reason = "ShutdownTimeout"
            raise TimeoutError("multi-regime runtime worker shutdown timed out")
        if self.failure_reason is not None:
            raise RuntimeError("multi-regime runtime is failed")
        assert done
        drain_task = asyncio.create_task(
            self.flush(),
            name="multi-regime-canonical-consumer-final-drain",
        )
        _, pending = await asyncio.wait((drain_task,), timeout=timeout_seconds)
        if pending:
            drain_task.cancel()
            await asyncio.wait(
                (drain_task,),
                timeout=min(timeout_seconds, 1.0),
            )
            self.failure_reason = "ShutdownTimeout"
            raise TimeoutError("multi-regime runtime final drain timed out")
        await drain_task

    async def flush(self) -> None:
        """Process the durable journal through a stable barrier for this cycle."""

        if self.failure_reason is not None:
            raise RuntimeError("multi-regime runtime is failed")
        async with self._lock:
            try:
                await self._catch_up()
            except Exception as error:
                self.failure_reason = type(error).__name__
                raise

    async def _run_worker(self) -> None:
        while True:
            await self._wake_event.wait()
            self._wake_event.clear()
            async with self._lock:
                if self.failure_reason is not None:
                    return
                try:
                    await self._catch_up()
                except Exception as error:
                    self.failure_reason = type(error).__name__
                    logger.exception("multi_regime_runtime_failed")
                    return
            if self._stop_requested:
                return

    async def publish(self, _event: EventEnvelope[Any]) -> None:
        if self.failure_reason is not None:
            raise RuntimeError("multi-regime runtime is failed")
        if self._worker_task is not None:
            if self._worker_task.done():
                raise RuntimeError("multi-regime runtime worker is stopped")
            self._wake_event.set()
            return
        async with self._lock:
            try:
                await self._catch_up()
            except Exception as error:
                self.failure_reason = type(error).__name__
                raise
