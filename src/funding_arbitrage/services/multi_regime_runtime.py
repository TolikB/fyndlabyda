"""Durable async consumer for the canonical multi-regime decision engine."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.domain.decisions import MarketRegime, SignalIntent
from funding_arbitrage.domain.events import (
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
    load_directional_paper_checkpoint,
    load_directional_paper_positions,
    save_directional_paper_event,
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
from funding_arbitrage.execution.directional_paper import DirectionalPaperBroker
from funding_arbitrage.services.multi_regime import (
    MultiRegimeDecisionBatch,
    MultiRegimeEngine,
)


class RuntimePortfolioRiskContextProvider:
    """Build paper/shadow risk context from current typed market and portfolio state."""

    def __init__(
        self,
        runtime: RuntimeState,
        paper_broker: DirectionalPaperBroker | None = None,
    ) -> None:
        self.runtime = runtime
        self.paper_broker = paper_broker

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
        directional_asset_exposure = broker.asset_exposure(asset) if broker else Decimal("0")
        directional_strategy_exposure = (
            broker.strategy_exposure(intent.strategy_id) if broker else Decimal("0")
        )
        directional_venue_exposure = broker.venue_exposure(venue) if broker else Decimal("0")
        directional_correlation_exposure = (
            sum((broker.asset_exposure(item) for item in group), Decimal("0"))
            if broker
            else Decimal("0")
        )
        directional_reserved = broker.reserved_notional if broker else Decimal("0")
        directional_total_pnl = broker.total_net_pnl if broker else Decimal("0")
        directional_gross = broker.gross_exposure if broker else Decimal("0")
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
        available_cash = max(
            Decimal("0"),
            account.cash - directional_reserved,
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
            equity_usd=account.equity + directional_total_pnl,
            cash_usd=available_cash,
            portfolio_gross_notional_usd=account.locked_capital + directional_gross,
            portfolio_net_delta_usd=directional_net_delta,
            position_exposure_usd=(
                portfolio.asset_exposure(asset) + directional_asset_exposure
            ),
            asset_exposures_usd={
                asset: portfolio.asset_exposure(asset) + directional_asset_exposure
            },
            strategy_exposures_usd={
                intent.strategy_id: (
                    portfolio.strategy_exposure(intent.strategy_id)
                    + directional_strategy_exposure
                )
            },
            venue_exposures_usd={
                venue: (
                    portfolio.exchange_exposure(venue.lower())
                    + directional_venue_exposure
                )
            },
            correlation_exposures_usd={
                "runtime:" + ",".join(sorted(group)): (
                    portfolio.correlated_exposure(
                        asset,
                        self.runtime.settings.paper_correlation_group_values,
                    )
                    + directional_correlation_exposure
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
        runtime_state: RuntimeState | None = None,
    ) -> None:
        if paper_broker is not None and engine.config.mode is not TradingMode.PAPER:
            raise ValueError("directional paper broker requires PAPER engine mode")
        self.engine = engine
        self.session_factory = session_factory
        self.paper_broker = paper_broker
        self.runtime_state = runtime_state
        self.latest_by_instrument: dict[str, MultiRegimeDecisionBatch] = {}
        self.persisted_batches = 0
        self.restored_events = 0
        self.paper_replayed_events = 0
        self.failure_reason: str | None = None
        self._processed_event_row_id = 0
        self._paper_consumer_name = (
            (
                "multi_regime_paper_v1"
                if paper_broker.simulation_version == "v1-legacy"
                else "multi_regime_paper_"
                + hashlib.sha256(
                    paper_broker.simulation_version.encode()
                ).hexdigest()[:32]
            )
            if paper_broker is not None
            else "multi_regime_paper_disabled"
        )
        self._lock = asyncio.Lock()

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
                    checkpoint = (
                        await load_directional_paper_checkpoint(
                            session,
                            consumer_name=self._paper_consumer_name,
                        )
                        if self.paper_broker is not None
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
                if self.paper_broker is not None:
                    self.paper_broker.restore(positions)
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
        assert self.paper_broker is not None
        batches_by_event: dict[str, list[MultiRegimeDecisionBatch]] = {}
        for batch in batches:
            batches_by_event.setdefault(batch.source_event_id, []).append(batch)
        for row_id, event in events:
            updates = list(self.paper_broker.advance(event))
            for batch in batches_by_event.get(event.metadata.event_id, ()):
                updates.extend(self.paper_broker.submit(batch))
                self.latest_by_instrument[batch.instrument.canonical_id] = batch
            async with self.session_factory() as session:
                await save_directional_paper_event(
                    session,
                    event,
                    updates,
                    event_row_id=row_id,
                    portfolio_snapshot=(
                        self.combined_portfolio_snapshot(
                            event.metadata.exchange_timestamp
                        )
                        if updates
                        else None
                    ),
                    consumer_name=self._paper_consumer_name,
                )
            self.paper_replayed_events += 1

    async def _catch_up(self) -> None:
        while True:
            async with self.session_factory() as session:
                pending = await load_ingestion_events(
                    session,
                    after_row_id=self._processed_event_row_id,
                )
            if not pending:
                return
            for row_id, event in pending:
                batch = self.engine.process(event)
                if batch is not None:
                    async with self.session_factory() as session:
                        inserted = await save_multi_regime_batch(session, batch)
                    self.latest_by_instrument[batch.instrument.canonical_id] = batch
                    self.persisted_batches += int(inserted)
                if self.paper_broker is not None:
                    updates = list(self.paper_broker.advance(event))
                    if batch is not None:
                        updates.extend(self.paper_broker.submit(batch))
                    async with self.session_factory() as session:
                        await save_directional_paper_event(
                            session,
                            event,
                            updates,
                            event_row_id=row_id,
                            portfolio_snapshot=(
                                self.combined_portfolio_snapshot(
                                    event.metadata.exchange_timestamp
                                )
                                if updates
                                else None
                            ),
                            consumer_name=self._paper_consumer_name,
                        )
                self._processed_event_row_id = row_id
    def combined_portfolio_snapshot(
        self,
        timestamp: datetime,
    ) -> PortfolioSnapshot | None:
        if self.paper_broker is None or self.runtime_state is None:
            return None
        legacy = self.runtime_state.portfolio.snapshot(timestamp)
        reserved = self.paper_broker.reserved_notional
        cash = legacy.cash - reserved
        if cash < 0:
            raise RuntimeError("directional paper reserve exceeds virtual cash")
        locked = legacy.locked_capital + reserved
        total_pnl = legacy.total_pnl + self.paper_broker.total_net_pnl
        fees = legacy.fees + sum(
            (position.total_fee for position in self.paper_broker.positions),
            Decimal("0"),
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

    async def publish(self, _event: EventEnvelope[Any]) -> None:
        async with self._lock:
            if self.failure_reason is not None:
                raise RuntimeError("multi-regime runtime is failed")
            try:
                await self._catch_up()
            except Exception as error:
                self.failure_reason = type(error).__name__
                raise
