"""As-of synchronized advanced-strategy contexts for the canonical runtime.

The market collector finishes a venue snapshot after individual exchange events
have occurred.  A canonical decision must therefore rebuild an as-of view from
the timestamped rows instead of consuming the collector's already-computed
opportunities, which could contain observations newer than the source event.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import TYPE_CHECKING

from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
)
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    NormalizedInstrument,
    OrderBook,
)
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import (
    funding_statistics,
    robust_funding_rate,
)
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.models import Opportunity, StrategyName
from funding_arbitrage.services.multi_regime import MultiRegimeStrategySnapshot
from funding_arbitrage.services.strategy_suite import (
    LeadLagStrategyContext,
    SupplementalStrategyContexts,
)
from funding_arbitrage.strategies import (
    BasisMarketLeg,
    DatedBasisContext,
    DatedBasisCosts,
    ForecastFundingEvent,
    FundingBasisContext,
    FundingForecastEvent,
    FundingHarvestCosts,
    FundingLegForecast,
    FundingMarketLeg,
    LeadLagCostModel,
    VenueFairValueInput,
)

if TYPE_CHECKING:
    from funding_arbitrage.services.runtime import RuntimeState

BPS = Decimal("10000")
ZERO = Decimal("0")
ONE = Decimal("1")
MAX_DATED_BASIS_DAYS = Decimal("120")
FUNDING_STRATEGIES = frozenset(
    {
        StrategyName.SPOT_PERP.value,
        StrategyName.CROSS_EXCHANGE_FUNDING.value,
        StrategyName.PERP_PERP.value,
    }
)


@dataclass(frozen=True)
class _MarketView:
    metadata: NormalizedInstrument
    instrument: InstrumentKey
    book: BookSnapshot
    mid_price: Decimal
    microprice: Decimal
    spread_bps: Decimal
    liquidity_score: Decimal
    roundtrip_slippage_bps: Decimal


class RuntimeSynchronizedContextBuilder:
    """Build only contexts supported by complete, non-future public evidence."""

    def __init__(self, runtime: RuntimeState) -> None:
        self.runtime = runtime
        source = runtime.opportunity_engine
        # This private projection engine owns its forecast caches and cannot alter
        # the scanner's visible candidates, debouncer, metrics, or trading state.
        self._funding_engine = OpportunityEngine(
            cost_engine=source.cost_engine,
            filter_config=source.filter_config,
            size_grid=source.size_grid,
            funding_horizon_hours=source.funding_horizon_hours,
            allow_spot_short=False,
            forecast_mode=source.forecast_mode,
            diagnostic_quote_limit=0,
        )

    def build(
        self,
        snapshot: MultiRegimeStrategySnapshot,
    ) -> SupplementalStrategyContexts:
        market = self.runtime.last_completed_snapshot or self.runtime.latest_snapshot
        if market is None:
            return SupplementalStrategyContexts()
        as_of = self._as_of_market(market, snapshot.timestamp)
        return SupplementalStrategyContexts(
            funding_basis=self._funding_contexts(snapshot, as_of),
            lead_lag=self._lead_lag_contexts(snapshot, as_of),
            dated_basis=self._dated_basis_contexts(snapshot, as_of),
        )

    def _as_of_market(
        self,
        market: MarketSnapshot,
        timestamp: datetime,
    ) -> MarketSnapshot:
        stale_after = min(
            market.stale_after_seconds,
            self.runtime.settings.multi_regime_stale_after_seconds,
        )

        def usable(observed_at: datetime) -> bool:
            age = (timestamp - observed_at).total_seconds()
            return 0 <= age <= stale_after

        tickers = [item for item in market.tickers if usable(item.timestamp)]
        funding = [item for item in market.funding if usable(item.timestamp)]
        orderbooks = {
            key: book for key, book in market.orderbooks.items() if usable(book.timestamp)
        }
        refresh_markers = market.funding_history_refreshed
        history = {
            key: [
                point
                for point in points
                if point.funding_timestamp <= timestamp
            ]
            for key, points in (market.funding_history or {}).items()
            if (refreshed_at := refresh_markers.get(key)) is not None
            and refreshed_at <= timestamp
        }
        history = {key: points for key, points in history.items() if points}
        refreshed = {
            key: value
            for key, value in refresh_markers.items()
            if key in history
        }
        return MarketSnapshot(
            instruments=market.instruments,
            tickers=tickers,
            funding=funding,
            orderbooks=orderbooks,
            captured_at=timestamp,
            funding_history=history,
            stale_after_seconds=stale_after,
            incomplete_venues=market.incomplete_venues,
            funding_history_refreshed=refreshed,
        )

    def _funding_contexts(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        market: MarketSnapshot,
    ) -> tuple[FundingBasisContext, ...]:
        candidates = self._funding_engine.scan(market)
        contexts: list[FundingBasisContext] = []
        for opportunity in candidates:
            if str(opportunity.strategy) not in FUNDING_STRATEGIES:
                continue
            context = self._funding_context(snapshot, market, opportunity)
            if context is not None:
                contexts.append(context)
        return tuple(
            sorted(
                contexts,
                key=lambda item: (
                    item.leg_a.instrument.canonical_id,
                    item.leg_b.instrument.canonical_id,
                ),
            )
        )

    def _funding_context(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        market: MarketSnapshot,
        opportunity: Opportunity,
    ) -> FundingBasisContext | None:
        if opportunity.symbol_a is None or opportunity.symbol_b is None:
            return None
        try:
            type_a = LegacyInstrumentType(opportunity.leg_a_type)
            type_b = LegacyInstrumentType(opportunity.leg_b_type)
            side_a = Side(opportunity.leg_a_side)
            side_b = Side(opportunity.leg_b_side)
        except ValueError:
            return None
        metadata_a = _metadata(
            market,
            opportunity.venue_a,
            opportunity.symbol_a,
            type_a,
        )
        metadata_b = _metadata(
            market,
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b,
            type_b,
        )
        if metadata_a is None or metadata_b is None:
            return None
        view_a = self._view(snapshot, market, metadata_a)
        view_b = self._view(snapshot, market, metadata_b)
        if view_a is None or view_b is None:
            return None
        if not _triggered_by(snapshot, (view_a, view_b)):
            return None
        if (
            (type_a is LegacyInstrumentType.SPOT and side_a is Side.SELL)
            or (type_b is LegacyInstrumentType.SPOT and side_b is Side.SELL)
        ):
            # A configured borrow estimate is not proof that inventory can be
            # borrowed.  No authenticated borrow provider exists in this path.
            return None
        minimum_rate = self.runtime.settings.paper_minimum_funding_rate
        if max(
            abs(opportunity.funding_a),
            abs(opportunity.funding_b),
            abs(opportunity.funding_a - opportunity.funding_b),
        ) < minimum_rate:
            return None
        size = max(
            (
                quote
                for quote in opportunity.size_quotes
                if quote.fully_filled
                and quote.net_profit > ZERO
                and quote.capital * Decimal("2")
                <= self.runtime.settings.paper_max_funding_capital_usd
            ),
            key=lambda quote: (quote.net_profit, quote.capital),
            default=None,
        )
        if size is None or not self._venue_cash_available(
            (view_a.instrument.venue, view_b.instrument.venue),
            size.capital,
        ):
            return None
        horizon_seconds = max(
            1,
            int(opportunity.expected_holding_hours * Decimal("3600")),
        )
        horizon_end = snapshot.timestamp + timedelta(seconds=horizon_seconds)
        raw_forecasts: list[FundingLegForecast] = []
        for view in (view_a, view_b):
            if view.instrument.instrument_type is not InstrumentType.PERPETUAL:
                continue
            forecast = self._funding_forecast(
                market,
                view.instrument,
                horizon_end,
                snapshot.timestamp,
            )
            if forecast is None:
                return None
            raw_forecasts.append(forecast)
        forecasts = _align_forecasts_to_opportunity(
            tuple(raw_forecasts),
            ((view_a.instrument, side_a), (view_b.instrument, side_b)),
            opportunity.gross_edge,
        )
        if forecasts is None:
            return None
        fee_a = self._fee_schedule(view_a.instrument.venue)
        fee_b = self._fee_schedule(view_b.instrument.venue)
        if fee_a is None or fee_b is None:
            return None
        costs = size.costs
        return FundingBasisContext(
            leg_a=FundingMarketLeg(
                instrument=view_a.instrument,
                price=view_a.mid_price,
                timestamp=view_a.book.exchange_timestamp,
                data_quality=DataQuality.VALID,
                liquidity_score=view_a.liquidity_score,
            ),
            leg_b=FundingMarketLeg(
                instrument=view_b.instrument,
                price=view_b.mid_price,
                timestamp=view_b.book.exchange_timestamp,
                data_quality=DataQuality.VALID,
                liquidity_score=view_b.liquidity_score,
            ),
            forecasts=forecasts,
            costs=FundingHarvestCosts(
                leg_a_entry_fee_bps=fee_a[1] * BPS,
                leg_a_exit_fee_bps=fee_a[1] * BPS,
                leg_b_entry_fee_bps=fee_b[1] * BPS,
                leg_b_exit_fee_bps=fee_b[1] * BPS,
                spread_bps=(
                    costs.entry_spread + costs.exit_spread
                )
                / size.capital
                * BPS,
                slippage_bps=(
                    costs.entry_slippage + costs.exit_slippage
                )
                / size.capital
                * BPS,
                legging_risk_bps=costs.legging_cost / size.capital * BPS,
                transfer_cost_bps=costs.network_cost / size.capital * BPS,
            ),
            requested_notional_usd=size.capital,
            expected_basis_convergence_bps=ZERO,
            holding_horizon_seconds=horizon_seconds,
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            regime=snapshot.regime.regime,
            margin_available=True,
            live_operator_authorized=False,
        )

    def _lead_lag_contexts(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        market: MarketSnapshot,
    ) -> tuple[LeadLagStrategyContext, ...]:
        if snapshot.instrument.instrument_type is not InstrumentType.PERPETUAL:
            return ()
        candidates = [
            metadata
            for metadata in market.instruments
            if metadata.is_active
            and metadata.instrument_type is LegacyInstrumentType.PERPETUAL
            and metadata.base_asset.upper() == snapshot.instrument.base_asset
            and metadata.quote_asset.upper() == snapshot.instrument.quote_asset
        ]
        views = tuple(
            view
            for metadata in candidates
            if (view := self._view(snapshot, market, metadata)) is not None
        )
        primary = next(
            (view for view in views if view.instrument == snapshot.instrument),
            None,
        )
        if primary is None:
            return ()
        references = tuple(
            sorted(
                (
                    view
                    for view in views
                    if view.instrument.venue != primary.instrument.venue
                    and self._venue_cash_available(
                        (view.instrument.venue,),
                        self.runtime.settings.paper_position_size_usd,
                    )
                ),
                key=lambda item: item.instrument.canonical_id,
            )
        )
        if len({view.instrument.venue for view in references}) < 2:
            return ()
        primary_fee = self._fee_schedule(primary.instrument.venue)
        reference_fees = [
            self._fee_schedule(view.instrument.venue) for view in references
        ]
        if primary_fee is None or any(item is None for item in reference_fees):
            return ()
        assert all(item is not None for item in reference_fees)
        worst_reference_fee = max(item[1] for item in reference_fees if item is not None)
        worst_reference_spread = max(view.spread_bps for view in references)
        worst_reference_slippage = max(
            view.roundtrip_slippage_bps for view in references
        )
        adverse_selection = (
            abs(snapshot.orderflow.microprice - primary.mid_price)
            / primary.mid_price
            * BPS
            if snapshot.orderflow.microprice is not None
            else self.runtime.settings.multi_regime_estimated_cost_bps
        )
        funding_bps = max(
            (
                self._holding_funding_cost_bps(
                    market,
                    view.instrument,
                    snapshot.timestamp,
                    30,
                )
                for view in (primary, *references)
            ),
            default=ZERO,
        )
        context = LeadLagStrategyContext(
            primary=_fair_value_input(primary),
            references=tuple(_fair_value_input(view) for view in references),
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            regime=snapshot.regime.regime,
            costs=LeadLagCostModel(
                fees_bps=(primary_fee[1] + worst_reference_fee) * BPS * Decimal("2"),
                spread_bps=primary.spread_bps + worst_reference_spread,
                slippage_bps=(
                    primary.roundtrip_slippage_bps + worst_reference_slippage
                ),
                adverse_selection_bps=adverse_selection,
                funding_bps=funding_bps,
                legging_risk_bps=(
                    self.runtime.settings.paper_legging_move_percent * BPS
                ),
            ),
            inventory_available=self._venue_cash_available(
                (primary.instrument.venue,),
                self.runtime.settings.paper_position_size_usd,
            ),
            transfer_ready=True,
        )
        return (context,)

    def _dated_basis_contexts(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        market: MarketSnapshot,
    ) -> tuple[DatedBasisContext, ...]:
        if snapshot.instrument.instrument_type is not InstrumentType.FUTURE:
            return ()
        future_metadata = _metadata(
            market,
            snapshot.instrument.venue,
            snapshot.instrument.exchange_symbol,
            LegacyInstrumentType.FUTURE,
        )
        if future_metadata is None or future_metadata.expiry is None:
            return ()
        seconds_to_expiry = Decimal(
            str((future_metadata.expiry - snapshot.timestamp).total_seconds())
        )
        if (
            seconds_to_expiry <= ZERO
            or seconds_to_expiry
            > MAX_DATED_BASIS_DAYS * Decimal("86400")
        ):
            return ()
        perpetual_metadata = min(
            (
                item
                for item in market.instruments
                if item.is_active
                and item.exchange.lower() == future_metadata.exchange.lower()
                and item.base_asset.upper() == future_metadata.base_asset.upper()
                and item.quote_asset.upper() == future_metadata.quote_asset.upper()
                and item.instrument_type is LegacyInstrumentType.PERPETUAL
            ),
            key=lambda item: (
                item.exchange_symbol.upper(),
                item.settlement_asset or item.quote_asset,
            ),
            default=None,
        )
        if perpetual_metadata is None:
            return ()
        future = self._view(snapshot, market, future_metadata)
        perpetual = self._view(snapshot, market, perpetual_metadata)
        if future is None or perpetual is None:
            return ()
        if not self._venue_cash_available(
            (future.instrument.venue, perpetual.instrument.venue),
            self.runtime.settings.paper_position_size_usd,
        ):
            return ()
        forecast = self._funding_forecast(
            market,
            perpetual.instrument,
            future_metadata.expiry,
            snapshot.timestamp,
        )
        if forecast is None:
            return ()
        fee = self._fee_schedule(future.instrument.venue)
        if fee is None:
            return ()
        context = DatedBasisContext(
            perpetual=BasisMarketLeg(
                instrument=perpetual.instrument,
                price=perpetual.mid_price,
                timestamp=perpetual.book.exchange_timestamp,
                data_quality=DataQuality.VALID,
                liquidity_score=perpetual.liquidity_score,
            ),
            future=BasisMarketLeg(
                instrument=future.instrument,
                price=future.mid_price,
                timestamp=future.book.exchange_timestamp,
                data_quality=DataQuality.VALID,
                liquidity_score=future.liquidity_score,
            ),
            funding_events=tuple(
                ForecastFundingEvent(
                    settlement_time=event.settlement_time,
                    funding_rate=event.predicted_rate,
                )
                for event in forecast.events
            ),
            costs=DatedBasisCosts(
                entry_exit_fees_bps=fee[1] * BPS * Decimal("4"),
                spread_bps=perpetual.spread_bps + future.spread_bps,
                slippage_bps=(
                    perpetual.roundtrip_slippage_bps
                    + future.roundtrip_slippage_bps
                ),
                operational_buffer_bps=(
                    self.runtime.settings.paper_legging_move_percent * BPS
                ),
            ),
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            regime=snapshot.regime.regime,
            margin_available=True,
        )
        return (context,)

    def _view(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        market: MarketSnapshot,
        metadata: NormalizedInstrument,
    ) -> _MarketView | None:
        if metadata.exchange.lower() in {
            venue.lower() for venue in market.incomplete_venues
        }:
            return None
        instrument = _canonical_instrument(metadata)
        if instrument == snapshot.instrument:
            book = snapshot.book
        else:
            legacy = market.orderbook(
                metadata.exchange,
                metadata.exchange_symbol,
                metadata.instrument_type,
            )
            if legacy is None:
                return None
            book = _canonical_book(instrument, legacy)
        age = Decimal(str((snapshot.timestamp - book.exchange_timestamp).total_seconds()))
        if age < ZERO or age > Decimal(str(market.stale_after_seconds)):
            return None
        if not book.bids or not book.asks or book.bids[0].price >= book.asks[0].price:
            return None
        mid = (book.bids[0].price + book.asks[0].price) / Decimal("2")
        bid_quantity = book.bids[0].quantity
        ask_quantity = book.asks[0].quantity
        touch_quantity = bid_quantity + ask_quantity
        microprice = (
            (
                book.asks[0].price * bid_quantity
                + book.bids[0].price * ask_quantity
            )
            / touch_quantity
            if touch_quantity > ZERO
            else mid
        )
        spread_bps = (book.asks[0].price - book.bids[0].price) / mid * BPS
        requested_notional = self.runtime.settings.paper_position_size_usd
        depth_notional = min(
            sum((level.price * level.quantity for level in book.bids[:20]), ZERO),
            sum((level.price * level.quantity for level in book.asks[:20]), ZERO),
        )
        spread_limit = self.runtime.settings.scanner_maximum_spread_percent * BPS
        if depth_notional <= ZERO or spread_limit <= ZERO:
            return None
        depth_score = min(ONE, depth_notional / requested_notional)
        spread_score = max(ZERO, ONE - spread_bps / spread_limit)
        liquidity_score = min(depth_score, spread_score)
        slippage = _roundtrip_slippage_bps(book, requested_notional)
        if slippage is None:
            return None
        return _MarketView(
            metadata=metadata,
            instrument=instrument,
            book=book,
            mid_price=mid,
            microprice=microprice,
            spread_bps=spread_bps,
            liquidity_score=liquidity_score,
            roundtrip_slippage_bps=slippage,
        )

    def _funding_forecast(
        self,
        market: MarketSnapshot,
        instrument: InstrumentKey,
        horizon_end: datetime,
        timestamp: datetime,
    ) -> FundingLegForecast | None:
        funding = _funding_snapshot(market, instrument)
        if funding is None or funding.next_funding_time is None:
            return None
        history = list(
            (market.funding_history or {}).get(
                (funding.exchange, funding.symbol),
                (),
            )
        )
        if len(history) < self.runtime.settings.scanner_minimum_funding_samples:
            return None
        refreshed = market.funding_history_refreshed.get(
            (funding.exchange, funding.symbol)
        )
        if refreshed is None:
            return None
        refresh_age = (timestamp - refreshed).total_seconds()
        if (
            refresh_age < 0
            or refresh_age > self.runtime.settings.paper_history_refresh_seconds
        ):
            return None
        stats = funding_statistics(history, funding.funding_rate, timestamp)
        predicted = robust_funding_rate(history, funding.funding_rate)
        events = _funding_events(funding, timestamp, horizon_end, predicted)
        if not events:
            return None
        values = [point.funding_rate for point in history]
        return FundingLegForecast(
            instrument=instrument,
            generated_at=timestamp,
            events=events,
            median_rate=stats.median,
            ewma_rate=_ewma(values),
            persistence_score=stats.persistence_score / Decimal("100"),
            sign_change_count=stats.sign_changes,
            two_sided_outlier_score=_robust_outlier_score(
                values,
                funding.funding_rate,
            ),
            sample_count=stats.sample_count,
            data_quality=DataQuality.VALID,
        )

    def _holding_funding_cost_bps(
        self,
        market: MarketSnapshot,
        instrument: InstrumentKey,
        timestamp: datetime,
        holding_seconds: int,
    ) -> Decimal:
        funding = _funding_snapshot(market, instrument)
        if funding is None or funding.next_funding_time is None:
            return ZERO
        horizon = timestamp + timedelta(seconds=holding_seconds)
        return (
            abs(funding.funding_rate) * BPS
            if timestamp < funding.next_funding_time <= horizon
            else ZERO
        )

    def _fee_schedule(
        self,
        venue: str,
    ) -> tuple[Decimal, Decimal] | None:
        return self.runtime.settings.fee_schedules.get(venue.lower())

    def _venue_cash_available(
        self,
        venues: tuple[str, ...],
        per_leg_notional: Decimal,
    ) -> bool:
        requirements = {
            venue.lower(): per_leg_notional
            * Decimal(sum(item.lower() == venue.lower() for item in venues))
            for venue in venues
        }
        balances = self.runtime.portfolio.balances
        return all(
            balances.get(venue, ZERO) >= amount
            for venue, amount in requirements.items()
        )


def _metadata(
    market: MarketSnapshot,
    venue: str,
    symbol: str,
    instrument_type: LegacyInstrumentType,
) -> NormalizedInstrument | None:
    return next(
        (
            item
            for item in market.instruments
            if item.is_active
            and item.exchange.lower() == venue.lower()
            and item.exchange_symbol.upper() == symbol.upper()
            and item.instrument_type is instrument_type
        ),
        None,
    )


def _canonical_instrument(metadata: NormalizedInstrument) -> InstrumentKey:
    return InstrumentKey(
        venue=metadata.exchange,
        exchange_symbol=metadata.exchange_symbol,
        base_asset=metadata.base_asset,
        quote_asset=metadata.quote_asset,
        instrument_type=InstrumentType(metadata.instrument_type.value),
        settlement_asset=metadata.settlement_asset or metadata.quote_asset,
        expiry=metadata.expiry,
    )


def _canonical_book(instrument: InstrumentKey, book: OrderBook) -> BookSnapshot:
    return BookSnapshot(
        instrument=instrument,
        bids=tuple(
            BookLevel(price=level.price, quantity=level.quantity)
            for level in book.bids
            if level.quantity > ZERO
        ),
        asks=tuple(
            BookLevel(price=level.price, quantity=level.quantity)
            for level in book.asks
            if level.quantity > ZERO
        ),
        sequence=book.sequence or 0,
        exchange_timestamp=book.timestamp,
    )


def _triggered_by(
    snapshot: MultiRegimeStrategySnapshot,
    views: tuple[_MarketView, ...],
) -> bool:
    trigger = max(
        views,
        key=lambda item: (
            item.book.exchange_timestamp,
            item.instrument.canonical_id,
        ),
    )
    return (
        trigger.instrument == snapshot.instrument
        and trigger.book.exchange_timestamp == snapshot.book.exchange_timestamp
    )


def _fair_value_input(view: _MarketView) -> VenueFairValueInput:
    return VenueFairValueInput(
        instrument=view.instrument,
        timestamp=view.book.exchange_timestamp,
        data_quality=DataQuality.VALID,
        mid_price=view.mid_price,
        microprice=view.microprice,
        liquidity_score=view.liquidity_score,
    )


def _roundtrip_slippage_bps(
    book: BookSnapshot,
    notional: Decimal,
) -> Decimal | None:
    mid = (book.bids[0].price + book.asks[0].price) / Decimal("2")
    quantity = notional / mid
    ask_average = _walk_average(book.asks, quantity)
    bid_average = _walk_average(book.bids, quantity)
    if ask_average is None or bid_average is None:
        return None
    buy_slippage = max(ZERO, ask_average - book.asks[0].price) / mid * BPS
    sell_slippage = max(ZERO, book.bids[0].price - bid_average) / mid * BPS
    return buy_slippage + sell_slippage


def _walk_average(
    levels: tuple[BookLevel, ...],
    quantity: Decimal,
) -> Decimal | None:
    remaining = quantity
    notional = ZERO
    for level in levels[:20]:
        consumed = min(remaining, level.quantity)
        notional += consumed * level.price
        remaining -= consumed
        if remaining <= ZERO:
            return notional / quantity
    return None


def _funding_snapshot(
    market: MarketSnapshot,
    instrument: InstrumentKey,
) -> FundingSnapshot | None:
    if instrument.instrument_type is not InstrumentType.PERPETUAL:
        return None
    return next(
        (
            item
            for item in market.funding
            if item.exchange.lower() == instrument.venue.lower()
            and item.symbol.upper() == instrument.exchange_symbol.upper()
        ),
        None,
    )


def _funding_events(
    funding: FundingSnapshot,
    timestamp: datetime,
    horizon_end: datetime,
    predicted_rate: Decimal,
) -> tuple[FundingForecastEvent, ...]:
    next_time = funding.next_funding_time
    if next_time is None:
        return ()
    step = timedelta(hours=float(funding.funding_interval_hours))
    while next_time <= timestamp:
        next_time += step
    events: list[FundingForecastEvent] = []
    while next_time <= horizon_end:
        events.append(
            FundingForecastEvent(
                settlement_time=next_time,
                predicted_rate=predicted_rate,
                source="runtime-robust-history",
            )
        )
        next_time += step
    return tuple(events)


def _align_forecasts_to_opportunity(
    forecasts: tuple[FundingLegForecast, ...],
    legs: tuple[tuple[InstrumentKey, Side], ...],
    target_gross_rate: Decimal,
) -> tuple[FundingLegForecast, ...] | None:
    if not forecasts:
        return None
    sides = {instrument.canonical_id: side for instrument, side in legs}
    baseline = ZERO
    for forecast in forecasts:
        side = sides.get(forecast.instrument.canonical_id)
        if side is None or not forecast.events:
            return None
        coefficient = ONE if side is Side.SELL else -ONE
        baseline += coefficient * sum(
            (event.predicted_rate for event in forecast.events),
            ZERO,
        )
    delta = target_gross_rate - baseline
    count = Decimal(len(forecasts))
    adjusted: list[FundingLegForecast] = []
    for forecast in forecasts:
        side = sides[forecast.instrument.canonical_id]
        coefficient = ONE if side is Side.SELL else -ONE
        per_event_delta = coefficient * delta / count / Decimal(len(forecast.events))
        adjusted.append(
            forecast.model_copy(
                update={
                    "events": tuple(
                        event.model_copy(
                            update={
                                "predicted_rate": event.predicted_rate
                                + per_event_delta,
                                "source": "runtime-synchronized-history",
                            }
                        )
                        for event in forecast.events
                    )
                }
            )
        )
    realized = ZERO
    for forecast in adjusted:
        coefficient = (
            ONE
            if sides[forecast.instrument.canonical_id] is Side.SELL
            else -ONE
        )
        realized += coefficient * sum(
            (event.predicted_rate for event in forecast.events),
            ZERO,
        )
    if abs(realized - target_gross_rate) > Decimal("0.000000000000000001"):
        raise ValueError("synchronized funding forecast invariant failed")
    return tuple(adjusted)


def _ewma(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    alpha = Decimal("0.30")
    value = values[0]
    for current in values[1:]:
        value = alpha * current + (ONE - alpha) * value
    return value


def _robust_outlier_score(
    values: list[Decimal],
    current: Decimal,
) -> Decimal:
    if not values:
        return ZERO
    center = Decimal(str(median(values)))
    deviations = [abs(value - center) for value in values]
    mad = Decimal(str(median(deviations)))
    if mad == ZERO:
        return ZERO if current == center else Decimal("999")
    return abs(current - center) / (mad * Decimal("1.4826"))
