"""Durable, replayable dynamic universe selection for the canonical runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import monotonic_ns
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.events import load_latest_event_by_kind
from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    UniverseSelectionEntry,
    UniverseSelectionExclusion,
    UniverseSelectionSnapshot,
    deterministic_event_id,
)
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    NormalizedInstrument,
    OrderBook,
)
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import funding_statistics, robust_funding_rate
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.strategies.universe import (
    LiquidAltcoinUniverseSelector,
    UniverseCandidate,
    UniverseExclusion,
    UniverseScore,
    UniverseSelection,
    UniverseSelectorConfig,
)

BPS = Decimal("10000")
TARGET_SLIPPAGE_NOTIONAL_USD = Decimal("10000")
SUPPORTED_QUOTES = frozenset({"USD", "USDC", "USDT"})
SOURCE = "SYSTEM:runtime-universe-v1"

CanonicalEventSink = Callable[[EventEnvelope[Any]], Awaitable[None]]


class RuntimeUniversePublisher:
    """Select from one completed as-of snapshot before its mirrored events."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        event_sink: CanonicalEventSink,
        *,
        selector_config: UniverseSelectorConfig | None = None,
        rebalance_seconds: int = 3600,
        enabled: bool = True,
    ) -> None:
        if rebalance_seconds <= 0:
            raise ValueError("universe rebalance interval must be positive")
        self.session_factory = session_factory
        self.event_sink = event_sink
        self.selector = LiquidAltcoinUniverseSelector(selector_config)
        self.rebalance_seconds = rebalance_seconds
        self.enabled = enabled
        self._restored = False
        self._previous: UniverseSelection | None = None

    @property
    def previous(self) -> UniverseSelection | None:
        return self._previous

    async def observe_snapshot(
        self, snapshot: MarketSnapshot
    ) -> EventEnvelope[UniverseSelectionSnapshot] | None:
        if not self.enabled:
            return None
        await self._restore()
        as_of = _utc(snapshot.captured_at)
        if self._previous is not None:
            elapsed = (as_of - self._previous.as_of).total_seconds()
            if elapsed < 0:
                raise ValueError("market snapshot predates the durable universe")
            if elapsed < self.rebalance_seconds:
                return None
        candidates = build_universe_candidates(snapshot, self.selector.config)
        selection = self.selector.select(candidates, as_of, self._previous)
        payload = _selection_payload(selection)
        received_at = datetime.now(UTC)
        observed_monotonic_ns = monotonic_ns()
        sequence_id = selection.selection_id
        event_id = deterministic_event_id(
            source=SOURCE,
            kind=EventKind.UNIVERSE_SELECTION_SNAPSHOT,
            sequence_id=sequence_id,
            exchange_timestamp=as_of,
            payload=payload,
        )
        event = EventEnvelope[UniverseSelectionSnapshot](
            kind=EventKind.UNIVERSE_SELECTION_SNAPSHOT,
            metadata=EventMetadata(
                event_id=event_id,
                exchange_timestamp=as_of,
                receive_timestamp=received_at,
                monotonic_ns=observed_monotonic_ns,
                sequence_id=sequence_id,
                source=SOURCE,
                correlation_id=selection.selection_id,
                payload_version=1,
            ),
            payload=payload,
        )
        await self.event_sink(event)
        self._previous = selection
        return event

    async def _restore(self) -> None:
        if self._restored:
            return
        async with self.session_factory() as session:
            event = await load_latest_event_by_kind(
                session, EventKind.UNIVERSE_SELECTION_SNAPSHOT
            )
        if event is not None:
            payload = event.payload
            if not isinstance(payload, UniverseSelectionSnapshot):
                raise TypeError("durable universe event has an unexpected payload")
            self._previous = _selection_from_payload(payload)
        self._restored = True


def build_universe_candidates(
    snapshot: MarketSnapshot,
    config: UniverseSelectorConfig | None = None,
) -> tuple[UniverseCandidate, ...]:
    """Build conservative per-venue candidates from one completed snapshot.

    The earliest available funding observation is used as a conservative
    listing-history lower bound.  An asset therefore cannot pass the listing-age
    gate unless the journal actually contains at least that much past evidence.
    """

    policy = config or UniverseSelectorConfig()
    as_of = _utc(snapshot.captured_at)
    incomplete = {venue.lower() for venue in snapshot.incomplete_venues}
    raw: list[UniverseCandidate] = []
    for instrument in sorted(
        (
            item
            for item in snapshot.instruments
            if item.is_active
            and item.instrument_type is LegacyInstrumentType.PERPETUAL
            and item.quote_asset.upper() in SUPPORTED_QUOTES
        ),
        key=lambda item: (item.base_asset, item.exchange, item.exchange_symbol),
    ):
        ticker = snapshot.ticker(
            instrument.exchange,
            instrument.exchange_symbol,
            LegacyInstrumentType.PERPETUAL,
        )
        funding = snapshot.funding_rate(
            instrument.exchange, instrument.exchange_symbol
        )
        book = snapshot.orderbook(
            instrument.exchange,
            instrument.exchange_symbol,
            LegacyInstrumentType.PERPETUAL,
        )
        history = _history(snapshot, instrument, as_of)
        refreshed_at = snapshot.funding_history_refreshed.get(
            (instrument.exchange, instrument.exchange_symbol)
        )
        quality = _candidate_quality(
            as_of,
            instrument,
            ticker_timestamp=ticker.timestamp if ticker is not None else None,
            funding_timestamp=funding.timestamp if funding is not None else None,
            book=book,
            refreshed_at=refreshed_at,
            maximum_age_seconds=policy.maximum_data_age_seconds,
            venue_incomplete=instrument.exchange.lower() in incomplete,
        )
        timestamps = tuple(
            value
            for value in (
                ticker.timestamp if ticker is not None else None,
                funding.timestamp if funding is not None else None,
                book.timestamp if book is not None else None,
            )
            if value is not None
        )
        observed_at = min(timestamps) if timestamps else (
            as_of
            - timedelta(seconds=float(policy.maximum_data_age_seconds) + 1)
        )
        history_start = history[0].funding_timestamp if history else as_of
        history_end = history[-1].funding_timestamp if history else as_of
        current_rate = funding.funding_rate if funding is not None else Decimal("0")
        stats = funding_statistics(history, current_rate=current_rate, now=as_of)
        robust_rate = robust_funding_rate(history, current_rate)
        interval_hours = (
            funding.funding_interval_hours
            if funding is not None
            else Decimal(instrument.funding_interval or 8)
        )
        funding_potential = (
            abs(robust_rate) * Decimal("24") / interval_hours * BPS
        )
        stability = _funding_stability(
            stats.sample_count, stats.sign_changes, stats.persistence_score
        )
        coverage = _history_coverage(
            history_start, history_end, len(history), interval_hours
        )
        mid = _mid_price(book, ticker.last_price if ticker is not None else None)
        spread_bps = _spread_bps(book)
        depth_usd = _depth_within_bps(book, Decimal("25"))
        slippage_bps = _slippage_10k_bps(book, mid)
        quote_volume = ticker.volume_24h if ticker is not None else Decimal("0")
        open_interest_usd = _open_interest_usd(
            instrument, ticker.open_interest if ticker else None, mid
        )
        raw.append(
            UniverseCandidate(
                instrument=_canonical_instrument(instrument),
                observed_at=observed_at,
                statistics_window_start=history_start,
                statistics_window_end=history_end,
                listed_at=history_start,
                data_quality=quality,
                venue_count=1,
                quote_volume_24h_usd=quote_volume,
                depth_within_25bps_usd=depth_usd,
                open_interest_usd=open_interest_usd,
                spread_bps=spread_bps,
                slippage_10k_bps=slippage_bps,
                funding_samples=len(history),
                funding_potential_bps_daily=funding_potential,
                funding_stability_score=stability,
                market_data_coverage=coverage,
            )
        )
    valid_venues: dict[str, set[str]] = {}
    for candidate in raw:
        if candidate.data_quality is DataQuality.VALID:
            valid_venues.setdefault(candidate.instrument.base_asset, set()).add(
                candidate.instrument.venue
            )
    return tuple(
        candidate.model_copy(
            update={
                "venue_count": max(
                    1, len(valid_venues.get(candidate.instrument.base_asset, set()))
                )
            }
        )
        for candidate in raw
    )


def _candidate_quality(
    as_of: datetime,
    instrument: NormalizedInstrument,
    *,
    ticker_timestamp: datetime | None,
    funding_timestamp: datetime | None,
    book: OrderBook | None,
    refreshed_at: datetime | None,
    maximum_age_seconds: Decimal,
    venue_incomplete: bool,
) -> DataQuality:
    if venue_incomplete:
        return DataQuality.UNAVAILABLE
    timestamps = (ticker_timestamp, funding_timestamp, book.timestamp if book else None)
    if any(value is None for value in timestamps) or refreshed_at is None:
        return DataQuality.UNAVAILABLE
    normalized = tuple(_utc(value) for value in timestamps if value is not None)
    if any(value > as_of for value in (*normalized, _utc(refreshed_at))):
        return DataQuality.INVALID
    if any(
        Decimal(str((as_of - value).total_seconds())) > maximum_age_seconds
        for value in normalized
    ):
        return DataQuality.STALE
    if book is None or not book.bids or not book.asks:
        return DataQuality.UNAVAILABLE
    if book.bids[0].price >= book.asks[0].price:
        return DataQuality.CROSSED
    if instrument.contract_size <= 0:
        return DataQuality.INVALID
    return DataQuality.VALID


def _history(
    snapshot: MarketSnapshot,
    instrument: NormalizedInstrument,
    as_of: datetime,
) -> list[FundingHistoryPoint]:
    points = (snapshot.funding_history or {}).get(
        (instrument.exchange, instrument.exchange_symbol), []
    )
    return sorted(
        (point for point in points if point.funding_timestamp <= as_of),
        key=lambda point: point.funding_timestamp,
    )


def _canonical_instrument(instrument: NormalizedInstrument) -> InstrumentKey:
    return InstrumentKey(
        venue=instrument.exchange,
        exchange_symbol=instrument.exchange_symbol,
        base_asset=instrument.base_asset,
        quote_asset=instrument.quote_asset,
        settlement_asset=instrument.settlement_asset or instrument.quote_asset,
        instrument_type=InstrumentType.PERPETUAL,
        expiry=instrument.expiry,
    )


def _mid_price(book: OrderBook | None, fallback: Decimal | None) -> Decimal:
    if book is not None and book.bids and book.asks:
        return (book.bids[0].price + book.asks[0].price) / Decimal("2")
    return fallback if fallback is not None and fallback > 0 else Decimal("0")


def _spread_bps(book: OrderBook | None) -> Decimal:
    mid = _mid_price(book, None)
    if book is None or not book.bids or not book.asks or mid <= 0:
        return Decimal("1000000")
    return (book.asks[0].price - book.bids[0].price) / mid * BPS


def _depth_within_bps(book: OrderBook | None, distance_bps: Decimal) -> Decimal:
    mid = _mid_price(book, None)
    if book is None or mid <= 0:
        return Decimal("0")
    lower = mid * (Decimal("1") - distance_bps / BPS)
    upper = mid * (Decimal("1") + distance_bps / BPS)
    bid_depth = sum(
        (level.price * level.quantity for level in book.bids if level.price >= lower),
        Decimal("0"),
    )
    ask_depth = sum(
        (level.price * level.quantity for level in book.asks if level.price <= upper),
        Decimal("0"),
    )
    return min(bid_depth, ask_depth)


def _slippage_10k_bps(book: OrderBook | None, mid: Decimal) -> Decimal:
    if book is None or mid <= 0:
        return Decimal("1000000")
    quantity = TARGET_SLIPPAGE_NOTIONAL_USD / mid
    estimates = (
        calculate_execution_price(book, OrderSide.BUY, quantity),
        calculate_execution_price(book, OrderSide.SELL, quantity),
    )
    if any(not estimate.is_fully_filled for estimate in estimates):
        return Decimal("1000000")
    return max(estimate.slippage_percent for estimate in estimates) * BPS


def _open_interest_usd(
    instrument: NormalizedInstrument,
    open_interest: Decimal | None,
    mid: Decimal,
) -> Decimal:
    if open_interest is None or mid <= 0:
        return Decimal("0")
    base_quantity = abs(open_interest)
    # Gate and OKX expose contract count in their ticker payload; the other
    # adapters normalize open interest to base quantity.
    if instrument.exchange.lower() in {"gate", "okx"}:
        base_quantity *= instrument.contract_size
    return base_quantity * mid


def _history_coverage(
    start: datetime,
    end: datetime,
    sample_count: int,
    interval_hours: Decimal,
) -> Decimal:
    if sample_count <= 0 or end < start or interval_hours <= 0:
        return Decimal("0")
    span_hours = Decimal(str((end - start).total_seconds())) / Decimal("3600")
    expected = max(Decimal("1"), span_hours / interval_hours + Decimal("1"))
    return min(Decimal("1"), Decimal(sample_count) / expected)


def _funding_stability(
    sample_count: int,
    sign_changes: int,
    persistence_score: Decimal,
) -> Decimal:
    if sample_count <= 1:
        return Decimal("0")
    sign_consistency = Decimal("1") - min(
        Decimal("1"), Decimal(sign_changes) / Decimal(sample_count - 1)
    )
    return min(
        Decimal("1"),
        max(Decimal("0"), persistence_score / Decimal("100") * sign_consistency),
    )


def _selection_payload(selection: UniverseSelection) -> UniverseSelectionSnapshot:
    return UniverseSelectionSnapshot(
        exchange_timestamp=selection.as_of,
        selection_id=selection.selection_id,
        selector_version=selection.selector_version,
        selected=tuple(
            UniverseSelectionEntry(
                instrument=item.instrument,
                asset=item.asset,
                score=item.score,
                liquidity_score=item.liquidity_score,
                funding_score=item.funding_score,
                quality_score=item.quality_score,
                retained_from_previous=item.retained_from_previous,
            )
            for item in selection.selected
        ),
        excluded=tuple(
            UniverseSelectionExclusion(
                instrument=item.instrument,
                reason=item.reason,
            )
            for item in selection.excluded
        ),
        input_fingerprint=selection.input_fingerprint,
    )


def _selection_from_payload(payload: UniverseSelectionSnapshot) -> UniverseSelection:
    return UniverseSelection(
        selection_id=payload.selection_id,
        selector_version=payload.selector_version,
        as_of=payload.exchange_timestamp,
        selected=tuple(
            UniverseScore(
                instrument=item.instrument,
                asset=item.asset,
                score=item.score,
                liquidity_score=item.liquidity_score,
                funding_score=item.funding_score,
                quality_score=item.quality_score,
                retained_from_previous=item.retained_from_previous,
            )
            for item in payload.selected
        ),
        excluded=tuple(
            UniverseExclusion(instrument=item.instrument, reason=item.reason)
            for item in payload.excluded
        ),
        input_fingerprint=payload.input_fingerprint,
    )


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
