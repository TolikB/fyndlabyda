"""Restart-safe market-data collection coordinator."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.events import (
    BookEvent,
    EventEnvelope,
    InstrumentKey,
    OptionQuoteSnapshot,
)
from funding_arbitrage.domain.events import (
    InstrumentType as CanonicalInstrumentType,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.canonical_snapshot import canonical_snapshot_event
from funding_arbitrage.market_data.health import CircuitBreaker
from funding_arbitrage.market_data.option_quotes import (
    bounded_option_chain,
    canonical_option_quote_event,
)
from funding_arbitrage.monitoring.metrics import (
    exchange_stream_last_message_timestamp,
    funding_history_coverage_ratio,
    market_data_age_seconds,
    market_data_dropped_total,
    market_data_phase_duration_seconds,
    market_tickers_usable,
    orderbook_coverage_ratio,
    stale_or_missing_orderbooks,
)

logger = logging.getLogger(__name__)

# Venue clocks drift by a little; tolerate that before calling a ticker future dated.
_MAX_TICKER_CLOCK_SKEW_SECONDS = 2.0

# Repair attempts for a venue that runs out of fresh tickers while the snapshot
# is being assembled. Bounded so a struggling venue cannot stall the cycle.
_MAX_TICKER_REPAIR_ROUNDS = 2

CanonicalBookEventSink = Callable[[BookEvent], Awaitable[None]]
CanonicalOptionEventSink = Callable[
    [EventEnvelope[OptionQuoteSnapshot]], Awaitable[None]
]


class _CanonicalBookPublicationError(RuntimeError):
    """A fresh REST order book could not reach the durable canonical boundary."""


@dataclass(frozen=True)
class MarketSnapshot:
    """In-memory normalized snapshot used by scanner and paper components."""

    instruments: list[NormalizedInstrument]
    tickers: list[Ticker]
    funding: list[FundingSnapshot]
    orderbooks: dict[tuple[str, str, InstrumentType], OrderBook]
    captured_at: datetime
    funding_history: dict[tuple[str, str], list[FundingHistoryPoint]] | None = None
    stale_after_seconds: int = 30
    incomplete_venues: tuple[str, ...] = ()
    funding_history_refreshed: dict[tuple[str, str], datetime] = field(default_factory=dict)
    option_quotes: tuple[OptionQuoteSnapshot, ...] = ()
    _ticker_index: dict[tuple[str, str, InstrumentType], Ticker] = field(
        init=False, repr=False, compare=False
    )
    _funding_index: dict[tuple[str, str], FundingSnapshot] = field(
        init=False, repr=False, compare=False
    )
    _instrument_index: dict[
        tuple[str, str, InstrumentType], NormalizedInstrument
    ] = field(
        init=False, repr=False, compare=False
    )
    _option_index: dict[str, OptionQuoteSnapshot] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_ticker_index",
            {
                (item.exchange, item.symbol, item.instrument_type): item
                for item in self.tickers
            },
        )
        object.__setattr__(
            self,
            "_funding_index",
            {(item.exchange, item.symbol): item for item in self.funding},
        )
        object.__setattr__(
            self,
            "_instrument_index",
            {
                (item.exchange, item.exchange_symbol, item.instrument_type): item
                for item in self.instruments
            },
        )
        object.__setattr__(
            self,
            "_option_index",
            {
                item.instrument.canonical_id: item
                for item in self.option_quotes
            },
        )

    def ticker(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> Ticker | None:
        return self._ticker_index.get((exchange, symbol, instrument_type))

    def funding_rate(self, exchange: str, symbol: str) -> FundingSnapshot | None:
        return self._funding_index.get((exchange, symbol))

    def instrument(
        self,
        exchange: str,
        symbol: str,
        instrument_type: InstrumentType | None = None,
    ) -> NormalizedInstrument | None:
        if instrument_type is not None:
            return self._instrument_index.get((exchange, symbol, instrument_type))
        matches = [
            instrument
            for (venue, venue_symbol, _kind), instrument in self._instrument_index.items()
            if venue == exchange and venue_symbol == symbol
        ]
        return matches[0] if len(matches) == 1 else None

    def orderbook(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> OrderBook | None:
        return self.orderbooks.get((exchange, symbol, instrument_type))

    def option_quote(self, instrument: InstrumentKey) -> OptionQuoteSnapshot | None:
        return self._option_index.get(instrument.canonical_id)


@dataclass(frozen=True)
class _VenueCollection:
    instruments: list[NormalizedInstrument]
    tickers: list[Ticker]
    funding: list[FundingSnapshot]
    orderbooks: dict[tuple[str, str, InstrumentType], OrderBook]
    funding_history: dict[tuple[str, str], list[FundingHistoryPoint]]
    operationally_complete: bool
    funding_history_refreshed: dict[tuple[str, str], datetime] = field(
        default_factory=dict
    )
    option_quotes: tuple[OptionQuoteSnapshot, ...] = ()
    # Completeness of everything the snapshot-boundary ticker refresh cannot
    # re-verify, so that refresh can restore a venue instead of only demoting it.
    non_ticker_complete: bool = False


class MarketDataCollector:
    def __init__(
        self,
        adapters: Iterable[ExchangeAdapter],
        orderbook_symbol_limit: int = 20,
        market_asset_limit: int | None = None,
        history_symbol_limit: int = 20,
        stale_after_seconds: int = 30,
        enable_streams: bool = False,
        rest_validation_seconds: int = 60,
        *,
        funding_stale_after_seconds: int | None = None,
        book_stale_after_seconds: int | None = None,
        option_assets: Iterable[str] = (),
        option_refresh_seconds: float = 5.0,
        option_maximum_expiries: int = 2,
        option_strikes_per_expiry: int = 3,
        canonical_book_event_sink: CanonicalBookEventSink | None = None,
        canonical_option_event_sink: CanonicalOptionEventSink | None = None,
        canonical_book_snapshot_from_selected: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.adapters = tuple(adapters)
        if orderbook_symbol_limit <= 0:
            raise ValueError("orderbook_symbol_limit must be positive")
        if market_asset_limit is not None and market_asset_limit <= 0:
            raise ValueError("market_asset_limit must be positive")
        if history_symbol_limit <= 0:
            raise ValueError("history_symbol_limit must be positive")
        if option_refresh_seconds <= 0:
            raise ValueError("option_refresh_seconds must be positive")
        if option_maximum_expiries <= 0 or option_strikes_per_expiry <= 0:
            raise ValueError("option chain bounds must be positive")
        self.orderbook_symbol_limit = orderbook_symbol_limit
        self.market_asset_limit = market_asset_limit
        self.history_symbol_limit = history_symbol_limit
        self.stale_after_seconds = stale_after_seconds
        # A funding rate is published on a venue schedule measured in hours, so it
        # carries its own, far longer budget. Holding it to the market-data budget
        # re-fetches every venue's funding page several times a minute for nothing.
        self.funding_stale_after_seconds = (
            stale_after_seconds
            if funding_stale_after_seconds is None
            else funding_stale_after_seconds
        )
        # Refreshing only once the budget has already lapsed leaves the page
        # sitting on the ceiling, and the rest of the pass tips it over. Half the
        # budget keeps a funding page comfortably inside it.
        self.funding_revalidation_seconds = max(
            1, self.funding_stale_after_seconds // 2
        )
        # An order book streams continuously and carries its own budget too.
        # Validating one against the ticker budget spends REST calls on books the
        # contract still considers tradeable, and books dominate the pass.
        self.book_stale_after_seconds = (
            stale_after_seconds
            if book_stale_after_seconds is None
            else book_stale_after_seconds
        )
        self.book_revalidation_seconds = max(1, self.book_stale_after_seconds // 2)
        # A book enters the snapshot from the venue's own stage, which is seconds
        # before the snapshot closes, so accepting one right on the budget lets it
        # reach readiness over the budget. Keep the same margin the tickers use.
        self.book_usable_seconds = max(
            1.0,
            self.book_stale_after_seconds
            - max(1.0, self.book_stale_after_seconds / 6),
        )
        self.enable_streams = enable_streams
        # A cached page has to still be inside the staleness budget when the
        # snapshot closes, so the configured interval is an upper bound only.
        # Half the budget leaves room for the pass itself; the boundary repair
        # then stays the exception rather than every venue, every cycle.
        self.rest_validation_seconds = min(
            rest_validation_seconds, max(1, stale_after_seconds // 2)
        )
        self.option_assets = tuple(
            dict.fromkeys(
                asset.strip().upper() for asset in option_assets if asset.strip()
            )
        )
        self.option_refresh_seconds = option_refresh_seconds
        self.option_maximum_expiries = option_maximum_expiries
        self.option_strikes_per_expiry = option_strikes_per_expiry
        self.canonical_book_event_sink = canonical_book_event_sink
        self.canonical_option_event_sink = canonical_option_event_sink
        self.canonical_book_snapshot_from_selected = (
            canonical_book_snapshot_from_selected
        )
        self._clock = clock
        self.health = {adapter.name: CircuitBreaker() for adapter in self.adapters}
        self._funding_history_cache: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        self._instrument_cache: dict[str, list[NormalizedInstrument]] = {}
        self._rest_ticker_cache: dict[str, list[Ticker]] = {}
        self._funding_cache: dict[str, list[FundingSnapshot]] = {}
        self._last_rest_ticker_fetch: dict[str, datetime] = {}
        self._last_funding_fetch: dict[str, datetime] = {}
        self._option_quote_cache: dict[str, tuple[OptionQuoteSnapshot, ...]] = {}
        self._last_option_fetch: dict[str, datetime] = {}
        self._stream_ticker_cache: dict[tuple[str, str, InstrumentType], Ticker] = {}
        self._stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._stream_ticker_requests: dict[
            str, frozenset[tuple[str, InstrumentType]]
        ] = {}
        self._stream_orderbook_cache: dict[
            tuple[str, str, InstrumentType], OrderBook
        ] = {}
        self._orderbook_stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._orderbook_stream_requests: dict[
            str, frozenset[tuple[str, InstrumentType]]
        ] = {}
        self._last_rest_book_fetch: dict[
            tuple[str, str, InstrumentType], datetime
        ] = {}

    async def close(self) -> None:
        tasks = [*self._stream_tasks.values(), *self._orderbook_stream_tasks.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._stream_tasks.clear()
        self._stream_ticker_requests.clear()
        self._orderbook_stream_tasks.clear()
        self._orderbook_stream_requests.clear()
        for adapter in self.adapters:
            exchange_stream_last_message_timestamp.labels(
                adapter.name, "ticker"
            ).set(0)
            exchange_stream_last_message_timestamp.labels(
                adapter.name, "orderbook"
            ).set(0)

    def seed_funding_history(
        self, history: dict[tuple[str, str], list[FundingHistoryPoint]]
    ) -> None:
        """Warm the restart-local cache from the durable public-data ledger."""

        self._funding_history_cache.update(
            {
                key: sorted(points, key=lambda point: point.funding_timestamp)
                for key, points in history.items()
            }
        )

    async def collect_once(
        self,
        orderbook_symbols: dict[str, list[tuple[str, InstrumentType]]] | None = None,
        include_history: bool = False,
        history_symbols: dict[str, list[str]] | None = None,
        force_history_refresh: bool = True,
        force_history_symbols: dict[str, list[str]] | None = None,
        discovery_orderbook_symbols: dict[
            str, list[tuple[str, InstrumentType]]
        ]
        | None = None,
        discovery_history_symbols: dict[str, list[str]] | None = None,
    ) -> MarketSnapshot:
        bounded_discovery_books = {
            venue: list(dict.fromkeys(markets))[: self.orderbook_symbol_limit]
            for venue, markets in (discovery_orderbook_symbols or {}).items()
        }
        bounded_discovery_history = {
            venue: list(dict.fromkeys(symbols))[: self.history_symbol_limit]
            for venue, symbols in (discovery_history_symbols or {}).items()
        }
        active_adapters = tuple(
            adapter
            for adapter in self.adapters
            if self.health[adapter.name].can_attempt()
        )
        collections = await asyncio.gather(
            *(
                self._collect_venue(
                    adapter,
                    (orderbook_symbols or {}).get(adapter.name),
                    bounded_discovery_books.get(adapter.name),
                    include_history,
                    (history_symbols or {}).get(adapter.name, []),
                    bounded_discovery_history.get(adapter.name, []),
                    force_history_refresh,
                    (force_history_symbols or {}).get(adapter.name, []),
                )
                for adapter in active_adapters
            )
        )
        with market_data_phase_duration_seconds.labels(
            "_boundary", "funding_refresh"
        ).time():
            collections = await self._refresh_funding_aged_during_collection(
                active_adapters, list(collections)
            )
        boundary_started = time.monotonic()
        collections, captured_at = await self._refresh_required_tickers_aged_during_collection(
            active_adapters,
            collections,
            orderbook_symbols or {},
            bounded_discovery_books,
        )
        market_data_phase_duration_seconds.labels(
            "_boundary", "ticker_settle"
        ).observe(time.monotonic() - boundary_started)
        instruments = [item for result in collections for item in result.instruments]
        tickers = [item for result in collections for item in result.tickers]
        funding = [item for result in collections for item in result.funding]
        orderbooks = {
            key: book for result in collections for key, book in result.orderbooks.items()
        }
        funding_history = {
            key: points
            for result in collections
            for key, points in result.funding_history.items()
        }
        funding_history_refreshed = dict(
            item
            for result in collections
            for item in result.funding_history_refreshed.items()
        )
        option_quotes = tuple(
            item for result in collections for item in result.option_quotes
        )
        collection_by_venue = {
            adapter.name: result
            for adapter, result in zip(active_adapters, collections, strict=True)
        }
        incomplete_venues = tuple(
            sorted(
                adapter.name
                for adapter in self.adapters
                if not collection_by_venue.get(adapter.name)
                or not collection_by_venue[adapter.name].operationally_complete
            )
        )
        self._funding_history_cache.update(funding_history)
        active_history_keys = {(item.exchange, item.symbol) for item in funding}
        active_venues = {adapter.name for adapter in active_adapters}
        for key in tuple(self._funding_history_cache):
            if key[0] in active_venues and key not in active_history_keys:
                self._funding_history_cache.pop(key, None)
        return MarketSnapshot(
            instruments=instruments,
            tickers=tickers,
            funding=funding,
            orderbooks=orderbooks,
            captured_at=captured_at,
            funding_history={**self._funding_history_cache, **funding_history},
            stale_after_seconds=self.stale_after_seconds,
            incomplete_venues=incomplete_venues,
            funding_history_refreshed=funding_history_refreshed,
            option_quotes=option_quotes,
        )

    async def _refresh_funding_aged_during_collection(
        self,
        adapters: tuple[ExchangeAdapter, ...],
        collections: list[_VenueCollection],
    ) -> list[_VenueCollection]:
        """Refresh venue funding that became stale while slow books/history loaded."""

        # Look past the rest of the pass, so a page that is merely close to the
        # budget now cannot reach the snapshot over it.
        horizon = datetime.now(UTC) + timedelta(
            seconds=max(1.0, self.funding_stale_after_seconds / 6)
        )
        stale_indexes = [
            index
            for index, result in enumerate(collections)
            if result.funding
            and any(
                (horizon - item.timestamp).total_seconds()
                > self.funding_stale_after_seconds
                for item in result.funding
            )
        ]
        if not stale_indexes:
            return collections
        refreshed = await asyncio.gather(
            *(adapters[index].get_funding_rates() for index in stale_indexes),
            return_exceptions=True,
        )
        refreshed_at = datetime.now(UTC)
        for index, value in zip(stale_indexes, refreshed, strict=True):
            adapter = adapters[index]
            if not isinstance(value, list):
                current = collections[index]
                collections[index] = _VenueCollection(
                    current.instruments,
                    current.tickers,
                    current.funding,
                    current.orderbooks,
                    current.funding_history,
                    False,
                    current.funding_history_refreshed,
                    current.option_quotes,
                    False,
                )
                logger.warning(
                    "stale_funding_refresh_failed",
                    extra={
                        "exchange": adapter.name,
                        "event": "market_data_validation",
                        "error": str(value),
                    },
                )
                continue
            current = collections[index]
            selected_keys = {
                (item.exchange, item.symbol) for item in current.funding
            }
            normalized = [
                item.model_copy(update={"timestamp": refreshed_at})
                for item in value
                if (item.exchange, item.symbol) in selected_keys
            ]
            collections[index] = _VenueCollection(
                current.instruments,
                current.tickers,
                normalized,
                current.orderbooks,
                current.funding_history,
                current.operationally_complete,
                current.funding_history_refreshed,
                current.option_quotes,
                current.non_ticker_complete,
            )
            self._funding_cache[adapter.name] = value
            self._last_funding_fetch[adapter.name] = refreshed_at
        return collections

    async def _refresh_required_tickers_aged_during_collection(
        self,
        adapters: tuple[ExchangeAdapter, ...],
        collections: list[_VenueCollection],
        required_books: dict[str, list[tuple[str, InstrumentType]]],
        pinned_discovery_books: dict[str, list[tuple[str, InstrumentType]]],
    ) -> tuple[list[_VenueCollection], datetime]:
        """Age every ticker against the instant the snapshot closes.

        Venues are collected concurrently but the whole pass takes seconds, so a
        ticker that was inside the budget when its venue was collected can be
        outside it by the time the snapshot closes. Readiness measures every age
        against the snapshot timestamp, so the collector settles freshness against
        that same instant and returns it as the snapshot's `captured_at`.
        """

        # Look past the re-fetch itself: a venue merely close to the budget now
        # would be over it by the time the snapshot closes.
        lookahead = max(1.0, self.stale_after_seconds / 6)
        stale_indexes = self._age_tickers(
            adapters,
            collections,
            required_books,
            datetime.now(UTC),
            lookahead_seconds=lookahead,
        )
        for _ in range(_MAX_TICKER_REPAIR_ROUNDS):
            if not stale_indexes:
                break
            await self._refetch_venue_tickers(
                adapters,
                collections,
                stale_indexes,
                required_books,
                pinned_discovery_books,
            )
            # Re-age straight after the fetch, with no further I/O in between,
            # so a venue that is still empty gets one more chance rather than
            # reaching the snapshot with nothing.
            stale_indexes = self._age_tickers(
                adapters,
                collections,
                required_books,
                datetime.now(UTC),
                lookahead_seconds=lookahead,
            )
        # The snapshot closes now, so settle every venue against this instant:
        # readiness measures ages against `captured_at`, not against when this
        # pass started.
        captured_at = datetime.now(UTC)
        self._age_tickers(adapters, collections, required_books, captured_at)
        for adapter, current in zip(adapters, collections, strict=True):
            market_data_age_seconds.labels(adapter.name).set(
                0 if current.operationally_complete else -1
            )
        return collections, captured_at

    async def _refetch_venue_tickers(
        self,
        adapters: tuple[ExchangeAdapter, ...],
        collections: list[_VenueCollection],
        stale_indexes: list[int],
        required_books: dict[str, list[tuple[str, InstrumentType]]],
        pinned_discovery_books: dict[str, list[tuple[str, InstrumentType]]],
    ) -> None:
        """Re-fetch one bulk ticker page for each venue that ran out of fresh data."""

        if stale_indexes:
            market_data_phase_duration_seconds.labels(
                "_boundary", "repair_rounds"
            ).observe(len(stale_indexes))
            refreshed = await asyncio.gather(
                *(adapters[index].get_tickers() for index in stale_indexes),
                return_exceptions=True,
            )
            refreshed_at = datetime.now(UTC)
            for index, value in zip(stale_indexes, refreshed, strict=True):
                adapter = adapters[index]
                current = collections[index]
                if not isinstance(value, list):
                    collections[index] = _VenueCollection(
                        current.instruments,
                        current.tickers,
                        current.funding,
                        current.orderbooks,
                        current.funding_history,
                        False,
                        current.funding_history_refreshed,
                        current.option_quotes,
                        current.non_ticker_complete,
                    )
                    logger.warning(
                        "stale_required_ticker_refresh_failed",
                        extra={
                            "exchange": adapter.name,
                            "event": "market_data_validation",
                            "error": str(value),
                        },
                    )
                    continue
                valid_tickers = self._usable_tickers(
                    adapter.name,
                    self._merge_stream_tickers(adapter.name, value, refreshed_at),
                    refreshed_at,
                )
                venue_instruments = current.instruments
                venue_funding = current.funding
                if self.market_asset_limit is not None:
                    pinned_markets = set(
                        [
                            *required_books.get(adapter.name, ()),
                            *pinned_discovery_books.get(adapter.name, ()),
                        ]
                    )
                    venue_instruments, valid_tickers, venue_funding = (
                        _limit_venue_universe(
                            venue_instruments,
                            valid_tickers,
                            venue_funding,
                            self.market_asset_limit,
                            required_markets=pinned_markets,
                        )
                    )
                operationally_complete = (
                    current.non_ticker_complete
                    and bool(valid_tickers)
                    and _required_tickers_are_fresh(
                        adapter.name,
                        valid_tickers,
                        required_books.get(adapter.name),
                        refreshed_at,
                        self.stale_after_seconds,
                    )
                )
                collections[index] = _VenueCollection(
                    venue_instruments,
                    valid_tickers,
                    venue_funding,
                    current.orderbooks,
                    current.funding_history,
                    operationally_complete,
                    current.funding_history_refreshed,
                    current.option_quotes,
                    current.non_ticker_complete,
                )
                self._rest_ticker_cache[adapter.name] = value
                self._last_rest_ticker_fetch[adapter.name] = refreshed_at
                market_tickers_usable.labels(adapter.name).set(len(valid_tickers))

    def _age_tickers(
        self,
        adapters: tuple[ExchangeAdapter, ...],
        collections: list[_VenueCollection],
        required_books: dict[str, list[tuple[str, InstrumentType]]],
        moment: datetime,
        *,
        lookahead_seconds: float = 0.0,
    ) -> list[int]:
        """Re-age every venue's tickers against one instant, in place.

        Returns the venues that hold no usable ticker, or whose open-position
        mark is stale, at `moment` plus `lookahead_seconds`. The tickers kept are
        always the ones usable at `moment`; the look-ahead only decides who is
        worth re-fetching before the snapshot closes.
        """

        horizon = moment + timedelta(seconds=lookahead_seconds)
        stale_indexes: list[int] = []
        for index, (adapter, current) in enumerate(
            zip(adapters, collections, strict=True)
        ):
            merged = self._usable_tickers(
                adapter.name,
                self._merge_stream_tickers(adapter.name, current.tickers, moment),
                moment,
            )
            surviving = [
                ticker
                for ticker in merged
                if (horizon - ticker.timestamp).total_seconds()
                <= self.stale_after_seconds
            ]
            required_fresh = _required_tickers_are_fresh(
                adapter.name,
                surviving,
                required_books.get(adapter.name),
                horizon,
                self.stale_after_seconds,
            )
            collections[index] = _VenueCollection(
                current.instruments,
                merged,
                current.funding,
                current.orderbooks,
                current.funding_history,
                current.non_ticker_complete and bool(merged) and required_fresh,
                current.funding_history_refreshed,
                current.option_quotes,
                current.non_ticker_complete,
            )
            if not surviving or not required_fresh:
                stale_indexes.append(index)
        return stale_indexes

    async def _collect_venue(
        self,
        adapter: ExchangeAdapter,
        requested_books: list[tuple[str, InstrumentType]] | None,
        pinned_discovery_books: list[tuple[str, InstrumentType]] | None,
        include_history: bool,
        required_history: list[str],
        discovery_history: list[str],
        force_history_refresh: bool,
        forced_history: list[str],
    ) -> _VenueCollection:
        breaker = self.health[adapter.name]
        orderbooks: dict[tuple[str, str, InstrumentType], OrderBook] = {}
        funding_history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        funding_history_refreshed: dict[tuple[str, str], datetime] = {}
        history_complete = True
        try:
            requested_books = list(dict.fromkeys(requested_books or ()))
            pinned_discovery_books = list(dict.fromkeys(pinned_discovery_books or ()))[
                : self.orderbook_symbol_limit
            ]
            required_history = list(
                dict.fromkeys([*required_history, *forced_history])
            )
            discovery_history = list(dict.fromkeys(discovery_history))[
                : self.history_symbol_limit
            ]
            venue_instruments = self._instrument_cache.get(adapter.name)
            if venue_instruments is None:
                venue_instruments = await adapter.get_instruments()
                self._instrument_cache[adapter.name] = venue_instruments
            now = datetime.now(UTC)
            cached_tickers = self._rest_ticker_cache.get(adapter.name)
            last_ticker_fetch = self._last_rest_ticker_fetch.get(adapter.name)
            refresh_tickers = (
                not self.enable_streams
                or cached_tickers is None
                or last_ticker_fetch is None
                or (now - last_ticker_fetch).total_seconds() >= self.rest_validation_seconds
            )
            cached_funding = self._funding_cache.get(adapter.name)
            last_funding_fetch = self._last_funding_fetch.get(adapter.name)
            refresh_funding = (
                cached_funding is None
                or last_funding_fetch is None
                or (now - last_funding_fetch).total_seconds()
                >= self.funding_revalidation_seconds
            )
            market_started = time.monotonic()
            if refresh_tickers and refresh_funding:
                venue_tickers, all_venue_funding = await asyncio.gather(
                    self._load_tickers_with_stream_fallback(
                        adapter, cached_tickers, now
                    ),
                    adapter.get_funding_rates(),
                )
                self._funding_cache[adapter.name] = all_venue_funding
                self._last_funding_fetch[adapter.name] = now
            elif refresh_tickers:
                venue_tickers = await self._load_tickers_with_stream_fallback(
                    adapter, cached_tickers, now
                )
                all_venue_funding = cached_funding or []
            elif refresh_funding:
                venue_tickers = cached_tickers or []
                all_venue_funding = await adapter.get_funding_rates()
                self._funding_cache[adapter.name] = all_venue_funding
                self._last_funding_fetch[adapter.name] = now
            else:
                venue_tickers = cached_tickers or []
                all_venue_funding = cached_funding or []
            market_data_phase_duration_seconds.labels(
                adapter.name, "tickers_funding"
            ).observe(time.monotonic() - market_started)
            # The fetch above took time, and several venues stamp a ticker when
            # it is parsed rather than from venue data. Ageing those against a
            # reference taken before the fetch makes them look future dated, and
            # a fetch slower than the clock-skew tolerance loses the whole page.
            now = datetime.now(UTC)
            venue_tickers = self._merge_stream_tickers(adapter.name, venue_tickers, now)
            valid_tickers = self._usable_tickers(adapter.name, venue_tickers, now)
            if not valid_tickers and not refresh_tickers:
                # The cached page expired before this pass reached the venue. The
                # rest of the pass ranks the tradeable universe and the order books
                # off these tickers, so carrying on with none would cost the venue
                # its books for the cycle, and the snapshot boundary only repairs
                # tickers.
                now = datetime.now(UTC)
                venue_tickers = self._merge_stream_tickers(
                    adapter.name,
                    await self._load_tickers_with_stream_fallback(
                        adapter, cached_tickers, now
                    ),
                    now,
                )
                valid_tickers = self._usable_tickers(adapter.name, venue_tickers, now)
            venue_funding = all_venue_funding
            pinned_markets = set(
                [
                    *(requested_books or ()),
                    *(pinned_discovery_books or ()),
                ]
            )
            if self.market_asset_limit is not None and valid_tickers:
                # With no usable ticker there is nothing to rank, and letting the
                # limiter run would take the venue's funding and instruments down
                # with it — leaving the snapshot boundary nothing to repair from.
                venue_instruments, valid_tickers, venue_funding = _limit_venue_universe(
                    venue_instruments,
                    valid_tickers,
                    venue_funding,
                    self.market_asset_limit,
                    required_markets=pinned_markets,
                )
            if valid_tickers:
                # A venue can publish one page-wide timestamp, so a lagging
                # snapshot makes every ticker stale at once. Re-subscribing to
                # nothing would tear down the stream that covers exactly that
                # case, and the markets we want have not changed.
                self._ensure_ticker_stream(adapter, valid_tickers)
            ranked_discovery_books = _rank_orderbook_requests(
                valid_tickers, venue_funding, venue_instruments
            )
            discovery_book_requests = list(
                dict.fromkeys([*pinned_discovery_books, *ranked_discovery_books])
            )[: self.orderbook_symbol_limit]
            book_requests = list(
                dict.fromkeys(
                    [
                        *requested_books,
                        *discovery_book_requests,
                    ]
                )
            )
            self._ensure_orderbook_stream(adapter, book_requests)
            for symbol, instrument_type in book_requests:
                key = (adapter.name, symbol, instrument_type)
                streamed = self._stream_orderbook_cache.get(key)
                if (
                    streamed is not None
                    and (now - streamed.timestamp).total_seconds()
                    <= self.book_usable_seconds
                ):
                    orderbooks[key] = streamed
            rest_book_requests = [
                request
                for request in book_requests
                if self._book_needs_rest_validation(adapter.name, request, now)
            ]
            with market_data_phase_duration_seconds.labels(
                adapter.name, "orderbooks"
            ).time():
                book_results = await asyncio.gather(
                    *(
                        adapter.get_orderbook(symbol, 20, instrument_type)
                        for symbol, instrument_type in rest_book_requests
                    ),
                    return_exceptions=True,
                )
            accepted_rest_books: list[OrderBook] = []
            for (symbol, instrument_type), book_result in zip(
                rest_book_requests, book_results, strict=True
            ):
                if not isinstance(book_result, OrderBook):
                    market_data_dropped_total.labels(
                        adapter.name, "orderbook_fetch_error"
                    ).inc()
                    logger.warning(
                        "orderbook_fetch_failed",
                        extra={
                            "exchange": adapter.name,
                            "symbol": symbol,
                            "instrument_type": instrument_type.value,
                            "error_type": type(book_result).__name__,
                        },
                    )
                    continue
                key = (adapter.name, symbol, instrument_type)
                self._last_rest_book_fetch[key] = now
                current = orderbooks.get(key)
                latest_streamed = self._stream_orderbook_cache.get(key)
                if (
                    latest_streamed is not None
                    and (
                        current is None
                        or latest_streamed.timestamp > current.timestamp
                    )
                ):
                    current = latest_streamed
                    orderbooks[key] = latest_streamed
                if current is None or book_result.timestamp >= current.timestamp:
                    orderbooks[key] = book_result
                    accepted_rest_books.append(book_result)
            books_to_publish = accepted_rest_books
            book_event_source = (
                f"{adapter.name.upper()}.PUBLIC.ORDERBOOK.REST_VALIDATION"
            )
            if self.canonical_book_snapshot_from_selected:
                books_to_publish = [
                    book
                    for symbol, instrument_type in book_requests
                    if (
                        book := orderbooks.get(
                            (adapter.name, symbol, instrument_type)
                        )
                    )
                    is not None
                ]
                book_event_source = (
                    f"{adapter.name.upper()}.PUBLIC.ORDERBOOK.COLLECTOR_SNAPSHOT"
                )
            publish_started = time.monotonic()
            await self._publish_rest_book_events(
                adapter.name,
                venue_instruments,
                books_to_publish,
                source=book_event_source,
            )
            market_data_phase_duration_seconds.labels(
                adapter.name, "canonical_publish"
            ).observe(time.monotonic() - publish_started)
            orderbook_coverage_ratio.labels(adapter.name).set(
                len(orderbooks) / len(book_requests) if book_requests else 0
            )
            stale_or_missing_orderbooks.labels(adapter.name).set(
                max(0, len(book_requests) - len(orderbooks))
            )
            history_started = time.monotonic()
            if include_history:
                end = datetime.now(UTC)
                start = end - timedelta(days=30)
                ranked = _rank_funding_symbols(
                    venue_funding, valid_tickers, venue_instruments
                )
                discovery_selected = list(
                    dict.fromkeys([*discovery_history, *ranked])
                )[: self.history_symbol_limit]
                selected = list(
                    dict.fromkeys([*required_history, *discovery_selected])
                )
                valid_symbols = {item.symbol for item in all_venue_funding}
                selected = [
                    symbol for symbol in selected
                    if symbol in valid_symbols or symbol in forced_history
                ]
                symbols_to_fetch = [
                    symbol
                    for symbol in selected
                    if force_history_refresh
                    or symbol in forced_history
                    or (adapter.name, symbol) not in self._funding_history_cache
                ]
                history_results = await asyncio.gather(
                    *(
                        adapter.get_funding_history(symbol, start, end)
                        for symbol in symbols_to_fetch
                    ),
                    return_exceptions=True,
                )
                for symbol, history_result in zip(
                    symbols_to_fetch, history_results, strict=True
                ):
                    if isinstance(history_result, list):
                        funding_history[(adapter.name, symbol)] = sorted(
                            history_result,
                            key=lambda point: point.funding_timestamp,
                        )
                        funding_history_refreshed[(adapter.name, symbol)] = end
                covered = {
                    symbol
                    for symbol in selected
                    if (adapter.name, symbol) in self._funding_history_cache
                    or (adapter.name, symbol) in funding_history
                }
                funding_history_coverage_ratio.labels(adapter.name).set(
                    len(covered) / len(selected) if selected else 0
                )
                history_complete = len(covered) == len(selected)
            market_data_phase_duration_seconds.labels(
                adapter.name, "funding_history"
            ).observe(time.monotonic() - history_started)
            with market_data_phase_duration_seconds.labels(
                adapter.name, "options"
            ).time():
                option_quotes = await self._load_option_quotes(adapter, self._clock())
            non_ticker_complete = (
                bool(venue_funding)
                and all(
                    (adapter.name, symbol, instrument_type) in orderbooks
                    for symbol, instrument_type in requested_books or ()
                )
                and history_complete
            )
            operationally_complete = bool(valid_tickers) and non_ticker_complete
            if not operationally_complete:
                logger.warning(
                    "venue_incomplete",
                    extra={
                        "exchange": adapter.name,
                        "event": "market_data_collection",
                        "error": (
                            f"tickers={len(valid_tickers)} funding={len(venue_funding)} "
                            f"books={len(orderbooks)} requested={len(requested_books or ())} "
                            f"history_complete={history_complete}"
                        ),
                    },
                )
            breaker.record_success()
            market_data_age_seconds.labels(adapter.name).set(
                0 if operationally_complete else -1
            )
            return _VenueCollection(
                venue_instruments,
                valid_tickers,
                venue_funding,
                orderbooks,
                funding_history,
                operationally_complete,
                funding_history_refreshed,
                option_quotes,
                non_ticker_complete,
            )
        except _CanonicalBookPublicationError:
            raise
        except Exception:
            breaker.record_failure()
            market_data_age_seconds.labels(adapter.name).set(-1)
            stale_or_missing_orderbooks.labels(adapter.name).set(
                len(requested_books or ())
            )
            logger.exception(
                "market_data_venue_collection_failed",
                extra={"exchange": adapter.name, "event": "market_data_collection"},
            )
            return _VenueCollection([], [], [], {}, {}, False)

    async def _load_option_quotes(
        self,
        adapter: ExchangeAdapter,
        now: datetime,
    ) -> tuple[OptionQuoteSnapshot, ...]:
        if not self.option_assets:
            return ()
        cached = self._option_quote_cache.get(adapter.name, ())
        last_fetch = self._last_option_fetch.get(adapter.name)
        if (
            last_fetch is not None
            and (now - last_fetch).total_seconds() < self.option_refresh_seconds
        ):
            return _fresh_option_quotes(cached, now, self.stale_after_seconds)
        try:
            raw_quotes = await adapter.get_option_chain(self.option_assets)
            observed_at = max(now, self._clock())
            bounded = bounded_option_chain(
                raw_quotes,
                as_of=observed_at,
                maximum_expiries=self.option_maximum_expiries,
                strikes_per_expiry=self.option_strikes_per_expiry,
            )
            identities: set[str] = set()
            valid: list[OptionQuoteSnapshot] = []
            for quote in bounded:
                identity = quote.instrument.canonical_id
                age = (observed_at - quote.exchange_timestamp).total_seconds()
                if (
                    quote.instrument.venue.lower() != adapter.name.lower()
                    or identity in identities
                    or age < 0
                    or age > self.stale_after_seconds
                ):
                    continue
                identities.add(identity)
                valid.append(quote)
            quotes = tuple(valid)
            await self._publish_option_quote_events(
                adapter.name,
                quotes,
                observed_at,
            )
        except Exception:
            failed_at = max(now, self._clock())
            logger.warning(
                "option_chain_refresh_failed",
                extra={
                    "exchange": adapter.name,
                    "event": "option_market_data_recovery",
                },
                exc_info=True,
            )
            return _fresh_option_quotes(cached, failed_at, self.stale_after_seconds)
        self._option_quote_cache[adapter.name] = quotes
        self._last_option_fetch[adapter.name] = observed_at
        return quotes

    async def _publish_option_quote_events(
        self,
        exchange: str,
        quotes: tuple[OptionQuoteSnapshot, ...],
        observed_at: datetime,
    ) -> None:
        sink = self.canonical_option_event_sink
        if sink is None or not quotes:
            return
        events = tuple(
            canonical_option_quote_event(
                quote,
                source=f"{exchange}.PUBLIC.OPTION.REST",
                receive_timestamp=observed_at,
            )
            for quote in quotes
        )
        for event in events:
            await sink(event)

    async def _publish_rest_book_events(
        self,
        exchange: str,
        instruments: list[NormalizedInstrument],
        books: list[OrderBook],
        *,
        source: str,
    ) -> None:
        sink = self.canonical_book_event_sink
        if sink is None or not books:
            return
        instrument_by_market = {
            (instrument.exchange_symbol, instrument.instrument_type): instrument
            for instrument in instruments
        }
        observed_at = datetime.now(UTC)
        events: list[BookEvent] = []
        for book in books:
            instrument = instrument_by_market.get(
                (book.symbol, book.instrument_type)
            )
            if instrument is None:
                market_data_dropped_total.labels(
                    exchange, "incomplete_orderbook_snapshot"
                ).inc()
                logger.warning(
                    "canonical_rest_orderbook_instrument_missing",
                    extra={
                        "exchange": exchange,
                        "symbol": book.symbol,
                        "instrument_type": book.instrument_type.value,
                    },
                )
                continue
            canonical_instrument = InstrumentKey(
                venue=instrument.exchange,
                exchange_symbol=instrument.exchange_symbol,
                base_asset=instrument.base_asset,
                quote_asset=instrument.quote_asset,
                instrument_type=CanonicalInstrumentType(
                    instrument.instrument_type.value
                ),
                settlement_asset=instrument.settlement_asset or None,
                expiry=instrument.expiry,
            )
            try:
                events.append(
                    canonical_snapshot_event(
                        book,
                        canonical_instrument,
                        source=source,
                        receive_timestamp=observed_at,
                    )
                )
            except InvalidResponseError:
                market_data_dropped_total.labels(
                    exchange, "uncanonical_orderbook_snapshot"
                ).inc()
                logger.warning(
                    "canonical_rest_orderbook_rejected",
                    extra={
                        "exchange": exchange,
                        "symbol": book.symbol,
                        "instrument_type": book.instrument_type.value,
                    },
                )
        if not events:
            return
        try:
            await asyncio.gather(*(sink(event) for event in events))
        except Exception as error:
            raise _CanonicalBookPublicationError(
                "fresh REST order-book canonical publication failed"
            ) from error

    async def _load_tickers_with_stream_fallback(
        self,
        adapter: ExchangeAdapter,
        cached_tickers: list[Ticker] | None,
        now: datetime,
    ) -> list[Ticker]:
        """Keep fresh WS data primary when periodic REST validation is unavailable."""

        try:
            tickers = await adapter.get_tickers()
        except Exception:
            has_fresh_stream = any(
                key[0] == adapter.name
                and (now - ticker.timestamp).total_seconds()
                <= self.stale_after_seconds
                for key, ticker in self._stream_ticker_cache.items()
            )
            if not self.enable_streams or not has_fresh_stream:
                raise
            market_data_dropped_total.labels(
                adapter.name, "rest_ticker_validation_error"
            ).inc()
            logger.warning(
                "rest_ticker_validation_failed_using_stream",
                extra={
                    "exchange": adapter.name,
                    "event": "market_data_recovery",
                },
                exc_info=True,
            )
            return cached_tickers or []
        self._rest_ticker_cache[adapter.name] = tickers
        self._last_rest_ticker_fetch[adapter.name] = now
        return tickers

    def _ensure_ticker_stream(
        self, adapter: ExchangeAdapter, tickers: list[Ticker]
    ) -> None:
        if not self.enable_streams:
            return
        symbols = list(
            dict.fromkeys(
                (ticker.symbol, ticker.instrument_type)
                for ticker in tickers
            )
        )
        target = frozenset(symbols)
        self._prune_stream_ticker_cache(adapter.name, target)
        existing = self._stream_tasks.get(adapter.name)
        if (
            existing is not None
            and not existing.done()
            and self._stream_ticker_requests.get(adapter.name) == target
        ):
            return
        if existing is not None and not existing.done():
            existing.cancel()
        self._stream_ticker_requests[adapter.name] = target
        exchange_stream_last_message_timestamp.labels(adapter.name, "ticker").set(0)
        if not target:
            self._stream_tasks.pop(adapter.name, None)
            return
        self._stream_tasks[adapter.name] = asyncio.create_task(
            self._consume_ticker_stream(adapter, symbols),
            name=f"market-tickers-{adapter.name}",
        )

    async def _consume_ticker_stream(
        self,
        adapter: ExchangeAdapter,
        symbols: list[tuple[str, InstrumentType]],
    ) -> None:
        try:
            async for ticker in adapter.stream_tickers(symbols):
                requested = self._stream_ticker_requests.get(adapter.name)
                market = (ticker.symbol, ticker.instrument_type)
                if requested is not None and market not in requested:
                    continue
                if _is_valid_ticker(ticker):
                    self._stream_ticker_cache[
                        (ticker.exchange, ticker.symbol, ticker.instrument_type)
                    ] = ticker
                    exchange_stream_last_message_timestamp.labels(
                        adapter.name, "ticker"
                    ).set(datetime.now(UTC).timestamp())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "market_ticker_stream_failed",
                extra={"exchange": adapter.name, "event": "market_data_stream"},
            )

    def _ensure_orderbook_stream(
        self,
        adapter: ExchangeAdapter,
        requests: list[tuple[str, InstrumentType]],
    ) -> None:
        if not self.enable_streams:
            return
        target = frozenset(requests)
        self._prune_stream_orderbook_cache(adapter.name, target)
        existing = self._orderbook_stream_tasks.get(adapter.name)
        if (
            existing is not None
            and not existing.done()
            and self._orderbook_stream_requests.get(adapter.name) == target
        ):
            return
        if existing is not None and not existing.done():
            existing.cancel()
        self._orderbook_stream_requests[adapter.name] = target
        exchange_stream_last_message_timestamp.labels(
            adapter.name, "orderbook"
        ).set(0)
        if not target:
            self._orderbook_stream_tasks.pop(adapter.name, None)
            return
        self._orderbook_stream_tasks[adapter.name] = asyncio.create_task(
            self._consume_orderbook_stream(adapter, list(target)),
            name=f"market-orderbooks-{adapter.name}",
        )

    async def _consume_orderbook_stream(
        self,
        adapter: ExchangeAdapter,
        requests: list[tuple[str, InstrumentType]],
    ) -> None:
        try:
            async for book in adapter.stream_orderbooks(requests, 20):
                requested = self._orderbook_stream_requests.get(adapter.name)
                market = (book.symbol, book.instrument_type)
                if requested is not None and market not in requested:
                    continue
                self._stream_orderbook_cache[
                    (book.exchange, book.symbol, book.instrument_type)
                ] = book
                exchange_stream_last_message_timestamp.labels(
                    adapter.name, "orderbook"
                ).set(datetime.now(UTC).timestamp())
        except asyncio.CancelledError:
            raise
        except NotImplementedError:
            return
        except Exception:
            market_data_dropped_total.labels(
                adapter.name, "orderbook_stream_error"
            ).inc()
            logger.exception(
                "market_orderbook_stream_failed",
                extra={"exchange": adapter.name, "event": "market_data_stream"},
            )

    def _prune_stream_ticker_cache(
        self,
        exchange: str,
        target: frozenset[tuple[str, InstrumentType]],
    ) -> None:
        for key in tuple(self._stream_ticker_cache):
            if key[0] == exchange and (key[1], key[2]) not in target:
                self._stream_ticker_cache.pop(key, None)

    def _prune_stream_orderbook_cache(
        self,
        exchange: str,
        target: frozenset[tuple[str, InstrumentType]],
    ) -> None:
        for key in tuple(self._stream_orderbook_cache):
            if key[0] == exchange and (key[1], key[2]) not in target:
                self._stream_orderbook_cache.pop(key, None)
        for key in tuple(self._last_rest_book_fetch):
            if key[0] == exchange and (key[1], key[2]) not in target:
                self._last_rest_book_fetch.pop(key, None)

    def _book_needs_rest_validation(
        self,
        exchange: str,
        request: tuple[str, InstrumentType],
        now: datetime,
    ) -> bool:
        key = (exchange, request[0], request[1])
        streamed = self._stream_orderbook_cache.get(key)
        last_rest = self._last_rest_book_fetch.get(key)
        if streamed is None:
            return True
        if (now - streamed.timestamp).total_seconds() > self.book_usable_seconds:
            return True
        return (
            last_rest is None
            or (now - last_rest).total_seconds() >= self.book_revalidation_seconds
        )

    def _usable_tickers(
        self, exchange: str, tickers: list[Ticker], now: datetime
    ) -> list[Ticker]:
        """Keep only tickers a strategy may legitimately price against.

        A venue publishes a last-trade timestamp per market, so a thin market can
        report a price hours old. Such a ticker is not market data any more, and
        carrying it forward would both poison the venue's freshness health and let
        the universe limiter rank a dead market into the tradeable set.
        """

        usable: list[Ticker] = []
        invalid = 0
        stale = 0
        for ticker in tickers:
            if not _is_valid_ticker(ticker):
                invalid += 1
                continue
            age = (now - ticker.timestamp).total_seconds()
            if age > self.stale_after_seconds or age < -_MAX_TICKER_CLOCK_SKEW_SECONDS:
                stale += 1
                continue
            usable.append(ticker)
        if invalid:
            market_data_dropped_total.labels(exchange, "invalid_ticker").inc(invalid)
            logger.warning(
                "invalid_tickers_dropped",
                extra={
                    "exchange": exchange,
                    "event": "market_data_validation",
                    "error": f"dropped={invalid}",
                },
            )
        if stale:
            market_data_dropped_total.labels(exchange, "stale_ticker").inc(stale)
            logger.warning(
                "stale_tickers_dropped",
                extra={
                    "exchange": exchange,
                    "event": "market_data_validation",
                    "error": f"dropped={stale}",
                },
            )
        market_tickers_usable.labels(exchange).set(len(usable))
        return usable

    def _merge_stream_tickers(
        self, exchange: str, rest_tickers: list[Ticker], now: datetime
    ) -> list[Ticker]:
        merged = {
            (ticker.exchange, ticker.symbol, ticker.instrument_type): ticker
            for ticker in rest_tickers
        }
        for key, ticker in self._stream_ticker_cache.items():
            if key[0] != exchange:
                continue
            if (now - ticker.timestamp).total_seconds() <= self.stale_after_seconds:
                current = merged.get(key)
                if current is None:
                    merged[key] = ticker
                else:
                    merged[key] = Ticker(
                        exchange=ticker.exchange,
                        symbol=ticker.symbol,
                        instrument_type=ticker.instrument_type,
                        last_price=ticker.last_price,
                        mark_price=ticker.mark_price or current.mark_price,
                        index_price=ticker.index_price or current.index_price,
                        best_bid=ticker.best_bid or current.best_bid,
                        best_ask=ticker.best_ask or current.best_ask,
                        volume_24h=(
                            ticker.volume_24h
                            if ticker.volume_24h > 0
                            else current.volume_24h
                        ),
                        open_interest=ticker.open_interest or current.open_interest,
                        timestamp=ticker.timestamp,
                    )
        return list(merged.values())


def _is_valid_ticker(ticker: Ticker) -> bool:
    if ticker.last_price <= 0 or ticker.volume_24h < 0:
        return False
    if ticker.best_bid is not None and ticker.best_bid <= 0:
        return False
    if ticker.best_ask is not None and ticker.best_ask <= 0:
        return False
    return not (
        ticker.best_bid is not None
        and ticker.best_ask is not None
        and ticker.best_bid > ticker.best_ask
    )


def _fresh_option_quotes(
    quotes: tuple[OptionQuoteSnapshot, ...],
    now: datetime,
    stale_after_seconds: int,
) -> tuple[OptionQuoteSnapshot, ...]:
    return tuple(
        quote
        for quote in quotes
        if 0
        <= (now - quote.exchange_timestamp).total_seconds()
        <= stale_after_seconds
    )


def _required_tickers_are_fresh(
    exchange: str,
    tickers: list[Ticker],
    required_markets: list[tuple[str, InstrumentType]] | None,
    now: datetime,
    stale_after_seconds: int,
) -> bool:
    ticker_by_market = {
        (ticker.symbol, ticker.instrument_type): ticker
        for ticker in tickers
        if ticker.exchange == exchange
    }
    return all(
        (ticker := ticker_by_market.get((symbol, instrument_type))) is not None
        and (now - ticker.timestamp).total_seconds() <= stale_after_seconds
        for symbol, instrument_type in required_markets or ()
    )


def _limit_venue_universe(
    instruments: list[NormalizedInstrument],
    tickers: list[Ticker],
    funding: list[FundingSnapshot],
    asset_limit: int,
    required_markets: set[tuple[str, InstrumentType]] | None = None,
) -> tuple[list[NormalizedInstrument], list[Ticker], list[FundingSnapshot]]:
    """Keep a liquid universe while pinning markets needed by open positions."""

    instrument_by_symbol = {item.exchange_symbol: item for item in instruments if item.is_active}
    supported_quotes = {"USD", "USDC", "USDT"}
    volume_by_asset: dict[str, Decimal] = {}
    funding_by_asset: dict[str, Decimal] = {}
    for ticker in tickers:
        instrument = instrument_by_symbol.get(ticker.symbol)
        if instrument is None or instrument.quote_asset not in supported_quotes:
            continue
        volume_by_asset[instrument.base_asset] = max(
            volume_by_asset.get(instrument.base_asset, Decimal("0")), ticker.volume_24h
        )
    for item in funding:
        instrument = instrument_by_symbol.get(item.symbol)
        if instrument is None:
            continue
        funding_by_asset[instrument.base_asset] = max(
            funding_by_asset.get(instrument.base_asset, Decimal("0")),
            abs(item.funding_rate_daily),
        )
    popular_rank = {"BTC": 0, "ETH": 1, "SOL": 2}
    # An asset with funding but no usable ticker cannot be priced, so spending a
    # universe slot on it only starves the venue of tradeable markets.
    assets = set(volume_by_asset)
    required_assets = {
        instrument.base_asset
        for symbol, _ in required_markets or ()
        if (instrument := instrument_by_symbol.get(symbol)) is not None
    }
    assets |= required_assets
    volume_rank = {
        asset: index
        for index, asset in enumerate(
            sorted(assets, key=lambda value: volume_by_asset.get(value, Decimal("0")), reverse=True)
        )
    }
    funding_rank = {
        asset: index
        for index, asset in enumerate(
            sorted(
                assets,
                key=lambda value: funding_by_asset.get(value, Decimal("0")),
                reverse=True,
            )
        )
    }
    selected_assets = {
        asset
        for asset in sorted(
            assets,
            key=lambda value: (
                0 if value in popular_rank else 1,
                popular_rank.get(value, 0),
                funding_rank[value] * 2 + volume_rank[value],
                value,
            ),
        )[:asset_limit]
    }
    selected_markets = {
        (item.exchange_symbol, item.instrument_type)
        for item in instruments
        if item.is_active
        and item.base_asset in selected_assets
        and item.quote_asset in supported_quotes
    }
    selected_markets.update(required_markets or ())
    selected_funding_symbols = {
        symbol
        for symbol, instrument_type in selected_markets
        if instrument_type is InstrumentType.PERPETUAL
    }
    return (
        [
            item
            for item in instruments
            if (item.exchange_symbol, item.instrument_type) in selected_markets
        ],
        [
            item
            for item in tickers
            if (item.symbol, item.instrument_type) in selected_markets
        ],
        [item for item in funding if item.symbol in selected_funding_symbols],
    )


def _rank_funding_symbols(
    funding: list[FundingSnapshot],
    tickers: list[Ticker],
    instruments: list[NormalizedInstrument],
) -> list[str]:
    """Prioritize core assets, then liquidity, for the bounded history budget."""

    ticker_by_symbol = {item.symbol: item for item in tickers}
    funding_by_symbol = {item.symbol: item for item in funding}
    instrument_by_symbol = {item.exchange_symbol: item for item in instruments}
    base_by_symbol = {
        symbol: instrument.base_asset for symbol, instrument in instrument_by_symbol.items()
    }
    popular_rank = {"BTC": 0, "ETH": 1, "SOL": 2}
    return sorted(
        {item.symbol for item in funding},
        key=lambda symbol: (
            popular_rank.get(
                base_by_symbol.get(symbol, ""),
                len(popular_rank),
            ),
            -abs(funding_by_symbol[symbol].funding_rate_daily),
            -(
                ticker_by_symbol[symbol].volume_24h
                if symbol in ticker_by_symbol
                else Decimal("0")
            ),
            symbol,
        ),
    )


def _rank_orderbook_requests(
    tickers: list[Ticker],
    funding: list[FundingSnapshot],
    instruments: list[NormalizedInstrument],
) -> list[tuple[str, InstrumentType]]:
    """Rank discovery books by funding potential and liquidity, not volume alone."""

    instrument_by_key = {
        (item.exchange_symbol, item.instrument_type): item
        for item in instruments
        if item.is_active
    }
    funding_by_asset: dict[str, Decimal] = {}
    volume_by_asset: dict[str, Decimal] = {}
    for item in funding:
        instrument = instrument_by_key.get(
            (item.symbol, InstrumentType.PERPETUAL)
        )
        if instrument is not None:
            funding_by_asset[instrument.base_asset] = max(
                funding_by_asset.get(instrument.base_asset, Decimal("0")),
                abs(item.funding_rate_daily),
            )
    for ticker in tickers:
        instrument = instrument_by_key.get((ticker.symbol, ticker.instrument_type))
        if instrument is not None:
            volume_by_asset[instrument.base_asset] = max(
                volume_by_asset.get(instrument.base_asset, Decimal("0")),
                ticker.volume_24h,
            )
    assets = set(funding_by_asset) | set(volume_by_asset)
    funding_rank = {
        asset: index
        for index, asset in enumerate(
            sorted(
                assets,
                key=lambda value: funding_by_asset.get(value, Decimal("0")),
                reverse=True,
            )
        )
    }
    volume_rank = {
        asset: index
        for index, asset in enumerate(
            sorted(
                assets,
                key=lambda value: volume_by_asset.get(value, Decimal("0")),
                reverse=True,
            )
        )
    }
    core_rank = {"BTC": 0, "ETH": 1, "SOL": 2}

    def rank(ticker: Ticker) -> tuple[int, int, int, int, str, str]:
        instrument = instrument_by_key.get((ticker.symbol, ticker.instrument_type))
        asset = instrument.base_asset if instrument is not None else ticker.symbol
        return (
            0 if asset in core_rank else 1,
            core_rank.get(asset, 0),
            funding_rank.get(asset, len(assets)) * 2
            + volume_rank.get(asset, len(assets)),
            0 if ticker.instrument_type is InstrumentType.PERPETUAL else 1,
            asset,
            ticker.symbol,
        )

    return list(
        dict.fromkeys(
            (ticker.symbol, ticker.instrument_type)
            for ticker in sorted(tickers, key=rank)
            if (ticker.symbol, ticker.instrument_type) in instrument_by_key
        )
    )
