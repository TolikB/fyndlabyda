"""Restart-safe market-data collection coordinator."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.health import CircuitBreaker
from funding_arbitrage.monitoring.metrics import (
    exchange_stream_last_message_timestamp,
    funding_history_coverage_ratio,
    market_data_age_seconds,
    market_data_dropped_total,
    market_tickers_usable,
    orderbook_coverage_ratio,
    stale_or_missing_orderbooks,
)

logger = logging.getLogger(__name__)


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
    _ticker_index: dict[tuple[str, str, InstrumentType], Ticker] = field(
        init=False, repr=False, compare=False
    )
    _funding_index: dict[tuple[str, str], FundingSnapshot] = field(
        init=False, repr=False, compare=False
    )
    _instrument_index: dict[tuple[str, str], NormalizedInstrument] = field(
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
                (item.exchange, item.exchange_symbol): item
                for item in self.instruments
            },
        )

    def ticker(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> Ticker | None:
        return self._ticker_index.get((exchange, symbol, instrument_type))

    def funding_rate(self, exchange: str, symbol: str) -> FundingSnapshot | None:
        return self._funding_index.get((exchange, symbol))

    def instrument(self, exchange: str, symbol: str) -> NormalizedInstrument | None:
        return self._instrument_index.get((exchange, symbol))

    def orderbook(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> OrderBook | None:
        return self.orderbooks.get((exchange, symbol, instrument_type))


@dataclass(frozen=True)
class _VenueCollection:
    instruments: list[NormalizedInstrument]
    tickers: list[Ticker]
    funding: list[FundingSnapshot]
    orderbooks: dict[tuple[str, str, InstrumentType], OrderBook]
    funding_history: dict[tuple[str, str], list[FundingHistoryPoint]]
    operationally_complete: bool


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
    ) -> None:
        self.adapters = tuple(adapters)
        if orderbook_symbol_limit <= 0:
            raise ValueError("orderbook_symbol_limit must be positive")
        if market_asset_limit is not None and market_asset_limit <= 0:
            raise ValueError("market_asset_limit must be positive")
        if history_symbol_limit <= 0:
            raise ValueError("history_symbol_limit must be positive")
        self.orderbook_symbol_limit = orderbook_symbol_limit
        self.market_asset_limit = market_asset_limit
        self.history_symbol_limit = history_symbol_limit
        self.stale_after_seconds = stale_after_seconds
        self.enable_streams = enable_streams
        self.rest_validation_seconds = rest_validation_seconds
        self.health = {adapter.name: CircuitBreaker() for adapter in self.adapters}
        self._funding_history_cache: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        self._instrument_cache: dict[str, list[NormalizedInstrument]] = {}
        self._rest_ticker_cache: dict[str, list[Ticker]] = {}
        self._funding_cache: dict[str, list[FundingSnapshot]] = {}
        self._last_rest_ticker_fetch: dict[str, datetime] = {}
        self._last_funding_fetch: dict[str, datetime] = {}
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
    ) -> MarketSnapshot:
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
                    include_history,
                    (history_symbols or {}).get(adapter.name, []),
                    force_history_refresh,
                    (force_history_symbols or {}).get(adapter.name, []),
                )
                for adapter in active_adapters
            )
        )
        collections = await self._refresh_funding_aged_during_collection(
            active_adapters, list(collections)
        )
        captured_at = datetime.now(UTC)
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
        return MarketSnapshot(
            instruments=instruments,
            tickers=tickers,
            funding=funding,
            orderbooks=orderbooks,
            captured_at=captured_at,
            funding_history=dict(self._funding_history_cache),
            stale_after_seconds=self.stale_after_seconds,
            incomplete_venues=incomplete_venues,
        )

    async def _refresh_funding_aged_during_collection(
        self,
        adapters: tuple[ExchangeAdapter, ...],
        collections: list[_VenueCollection],
    ) -> list[_VenueCollection]:
        """Refresh venue funding that became stale while slow books/history loaded."""

        now = datetime.now(UTC)
        stale_indexes = [
            index
            for index, result in enumerate(collections)
            if result.funding
            and any(
                (now - item.timestamp).total_seconds() > self.stale_after_seconds
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
            )
            self._funding_cache[adapter.name] = value
            self._last_funding_fetch[adapter.name] = refreshed_at
        return collections

    async def _collect_venue(
        self,
        adapter: ExchangeAdapter,
        requested_books: list[tuple[str, InstrumentType]] | None,
        include_history: bool,
        required_history: list[str],
        force_history_refresh: bool,
        forced_history: list[str],
    ) -> _VenueCollection:
        breaker = self.health[adapter.name]
        orderbooks: dict[tuple[str, str, InstrumentType], OrderBook] = {}
        funding_history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        history_complete = True
        try:
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
                >= self.stale_after_seconds
            )
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
            venue_tickers = self._merge_stream_tickers(adapter.name, venue_tickers, now)
            valid_tickers = [item for item in venue_tickers if _is_valid_ticker(item)]
            dropped_tickers = len(venue_tickers) - len(valid_tickers)
            if dropped_tickers:
                market_data_dropped_total.labels(adapter.name, "invalid_ticker").inc(
                    dropped_tickers
                )
                logger.warning(
                    "invalid_tickers_dropped",
                    extra={
                        "exchange": adapter.name,
                        "event": "market_data_validation",
                        "error": f"dropped={dropped_tickers}",
                    },
                )
            market_tickers_usable.labels(adapter.name).set(len(valid_tickers))
            venue_funding = all_venue_funding
            if self.market_asset_limit is not None:
                venue_instruments, valid_tickers, venue_funding = _limit_venue_universe(
                    venue_instruments,
                    valid_tickers,
                    venue_funding,
                    self.market_asset_limit,
                    required_markets=set(requested_books or ()),
                )
            self._ensure_ticker_stream(adapter, valid_tickers)
            discovery_books = _rank_orderbook_requests(
                valid_tickers, venue_funding, venue_instruments
            )[: self.orderbook_symbol_limit]
            book_requests = list(
                dict.fromkeys([*(requested_books or []), *discovery_books])
            )
            self._ensure_orderbook_stream(adapter, book_requests)
            for symbol, instrument_type in book_requests:
                key = (adapter.name, symbol, instrument_type)
                streamed = self._stream_orderbook_cache.get(key)
                if (
                    streamed is not None
                    and (now - streamed.timestamp).total_seconds()
                    <= self.stale_after_seconds
                ):
                    orderbooks[key] = streamed
            rest_book_requests = [
                request
                for request in book_requests
                if self._book_needs_rest_validation(adapter.name, request, now)
            ]
            book_results = await asyncio.gather(
                *(
                    adapter.get_orderbook(symbol, 20, instrument_type)
                    for symbol, instrument_type in rest_book_requests
                ),
                return_exceptions=True,
            )
            for (symbol, instrument_type), book_result in zip(
                rest_book_requests, book_results, strict=True
            ):
                if isinstance(book_result, OrderBook):
                    key = (adapter.name, symbol, instrument_type)
                    self._last_rest_book_fetch[key] = now
                    current = orderbooks.get(key)
                    if current is None or book_result.timestamp >= current.timestamp:
                        orderbooks[key] = book_result
            orderbook_coverage_ratio.labels(adapter.name).set(
                len(orderbooks) / len(book_requests) if book_requests else 0
            )
            stale_or_missing_orderbooks.labels(adapter.name).set(
                max(0, len(book_requests) - len(orderbooks))
            )
            if include_history:
                end = datetime.now(UTC)
                start = end - timedelta(days=30)
                ranked = _rank_funding_symbols(
                    venue_funding, valid_tickers, venue_instruments
                )
                ranked_budget = (
                    len(ranked)
                    if self.market_asset_limit is not None
                    else self.history_symbol_limit
                )
                selected = list(
                    dict.fromkeys([*required_history, *ranked[:ranked_budget]])
                )
                valid_symbols = {item.symbol for item in all_venue_funding}
                selected = [symbol for symbol in selected if symbol in valid_symbols]
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
            operationally_complete = (
                bool(valid_tickers)
                and bool(venue_funding)
                and len(orderbooks) == len(book_requests)
                and history_complete
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
            )
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
        if not symbols:
            return
        target = frozenset(symbols)
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
        if not self.enable_streams or not requests:
            return
        target = frozenset(requests)
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
        if (now - streamed.timestamp).total_seconds() > self.stale_after_seconds:
            return True
        return (
            last_rest is None
            or (now - last_rest).total_seconds() >= self.rest_validation_seconds
        )

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
    assets = set(volume_by_asset) | set(funding_by_asset)
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
