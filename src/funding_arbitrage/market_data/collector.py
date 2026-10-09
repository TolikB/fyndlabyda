"""Restart-safe market-data collection coordinator."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from functools import cached_property
from time import perf_counter

from funding_arbitrage.exchanges.base.exceptions import RateLimitError
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    MarketKey,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.health import CircuitBreaker, VenueStatus
from funding_arbitrage.monitoring.metrics import (
    market_data_age_seconds,
    market_data_errors_total,
    market_data_latency_seconds,
    market_venue_up,
    orderbook_fetch_errors_total,
)

logger = logging.getLogger(__name__)

FundingKey = tuple[str, str]


@dataclass(frozen=True)
class VenueState:
    """Collection outcome of one venue for one snapshot."""

    exchange: str
    status: VenueStatus
    collected: bool
    last_success_at: datetime | None = None
    last_error: str | None = None
    latency_ms: float | None = None
    retry_at: datetime | None = None


@dataclass(frozen=True)
class MarketSnapshot:
    """In-memory normalized snapshot shared by the scanner and every paper series.

    ``orderbooks`` is filled on demand after the first scan pass, only for the
    markets a decision needs, so a snapshot never carries stale depth.
    """

    instruments: list[NormalizedInstrument]
    tickers: list[Ticker]
    funding: list[FundingSnapshot]
    orderbooks: dict[MarketKey, OrderBook]
    captured_at: datetime
    funding_history: dict[FundingKey, list[FundingHistoryPoint]] | None = None
    venues: dict[str, VenueState] = field(default_factory=dict)

    def with_funding_history(
        self, history: dict[FundingKey, list[FundingHistoryPoint]] | None
    ) -> MarketSnapshot:
        """The same market data with refreshed history, keeping built lookup indexes.

        Tickers, instruments and funding are shared, so their indexes stay valid;
        rebuilding them for the second scan of every cycle was measurable CPU.
        """

        updated = replace(self, funding_history=history)
        for name in ("ticker_index", "instrument_index", "funding_index"):
            if name in self.__dict__:
                updated.__dict__[name] = self.__dict__[name]
        return updated

    @cached_property
    def ticker_index(self) -> dict[MarketKey, Ticker]:
        return {ticker.key: ticker for ticker in self.tickers}

    @cached_property
    def instrument_index(self) -> dict[MarketKey, NormalizedInstrument]:
        return {instrument.key: instrument for instrument in self.instruments}

    @cached_property
    def funding_index(self) -> dict[FundingKey, FundingSnapshot]:
        return {(item.exchange, item.symbol): item for item in self.funding}

    def ticker(self, exchange: str, symbol: str, instrument_type: InstrumentType) -> Ticker | None:
        return self.ticker_index.get(MarketKey(exchange, instrument_type, symbol))

    def instrument(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> NormalizedInstrument | None:
        return self.instrument_index.get(MarketKey(exchange, instrument_type, symbol))

    def funding_for(self, exchange: str, symbol: str) -> FundingSnapshot | None:
        return self.funding_index.get((exchange, symbol))

    def orderbook(
        self, exchange: str, symbol: str, instrument_type: InstrumentType
    ) -> OrderBook | None:
        return self.orderbooks.get(MarketKey(exchange, instrument_type, symbol))

    def history(self, exchange: str, symbol: str) -> list[FundingHistoryPoint]:
        if not self.funding_history:
            return []
        return self.funding_history.get((exchange, symbol), [])

    def venue_collected(self, exchange: str) -> bool:
        state = self.venues.get(exchange)
        return state is None or state.collected


@dataclass
class _VenueResult:
    instruments: list[NormalizedInstrument] = field(default_factory=list)
    tickers: list[Ticker] = field(default_factory=list)
    funding: list[FundingSnapshot] = field(default_factory=list)


class MarketDataCollector:
    """Collect all venues concurrently; one slow or failing venue never stalls the rest."""

    def __init__(
        self,
        adapters: Iterable[ExchangeAdapter],
        *,
        venue_timeout_seconds: float = 25.0,
        instrument_refresh_seconds: float = 3600.0,
        history_ttl_seconds: float = 6 * 3600.0,
        history_days: int = 30,
        book_depth: int = 50,
        max_book_requests_per_venue: int = 4,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.adapters = {adapter.name: adapter for adapter in adapters}
        if venue_timeout_seconds <= 0 or book_depth <= 0:
            raise ValueError("venue timeout and book depth must be positive")
        self.venue_timeout_seconds = venue_timeout_seconds
        self.instrument_refresh = timedelta(seconds=instrument_refresh_seconds)
        self.history_ttl = timedelta(seconds=history_ttl_seconds)
        self.history_days = history_days
        self.book_depth = book_depth
        self.clock = clock or (lambda: datetime.now(UTC))
        self.health = {name: CircuitBreaker() for name in self.adapters}
        self._latency_ms: dict[str, float] = {}
        self._instruments: dict[str, tuple[datetime, list[NormalizedInstrument]]] = {}
        self._history: dict[FundingKey, tuple[datetime, list[FundingHistoryPoint]]] = {}
        self._book_slots = {
            name: asyncio.Semaphore(max_book_requests_per_venue) for name in self.adapters
        }

    # ------------------------------------------------------------------ snapshot
    async def collect_once(
        self, include_history: bool = False, history_symbols_per_venue: int = 10
    ) -> MarketSnapshot:
        now = self.clock()
        names = list(self.adapters)
        results = await asyncio.gather(*(self._collect_venue(name, now) for name in names))
        instruments: list[NormalizedInstrument] = []
        tickers: list[Ticker] = []
        funding: list[FundingSnapshot] = []
        venues: dict[str, VenueState] = {}
        for name, result in zip(names, results, strict=True):
            breaker = self.health[name]
            collected = result is not None
            if result is not None:
                instruments.extend(result.instruments)
                tickers.extend(result.tickers)
                funding.extend(result.funding)
            venues[name] = VenueState(
                exchange=name,
                status=breaker.status,
                collected=collected,
                last_success_at=breaker.last_success_at,
                last_error=breaker.last_error,
                latency_ms=self._latency_ms.get(name),
                retry_at=breaker.retry_at,
            )
            age = (
                (now - breaker.last_success_at).total_seconds() if breaker.last_success_at else -1.0
            )
            market_data_age_seconds.labels(name).set(0 if collected else age)
            market_venue_up.labels(name).set(1 if collected else 0)
        snapshot = MarketSnapshot(
            instruments=instruments,
            tickers=tickers,
            funding=funding,
            orderbooks={},
            captured_at=self.clock(),
            funding_history=self.history_view(),
            venues=venues,
        )
        if include_history:
            await self.ensure_funding_history(
                self._top_funding_keys(snapshot, history_symbols_per_venue),
                budget=history_symbols_per_venue * len(self.adapters),
            )
            snapshot = MarketSnapshot(
                instruments=snapshot.instruments,
                tickers=snapshot.tickers,
                funding=snapshot.funding,
                orderbooks=snapshot.orderbooks,
                captured_at=snapshot.captured_at,
                funding_history=self.history_view(),
                venues=snapshot.venues,
            )
        return snapshot

    async def _collect_venue(self, name: str, now: datetime) -> _VenueResult | None:
        adapter = self.adapters[name]
        breaker = self.health[name]
        if not breaker.allow(now):
            return None
        started = perf_counter()
        try:
            async with asyncio.timeout(self.venue_timeout_seconds):
                instruments = await self._venue_instruments(adapter, now)
                tickers = await adapter.get_tickers()
                funding = await adapter.get_funding_rates()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"[:300]
            cooldown = exc.retry_after if isinstance(exc, RateLimitError) else None
            breaker.record_failure(now, error=error, cooldown=cooldown)
            market_data_errors_total.labels(name, type(exc).__name__).inc()
            logger.warning(
                "venue_collection_failed",
                extra={
                    "exchange": name,
                    "error": error,
                    "status": str(breaker.status),
                    "retry_at": breaker.retry_at,
                },
            )
            return None
        elapsed = perf_counter() - started
        self._latency_ms[name] = elapsed * 1000
        market_data_latency_seconds.labels(name).observe(elapsed)
        breaker.record_success(now)
        return _VenueResult(instruments=instruments, tickers=tickers, funding=funding)

    async def _venue_instruments(
        self, adapter: ExchangeAdapter, now: datetime
    ) -> list[NormalizedInstrument]:
        cached = self._instruments.get(adapter.name)
        if cached is not None and now - cached[0] < self.instrument_refresh:
            return cached[1]
        try:
            instruments = await adapter.get_instruments()
        except Exception:
            if cached is None:
                raise
            # Contract metadata changes rarely; a failed refresh keeps the last good copy.
            logger.warning("instrument_refresh_failed", extra={"exchange": adapter.name})
            return cached[1]
        self._instruments[adapter.name] = (now, instruments)
        return instruments

    def instrument_refreshes(self) -> dict[str, datetime]:
        """When each venue's instrument metadata was last loaded."""

        return {name: refreshed for name, (refreshed, _) in self._instruments.items()}

    # ---------------------------------------------------------------- orderbooks
    async def fetch_orderbooks(
        self, snapshot: MarketSnapshot, keys: Iterable[MarketKey]
    ) -> dict[MarketKey, str]:
        """Fetch fresh books into ``snapshot.orderbooks``; return failures by key."""

        wanted = [key for key in dict.fromkeys(keys) if key.exchange in self.adapters]
        outcomes = await asyncio.gather(*(self._fetch_book(key) for key in wanted))
        failures: dict[MarketKey, str] = {}
        for key, outcome in zip(wanted, outcomes, strict=True):
            if isinstance(outcome, OrderBook):
                snapshot.orderbooks[key] = outcome
            else:
                failures[key] = outcome
        return failures

    async def _fetch_book(self, key: MarketKey) -> OrderBook | str:
        adapter = self.adapters[key.exchange]
        async with self._book_slots[key.exchange]:
            try:
                async with asyncio.timeout(self.venue_timeout_seconds):
                    return await adapter.get_orderbook(
                        key.symbol, self.book_depth, key.instrument_type
                    )
            except Exception as exc:
                orderbook_fetch_errors_total.labels(key.exchange, type(exc).__name__).inc()
                logger.warning(
                    "orderbook_fetch_failed",
                    extra={
                        "exchange": key.exchange,
                        "symbol": key.symbol,
                        "instrument_type": str(key.instrument_type),
                        "error": f"{type(exc).__name__}: {exc}"[:200],
                    },
                )
                return f"{type(exc).__name__}: {exc}"[:200]

    # ------------------------------------------------------------ funding history
    def history_view(self) -> dict[FundingKey, list[FundingHistoryPoint]]:
        return {key: points for key, (_, points) in self._history.items()}

    def history_fresh(self, key: FundingKey, now: datetime | None = None) -> bool:
        cached = self._history.get(key)
        return cached is not None and (now or self.clock()) - cached[0] < self.history_ttl

    async def ensure_funding_history(
        self, keys: Iterable[FundingKey], budget: int
    ) -> list[FundingKey]:
        """Load 30-day history for keys lacking a fresh copy; at most ``budget`` requests.

        Returns the keys whose history was (re)loaded.
        """

        now = self.clock()
        pending = [
            key
            for key in dict.fromkeys(keys)
            if key[0] in self.adapters and not self.history_fresh(key, now)
        ][: max(0, budget)]
        if not pending:
            return []
        start = now - timedelta(days=self.history_days)
        results = await asyncio.gather(*(self._load_history(key, start, now) for key in pending))
        return [key for key, ok in zip(pending, results, strict=True) if ok]

    async def _load_history(self, key: FundingKey, start: datetime, end: datetime) -> bool:
        exchange, symbol = key
        try:
            async with asyncio.timeout(self.venue_timeout_seconds):
                points = await self.adapters[exchange].get_funding_history(symbol, start, end)
        except Exception as exc:
            logger.warning(
                "funding_history_fetch_failed",
                extra={
                    "exchange": exchange,
                    "symbol": symbol,
                    "error": f"{type(exc).__name__}: {exc}"[:200],
                },
            )
            return False
        self._history[key] = (end, sorted(points, key=lambda point: point.funding_timestamp))
        return True

    async def funding_history_between(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        """Uncached exact history used for settlement; errors propagate to the caller."""

        adapter = self.adapters[exchange]
        async with asyncio.timeout(self.venue_timeout_seconds):
            points = await adapter.get_funding_history(symbol, start, end)
        return sorted(
            (point for point in points if start <= point.funding_timestamp <= end),
            key=lambda point: point.funding_timestamp,
        )

    def set_priority_symbols(self, symbols: dict[str, set[str]]) -> None:
        for name, adapter in self.adapters.items():
            adapter.set_priority_symbols(symbols.get(name, set()))

    @staticmethod
    def _top_funding_keys(snapshot: MarketSnapshot, per_venue: int) -> list[FundingKey]:
        by_venue: dict[str, list[FundingSnapshot]] = {}
        for item in snapshot.funding:
            by_venue.setdefault(item.exchange, []).append(item)
        keys: list[FundingKey] = []
        for items in by_venue.values():
            ranked = sorted(items, key=lambda item: abs(item.funding_rate_8h), reverse=True)
            keys.extend((item.exchange, item.symbol) for item in ranked[:per_venue])
        return keys
