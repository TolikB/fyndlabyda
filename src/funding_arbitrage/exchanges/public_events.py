"""Bounded public event streams for canonical research and replay data.

Native venue adapters remain authoritative for scanner snapshots.  This module
adds trades, one-minute candles, open interest, and public liquidations through
CCXT Pro, and mirrors exact native funding/mark/index observations into the
canonical journal.  Missing exchange capabilities are exposed explicitly; no
synthetic liquidation or open-interest records are produced.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from time import monotonic, monotonic_ns
from typing import Any

from pydantic import BaseModel

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import (
    Candle,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    LiquidationTick,
    OpenInterestSnapshot,
    Side,
    TradeTick,
    deterministic_event_id,
    snapshot_occurrence_id,
)
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.quality import StreamIdentity
from funding_arbitrage.market_data.venue_metadata import (
    VenueMetadataRegistry,
    VenueMetadataSnapshot,
)
from funding_arbitrage.monitoring.metrics import (
    market_data_dropped_total,
    public_event_capability,
    public_events_total,
    venue_clock_offset_seconds,
    venue_metadata_instruments,
    venue_rate_limit_milliseconds,
    websocket_reconnects_total,
)
from funding_arbitrage.services.event_writer import EventWriterFailed

logger = logging.getLogger(__name__)

_CANDLE_INTERVAL_MILLISECONDS = 60_000
_CANDLE_BACKFILL_LIMIT = 10
_CANDLE_FINALITY_DELAY = timedelta(seconds=5)

CanonicalEventSink = Callable[[EventEnvelope[Any]], Awaitable[None]]
SnapshotObserver = Callable[[MarketSnapshot], Awaitable[object]]


@dataclass(frozen=True)
class PublicEventProfile:
    venue: str
    account: str
    exchange_class: str
    default_type: str
    instrument_type: InstrumentType
    params: Mapping[str, object] = field(default_factory=dict)
    ohlcv_volume_in_contracts: bool = False
    open_interest_amount_in_contracts: bool = False
    open_interest_base_volume_in_contracts: bool = False


@dataclass(frozen=True)
class PublicEventAccount:
    profile: PublicEventProfile
    exchange: Any


class PublicDataNormalizationError(ValueError):
    """A public payload cannot be represented without inventing data."""


class CcxtPublicEventNormalizer:
    """Convert unified CCXT payloads to exact immutable domain events."""

    def __init__(self, profile: PublicEventProfile, exchange: Any) -> None:
        self.profile = profile
        self.exchange = exchange

    def trade_events(
        self, rows: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        return tuple(self._trade_event(row, received_at) for row in _rows(rows))

    def liquidation_events(
        self, rows: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        return tuple(self._liquidation_event(row, received_at) for row in _rows(rows))

    def candle_event(
        self,
        symbol: str,
        row: object,
        *,
        received_at: datetime,
        received_monotonic_ns: int | None = None,
    ) -> EventEnvelope[Any]:
        values = _sequence(row, "OHLCV")
        if len(values) < 6:
            raise PublicDataNormalizationError("OHLCV payload has fewer than six values")
        market = self._market(symbol)
        open_time = _milliseconds(values[0])
        close_time = open_time + timedelta(minutes=1)
        volume = _non_negative(values[5], "candle volume")
        if self.profile.ohlcv_volume_in_contracts:
            volume *= _contract_size(market)
        payload = Candle(
            instrument=_instrument(self.profile.venue, market, self.profile.instrument_type),
            interval_seconds=60,
            open_time=open_time,
            close_time=close_time,
            open=_positive(values[1], "candle open"),
            high=_positive(values[2], "candle high"),
            low=_positive(values[3], "candle low"),
            close=_positive(values[4], "candle close"),
            volume=volume,
            closed=received_at >= close_time,
            exchange_timestamp=close_time,
        )
        return _envelope(
            payload,
            kind=EventKind.CANDLE,
            source=f"ccxt-pro:{self.profile.venue}:{self.profile.account}:ohlcv",
            sequence_id=f"{symbol}:{int(open_time.timestamp() * 1000)}:60",
            received_at=received_at,
            received_monotonic_ns=received_monotonic_ns,
        )

    def closed_candle_events(
        self,
        symbol: str,
        rows: object,
        *,
        received_at: datetime,
        received_monotonic_ns: int | None = None,
    ) -> tuple[EventEnvelope[Any], ...]:
        """Return only immutable candles whose interval has already closed.

        REST venues normally return the current in-progress bar together with
        the latest closed bar.  Publishing that mutable bar would reuse its
        logical event ID with a different payload on the next poll and
        correctly trip the append-only journal integrity guard.
        """

        events = tuple(
            self.candle_event(
                symbol,
                row,
                received_at=received_at,
                received_monotonic_ns=received_monotonic_ns,
            )
            for row in _rows(rows)
        )
        return tuple(
            event
            for event in events
            if event.payload.closed
            and received_at >= event.payload.close_time + _CANDLE_FINALITY_DELAY
        )

    def open_interest_event(
        self,
        row: object,
        *,
        received_at: datetime,
        received_monotonic_ns: int | None = None,
    ) -> EventEnvelope[Any]:
        raw = _object(row, "open interest")
        symbol = _text(raw.get("symbol"))
        market = self._market(symbol)
        timestamp, quality = _exchange_time(raw, received_at)
        base = _optional_non_negative(raw.get("baseVolume"))
        if base is not None and self.profile.open_interest_base_volume_in_contracts:
            base *= _contract_size(market)
        if base is None:
            base = _optional_non_negative(raw.get("openInterestAmount"))
            if base is not None and self.profile.open_interest_amount_in_contracts:
                base *= _contract_size(market)
        quote = _optional_non_negative(raw.get("quoteVolume"))
        if quote is None:
            quote = _optional_non_negative(raw.get("openInterestValue"))
        payload = OpenInterestSnapshot(
            instrument=_instrument(self.profile.venue, market, self.profile.instrument_type),
            open_interest_base=base,
            open_interest_quote=quote,
            exchange_timestamp=timestamp,
        )
        observed_monotonic_ns = (
            received_monotonic_ns
            if received_monotonic_ns is not None
            else monotonic_ns()
        )
        return _envelope(
            payload,
            kind=EventKind.OPEN_INTEREST_SNAPSHOT,
            source=f"ccxt-pro:{self.profile.venue}:{self.profile.account}:open-interest",
            sequence_id=f"{market['id']}:{int(timestamp.timestamp() * 1000)}",
            received_at=received_at,
            quality=quality,
            received_monotonic_ns=observed_monotonic_ns,
            occurrence_id=snapshot_occurrence_id(
                receive_timestamp=received_at,
                receive_monotonic_ns=observed_monotonic_ns,
            ),
        )

    def _trade_event(self, row: object, received_at: datetime) -> EventEnvelope[Any]:
        raw = _object(row, "trade")
        symbol = _text(raw.get("symbol"))
        market = self._market(symbol)
        timestamp, quality = _exchange_time(raw, received_at)
        trade_id = _trade_id(raw, venue=self.profile.venue)
        if not trade_id:
            raise PublicDataNormalizationError("trade ID is missing")
        side = _side(raw.get("side"), required=False)
        payload = TradeTick(
            instrument=_instrument(self.profile.venue, market, self.profile.instrument_type),
            trade_id=trade_id,
            price=_positive(raw.get("price"), "trade price"),
            quantity=_positive(raw.get("amount"), "trade amount") * _contract_size(market),
            aggressor_side=side,
            exchange_timestamp=timestamp,
        )
        return _envelope(
            payload,
            kind=EventKind.TRADE_TICK,
            source=f"ccxt-pro:{self.profile.venue}:{self.profile.account}:trades",
            sequence_id=f"{market['id']}:{trade_id}",
            received_at=received_at,
            quality=quality,
        )

    def _liquidation_event(
        self, row: object, received_at: datetime
    ) -> EventEnvelope[Any]:
        raw = _object(row, "liquidation")
        symbol = _text(raw.get("symbol"))
        market = self._market(symbol)
        timestamp, quality = _exchange_time(raw, received_at)
        quantity = (
            _positive(raw.get("baseValue"), "liquidation base value")
            if raw.get("baseValue") not in (None, "")
            else _positive(raw.get("contracts"), "liquidation contracts")
            * _positive(
                raw.get("contractSize", market.get("contractSize", "1")),
                "liquidation contract size",
            )
        )
        liquidation_id = _text(raw.get("id"))
        if not liquidation_id:
            liquidation_id = ":".join(
                (
                    _text(market.get("id")),
                    str(int(timestamp.timestamp() * 1000)),
                    _text(raw.get("price")),
                    str(quantity),
                    _text(raw.get("side")),
                )
            )
        payload = LiquidationTick(
            instrument=_instrument(self.profile.venue, market, self.profile.instrument_type),
            liquidation_id=liquidation_id,
            side=_side(raw.get("side"), required=False),
            price=_positive(raw.get("price"), "liquidation price"),
            quantity=quantity,
            exchange_timestamp=timestamp,
        )
        return _envelope(
            payload,
            kind=EventKind.LIQUIDATION_TICK,
            source=f"ccxt-pro:{self.profile.venue}:{self.profile.account}:liquidations",
            sequence_id=f"{market['id']}:{liquidation_id}",
            received_at=received_at,
            quality=quality,
        )

    def _market(self, symbol: str) -> Mapping[str, Any]:
        if not symbol:
            raise PublicDataNormalizationError("public event symbol is missing")
        try:
            market = self.exchange.market(symbol)
        except Exception as exc:
            raise PublicDataNormalizationError("public event market is unknown") from exc
        if not isinstance(market, dict):
            raise PublicDataNormalizationError("public event market is invalid")
        return market


class PublicEventSupervisor:
    """Maintain bounded public streams and conservative REST recovery polls."""

    _STREAMS = ("trades", "ohlcv", "open_interest", "liquidations")

    def __init__(
        self,
        accounts: Sequence[PublicEventAccount],
        event_sink: CanonicalEventSink,
        *,
        symbol_limit: int,
        rest_interval_seconds: float,
        metadata_refresh_seconds: float = 3600.0,
        quality_stream_retention_seconds: float = 540.0,
        quality_stream_clock: Callable[[], float] = monotonic,
        reconnect_initial_seconds: float,
        reconnect_max_seconds: float,
    ) -> None:
        if symbol_limit <= 0 or rest_interval_seconds <= 0:
            raise ValueError("public event limits and intervals must be positive")
        if metadata_refresh_seconds <= 0:
            raise ValueError("public metadata refresh interval must be positive")
        if quality_stream_retention_seconds <= 0:
            raise ValueError("public quality stream retention must be positive")
        if not 0 < reconnect_initial_seconds <= reconnect_max_seconds:
            raise ValueError("public event reconnect bounds are invalid")
        self.accounts = tuple(accounts)
        self.event_sink = event_sink
        self.symbol_limit = symbol_limit
        self.rest_interval_seconds = rest_interval_seconds
        self.metadata_refresh_seconds = metadata_refresh_seconds
        self.quality_stream_retention_seconds = quality_stream_retention_seconds
        self._quality_stream_clock = quality_stream_clock
        self.reconnect_initial_seconds = reconnect_initial_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self._normalizers = {
            self._key(account): CcxtPublicEventNormalizer(account.profile, account.exchange)
            for account in self.accounts
        }
        self.metadata_registry = VenueMetadataRegistry()
        self._last_metadata_refresh: dict[tuple[str, str], datetime] = {}
        self._last_closed_candle_open_ms: dict[tuple[str, str, str], int] = {}
        self._available: set[tuple[str, str]] = set()
        self._desired_symbols: dict[tuple[str, str], tuple[str, ...]] = {}
        self._required_quality_streams: tuple[StreamIdentity, ...] = ()
        self._required_quality_last_seen: dict[StreamIdentity, float] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._symbol_tasks: dict[tuple[str, str, str, str], asyncio.Task[None]] = {}
        self._started = False
        self._stop_event = asyncio.Event()
        self._pre_mirror_snapshot_observer: SnapshotObserver | None = None

    def set_pre_mirror_snapshot_observer(self, observer: SnapshotObserver) -> None:
        """Attach one projection before this supervisor mirrors a snapshot."""

        if self._started:
            raise RuntimeError("snapshot observer must be attached before start")
        if self._pre_mirror_snapshot_observer is not None:
            raise ValueError("public snapshot observer is already configured")
        self._pre_mirror_snapshot_observer = observer

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        await asyncio.gather(*(self._load_account(account) for account in self.accounts))
        for account in self.accounts:
            key = self._key(account)
            if key in self._available:
                self._start_account_tasks(account)
            else:
                self._track(asyncio.create_task(self._recover_account(account)))

    async def close(self) -> None:
        self._stop_event.set()
        tasks = tuple(self._tasks) + tuple(self._symbol_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._symbol_tasks.clear()
        await asyncio.gather(
            *(account.exchange.close() for account in self.accounts),
            return_exceptions=True,
        )
        self._started = False

    async def observe_snapshot(self, snapshot: MarketSnapshot) -> None:
        """Mirror exact native funding and update the bounded CCXT universe."""

        if self._pre_mirror_snapshot_observer is not None:
            await self._pre_mirror_snapshot_observer(snapshot)
        # Register the new snapshot identities before durable mirroring starts.
        # Under sustained websocket load, publishing native rows can take longer
        # than the freshness window. Readiness must not keep judging only the
        # obsolete universe while current rows are already being committed.
        self._update_required_quality_streams(snapshot)
        received_at = datetime.now(UTC)
        await self._publish_snapshot_events(snapshot, received_at, monotonic_ns())
        for account in self.accounts:
            key = self._key(account)
            if key not in self._available:
                continue
            ranked = sorted(
                (
                    ticker
                    for ticker in snapshot.tickers
                    if ticker.exchange == account.profile.venue
                    and _legacy_type(ticker.instrument_type)
                    is account.profile.instrument_type
                ),
                key=lambda ticker: (-ticker.volume_24h, ticker.symbol),
            )
            resolved: list[str] = []
            for ticker in ranked:
                symbol = _resolve_unified_symbol(
                    account.exchange,
                    ticker.symbol,
                    account.profile.instrument_type,
                )
                if symbol and symbol not in resolved:
                    resolved.append(symbol)
                if len(resolved) >= self.symbol_limit:
                    break
            self._desired_symbols[key] = tuple(resolved)

    @property
    def required_quality_streams(self) -> tuple[StreamIdentity, ...]:
        return self._required_quality_streams

    def _update_required_quality_streams(self, snapshot: MarketSnapshot) -> None:
        required: set[StreamIdentity] = set()
        for exchange, symbol, instrument_type in snapshot.orderbooks:
            instrument = snapshot.instrument(exchange, symbol, instrument_type)
            if instrument is None:
                continue
            canonical = _legacy_instrument(instrument)
            required.add(StreamIdentity(canonical.venue, "BOOK", canonical.canonical_id))
        for funding in snapshot.funding:
            instrument = snapshot.instrument(
                funding.exchange, funding.symbol, LegacyInstrumentType.PERPETUAL
            )
            ticker = snapshot.ticker(
                funding.exchange, funding.symbol, LegacyInstrumentType.PERPETUAL
            )
            mark_price = funding.mark_price or (ticker.mark_price if ticker else None)
            index_price = funding.index_price or (ticker.index_price if ticker else None)
            if (
                instrument is None
                or funding.next_funding_time is None
                or mark_price is None
                or index_price is None
            ):
                continue
            canonical = _legacy_instrument(instrument)
            required.add(
                StreamIdentity(
                    canonical.venue,
                    EventKind.FUNDING_SNAPSHOT.value,
                    canonical.canonical_id,
                )
            )
        observed_at = self._quality_stream_clock()
        for identity in required:
            self._required_quality_last_seen[identity] = observed_at
        cutoff = observed_at - self.quality_stream_retention_seconds
        self._required_quality_last_seen = {
            identity: last_seen
            for identity, last_seen in self._required_quality_last_seen.items()
            if last_seen >= cutoff
        }
        self._required_quality_streams = tuple(
            sorted(self._required_quality_last_seen)
        )

    def _start_account_tasks(self, account: PublicEventAccount) -> None:
        self._track(asyncio.create_task(self._manage_symbols(account)))
        self._track(asyncio.create_task(self._rest_loop(account)))

    async def _recover_account(self, account: PublicEventAccount) -> None:
        delay = self.reconnect_initial_seconds
        while not self._stop_event.is_set():
            await self._wait(delay)
            if self._stop_event.is_set():
                return
            await self._load_account(account)
            if self._key(account) in self._available:
                self._start_account_tasks(account)
                return
            delay = min(delay * 2, self.reconnect_max_seconds)

    async def _load_account(self, account: PublicEventAccount) -> None:
        profile = account.profile
        key = self._key(account)
        try:
            await account.exchange.load_markets()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._available.discard(key)
            logger.exception(
                "public_event_market_load_failed",
                extra={"exchange": profile.venue, "account": profile.account},
            )
            for stream in self._STREAMS:
                public_event_capability.labels(profile.venue, profile.account, stream).set(0)
            return
        self._available.add(key)
        capabilities = {
            "trades": bool(account.exchange.has.get("watchTrades")),
            "ohlcv": bool(account.exchange.has.get("fetchOHLCV")),
            "open_interest": bool(account.exchange.has.get("fetchOpenInterest"))
            or (
                profile.instrument_type is not InstrumentType.SPOT
                and profile.venue in {"gate", "mexc"}
            ),
            "liquidations": bool(
                account.exchange.has.get("watchLiquidations")
                or account.exchange.has.get("fetchLiquidations")
            )
            and profile.instrument_type is not InstrumentType.SPOT,
        }
        for stream, available in capabilities.items():
            public_event_capability.labels(profile.venue, profile.account, stream).set(
                1 if available else 0
            )
        await self._refresh_metadata(account, reload=False)

    async def _refresh_metadata(
        self, account: PublicEventAccount, *, reload: bool
    ) -> None:
        profile = account.profile
        try:
            if reload:
                await account.exchange.load_markets(reload=True)
            before = datetime.now(UTC)
            server_time_ms = None
            if account.exchange.has.get("fetchTime"):
                server_time_ms = int(await account.exchange.fetch_time())
            after = datetime.now(UTC)
            observed_at = before + (after - before) / 2
            snapshot = self.metadata_registry.update_from_ccxt(
                venue=profile.venue,
                account=profile.account,
                exchange=account.exchange,
                expected_type=profile.instrument_type,
                observed_at=observed_at,
                server_time_ms=server_time_ms,
            )
            self._last_metadata_refresh[self._key(account)] = after
            venue_rate_limit_milliseconds.labels(
                profile.venue, profile.account
            ).set(float(snapshot.rate_limit_ms))
            venue_metadata_instruments.labels(profile.venue, profile.account).set(
                len(snapshot.instruments)
            )
            if snapshot.clock_offset_ms is not None:
                venue_clock_offset_seconds.labels(
                    profile.venue, profile.account
                ).set(snapshot.clock_offset_ms / 1000)
        except asyncio.CancelledError:
            raise
        except Exception:
            market_data_dropped_total.labels(
                profile.venue, "invalid_venue_metadata"
            ).inc()
            logger.exception(
                "public_venue_metadata_refresh_failed",
                extra={"exchange": profile.venue, "account": profile.account},
            )

    @property
    def metadata_snapshots(self) -> tuple[VenueMetadataSnapshot, ...]:
        return self.metadata_registry.snapshots()
    async def _manage_symbols(self, account: PublicEventAccount) -> None:
        profile = account.profile
        while not self._stop_event.is_set():
            desired = set(self._desired_symbols.get(self._key(account), ()))
            streams = ["trades"] if account.exchange.has.get("watchTrades") else []
            if (
                profile.instrument_type is not InstrumentType.SPOT
                and account.exchange.has.get("watchLiquidations")
            ):
                streams.append("liquidations")
            wanted = {
                (profile.venue, profile.account, stream, symbol)
                for stream in streams
                for symbol in desired
            }
            owned = {
                key
                for key in self._symbol_tasks
                if key[:2] == (profile.venue, profile.account)
            }
            for key in tuple(owned):
                task = self._symbol_tasks[key]
                if not task.done():
                    continue
                self._symbol_tasks.pop(key)
                owned.remove(key)
                if not task.cancelled():
                    error = task.exception()
                    if error is not None:
                        raise error
            cancelled: list[asyncio.Task[None]] = []
            for key in owned - wanted:
                task = self._symbol_tasks.pop(key)
                task.cancel()
                cancelled.append(task)
            if cancelled:
                await asyncio.gather(*cancelled, return_exceptions=True)
            for key in wanted - owned:
                _, _, stream, symbol = key
                self._symbol_tasks[key] = asyncio.create_task(
                    self._watch_symbol(account, stream, symbol)
                )
            await self._wait(1.0)

    async def _watch_symbol(
        self, account: PublicEventAccount, stream: str, symbol: str
    ) -> None:
        profile = account.profile
        delay = self.reconnect_initial_seconds
        while not self._stop_event.is_set() and symbol in self._desired_symbols.get(
            self._key(account), ()
        ):
            try:
                if stream == "trades":
                    rows = await account.exchange.watch_trades(symbol, params=profile.params)
                    events = self._normalizers[self._key(account)].trade_events(
                        rows, received_at=datetime.now(UTC)
                    )
                else:
                    rows = await account.exchange.watch_liquidations(
                        symbol, params=profile.params
                    )
                    events = self._normalizers[self._key(account)].liquidation_events(
                        rows, received_at=datetime.now(UTC)
                    )
                await self._publish(events, profile, stream, "websocket")
                delay = self.reconnect_initial_seconds
            except asyncio.CancelledError:
                raise
            except PublicDataNormalizationError:
                market_data_dropped_total.labels(profile.venue, f"invalid_{stream}").inc()
                logger.warning(
                    "public_event_rejected",
                    extra={"exchange": profile.venue, "stream": stream},
                )
            except EventWriterFailed:
                raise
            except Exception:
                websocket_reconnects_total.labels(profile.venue).inc()
                logger.exception(
                    "public_event_stream_failed",
                    extra={"exchange": profile.venue, "stream": stream},
                )
                await self._wait(delay)
                delay = min(delay * 2, self.reconnect_max_seconds)

    async def _rest_loop(self, account: PublicEventAccount) -> None:
        while not self._stop_event.is_set():
            started = datetime.now(UTC)
            last_refresh = self._last_metadata_refresh.get(self._key(account))
            if last_refresh is None or (
                started - last_refresh
            ).total_seconds() >= self.metadata_refresh_seconds:
                await self._refresh_metadata(account, reload=True)
            for symbol in self._desired_symbols.get(self._key(account), ()):
                await self._poll_symbol(account, symbol, started)
            elapsed = (datetime.now(UTC) - started).total_seconds()
            await self._wait(max(0.0, self.rest_interval_seconds - elapsed))

    async def _poll_symbol(
        self, account: PublicEventAccount, symbol: str, received_at: datetime
    ) -> None:
        profile = account.profile
        normalizer = self._normalizers[self._key(account)]
        if account.exchange.has.get("fetchOHLCV"):
            candle_key = (*self._key(account), symbol)
            previous_open_ms = self._last_closed_candle_open_ms.get(candle_key)
            since = (
                previous_open_ms + _CANDLE_INTERVAL_MILLISECONDS
                if previous_open_ms is not None
                else None
            )
            candle_events = await self._poll(
                account,
                "ohlcv",
                lambda: account.exchange.fetch_ohlcv(
                    symbol,
                    "1m",
                    since=since,
                    limit=_CANDLE_BACKFILL_LIMIT,
                    params=profile.params,
                ),
                lambda rows, observed_at, observed_monotonic_ns: (
                    normalizer.closed_candle_events(
                        symbol,
                        rows,
                        received_at=observed_at,
                        received_monotonic_ns=observed_monotonic_ns,
                    )
                ),
            )
            if candle_events:
                latest_open_ms = max(
                    int(event.payload.open_time.timestamp() * 1000)
                    for event in candle_events
                )
                self._last_closed_candle_open_ms[candle_key] = max(
                    previous_open_ms or latest_open_ms,
                    latest_open_ms,
                )
        if (
            profile.instrument_type is not InstrumentType.SPOT
            and account.exchange.has.get("fetchOpenInterest")
        ):
            await self._poll(
                account,
                "open_interest",
                lambda: account.exchange.fetch_open_interest(symbol, params=profile.params),
                lambda row, observed_at, observed_monotonic_ns: (
                    normalizer.open_interest_event(
                        row,
                        received_at=observed_at,
                        received_monotonic_ns=observed_monotonic_ns,
                    ),
                ),
            )
        if (
            profile.instrument_type is not InstrumentType.SPOT
            and not account.exchange.has.get("watchLiquidations")
            and account.exchange.has.get("fetchLiquidations")
        ):
            window_start = received_at - timedelta(
                seconds=self.rest_interval_seconds * 2
            )
            since = int(window_start.timestamp() * 1000)
            await self._poll(
                account,
                "liquidations",
                lambda: account.exchange.fetch_liquidations(
                    symbol, since=since, limit=100, params=profile.params
                ),
                lambda rows, observed_at, _: normalizer.liquidation_events(
                    rows, received_at=observed_at
                ),
            )

    async def _poll(
        self,
        account: PublicEventAccount,
        stream: str,
        fetch: Callable[[], Awaitable[object]],
        normalize: Callable[[object, datetime, int], Sequence[EventEnvelope[Any]]],
    ) -> tuple[EventEnvelope[Any], ...]:
        try:
            raw = await fetch()
            observed_at = datetime.now(UTC)
            observed_monotonic_ns = monotonic_ns()
            events = tuple(normalize(raw, observed_at, observed_monotonic_ns))
            await self._publish(
                events,
                account.profile,
                stream,
                "rest",
            )
            return events
        except asyncio.CancelledError:
            raise
        except PublicDataNormalizationError:
            market_data_dropped_total.labels(
                account.profile.venue, f"invalid_{stream}"
            ).inc()
            return ()
        except EventWriterFailed:
            raise
        except Exception:
            logger.exception(
                "public_event_rest_poll_failed",
                extra={"exchange": account.profile.venue, "stream": stream},
            )
            return ()

    async def _publish_snapshot_events(
        self,
        snapshot: MarketSnapshot,
        received_at: datetime,
        received_monotonic_ns: int | None = None,
    ) -> None:
        observed_monotonic_ns = (
            received_monotonic_ns
            if received_monotonic_ns is not None
            else monotonic_ns()
        )
        base_occurrence_id = snapshot_occurrence_id(
            receive_timestamp=received_at,
            receive_monotonic_ns=observed_monotonic_ns,
        )
        funding_rows: list[tuple[FundingSnapshot, str, str, str]] = []
        for funding in snapshot.funding:
            instrument = snapshot.instrument(
                funding.exchange, funding.symbol, LegacyInstrumentType.PERPETUAL
            )
            ticker = snapshot.ticker(
                funding.exchange, funding.symbol, LegacyInstrumentType.PERPETUAL
            )
            mark_price = funding.mark_price or (ticker.mark_price if ticker else None)
            index_price = funding.index_price or (ticker.index_price if ticker else None)
            if (
                instrument is None
                or funding.next_funding_time is None
                or mark_price is None
                or index_price is None
            ):
                market_data_dropped_total.labels(
                    funding.exchange, "incomplete_funding_snapshot"
                ).inc()
                continue
            interval_seconds = funding.funding_interval_hours * Decimal("3600")
            if interval_seconds != interval_seconds.to_integral_value():
                market_data_dropped_total.labels(
                    funding.exchange, "invalid_funding_interval"
                ).inc()
                continue
            funding_payload = FundingSnapshot(
                instrument=_legacy_instrument(instrument),
                funding_rate=funding.funding_rate,
                funding_interval_seconds=int(interval_seconds),
                next_funding_time=funding.next_funding_time,
                mark_price=mark_price,
                index_price=index_price,
                exchange_timestamp=funding.timestamp,
            )
            funding_rows.append(
                (
                    funding_payload,
                    f"native:{funding.exchange}:funding",
                    _polled_snapshot_sequence_id(
                        funding.symbol, funding.timestamp
                    ),
                    funding.exchange,
                )
            )
        funding_occurrences = _observation_occurrence_ids(
            base_occurrence_id,
            [
                (source, sequence_id, payload)
                for payload, source, sequence_id, _ in funding_rows
            ],
        )
        funding_events: list[tuple[EventEnvelope[Any], str, str]] = []
        for funding_row, funding_occurrence_id in zip(
            funding_rows, funding_occurrences, strict=True
        ):
            funding_event_payload, funding_source, funding_sequence_id, exchange = (
                funding_row
            )
            funding_events.append(
                (
                    _envelope(
                        funding_event_payload,
                        kind=EventKind.FUNDING_SNAPSHOT,
                        source=funding_source,
                        sequence_id=funding_sequence_id,
                        received_at=received_at,
                        received_monotonic_ns=observed_monotonic_ns,
                        occurrence_id=funding_occurrence_id,
                    ),
                    exchange,
                    "funding",
                )
            )
        await self._publish_native_snapshot_events(funding_events)
        unsupported_oi = {
            account.profile.venue
            for account in self.accounts
            if account.profile.instrument_type is not InstrumentType.SPOT
            and not account.exchange.has.get("fetchOpenInterest")
        }
        open_interest_rows: list[tuple[OpenInterestSnapshot, str, str, str]] = []
        for ticker in snapshot.tickers:
            if (
                ticker.exchange not in unsupported_oi
                or ticker.instrument_type is not LegacyInstrumentType.PERPETUAL
                or ticker.open_interest is None
            ):
                continue
            instrument = snapshot.instrument(
                ticker.exchange, ticker.symbol, LegacyInstrumentType.PERPETUAL
            )
            if instrument is None:
                continue
            oi_payload = OpenInterestSnapshot(
                instrument=_legacy_instrument(instrument),
                open_interest_base=ticker.open_interest,
                exchange_timestamp=ticker.timestamp,
            )
            open_interest_rows.append(
                (
                    oi_payload,
                    f"native:{ticker.exchange}:open-interest",
                    _polled_snapshot_sequence_id(ticker.symbol, ticker.timestamp),
                    ticker.exchange,
                )
            )
        open_interest_occurrences = _observation_occurrence_ids(
            base_occurrence_id,
            [
                (source, sequence_id, payload)
                for payload, source, sequence_id, _ in open_interest_rows
            ],
        )
        open_interest_events: list[tuple[EventEnvelope[Any], str, str]] = []
        for open_interest_row, open_interest_occurrence_id in zip(
            open_interest_rows, open_interest_occurrences, strict=True
        ):
            (
                open_interest_event_payload,
                open_interest_source,
                open_interest_sequence_id,
                exchange,
            ) = open_interest_row
            open_interest_events.append(
                (
                    _envelope(
                        open_interest_event_payload,
                        kind=EventKind.OPEN_INTEREST_SNAPSHOT,
                        source=open_interest_source,
                        sequence_id=open_interest_sequence_id,
                        received_at=received_at,
                        received_monotonic_ns=observed_monotonic_ns,
                        occurrence_id=open_interest_occurrence_id,
                    ),
                    exchange,
                    "open_interest",
                )
            )
        await self._publish_native_snapshot_events(open_interest_events)

    async def _publish_native_snapshot_events(
        self,
        events: Sequence[tuple[EventEnvelope[Any], str, str]],
    ) -> None:
        """Durably publish one bounded native snapshot without serial ACK waits."""

        async def publish_one(
            event: EventEnvelope[Any], exchange: str, stream: str
        ) -> None:
            await self.event_sink(event)
            public_events_total.labels(exchange, stream, "native").inc()

        results = await asyncio.gather(
            *(
                publish_one(event, exchange, stream)
                for event, exchange, stream in events
            ),
            return_exceptions=True,
        )
        failures = [
            result for result in results if isinstance(result, BaseException)
        ]
        if failures:
            primary = failures[0]
            for additional in failures[1:]:
                primary.add_note(
                    "concurrent native snapshot publication also failed: "
                    f"{type(additional).__name__}"
                )
            raise primary

    async def _publish(
        self,
        events: Sequence[EventEnvelope[Any]],
        profile: PublicEventProfile,
        stream: str,
        source: str,
    ) -> None:
        for event in events:
            await self.event_sink(event)
            public_events_total.labels(profile.venue, stream, source).inc()

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(seconds, 0.001))
        except TimeoutError:
            return

    def _track(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        logger.error(
            "public_event_supervisor_task_failed",
            extra={"error_type": type(error).__name__},
        )
        if isinstance(error, EventWriterFailed):
            self._stop_event.set()

    @staticmethod
    def _key(account: PublicEventAccount) -> tuple[str, str]:
        return account.profile.venue, account.profile.account


def public_event_profiles(venue: str) -> tuple[PublicEventProfile, ...]:
    spot = InstrumentType.SPOT
    perp = InstrumentType.PERPETUAL
    profiles: dict[str, tuple[PublicEventProfile, ...]] = {
        "binance": (
            PublicEventProfile("binance", "spot", "binance", "spot", spot),
            PublicEventProfile("binance", "linear", "binanceusdm", "future", perp),
        ),
        "bybit": (
            PublicEventProfile("bybit", "spot", "bybit", "spot", spot),
            PublicEventProfile("bybit", "linear", "bybit", "swap", perp),
        ),
        "gate": (
            PublicEventProfile("gate", "spot", "gate", "spot", spot),
            PublicEventProfile(
                "gate", "linear", "gate", "swap", perp,
                ohlcv_volume_in_contracts=True,
            ),
        ),
        "okx": (
            PublicEventProfile("okx", "spot", "okx", "spot", spot),
            PublicEventProfile(
                "okx",
                "linear",
                "okx",
                "swap",
                perp,
                {},
                open_interest_amount_in_contracts=True,
            ),
        ),
        "hyperliquid": (
            PublicEventProfile("hyperliquid", "linear", "hyperliquid", "swap", perp),
        ),
        "mexc": (
            PublicEventProfile("mexc", "spot", "mexc", "spot", spot),
            PublicEventProfile(
                "mexc", "linear", "mexc", "swap", perp,
                ohlcv_volume_in_contracts=True,
            ),
        ),
        "kucoin": (
            PublicEventProfile("kucoin", "spot", "kucoin", "spot", spot),
            PublicEventProfile(
                "kucoin",
                "linear",
                "kucoinfutures",
                "swap",
                perp,
                ohlcv_volume_in_contracts=True,
                open_interest_amount_in_contracts=True,
            ),
        ),
        "htx": (
            PublicEventProfile("htx", "spot", "htx", "spot", spot),
            PublicEventProfile(
                "htx", "linear", "htx", "swap", perp,
                open_interest_base_volume_in_contracts=True,
            ),
        ),
    }
    try:
        return profiles[venue.lower()]
    except KeyError as exc:
        raise ValueError(f"unsupported public event venue: {venue}") from exc


def create_public_event_supervisor(
    settings: Settings, event_sink: CanonicalEventSink
) -> PublicEventSupervisor:
    import ccxt.pro as ccxtpro  # type: ignore[import-untyped]

    venues = (
        settings.live_venue_values
        if settings.run_mode == "live"
        else settings.paper_venue_values
    )
    accounts: list[PublicEventAccount] = []
    for venue in venues:
        for profile in public_event_profiles(venue):
            options: dict[str, object] = {"defaultType": profile.default_type}
            if venue in {"binance", "bybit", "okx"}:
                options["adjustForTimeDifference"] = True
            config: dict[str, object] = {
                "enableRateLimit": True,
                "newUpdates": True,
                "timeout": int(settings.request_timeout_seconds * 1000),
                "options": options,
            }
            exchange = getattr(ccxtpro, profile.exchange_class)(config)
            if venue == "htx":
                exchange.urls["hostnames"]["contract"] = "api.hbdm.com"
            accounts.append(PublicEventAccount(profile, exchange))
    return PublicEventSupervisor(
        accounts,
        event_sink,
        symbol_limit=settings.public_event_symbol_limit_per_profile,
        rest_interval_seconds=settings.public_event_rest_interval_seconds,
        metadata_refresh_seconds=settings.public_metadata_refresh_seconds,
        quality_stream_retention_seconds=max(
            settings.orderbook_stream_stale_seconds * 3,
            settings.funding_snapshot_stale_seconds * 3,
        ),
        reconnect_initial_seconds=settings.public_event_reconnect_initial_seconds,
        reconnect_max_seconds=settings.public_event_reconnect_max_seconds,
    )


def _envelope(
    payload: Any,
    *,
    kind: EventKind,
    source: str,
    sequence_id: str,
    received_at: datetime,
    quality: DataQuality = DataQuality.VALID,
    received_monotonic_ns: int | None = None,
    occurrence_id: str | None = None,
) -> EventEnvelope[Any]:
    observed_monotonic_ns = (
        received_monotonic_ns
        if received_monotonic_ns is not None
        else monotonic_ns()
    )
    metadata = EventMetadata(
        event_id=deterministic_event_id(
            source=source,
            kind=kind,
            sequence_id=sequence_id,
            exchange_timestamp=payload.exchange_timestamp,
            payload=payload,
            occurrence_id=occurrence_id,
        ),
        exchange_timestamp=payload.exchange_timestamp,
        receive_timestamp=received_at,
        monotonic_ns=observed_monotonic_ns,
        sequence_id=sequence_id,
        source=source,
        correlation_id=f"{source}:{sequence_id}",
        payload_version=1,
        quality=quality,
    )
    return EventEnvelope[Any](kind=kind, metadata=metadata, payload=payload)


def _observation_occurrence_ids(
    base_occurrence_id: str,
    rows: Sequence[tuple[str, str, BaseModel]],
) -> tuple[str, ...]:
    """Disambiguate duplicate native identities without depending on row order."""

    grouped: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row_index, (source, native_identity, payload) in enumerate(rows):
        encoded = json.dumps(
            payload.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        payload_fingerprint = hashlib.sha256(encoded).hexdigest()
        grouped.setdefault((source, native_identity), []).append(
            (row_index, payload_fingerprint)
        )
    result = [base_occurrence_id] * len(rows)
    for duplicates in grouped.values():
        if len(duplicates) == 1:
            continue
        for duplicate_rank, (row_index, _) in enumerate(
            sorted(duplicates, key=lambda item: (item[1], item[0]))
        ):
            result[row_index] = f"{base_occurrence_id}:duplicate:{duplicate_rank}"
    return tuple(result)


def _polled_snapshot_sequence_id(
    symbol: str,
    exchange_timestamp: datetime,
) -> str:
    """Return a stable bounded identity for one venue snapshot stream point."""

    exchange_time = (
        exchange_timestamp
        if exchange_timestamp.tzinfo is not None
        else exchange_timestamp.replace(tzinfo=UTC)
    ).astimezone(UTC)
    symbol_digest = hashlib.sha256(symbol.encode()).hexdigest()[:24]
    return f"snapshot:{symbol_digest}:{int(exchange_time.timestamp() * 1_000_000)}"


def _instrument(
    venue: str, market: Mapping[str, Any], expected: InstrumentType
) -> InstrumentKey:
    actual = (
        InstrumentType.SPOT
        if market.get("spot")
        else InstrumentType.PERPETUAL
        if market.get("swap")
        else InstrumentType.FUTURE
        if market.get("future")
        else None
    )
    if actual is not expected:
        raise PublicDataNormalizationError("public event instrument type mismatch")
    base = _text(market.get("base"))
    quote = _text(market.get("quote"))
    exchange_symbol = _text(market.get("id"))
    if not base or not quote or not exchange_symbol:
        raise PublicDataNormalizationError("public event market identity is incomplete")
    expiry = market.get("expiry")
    return InstrumentKey(
        venue=venue,
        exchange_symbol=exchange_symbol,
        base_asset=base,
        quote_asset=quote,
        instrument_type=actual,
        settlement_asset=_text(market.get("settle")) or None,
        expiry=_milliseconds(expiry) if expiry is not None else None,
    )


def _legacy_instrument(instrument: Any) -> InstrumentKey:
    return InstrumentKey(
        venue=instrument.exchange,
        exchange_symbol=instrument.exchange_symbol,
        base_asset=instrument.base_asset,
        quote_asset=instrument.quote_asset,
        instrument_type=_legacy_type(instrument.instrument_type),
        settlement_asset=_text(instrument.settlement_asset) or None,
        expiry=instrument.expiry,
    )


def _legacy_type(value: LegacyInstrumentType) -> InstrumentType:
    return InstrumentType(value.value)


def _resolve_unified_symbol(
    exchange: Any, exchange_symbol: str, expected: InstrumentType
) -> str | None:
    candidates = next(
        (
            exchange.markets_by_id.get(candidate)
            for candidate in (
                exchange_symbol,
                exchange_symbol.upper(),
                exchange_symbol.lower(),
            )
            if exchange.markets_by_id.get(candidate) is not None
        ),
        None,
    )
    if isinstance(candidates, dict):
        candidates = [candidates]
    if not isinstance(candidates, list):
        return None
    for market in candidates:
        if not isinstance(market, dict):
            continue
        actual = (
            InstrumentType.SPOT
            if market.get("spot")
            else InstrumentType.PERPETUAL
            if market.get("swap")
            else InstrumentType.FUTURE
            if market.get("future")
            else None
        )
        symbol = _text(market.get("symbol"))
        if actual is expected and symbol:
            return symbol
    return None


def _rows(value: object) -> list[object]:
    if isinstance(value, list | tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    raise PublicDataNormalizationError("public update is not an object or list")


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PublicDataNormalizationError(f"{label} payload is not an object")
    return value


def _sequence(value: object, label: str) -> Sequence[object]:
    if not isinstance(value, list | tuple):
        raise PublicDataNormalizationError(f"{label} payload is not a sequence")
    return value


def _decimal(value: object | None, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PublicDataNormalizationError(f"{label} is invalid") from exc
    if not parsed.is_finite():
        raise PublicDataNormalizationError(f"{label} is not finite")
    return parsed


def _positive(value: object | None, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed <= 0:
        raise PublicDataNormalizationError(f"{label} is not positive")
    return parsed


def _non_negative(value: object | None, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed < 0:
        raise PublicDataNormalizationError(f"{label} is negative")
    return parsed


def _optional_non_negative(value: object | None) -> Decimal | None:
    if value is None or value == "":
        return None
    return _non_negative(value, "open interest")


def _contract_size(market: Mapping[str, Any]) -> Decimal:
    value = market.get("contractSize")
    if value is None:
        return Decimal("1")
    size = _positive(value, "contract size")
    return size


def _text(value: object | None) -> str:
    return str(value).strip() if value is not None else ""


def _trade_id(raw: Mapping[str, Any], *, venue: str) -> str:
    """Return the venue-native immutable trade identity.

    HTX WebSocket messages expose the exact trade identity inside the raw
    ``info`` object (``tradeId`` for spot and ``trade_id`` for derivatives).
    CCXT Pro currently routes the spot payload through a parser that does not
    consider the camel-case field and falls back to HTX's much larger aggregate
    ``id``.  HTX sends that aggregate ID as a JSON number that CCXT has already
    rounded to a float, so distinct trades can otherwise collapse to the same
    canonical event ID.
    """

    if venue.lower() == "htx":
        info = raw.get("info")
        if isinstance(info, Mapping):
            for key in ("tradeId", "trade_id", "trade-id"):
                native_id = _text(info.get(key))
                if native_id:
                    return native_id
    return _text(raw.get("id"))


def _milliseconds(value: object) -> datetime:
    milliseconds = _decimal(value, "timestamp")
    if milliseconds <= 0:
        raise PublicDataNormalizationError("timestamp is not positive")
    return datetime.fromtimestamp(float(milliseconds / 1000), tz=UTC)


def _exchange_time(
    raw: Mapping[str, Any], received_at: datetime
) -> tuple[datetime, DataQuality]:
    timestamp = raw.get("timestamp")
    if timestamp is None:
        return received_at, DataQuality.RECOVERING
    return _milliseconds(timestamp), DataQuality.VALID


def _side(value: object | None, *, required: bool) -> Side | None:
    normalized = _text(value).lower()
    if normalized == "buy":
        return Side.BUY
    if normalized == "sell":
        return Side.SELL
    if required:
        raise PublicDataNormalizationError("public event side is invalid")
    return None
