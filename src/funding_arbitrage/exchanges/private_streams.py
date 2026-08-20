"""Authenticated account streams normalized into the canonical event journal.

Private WebSocket updates reduce reaction latency, while periodic authenticated
REST reconciliation remains authoritative and closes reconnect gaps.  A stream
failure never causes an order retry; it only makes the live entry gate unhealthy.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic_ns
from typing import Any

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import (
    BalanceSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FillEvent,
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    OrderUpdate,
    PositionSnapshot,
    Side,
    deterministic_event_id,
)
from funding_arbitrage.execution.reconciliation import ReconciliationResult
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingAdapter,
    TradingOrderResult,
    VenueBalance,
    VenuePosition,
)
from funding_arbitrage.monitoring.metrics import (
    live_private_stream_events_total,
    live_private_stream_healthy,
    live_private_stream_normalization_errors_total,
    websocket_reconnects_total,
)

logger = logging.getLogger(__name__)

CanonicalEventSink = Callable[[EventEnvelope[Any]], Awaitable[None]]


@dataclass(frozen=True)
class PrivateStreamProfile:
    venue: str
    account: str
    exchange_class: str
    default_type: str
    watch_positions: bool
    positions_via_reconciliation: bool = False
    params: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class PrivateStreamAccount:
    profile: PrivateStreamProfile
    exchange: Any


@dataclass
class _ChannelState:
    active: bool = False
    last_message_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error_type: str | None = None


class PrivateStreamNormalizationError(ValueError):
    """A private venue payload cannot be represented without inventing data."""


class CcxtPrivateEventNormalizer:
    """Convert CCXT Pro unified private payloads into immutable domain events."""

    def __init__(self, venue: str, exchange: Any, account: str) -> None:
        self.venue = venue.lower()
        self.exchange = exchange
        self.account = account
        self._client_ids: dict[str, str] = {}

    def order_events(
        self, rows: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        return tuple(self._order_event(row, received_at) for row in _rows(rows))

    def fill_events(
        self, rows: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        return tuple(self._fill_event(row, received_at) for row in _rows(rows))

    def position_events(
        self, rows: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        return tuple(self._position_event(row, received_at) for row in _rows(rows))

    def balance_events(
        self, raw: object, *, received_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        if not isinstance(raw, dict):
            raise PrivateStreamNormalizationError("private balance payload is not an object")
        timestamp, quality = _exchange_time(raw, received_at)
        free = _decimal_map(raw.get("free"))
        used = _decimal_map(raw.get("used"))
        total = _decimal_map(raw.get("total"))
        debt = _decimal_map(raw.get("debt"))
        assets = sorted(set(free) | set(used) | set(total) | set(debt))
        events: list[EventEnvelope[Any]] = []
        for asset in assets:
            available = free.get(asset, Decimal("0"))
            locked = used.get(asset, Decimal("0"))
            amount = total.get(asset, available + locked)
            borrowed = debt.get(asset, Decimal("0"))
            payload = BalanceSnapshot(
                venue=self.venue,
                asset=asset,
                total=amount,
                available=available,
                locked=locked,
                borrowed=borrowed,
                exchange_timestamp=timestamp,
            )
            events.append(
                _envelope(
                    payload,
                    kind=EventKind.BALANCE_SNAPSHOT,
                    source=self._source("balance", "CCXT_PRO"),
                    sequence_id=(
                        f"balance:{asset}:{_timestamp_token(timestamp)}:"
                        f"{amount}:{available}:{locked}:{borrowed}"
                    ),
                    correlation_id=f"account:{self.venue.upper()}:{asset}",
                    received_at=received_at,
                    quality=quality,
                )
            )
        return tuple(events)

    def reconciliation_events(
        self,
        *,
        balance: VenueBalance | None,
        positions: Sequence[VenuePosition],
        orders: Sequence[TradingOrderResult],
        observed_at: datetime,
    ) -> tuple[EventEnvelope[Any], ...]:
        events: list[EventEnvelope[Any]] = []
        if balance is not None:
            events.extend(self._reconciled_balance_events(balance, observed_at))
        events.extend(self._reconciled_position_event(row, observed_at) for row in positions)
        events.extend(self._reconciled_order_event(row, observed_at) for row in orders)
        return tuple(events)

    def _order_event(self, raw: object, received_at: datetime) -> EventEnvelope[Any]:
        row = _object(raw, "private order")
        market = self._market(row.get("symbol"))
        instrument = _instrument(self.venue, market)
        exchange_order_id = _text(row.get("id"))
        client_order_id = _client_order_id(row)
        if not client_order_id:
            client_order_id = f"external:{exchange_order_id or 'unknown'}"
        if exchange_order_id:
            self._client_ids[exchange_order_id] = client_order_id
        requested = _base_quantity(row.get("amount"), market)
        filled = _base_quantity(row.get("filled"), market, allow_zero=True)
        requested = max(requested, filled)
        if requested <= 0:
            raise PrivateStreamNormalizationError("private order has no positive quantity")
        timestamp, quality = _exchange_time(row, received_at)
        status = _order_status(row.get("status"), filled, requested)
        payload = OrderUpdate(
            instrument=instrument,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status=status,
            side=_side(row.get("side")),
            order_type=_order_type(row.get("type")),
            requested_quantity=requested,
            filled_quantity=filled,
            limit_price=_positive_or_none(row.get("price")),
            average_fill_price=_positive_or_none(row.get("average")),
            reduce_only=bool(row.get("reduceOnly") or False),
            rejection_reason=_text(row.get("rejectReason")),
            exchange_timestamp=timestamp,
        )
        order_identity = exchange_order_id or client_order_id
        return _envelope(
            payload,
            kind=EventKind.ORDER_UPDATE,
            source=self._source("orders", "CCXT_PRO"),
            sequence_id=(
                f"order:{order_identity}:{_timestamp_token(timestamp)}:"
                f"{status.value}:{filled}"
            ),
            correlation_id=f"order:{self.venue.upper()}:{client_order_id}",
            received_at=received_at,
            quality=quality,
        )

    def _fill_event(self, raw: object, received_at: datetime) -> EventEnvelope[Any]:
        row = _object(raw, "private fill")
        market = self._market(row.get("symbol"))
        instrument = _instrument(self.venue, market)
        fill_id = _text(row.get("id"))
        exchange_order_id = _text(row.get("order"))
        if not fill_id or not exchange_order_id:
            raise PrivateStreamNormalizationError("private fill lacks trade or order identity")
        client_order_id = _client_order_id(row) or self._client_ids.get(exchange_order_id)
        client_order_id = client_order_id or f"external:{exchange_order_id}"
        timestamp, quality = _exchange_time(row, received_at)
        raw_fee = row.get("fee")
        fee: dict[str, Any] = raw_fee if isinstance(raw_fee, dict) else {}
        fee_cost = abs(_decimal(fee.get("cost")))
        fee_asset = _text(fee.get("currency")) or _text(market.get("settle"))
        fee_asset = fee_asset or _text(market.get("quote"))
        if not fee_asset:
            raise PrivateStreamNormalizationError("private fill fee asset is unavailable")
        payload = FillEvent(
            instrument=instrument,
            fill_id=fill_id,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side=_side(row.get("side")),
            price=_positive(row.get("price"), "private fill price"),
            quantity=_base_quantity(row.get("amount"), market),
            fee_amount=fee_cost,
            fee_asset=fee_asset,
            liquidity_role=_liquidity_role(row.get("takerOrMaker")),
            exchange_timestamp=timestamp,
        )
        return _envelope(
            payload,
            kind=EventKind.FILL,
            source=self._source("fills", "CCXT_PRO"),
            sequence_id=f"fill:{fill_id}",
            correlation_id=f"order:{self.venue.upper()}:{client_order_id}",
            received_at=received_at,
            quality=quality,
        )

    def _position_event(self, raw: object, received_at: datetime) -> EventEnvelope[Any]:
        row = _object(raw, "private position")
        market = self._market(row.get("symbol"))
        instrument = _instrument(self.venue, market)
        timestamp, quality = _exchange_time(row, received_at)
        contracts = _decimal(row.get("contracts"))
        base_quantity = abs(contracts) * _contract_size(market)
        side = _text(row.get("side")).lower()
        signed = -base_quantity if side == "short" else base_quantity
        if not side and contracts < 0:
            signed = -base_quantity
        mark = _positive_or_none(row.get("markPrice")) or _positive_or_none(
            row.get("entryPrice")
        )
        if mark is None:
            raise PrivateStreamNormalizationError("private position has no positive mark price")
        payload = PositionSnapshot(
            instrument=instrument,
            signed_quantity=signed,
            entry_price=_positive_or_none(row.get("entryPrice")),
            mark_price=mark,
            unrealized_pnl=_decimal(row.get("unrealizedPnl")),
            realized_pnl=_decimal(row.get("realizedPnl")),
            leverage=_positive_or_none(row.get("leverage")) or Decimal("1"),
            liquidation_price=_positive_or_none(row.get("liquidationPrice")),
            margin_used=max(_decimal(row.get("initialMargin")), Decimal("0")),
            exchange_timestamp=timestamp,
        )
        return _envelope(
            payload,
            kind=EventKind.POSITION_SNAPSHOT,
            source=self._source("positions", "CCXT_PRO"),
            sequence_id=(
                f"position:{instrument.exchange_symbol}:{_timestamp_token(timestamp)}:"
                f"{signed}:{payload.unrealized_pnl}"
            ),
            correlation_id=f"position:{instrument.canonical_id}",
            received_at=received_at,
            quality=quality,
        )

    def _reconciled_balance_events(
        self, balance: VenueBalance, observed_at: datetime
    ) -> tuple[EventEnvelope[Any], ...]:
        assets = sorted(set(balance.free) | set(balance.used) | set(balance.total))
        events: list[EventEnvelope[Any]] = []
        timestamp = _utc(balance.timestamp)
        for asset in assets:
            available = balance.free.get(asset, Decimal("0"))
            locked = balance.used.get(asset, Decimal("0"))
            total = balance.total.get(asset, available + locked)
            payload = BalanceSnapshot(
                venue=self.venue,
                asset=asset,
                total=total,
                available=available,
                locked=locked,
                exchange_timestamp=timestamp,
            )
            events.append(
                _envelope(
                    payload,
                    kind=EventKind.BALANCE_SNAPSHOT,
                    source=self._source("balance", "PRIVATE_REST_RECONCILIATION"),
                    sequence_id=(
                        f"balance:{asset}:{_timestamp_token(timestamp)}:"
                        f"{total}:{available}:{locked}"
                    ),
                    correlation_id=f"account:{self.venue.upper()}:{asset}",
                    received_at=observed_at,
                    quality=DataQuality.RECOVERING,
                )
            )
        return tuple(events)

    def _reconciled_position_event(
        self, row: VenuePosition, observed_at: datetime
    ) -> EventEnvelope[Any]:
        market = self._market(row.exchange_symbol)
        instrument = _instrument(self.venue, market, expected_type=row.instrument_type.value)
        mark = row.mark_price or row.entry_price
        if mark is None:
            raise PrivateStreamNormalizationError("reconciled position has no mark price")
        payload = PositionSnapshot(
            instrument=instrument,
            signed_quantity=row.signed_quantity,
            entry_price=row.entry_price,
            mark_price=mark,
            unrealized_pnl=row.unrealized_pnl,
            exchange_timestamp=observed_at,
        )
        return _envelope(
            payload,
            kind=EventKind.POSITION_SNAPSHOT,
            source=self._source("positions", "PRIVATE_REST_RECONCILIATION"),
            sequence_id=(
                f"position:{instrument.exchange_symbol}:{_timestamp_token(observed_at)}:"
                f"{row.signed_quantity}:{row.unrealized_pnl}"
            ),
            correlation_id=f"position:{instrument.canonical_id}",
            received_at=observed_at,
            quality=DataQuality.RECOVERING,
        )

    def _reconciled_order_event(
        self, row: TradingOrderResult, observed_at: datetime
    ) -> EventEnvelope[Any]:
        market = self._market(row.exchange_symbol)
        instrument = _instrument(self.venue, market, expected_type=row.instrument_type.value)
        status = _live_order_status(row.status)
        payload = OrderUpdate(
            instrument=instrument,
            client_order_id=row.client_order_id,
            exchange_order_id=row.exchange_order_id,
            status=status,
            side=_side(row.side),
            order_type=OrderType.LIMIT,
            requested_quantity=row.requested_base_quantity,
            filled_quantity=row.filled_base_quantity,
            average_fill_price=row.average_price,
            reduce_only=row.reduce_only,
            exchange_timestamp=_utc(row.timestamp),
        )
        if row.exchange_order_id:
            self._client_ids[row.exchange_order_id] = row.client_order_id
        return _envelope(
            payload,
            kind=EventKind.ORDER_UPDATE,
            source=self._source("orders", "PRIVATE_REST_RECONCILIATION"),
            sequence_id=(
                f"order:{row.exchange_order_id or row.client_order_id}:"
                f"{_timestamp_token(payload.exchange_timestamp)}:{status.value}:"
                f"{row.filled_base_quantity}"
            ),
            correlation_id=f"order:{self.venue.upper()}:{row.client_order_id}",
            received_at=observed_at,
            quality=DataQuality.RECOVERING,
        )

    def _market(self, symbol: object) -> dict[str, Any]:
        name = _text(symbol)
        if not name:
            raise PrivateStreamNormalizationError("private event symbol is unavailable")
        try:
            market = self.exchange.market(name)
        except Exception:
            market = None
        if isinstance(market, dict):
            return market
        markets = getattr(self.exchange, "markets", {}) or {}
        for candidate in markets.values():
            if isinstance(candidate, dict) and name in {
                _text(candidate.get("id")),
                _text(candidate.get("symbol")),
            }:
                return candidate
        raise PrivateStreamNormalizationError(f"private event market is unresolved: {name}")

    def _source(self, stream: str, transport: str) -> str:
        return f"{self.venue.upper()}.PRIVATE.{self.account.upper()}.{stream.upper()}.{transport}"


class PrivateStreamSupervisor:
    """Own private stream tasks and expose a synchronous fail-closed entry gate."""

    def __init__(
        self,
        accounts: Sequence[PrivateStreamAccount],
        adapters: Mapping[str, TradingAdapter],
        event_sink: CanonicalEventSink,
        *,
        reconciliation_max_age_seconds: float,
        reconnect_initial_seconds: float,
        reconnect_max_seconds: float,
    ) -> None:
        if not accounts:
            raise ValueError("at least one private stream account is required")
        if reconciliation_max_age_seconds <= 0:
            raise ValueError("private reconciliation max age must be positive")
        if not 0 < reconnect_initial_seconds <= reconnect_max_seconds:
            raise ValueError("private stream reconnect bounds are invalid")
        self.accounts = tuple(accounts)
        self.adapters = dict(adapters)
        self.event_sink = event_sink
        self.reconciliation_max_age_seconds = reconciliation_max_age_seconds
        self.reconnect_initial_seconds = reconnect_initial_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self._normalizers = {
            (account.profile.venue, account.profile.account): CcxtPrivateEventNormalizer(
                account.profile.venue, account.exchange, account.profile.account
            )
            for account in self.accounts
        }
        self._states: dict[tuple[str, str, str], _ChannelState] = {}
        self._tasks: dict[tuple[str, str, str], asyncio.Task[None]] = {}
        self._last_reconciliation_at: datetime | None = None
        self._started = False
        self._stopping = False

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("private stream supervisor already started")
        covered = {account.profile.venue for account in self.accounts}
        if covered != set(self.adapters):
            raise ValueError("private stream venue coverage does not match trading adapters")
        exchanges = _unique_exchanges(self.accounts)
        try:
            for exchange in exchanges:
                exchange.check_required_credentials()
            await asyncio.gather(
                *(exchange.load_markets(reload=True) for exchange in exchanges)
            )
        except BaseException:
            await asyncio.gather(
                *(exchange.close() for exchange in exchanges), return_exceptions=True
            )
            raise
        self._stopping = False
        self._started = True
        for account in self.accounts:
            profile = account.profile
            required = ("orders", "fills", "balance")
            for stream in required:
                self._start_channel(account, stream)
            if profile.watch_positions:
                if not bool(account.exchange.has.get("watchPositions")):
                    if not profile.positions_via_reconciliation:
                        await self.stop()
                        raise RuntimeError(
                            f"{profile.venue}:{profile.account} has no private position stream"
                        )
                else:
                    self._start_channel(account, "positions")
        self._set_health_metrics()

    async def stop(self) -> None:
        if not self._started:
            return
        self._stopping = True
        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(
            *(exchange.close() for exchange in _unique_exchanges(self.accounts)),
            return_exceptions=True,
        )
        self._tasks.clear()
        self._started = False
        self._set_health_metrics()

    async def ingest_reconciliation(
        self, result: ReconciliationResult, *, observed_at: datetime
    ) -> None:
        if not self._started:
            raise RuntimeError("private stream supervisor is not started")
        if not result.passed:
            raise RuntimeError("cannot journal a failed private reconciliation as healthy")
        normalized_at = _utc(observed_at)
        for venue in sorted(self.adapters):
            balance_normalizer = self._venue_normalizer(venue)
            events: list[EventEnvelope[Any]] = list(
                balance_normalizer.reconciliation_events(
                    balance=result.balances.get(venue),
                    positions=(),
                    orders=(),
                    observed_at=normalized_at,
                )
            )
            for position in result.positions:
                if position.exchange == venue:
                    events.extend(
                        self._venue_normalizer(
                            venue, position.instrument_type.value
                        ).reconciliation_events(
                            balance=None,
                            positions=(position,),
                            orders=(),
                            observed_at=normalized_at,
                        )
                    )
            for order in result.open_orders:
                if order.exchange == venue:
                    events.extend(
                        self._venue_normalizer(
                            venue, order.instrument_type.value
                        ).reconciliation_events(
                            balance=None,
                            positions=(),
                            orders=(order,),
                            observed_at=normalized_at,
                        )
                    )
            await self._publish(events, venue=venue, stream="reconciliation", source="rest")
        self._last_reconciliation_at = normalized_at
        self._set_health_metrics(now=normalized_at)

    def health(self, now: datetime | None = None) -> tuple[bool, str | None]:
        current = _utc(now or datetime.now(UTC))
        for venue in sorted(self.adapters):
            healthy, reason = self._venue_health(venue, current)
            if not healthy:
                return False, reason
        return True, None

    def _venue_health(self, venue: str, current: datetime) -> tuple[bool, str | None]:
        if not self._started or self._stopping:
            return False, "private_stream_supervisor_not_running"
        if self._last_reconciliation_at is None:
            return False, "private_stream_reconciliation_missing"
        age = (current - self._last_reconciliation_at).total_seconds()
        if age < 0 or age > self.reconciliation_max_age_seconds:
            return False, "private_stream_reconciliation_stale"
        for key, task in self._tasks.items():
            if key[0] != venue:
                continue
            if task.done():
                return False, "private_stream_task_stopped:" + ":".join(key)
            state = self._states[key]
            if not state.active:
                return False, "private_stream_reconnecting:" + ":".join(key)
        return True, None

    def snapshot(self, now: datetime | None = None) -> dict[str, object]:
        healthy, reason = self.health(now)
        return {
            "healthy": healthy,
            "reason": reason,
            "last_reconciliation_at": (
                self._last_reconciliation_at.isoformat()
                if self._last_reconciliation_at is not None
                else None
            ),
            "channels": {
                ":".join(key): {
                    "active": state.active,
                    "last_message_at": (
                        state.last_message_at.isoformat()
                        if state.last_message_at is not None
                        else None
                    ),
                    "last_error_at": (
                        state.last_error_at.isoformat()
                        if state.last_error_at is not None
                        else None
                    ),
                    "last_error_type": state.last_error_type,
                }
                for key, state in sorted(self._states.items())
            },
        }

    def _start_channel(self, account: PrivateStreamAccount, stream: str) -> None:
        profile = account.profile
        capability = {
            "orders": "watchOrders",
            "fills": "watchMyTrades",
            "balance": "watchBalance",
            "positions": "watchPositions",
        }[stream]
        if not bool(account.exchange.has.get(capability)):
            raise RuntimeError(
                f"{profile.venue}:{profile.account} lacks required {capability} capability"
            )
        key = (profile.venue, profile.account, stream)
        self._states[key] = _ChannelState(active=True)
        self._tasks[key] = asyncio.create_task(
            self._watch_loop(account, stream, key),
            name="private-" + "-".join(key),
        )

    async def _watch_loop(
        self,
        account: PrivateStreamAccount,
        stream: str,
        key: tuple[str, str, str],
    ) -> None:
        method_name = {
            "orders": "watch_orders",
            "fills": "watch_my_trades",
            "balance": "watch_balance",
            "positions": "watch_positions",
        }[stream]
        method = getattr(account.exchange, method_name)
        state = self._states[key]
        delay = self.reconnect_initial_seconds
        while not self._stopping:
            state.active = True
            try:
                raw = await method(params=dict(account.profile.params))
                received_at = datetime.now(UTC)
                events = self._normalize(account, stream, raw, received_at)
                await self._publish(
                    events,
                    venue=account.profile.venue,
                    stream=stream,
                    source="websocket",
                )
                state.last_message_at = received_at
                state.last_error_at = None
                state.last_error_type = None
                delay = self.reconnect_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.active = False
                state.last_error_at = datetime.now(UTC)
                state.last_error_type = type(exc).__name__
                websocket_reconnects_total.labels(account.profile.venue).inc()
                if isinstance(exc, PrivateStreamNormalizationError):
                    live_private_stream_normalization_errors_total.labels(
                        account.profile.venue, stream
                    ).inc()
                logger.warning(
                    "private_stream_reconnecting",
                    extra={
                        "exchange": account.profile.venue,
                        "account": account.profile.account,
                        "stream": stream,
                        "error_type": type(exc).__name__,
                    },
                )
                self._set_health_metrics()
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.reconnect_max_seconds)

    def _normalize(
        self,
        account: PrivateStreamAccount,
        stream: str,
        raw: object,
        received_at: datetime,
    ) -> tuple[EventEnvelope[Any], ...]:
        normalizer = self._normalizers[
            (account.profile.venue, account.profile.account)
        ]
        if stream == "orders":
            return normalizer.order_events(raw, received_at=received_at)
        if stream == "fills":
            return normalizer.fill_events(raw, received_at=received_at)
        if stream == "balance":
            return normalizer.balance_events(raw, received_at=received_at)
        return normalizer.position_events(raw, received_at=received_at)

    async def _publish(
        self,
        events: Sequence[EventEnvelope[Any]],
        *,
        venue: str,
        stream: str,
        source: str,
    ) -> None:
        for event in events:
            await self.event_sink(event)
            live_private_stream_events_total.labels(venue, stream, source).inc()

    def _venue_normalizer(
        self, venue: str, instrument_type: str | None = None
    ) -> CcxtPrivateEventNormalizer:
        for account in self.accounts:
            profile = account.profile
            derivative = profile.default_type in {"future", "swap"}
            expected_derivative = instrument_type in {"PERPETUAL", "FUTURE"}
            if profile.venue == venue and (
                instrument_type is None or derivative == expected_derivative
            ):
                return self._normalizers[(profile.venue, profile.account)]
        raise RuntimeError(f"no private stream normalizer for {venue}")

    def _set_health_metrics(self, now: datetime | None = None) -> None:
        current = _utc(now or datetime.now(UTC))
        for venue in self.adapters:
            healthy, _ = self._venue_health(venue, current)
            live_private_stream_healthy.labels(venue).set(1 if healthy else 0)


def private_stream_profiles(venue: str) -> tuple[PrivateStreamProfile, ...]:
    """Return the explicit account topology used for every supported live venue."""

    profiles: dict[str, tuple[PrivateStreamProfile, ...]] = {
        "binance": (
            PrivateStreamProfile(
                "binance", "spot", "binance", "spot", False, params={"type": "spot"}
            ),
            PrivateStreamProfile(
                "binance",
                "linear",
                "binance",
                "future",
                True,
                params={"type": "future"},
            ),
        ),
        "bybit": (
            PrivateStreamProfile("bybit", "unified", "bybit", "swap", True),
        ),
        "gate": (
            PrivateStreamProfile(
                "gate", "spot", "gate", "spot", False, params={"type": "spot"}
            ),
            PrivateStreamProfile(
                "gate",
                "linear",
                "gate",
                "swap",
                True,
                params={"type": "swap", "settle": "usdt"},
            ),
        ),
        "okx": (PrivateStreamProfile("okx", "unified", "okx", "swap", True),),
        "hyperliquid": (
            PrivateStreamProfile("hyperliquid", "unified", "hyperliquid", "swap", True),
        ),
        "mexc": (
            PrivateStreamProfile("mexc", "spot", "mexc", "spot", False, params={"type": "spot"}),
            PrivateStreamProfile(
                "mexc", "linear", "mexc", "swap", True, True, params={"type": "swap"}
            ),
        ),
        "kucoin": (
            PrivateStreamProfile("kucoin", "spot", "kucoin", "spot", False),
            PrivateStreamProfile("kucoin", "linear", "kucoinfutures", "swap", True),
        ),
        "htx": (
            PrivateStreamProfile("htx", "spot", "htx", "spot", False, params={"type": "spot"}),
            PrivateStreamProfile(
                "htx",
                "linear",
                "htx",
                "swap",
                True,
                True,
                params={"type": "swap", "subType": "linear"},
            ),
        ),
    }
    try:
        return profiles[venue.lower()]
    except KeyError as exc:
        raise ValueError(f"unsupported private stream venue: {venue}") from exc


def create_private_stream_supervisor(
    settings: Settings,
    adapters: Mapping[str, TradingAdapter],
    event_sink: CanonicalEventSink,
) -> PrivateStreamSupervisor:
    """Create CCXT Pro clients only after strict live configuration validation."""

    import ccxt.pro as ccxtpro  # type: ignore[import-untyped]

    accounts: list[PrivateStreamAccount] = []
    for venue in settings.live_venue_values:
        credentials = settings.live_credentials(venue)
        for profile in private_stream_profiles(venue):
            options: dict[str, object] = {"defaultType": profile.default_type}
            if venue in {"binance", "bybit", "okx"}:
                options["adjustForTimeDifference"] = True
            if venue == "kucoin":
                options["uta"] = False
            config: dict[str, object] = {
                **credentials,
                "enableRateLimit": True,
                "newUpdates": True,
                "timeout": int(settings.request_timeout_seconds * 1000),
                "options": options,
            }
            exchange_class = getattr(ccxtpro, profile.exchange_class)
            exchange = exchange_class(config)
            if venue == "htx":
                exchange.urls["hostnames"]["contract"] = "api.hbdm.com"
            if settings.live_sandbox:
                exchange.set_sandbox_mode(True)
            accounts.append(PrivateStreamAccount(profile=profile, exchange=exchange))
    return PrivateStreamSupervisor(
        accounts,
        adapters,
        event_sink,
        reconciliation_max_age_seconds=(
            settings.live_private_stream_reconciliation_max_age_seconds
        ),
        reconnect_initial_seconds=settings.live_private_stream_reconnect_initial_seconds,
        reconnect_max_seconds=settings.live_private_stream_reconnect_max_seconds,
    )


def _envelope(
    payload: Any,
    *,
    kind: EventKind,
    source: str,
    sequence_id: str,
    correlation_id: str,
    received_at: datetime,
    quality: DataQuality,
) -> EventEnvelope[Any]:
    metadata = EventMetadata(
        event_id=deterministic_event_id(
            source=source,
            kind=kind,
            sequence_id=sequence_id,
            exchange_timestamp=payload.exchange_timestamp,
            payload=payload,
        ),
        exchange_timestamp=payload.exchange_timestamp,
        receive_timestamp=received_at,
        monotonic_ns=monotonic_ns(),
        sequence_id=sequence_id,
        source=source,
        correlation_id=correlation_id,
        payload_version=1,
        quality=quality,
    )
    return EventEnvelope[Any](kind=kind, metadata=metadata, payload=payload)


def _instrument(
    venue: str, market: Mapping[str, Any], *, expected_type: str | None = None
) -> InstrumentKey:
    if market.get("spot"):
        instrument_type = InstrumentType.SPOT
    elif market.get("swap"):
        instrument_type = InstrumentType.PERPETUAL
    elif market.get("future"):
        instrument_type = InstrumentType.FUTURE
    else:
        raise PrivateStreamNormalizationError("private event market type is unsupported")
    if expected_type is not None and instrument_type.value != expected_type:
        raise PrivateStreamNormalizationError("private event instrument type mismatch")
    base = _text(market.get("base"))
    quote = _text(market.get("quote"))
    exchange_symbol = _text(market.get("id"))
    if not base or not quote or not exchange_symbol:
        raise PrivateStreamNormalizationError("private event market identity is incomplete")
    expiry = market.get("expiry")
    expiry_time = _milliseconds(expiry) if expiry is not None else None
    return InstrumentKey(
        venue=venue,
        exchange_symbol=exchange_symbol,
        base_asset=base,
        quote_asset=quote,
        instrument_type=instrument_type,
        settlement_asset=_text(market.get("settle")) or None,
        expiry=expiry_time,
    )


def _rows(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    raise PrivateStreamNormalizationError("private stream update is not an object or list")


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PrivateStreamNormalizationError(f"{label} payload is not an object")
    return value


def _decimal(value: object | None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise PrivateStreamNormalizationError("private numeric value is invalid") from exc
    if not parsed.is_finite():
        raise PrivateStreamNormalizationError("private numeric value is not finite")
    return parsed


def _positive(value: object | None, label: str) -> Decimal:
    parsed = _decimal(value)
    if parsed <= 0:
        raise PrivateStreamNormalizationError(f"{label} is not positive")
    return parsed


def _positive_or_none(value: object | None) -> Decimal | None:
    parsed = _decimal(value)
    return parsed if parsed > 0 else None


def _decimal_map(value: object) -> dict[str, Decimal]:
    if not isinstance(value, dict):
        return {}
    return {
        str(asset).upper(): _decimal(amount)
        for asset, amount in value.items()
        if amount is not None
    }


def _contract_size(market: Mapping[str, Any]) -> Decimal:
    value = _decimal(market.get("contractSize"))
    return value if value > 0 else Decimal("1")


def _base_quantity(
    value: object | None,
    market: Mapping[str, Any],
    *,
    allow_zero: bool = False,
) -> Decimal:
    amount = _decimal(value)
    if amount < 0 or (amount == 0 and not allow_zero):
        raise PrivateStreamNormalizationError("private quantity is not positive")
    return amount * _contract_size(market)


def _text(value: object | None) -> str:
    return str(value).strip() if value is not None else ""


def _client_order_id(row: Mapping[str, Any]) -> str:
    direct = _text(row.get("clientOrderId"))
    if direct:
        return direct
    info = row.get("info")
    if not isinstance(info, dict):
        return ""
    for key in ("clientOrderId", "clientOid", "clOrdId", "orderLinkId", "newClientOrderId"):
        candidate = _text(info.get(key))
        if candidate:
            return candidate
    return ""


def _side(value: object | None) -> Side:
    side = _text(value).upper()
    if side == "BUY":
        return Side.BUY
    if side == "SELL":
        return Side.SELL
    raise PrivateStreamNormalizationError("private side is neither buy nor sell")


def _order_type(value: object | None) -> OrderType:
    normalized = _text(value).lower().replace("-", "_")
    return {
        "market": OrderType.MARKET,
        "limit": OrderType.LIMIT,
        "stop": OrderType.STOP,
        "stop_limit": OrderType.STOP_LIMIT,
        "take_profit": OrderType.TAKE_PROFIT,
    }.get(normalized, OrderType.LIMIT)


def _order_status(value: object | None, filled: Decimal, requested: Decimal) -> OrderStatus:
    normalized = _text(value).lower()
    if normalized in {"open", "new"}:
        return OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.ACKNOWLEDGED
    if normalized in {"closed", "filled"}:
        return OrderStatus.FILLED if filled >= requested else OrderStatus.CANCELLED
    if normalized in {"canceled", "cancelled"}:
        return OrderStatus.CANCELLED
    if normalized == "rejected":
        return OrderStatus.REJECTED
    if normalized == "expired":
        return OrderStatus.EXPIRED
    if normalized in {"pending", "submitting"}:
        return OrderStatus.SUBMITTING
    return OrderStatus.UNKNOWN


def _live_order_status(value: LiveOrderStatus) -> OrderStatus:
    return {
        LiveOrderStatus.PENDING: OrderStatus.SUBMITTING,
        LiveOrderStatus.OPEN: OrderStatus.ACKNOWLEDGED,
        LiveOrderStatus.PARTIAL: OrderStatus.PARTIALLY_FILLED,
        LiveOrderStatus.FILLED: OrderStatus.FILLED,
        LiveOrderStatus.CANCELED: OrderStatus.CANCELLED,
        LiveOrderStatus.REJECTED: OrderStatus.REJECTED,
        LiveOrderStatus.UNKNOWN: OrderStatus.UNKNOWN,
    }[value]


def _liquidity_role(value: object | None) -> LiquidityRole:
    normalized = _text(value).lower()
    if normalized == "maker":
        return LiquidityRole.MAKER
    if normalized == "taker":
        return LiquidityRole.TAKER
    return LiquidityRole.UNKNOWN


def _exchange_time(
    row: Mapping[str, Any], received_at: datetime
) -> tuple[datetime, DataQuality]:
    timestamp = row.get("timestamp")
    if timestamp is not None:
        return _milliseconds(timestamp), DataQuality.VALID
    raw_datetime = row.get("datetime")
    if raw_datetime:
        try:
            parsed = datetime.fromisoformat(str(raw_datetime).replace("Z", "+00:00"))
        except ValueError as exc:
            raise PrivateStreamNormalizationError("private timestamp is invalid") from exc
        return _utc(parsed), DataQuality.VALID
    return _utc(received_at), DataQuality.RECOVERING


def _milliseconds(value: object) -> datetime:
    try:
        numeric = Decimal(str(value))
    except Exception as exc:
        raise PrivateStreamNormalizationError("private timestamp is invalid") from exc
    return datetime.fromtimestamp(float(numeric / Decimal("1000")), tz=UTC)


def _timestamp_token(value: datetime) -> int:
    return int(_utc(value).timestamp() * 1000)


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


def _unique_exchanges(accounts: Sequence[PrivateStreamAccount]) -> tuple[Any, ...]:
    seen: set[int] = set()
    output: list[Any] = []
    for account in accounts:
        identity = id(account.exchange)
        if identity not in seen:
            seen.add(identity)
            output.append(account.exchange)
    return tuple(output)
