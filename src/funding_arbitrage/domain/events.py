"""Canonical event contracts shared by runtime, storage, and deterministic replay."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SEQUENCE_ID_MAX_LENGTH = 128
_INSTRUMENT_DISCRIMINATOR_HEX_LENGTH = 32


class TradingMode(StrEnum):
    BACKTEST = "BACKTEST"
    REPLAY = "REPLAY"
    SHADOW = "SHADOW"
    PAPER = "PAPER"
    LIMITED_LIVE = "LIMITED_LIVE"
    LIVE = "LIVE"
    SAFE_MODE = "SAFE_MODE"


class DataQuality(StrEnum):
    VALID = "VALID"
    STALE = "STALE"
    GAP = "GAP"
    CROSSED = "CROSSED"
    INVALID = "INVALID"
    RECOVERING = "RECOVERING"
    UNAVAILABLE = "UNAVAILABLE"


class InstrumentType(StrEnum):
    SPOT = "SPOT"
    PERPETUAL = "PERPETUAL"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    DEX_POOL = "DEX_POOL"


class OptionRight(StrEnum):
    CALL = "CALL"
    PUT = "PUT"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


class BookDeltaAction(StrEnum):
    UPSERT = "UPSERT"
    DELETE = "DELETE"


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"


class OrderStatus(StrEnum):
    NEW = "NEW"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"


class LiquidityRole(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"
    UNKNOWN = "UNKNOWN"


class EventKind(StrEnum):
    TRADE_TICK = "TRADE_TICK"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    CANDLE = "CANDLE"
    FUNDING_SNAPSHOT = "FUNDING_SNAPSHOT"
    OPEN_INTEREST_SNAPSHOT = "OPEN_INTEREST_SNAPSHOT"
    LIQUIDATION_TICK = "LIQUIDATION_TICK"
    ORDER_UPDATE = "ORDER_UPDATE"
    FILL = "FILL"
    POSITION_SNAPSHOT = "POSITION_SNAPSHOT"
    BALANCE_SNAPSHOT = "BALANCE_SNAPSHOT"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class InstrumentKey(BaseModel):
    """Stable venue instrument identity; raw exchange symbols remain available."""

    model_config = ConfigDict(frozen=True)

    venue: str = Field(min_length=1)
    exchange_symbol: str = Field(min_length=1)
    base_asset: str = Field(min_length=1)
    quote_asset: str = Field(min_length=1)
    instrument_type: InstrumentType
    settlement_asset: str | None = None
    expiry: datetime | None = None
    strike_price: Decimal | None = Field(default=None, gt=0)
    option_right: OptionRight | None = None

    @field_validator("venue", "exchange_symbol", "base_asset", "quote_asset", "settlement_asset")
    @classmethod
    def normalize_identifiers(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("instrument identifiers cannot be blank")
        return normalized

    @field_validator("expiry")
    @classmethod
    def normalize_expiry(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_derivative_identity(self) -> InstrumentKey:
        option_fields = (self.expiry, self.strike_price, self.option_right)
        if self.instrument_type is InstrumentType.OPTION:
            if any(value is None for value in option_fields):
                raise ValueError("option identity requires expiry, strike_price, and option_right")
        elif self.strike_price is not None or self.option_right is not None:
            raise ValueError("option identity fields are only valid for options")
        return self

    @property
    def canonical_id(self) -> str:
        suffix = self.instrument_type.value
        if self.instrument_type is InstrumentType.OPTION:
            assert self.expiry is not None
            assert self.strike_price is not None
            assert self.option_right is not None
            strike = format(self.strike_price.normalize(), "f")
            suffix = (
                f"{suffix}:{self.expiry.isoformat()}:{strike}:{self.option_right.value}"
            )
        elif self.expiry is not None:
            suffix = f"{suffix}:{self.expiry.isoformat()}"
        return f"{self.venue}:{self.base_asset}-{self.quote_asset}:{suffix}"


def instrument_scoped_sequence_id(
    instrument: InstrumentKey, native_sequence_id: str
) -> str:
    """Scope a venue-native sequence to one instrument within the DB length bound."""

    native = native_sequence_id.strip()
    if not native:
        raise ValueError("native sequence ID cannot be blank")
    identity = json.dumps(
        instrument.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    discriminator = hashlib.sha256(identity).hexdigest()[
        :_INSTRUMENT_DISCRIMINATOR_HEX_LENGTH
    ]
    prefix = f"i:{discriminator}:"
    sequence_id = prefix + native
    if len(sequence_id) <= _SEQUENCE_ID_MAX_LENGTH:
        return sequence_id
    native_hash = hashlib.sha256(native.encode()).hexdigest()
    bounded = f"{prefix}n:{native_hash}"
    if len(bounded) > _SEQUENCE_ID_MAX_LENGTH:
        raise AssertionError("instrument-scoped sequence ID exceeds storage bound")
    return bounded


class ExchangeTimedModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange_timestamp: datetime

    @field_validator("exchange_timestamp")
    @classmethod
    def normalize_exchange_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class TradeTick(ExchangeTimedModel):
    instrument: InstrumentKey
    trade_id: str = Field(min_length=1)
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    aggressor_side: Side | None = None


class BookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)


class BookSnapshot(ExchangeTimedModel):
    instrument: InstrumentKey
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    sequence: int = Field(ge=0)
    checksum: str | None = None

    @model_validator(mode="after")
    def validate_levels(self) -> BookSnapshot:
        bid_prices = [level.price for level in self.bids]
        ask_prices = [level.price for level in self.asks]
        if bid_prices != sorted(bid_prices, reverse=True):
            raise ValueError("bids must be sorted by descending price")
        if ask_prices != sorted(ask_prices):
            raise ValueError("asks must be sorted by ascending price")
        if len(bid_prices) != len(set(bid_prices)) or len(ask_prices) != len(set(ask_prices)):
            raise ValueError("book levels must not contain duplicate prices")
        return self


class BookDeltaLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    side: BookSide
    action: BookDeltaAction
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_action_quantity(self) -> BookDeltaLevel:
        if self.action is BookDeltaAction.DELETE and self.quantity != 0:
            raise ValueError("DELETE book delta quantity must be zero")
        if self.action is BookDeltaAction.UPSERT and self.quantity <= 0:
            raise ValueError("UPSERT book delta quantity must be positive")
        return self


class BookDelta(ExchangeTimedModel):
    instrument: InstrumentKey
    updates: tuple[BookDeltaLevel, ...] = Field(min_length=1)
    first_sequence: int = Field(ge=0)
    last_sequence: int = Field(ge=0)
    previous_sequence: int | None = Field(default=None, ge=0)
    checksum: str | None = None

    @model_validator(mode="after")
    def validate_sequence_range(self) -> BookDelta:
        if self.last_sequence < self.first_sequence:
            raise ValueError("last_sequence cannot precede first_sequence")
        return self


class Candle(ExchangeTimedModel):
    instrument: InstrumentKey
    interval_seconds: int = Field(gt=0)
    open_time: datetime
    close_time: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    quote_volume: Decimal | None = Field(default=None, ge=0)
    closed: bool = True

    @field_validator("open_time", "close_time")
    @classmethod
    def normalize_candle_times(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_ohlcv(self) -> Candle:
        if self.close_time <= self.open_time:
            raise ValueError("close_time must be after open_time")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high is below another OHLC value")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low is above another OHLC value")
        return self


class FundingSnapshot(ExchangeTimedModel):
    instrument: InstrumentKey
    funding_rate: Decimal
    funding_interval_seconds: int = Field(gt=0)
    next_funding_time: datetime
    mark_price: Decimal = Field(gt=0)
    index_price: Decimal = Field(gt=0)

    @field_validator("next_funding_time")
    @classmethod
    def normalize_funding_time(cls, value: datetime) -> datetime:
        return _utc(value)


class OpenInterestSnapshot(ExchangeTimedModel):
    instrument: InstrumentKey
    open_interest_base: Decimal | None = Field(default=None, ge=0)
    open_interest_quote: Decimal | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def require_one_open_interest_unit(self) -> OpenInterestSnapshot:
        if self.open_interest_base is None and self.open_interest_quote is None:
            raise ValueError("at least one open-interest unit is required")
        return self


class LiquidationTick(ExchangeTimedModel):
    instrument: InstrumentKey
    liquidation_id: str = Field(min_length=1)
    side: Side | None = None
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)


class OrderUpdate(ExchangeTimedModel):
    instrument: InstrumentKey
    client_order_id: str = Field(min_length=1)
    exchange_order_id: str | None = None
    status: OrderStatus
    side: Side | None = None
    order_type: OrderType
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    average_fill_price: Decimal | None = Field(default=None, gt=0)
    reduce_only: bool = False
    rejection_reason: str | None = None

    @model_validator(mode="after")
    def validate_filled_quantity(self) -> OrderUpdate:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("filled quantity cannot exceed requested quantity")
        return self


class FillEvent(ExchangeTimedModel):
    instrument: InstrumentKey
    fill_id: str = Field(min_length=1)
    client_order_id: str = Field(min_length=1)
    exchange_order_id: str = Field(min_length=1)
    side: Side | None = None
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    fee_amount: Decimal = Field(ge=0)
    fee_asset: str = Field(min_length=1)
    liquidity_role: LiquidityRole = LiquidityRole.UNKNOWN

    @field_validator("fee_asset")
    @classmethod
    def normalize_fee_asset(cls, value: str) -> str:
        return value.strip().upper()


class PositionSnapshot(ExchangeTimedModel):
    instrument: InstrumentKey
    signed_quantity: Decimal
    entry_price: Decimal | None = Field(default=None, gt=0)
    mark_price: Decimal = Field(gt=0)
    unrealized_pnl: Decimal
    realized_pnl: Decimal = Decimal("0")
    leverage: Decimal = Field(default=Decimal("1"), gt=0)
    liquidation_price: Decimal | None = Field(default=None, gt=0)
    margin_used: Decimal = Field(default=Decimal("0"), ge=0)


class BalanceSnapshot(ExchangeTimedModel):
    venue: str = Field(min_length=1)
    asset: str = Field(min_length=1)
    total: Decimal
    available: Decimal
    locked: Decimal = Decimal("0")
    borrowed: Decimal = Decimal("0")

    @field_validator("venue", "asset")
    @classmethod
    def normalize_balance_identity(cls, value: str) -> str:
        return value.strip().upper()


EventPayload = (
    TradeTick
    | BookSnapshot
    | BookDelta
    | Candle
    | FundingSnapshot
    | OpenInterestSnapshot
    | LiquidationTick
    | OrderUpdate
    | FillEvent
    | PositionSnapshot
    | BalanceSnapshot
)

PAYLOAD_KIND: dict[type[BaseModel], EventKind] = {
    TradeTick: EventKind.TRADE_TICK,
    BookSnapshot: EventKind.BOOK_SNAPSHOT,
    BookDelta: EventKind.BOOK_DELTA,
    Candle: EventKind.CANDLE,
    FundingSnapshot: EventKind.FUNDING_SNAPSHOT,
    OpenInterestSnapshot: EventKind.OPEN_INTEREST_SNAPSHOT,
    LiquidationTick: EventKind.LIQUIDATION_TICK,
    OrderUpdate: EventKind.ORDER_UPDATE,
    FillEvent: EventKind.FILL,
    PositionSnapshot: EventKind.POSITION_SNAPSHOT,
    BalanceSnapshot: EventKind.BALANCE_SNAPSHOT,
}


class EventMetadata(BaseModel):
    """Metadata required for exact ordering, traceability, and incident replay."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=1)
    exchange_timestamp: datetime
    receive_timestamp: datetime
    monotonic_ns: int = Field(ge=0)
    sequence_id: str = Field(min_length=1, max_length=_SEQUENCE_ID_MAX_LENGTH)
    native_sequence: int | None = Field(default=None, ge=0, le=9_223_372_036_854_775_807)
    source: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    payload_version: int = Field(ge=1)
    quality: DataQuality = DataQuality.VALID

    @field_validator("exchange_timestamp", "receive_timestamp")
    @classmethod
    def normalize_timestamps(cls, value: datetime) -> datetime:
        return _utc(value)


class EventEnvelope[PayloadT: BaseModel](BaseModel):
    """Typed immutable event; transport and persistence wrap this exact shape."""

    model_config = ConfigDict(frozen=True)

    kind: EventKind
    metadata: EventMetadata
    payload: PayloadT

    @model_validator(mode="after")
    def validate_payload_kind_and_timestamp(self) -> EventEnvelope[PayloadT]:
        expected = PAYLOAD_KIND.get(type(self.payload))
        if expected is None:
            raise ValueError(f"unsupported event payload: {type(self.payload).__name__}")
        if expected is not self.kind:
            raise ValueError(f"payload {type(self.payload).__name__} requires kind {expected}")
        payload_timestamp = getattr(self.payload, "exchange_timestamp", None)
        if payload_timestamp != self.metadata.exchange_timestamp:
            raise ValueError("payload and metadata exchange timestamps must match")
        return self


BookEvent = EventEnvelope[BookSnapshot] | EventEnvelope[BookDelta]


def deterministic_event_id(
    *,
    source: str,
    kind: EventKind,
    sequence_id: str,
    exchange_timestamp: datetime,
    payload: BaseModel,
    occurrence_id: str | None = None,
) -> str:
    """Return a stable ID for a logical event or explicit observation.

    ``payload`` stays outside logical identity so corrected content still causes
    a journal integrity collision. ``occurrence_id`` lets snapshot producers
    distinguish observations when a venue's native identity is not unique.
    """

    del payload
    canonical = {
        "exchange_timestamp": _utc(exchange_timestamp).isoformat(),
        "kind": kind.value,
        "sequence_id": sequence_id,
        "source": source,
    }
    if occurrence_id is not None:
        if kind is not EventKind.BOOK_SNAPSHOT:
            raise ValueError("occurrence_id is only valid for book snapshots")
        normalized_occurrence = occurrence_id.strip()
        if not normalized_occurrence:
            raise ValueError("occurrence_id cannot be blank when provided")
        canonical["occurrence_id"] = normalized_occurrence
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return "evt_" + hashlib.sha256(encoded).hexdigest()


def snapshot_occurrence_id(
    *, receive_timestamp: datetime, receive_monotonic_ns: int
) -> str:
    """Return the stable identity of one received snapshot observation."""

    if receive_monotonic_ns < 0:
        raise ValueError("snapshot receive monotonic timestamp cannot be negative")
    return (
        "snapshot-observation-v1:"
        f"{_utc(receive_timestamp).isoformat()}:{receive_monotonic_ns}"
    )
