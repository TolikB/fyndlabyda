"""Venue-independent market-data models.

Quantities are always expressed in base-asset units. Adapters for venues that
quote derivatives in contracts (Gate, OKX) convert sizes at the boundary so the
scanner, the simulator, and the accounting never see contract counts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import NamedTuple

from pydantic import BaseModel, ConfigDict, Field, field_validator


class InstrumentType(StrEnum):
    SPOT = "SPOT"
    PERPETUAL = "PERPETUAL"
    FUTURE = "FUTURE"


class MarketKey(NamedTuple):
    """Unique market identity; spot and perpetual can share an exchange symbol."""

    exchange: str
    instrument_type: InstrumentType
    symbol: str


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class NormalizedInstrument(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    instrument_type: InstrumentType
    settlement_asset: str | None = None
    # Base-asset units represented by one exchange contract (informational).
    contract_size: Decimal = Decimal("1")
    tick_size: Decimal
    # Order quantity increment and minimum, both in base-asset units.
    step_size: Decimal
    min_order_size: Decimal
    # Minimum order value in quote units when the venue enforces one.
    min_notional: Decimal | None = None
    funding_interval: int | None = Field(default=None, gt=0)
    expiry: datetime | None = None
    is_active: bool = True

    @property
    def canonical_id(self) -> str:
        suffix = {
            InstrumentType.SPOT: "SPOT",
            InstrumentType.PERPETUAL: "PERP",
            InstrumentType.FUTURE: "FUTURE",
        }[self.instrument_type]
        return f"{self.base_asset}-{self.quote_asset}-{suffix}"

    @property
    def key(self) -> MarketKey:
        return MarketKey(self.exchange, self.instrument_type, self.exchange_symbol)


class Ticker(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    instrument_type: InstrumentType
    last_price: Decimal
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    # 24h traded value in quote units.
    volume_24h: Decimal = Decimal("0")
    open_interest: Decimal | None = None
    timestamp: datetime

    @field_validator("last_price", "volume_24h")
    @classmethod
    def validate_non_negative(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("market values cannot be negative")
        return value

    @field_validator("timestamp")
    @classmethod
    def validate_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> MarketKey:
        return MarketKey(self.exchange, self.instrument_type, self.symbol)

    @property
    def reference_price(self) -> Decimal:
        """Mark price for derivatives when published, otherwise the last trade."""

        return self.mark_price if self.mark_price and self.mark_price > 0 else self.last_price


class FundingSnapshot(BaseModel):
    """Current funding state; ``funding_rate`` applies at ``next_funding_time``."""

    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    funding_rate: Decimal
    funding_interval_hours: Decimal = Field(gt=0)
    next_funding_time: datetime | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def validate_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def funding_rate_daily(self) -> Decimal:
        return self.funding_rate * Decimal("24") / self.funding_interval_hours

    @property
    def funding_rate_annualized(self) -> Decimal:
        return self.funding_rate_daily * Decimal("365")

    @property
    def funding_rate_8h(self) -> Decimal:
        """Rate normalized to the conventional 8-hour settlement interval."""

        return self.funding_rate * Decimal("8") / self.funding_interval_hours


class FundingHistoryPoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    funding_rate: Decimal
    funding_timestamp: datetime
    mark_price: Decimal | None = None

    @field_validator("funding_timestamp")
    @classmethod
    def validate_utc(cls, value: datetime) -> datetime:
        return _utc(value)


class OrderBookLevel(BaseModel):
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(ge=0)


class OrderBook(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    timestamp: datetime
    sequence: int | None = None
    instrument_type: InstrumentType = InstrumentType.PERPETUAL
    # Local receive time; freshness never trusts a venue clock that runs ahead.
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("timestamp", "received_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def key(self) -> MarketKey:
        return MarketKey(self.exchange, self.instrument_type, self.symbol)

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Decimal | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / Decimal("2")

    def age_seconds(self, now: datetime) -> float:
        observed = min(self.timestamp, self.received_at)
        return (_utc(now) - observed).total_seconds()
