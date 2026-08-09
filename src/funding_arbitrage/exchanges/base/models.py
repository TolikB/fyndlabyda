"""Venue-independent market-data models."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class InstrumentType(StrEnum):
    SPOT = "SPOT"
    PERPETUAL = "PERPETUAL"
    FUTURE = "FUTURE"


class NormalizedInstrument(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    instrument_type: InstrumentType
    settlement_asset: str | None = None
    contract_size: Decimal = Decimal("1")
    tick_size: Decimal
    step_size: Decimal
    min_order_size: Decimal
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
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class FundingSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    funding_rate: Decimal
    funding_interval_hours: Decimal = Field(gt=0)
    next_funding_time: datetime | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    timestamp: datetime

    @property
    def funding_rate_daily(self) -> Decimal:
        return self.funding_rate * Decimal("24") / self.funding_interval_hours

    @property
    def funding_rate_annualized(self) -> Decimal:
        return self.funding_rate_daily * Decimal("365")


class FundingHistoryPoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    funding_rate: Decimal
    funding_timestamp: datetime
    mark_price: Decimal | None = None


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

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
