"""Authenticated trading contracts kept separate from public market-data adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from funding_arbitrage.exchanges.base.models import InstrumentType


class LiveOrderStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class LivePositionState(StrEnum):
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"


class TradingOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    intent_id: str
    client_order_id: str
    exchange: str
    exchange_symbol: str
    instrument_type: InstrumentType
    side: str
    base_quantity: Decimal = Field(gt=0)
    limit_price: Decimal = Field(gt=0)
    reduce_only: bool = False


class TradingOrderResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    exchange_order_id: str | None = None
    client_order_id: str
    exchange_symbol: str
    instrument_type: InstrumentType
    side: str
    requested_base_quantity: Decimal = Field(gt=0)
    filled_base_quantity: Decimal = Field(ge=0)
    average_price: Decimal | None = Field(default=None, gt=0)
    fee: Decimal = Field(default=Decimal("0"), ge=0)
    fee_currency: str | None = None
    status: LiveOrderStatus
    reduce_only: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    raw: dict[str, object] = Field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            LiveOrderStatus.PARTIAL,
            LiveOrderStatus.FILLED,
            LiveOrderStatus.CANCELED,
            LiveOrderStatus.REJECTED,
        }


class VenueBalance(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    free: dict[str, Decimal] = Field(default_factory=dict)
    used: dict[str, Decimal] = Field(default_factory=dict)
    total: dict[str, Decimal] = Field(default_factory=dict)
    spot_free: dict[str, Decimal] | None = None
    equity_usd: Decimal | None = None
    free_collateral_usd: Decimal | None = None
    derivative_free_collateral_usd: Decimal | None = None
    unrealized_pnl_usd: Decimal = Decimal("0")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def available(self, currency: str) -> Decimal:
        return self.free.get(currency.upper(), Decimal("0"))

    def spot_available(self, currency: str) -> Decimal:
        source = self.spot_free if self.spot_free is not None else self.free
        return source.get(currency.upper(), Decimal("0"))

    def collateral_available(self, instrument_type: InstrumentType) -> Decimal:
        if instrument_type is InstrumentType.SPOT:
            return sum(
                (self.spot_available(currency) for currency in ("USD", "USDT", "USDC")),
                Decimal("0"),
            )
        if self.derivative_free_collateral_usd is not None:
            return self.derivative_free_collateral_usd
        if self.free_collateral_usd is not None:
            return self.free_collateral_usd
        return sum(
            (self.available(currency) for currency in ("USD", "USDT", "USDC")),
            Decimal("0"),
        )


class VenuePosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    exchange_symbol: str
    instrument_type: InstrumentType
    side: str
    base_quantity: Decimal = Field(gt=0)
    entry_price: Decimal | None = Field(default=None, gt=0)
    mark_price: Decimal | None = Field(default=None, gt=0)
    unrealized_pnl: Decimal = Decimal("0")

    @property
    def signed_quantity(self) -> Decimal:
        return self.base_quantity if self.side.upper() == "LONG" else -self.base_quantity


class VenueFundingPayment(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    external_id: str
    exchange_symbol: str
    amount: Decimal
    currency: str
    timestamp: datetime


class LiveLeg(BaseModel):
    exchange: str
    exchange_symbol: str
    instrument_type: InstrumentType
    side: str
    requested_base_quantity: Decimal = Field(gt=0)
    filled_base_quantity: Decimal = Field(gt=0)
    average_price: Decimal = Field(gt=0)
    fee: Decimal = Field(default=Decimal("0"), ge=0)
    fee_currency: str | None = None
    residual_base_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    opening_order_ids: tuple[str, ...] = ()
    closing_order_ids: tuple[str, ...] = ()


class LivePosition(BaseModel):
    position_id: str
    intent_id: str
    opportunity_id: str
    opportunity_key: str
    strategy: str
    asset: str
    capital_per_leg: Decimal = Field(gt=0)
    state: LivePositionState
    leg_a: LiveLeg | None = None
    leg_b: LiveLeg | None = None
    target_settlements: tuple[datetime, ...] = ()
    edge_miss_count: int = Field(default=0, ge=0)
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    failure_reason: str | None = None


class TradingAdapter(ABC):
    name: str

    @abstractmethod
    async def initialize(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def preflight(self) -> dict[str, object]: ...

    @abstractmethod
    async def fetch_balance(self) -> VenueBalance: ...

    @abstractmethod
    async def fetch_positions(self) -> list[VenuePosition]: ...

    @abstractmethod
    async def fetch_open_orders(self) -> list[TradingOrderResult]: ...

    @abstractmethod
    async def fetch_funding_payments(
        self, since: datetime
    ) -> list[VenueFundingPayment]: ...

    @abstractmethod
    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal: ...

    @abstractmethod
    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal: ...

    @abstractmethod
    async def normalize_price(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        price: Decimal,
    ) -> Decimal: ...

    @abstractmethod
    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult: ...

    @abstractmethod
    async def cancel_order(self, order: TradingOrderResult) -> TradingOrderResult: ...

    @abstractmethod
    async def configure_derivative(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        leverage: int,
        margin_mode: str,
    ) -> None: ...
