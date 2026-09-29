"""Simulated fills; v1 exposes paper execution only and never places orders."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import InstrumentType


class FillStatus(StrEnum):
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"


class FillPurpose(StrEnum):
    OPEN = "open"
    CLOSE = "close"


class PaperFill(BaseModel):
    """One leg filled against a fresh public order book.

    ``price`` is the volume-weighted price of the walked levels, so price PnL
    already contains the execution cost; ``slippage`` is that cost measured
    against the book mid and is reported for attribution only.
    """

    fill_id: str = Field(default_factory=lambda: str(uuid4()))
    position_id: str
    series_id: str
    purpose: FillPurpose
    exchange: str
    symbol: str
    instrument_type: InstrumentType
    side: str
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    mid_price: Decimal = Field(gt=0)
    notional: Decimal = Field(gt=0)
    fee_rate: Decimal = Field(ge=0)
    fee: Decimal = Field(ge=0)
    slippage: Decimal = Field(ge=0)
    levels_consumed: int = Field(ge=1)
    book_timestamp: datetime
    book_age_ms: int
    status: FillStatus = FillStatus.FILLED
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
