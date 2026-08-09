"""Execution intents and fills."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field


class FillStatus(StrEnum):
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"


class ExecutionIntent(BaseModel):
    client_order_id: str = Field(default_factory=lambda: str(uuid4()))
    exchange: str
    symbol: str
    side: str
    quantity: Decimal = Field(gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)


class PaperFill(BaseModel):
    fill_id: str = Field(default_factory=lambda: str(uuid4()))
    client_order_id: str
    exchange: str
    symbol: str
    side: str
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    price: Decimal | None = Field(default=None, gt=0)
    fee: Decimal = Field(ge=0)
    slippage: Decimal = Field(ge=0)
    status: FillStatus
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
