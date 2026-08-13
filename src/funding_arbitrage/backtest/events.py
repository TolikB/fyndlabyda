"""Replayable market, funding, opportunity, fill, and position events."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    MARKET = "market"
    FUNDING = "funding"
    OPPORTUNITY = "opportunity"
    FILL = "fill"
    POSITION = "position"


class BacktestEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime
    event_type: EventType


class MarketEvent(BacktestEvent):
    event_type: EventType = EventType.MARKET
    exchange: str
    symbol: str
    price: Decimal
    mark_price: Decimal | None = None


class FundingEvent(BacktestEvent):
    event_type: EventType = EventType.FUNDING
    exchange: str
    symbol: str
    rate: Decimal
    notional: Decimal
    pnl: Decimal | None = None


class OpportunityEvent(BacktestEvent):
    event_type: EventType = EventType.OPPORTUNITY
    opportunity_id: str
    net_edge: Decimal


class FillEvent(BacktestEvent):
    event_type: EventType = EventType.FILL
    position_id: str
    notional: Decimal
    fee: Decimal
    spread: Decimal = Decimal("0")
    slippage: Decimal = Decimal("0")


class PositionEvent(BacktestEvent):
    event_type: EventType = EventType.POSITION
    position_id: str
    state: str
    pnl: Decimal = Decimal("0")


def sort_events(events: list[BacktestEvent]) -> list[BacktestEvent]:
    return sorted(events, key=lambda event: (event.timestamp.astimezone(UTC), event.event_id))
