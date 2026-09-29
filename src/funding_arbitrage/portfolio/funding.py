"""Funding payment records applied to paper position legs."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel


class RateSource(StrEnum):
    # The venue's published settlement record for this timestamp.
    HISTORY = "history"
    # Last pre-settlement rate from the live feed, used only after the grace period.
    SNAPSHOT = "snapshot"


class PriceSource(StrEnum):
    HISTORY = "history"
    PRE_FUNDING_MARK = "pre_funding_mark"
    CURRENT_MARK = "current_mark"
    ENTRY = "entry"


class FundingPayment(BaseModel):
    series_id: str
    position_id: str
    leg_index: int
    exchange: str
    symbol: str
    funding_timestamp: datetime
    funding_rate: Decimal
    quantity: Decimal
    mark_price: Decimal
    notional: Decimal
    # Signed cash effect: positive when the leg receives funding.
    amount: Decimal
    rate_source: RateSource
    price_source: PriceSource

    @property
    def reference(self) -> str:
        return f"{self.position_id}:{self.leg_index}:{int(self.funding_timestamp.timestamp())}"
