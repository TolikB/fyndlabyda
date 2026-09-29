"""Append-only cash ledger of a paper series.

Every cash movement is one entry with an idempotency reference. The account's
cash, locked collateral, and realized PnL are all derivable from the ledger,
which makes the reported PnL verifiable against the stored fills, funding
payments, and positions.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field


class LedgerEntryType(StrEnum):
    COLLATERAL_LOCK = "collateral_lock"
    COLLATERAL_RELEASE = "collateral_release"
    FEE = "fee"
    FUNDING = "funding"
    REALIZED_PNL = "realized_pnl"


COLLATERAL_TYPES = frozenset({LedgerEntryType.COLLATERAL_LOCK, LedgerEntryType.COLLATERAL_RELEASE})
PNL_TYPES = frozenset({LedgerEntryType.FEE, LedgerEntryType.FUNDING, LedgerEntryType.REALIZED_PNL})


class LedgerEntry(BaseModel):
    series_id: str
    position_id: str | None = None
    entry_type: LedgerEntryType
    # Signed effect on free cash.
    amount: Decimal
    reference: str = Field(min_length=1, max_length=160)
    timestamp: datetime


class LedgerTotals(BaseModel):
    """Aggregates of a series ledger (restored from the database on start)."""

    collateral: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    funding: Decimal = Decimal("0")
    realized_price_pnl: Decimal = Decimal("0")

    @property
    def locked(self) -> Decimal:
        return -self.collateral

    @property
    def realized(self) -> Decimal:
        return self.fees + self.funding + self.realized_price_pnl

    def apply(self, entry: LedgerEntry) -> None:
        if entry.entry_type in COLLATERAL_TYPES:
            self.collateral += entry.amount
        elif entry.entry_type is LedgerEntryType.FEE:
            self.fees += entry.amount
        elif entry.entry_type is LedgerEntryType.FUNDING:
            self.funding += entry.amount
        else:
            self.realized_price_pnl += entry.amount
