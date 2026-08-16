"""Deterministic local L2 reconstruction with fail-closed quality transitions."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookLevel,
    BookSide,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
)


class BookApplyStatus(StrEnum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    GAP = "GAP"
    REJECTED = "REJECTED"


class BookApplyResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: BookApplyStatus
    quality: DataQuality
    sequence: int | None
    reason: str | None = None


ChecksumValidator = Callable[[BookSnapshot, str], bool]


class LocalOrderBook:
    """Single-instrument book; adapters translate venue sequence rules first."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        max_depth: int = 200,
        checksum_validator: ChecksumValidator | None = None,
    ) -> None:
        if max_depth <= 0:
            raise ValueError("max_depth must be positive")
        self.instrument = instrument
        self.max_depth = max_depth
        self.checksum_validator = checksum_validator
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.sequence: int | None = None
        self.exchange_timestamp: datetime | None = None
        self.quality = DataQuality.RECOVERING
        self.recovery_reason: str | None = "snapshot_required"

    @property
    def tradable(self) -> bool:
        return self.quality is DataQuality.VALID

    @property
    def best_bid(self) -> Decimal | None:
        return max(self._bids) if self._bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self._asks) if self._asks else None

    @property
    def mid_price(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    def apply_snapshot(self, snapshot: BookSnapshot) -> BookApplyResult:
        if snapshot.instrument != self.instrument:
            return self._reject("instrument_mismatch")
        bids = {level.price: level.quantity for level in snapshot.bids}
        asks = {level.price: level.quantity for level in snapshot.asks}
        self._bids = self._trim(bids, reverse=True)
        self._asks = self._trim(asks, reverse=False)
        self.sequence = snapshot.sequence
        self.exchange_timestamp = snapshot.exchange_timestamp
        self._refresh_quality()
        if not self._checksum_valid(snapshot, snapshot.checksum):
            return self._gap("snapshot_checksum_mismatch")
        return BookApplyResult(
            status=BookApplyStatus.APPLIED,
            quality=self.quality,
            sequence=self.sequence,
            reason=self.recovery_reason,
        )

    def apply_delta(self, delta: BookDelta) -> BookApplyResult:
        if delta.instrument != self.instrument:
            return self._reject("instrument_mismatch")
        if self.sequence is None or self.quality in {
            DataQuality.GAP,
            DataQuality.INVALID,
            DataQuality.RECOVERING,
            DataQuality.UNAVAILABLE,
        }:
            return self._gap("snapshot_required")
        sequence_reset = (
            delta.previous_sequence == self.sequence and delta.last_sequence < self.sequence
        )
        if delta.last_sequence <= self.sequence and not sequence_reset:
            return BookApplyResult(
                status=BookApplyStatus.DUPLICATE,
                quality=self.quality,
                sequence=self.sequence,
                reason="already_applied",
            )
        if not self._is_contiguous(delta):
            return self._gap("sequence_gap")
        for update in delta.updates:
            levels = self._bids if update.side is BookSide.BID else self._asks
            if update.action is BookDeltaAction.DELETE:
                levels.pop(update.price, None)
            else:
                levels[update.price] = update.quantity
        self._bids = self._trim(self._bids, reverse=True)
        self._asks = self._trim(self._asks, reverse=False)
        self.sequence = delta.last_sequence
        self.exchange_timestamp = delta.exchange_timestamp
        self._refresh_quality()
        current = self.snapshot()
        if not self._checksum_valid(current, delta.checksum):
            return self._gap("delta_checksum_mismatch")
        return BookApplyResult(
            status=BookApplyStatus.APPLIED,
            quality=self.quality,
            sequence=self.sequence,
            reason=self.recovery_reason,
        )

    def mark_stale(self, now: datetime, max_age: timedelta) -> DataQuality:
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        normalized_now = (now if now.tzinfo else now.replace(tzinfo=UTC)).astimezone(UTC)
        if self.exchange_timestamp is None:
            self.quality = DataQuality.UNAVAILABLE
            self.recovery_reason = "missing_timestamp"
        elif normalized_now - self.exchange_timestamp > max_age:
            self.quality = DataQuality.STALE
            self.recovery_reason = "book_stale"
        return self.quality

    def start_recovery(self, reason: str) -> None:
        self.quality = DataQuality.RECOVERING
        self.recovery_reason = reason

    def snapshot(self) -> BookSnapshot:
        if self.sequence is None or self.exchange_timestamp is None:
            raise RuntimeError("book has no authoritative snapshot")
        return BookSnapshot(
            instrument=self.instrument,
            bids=tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self._bids.items(), reverse=True)
            ),
            asks=tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self._asks.items())
            ),
            sequence=self.sequence,
            exchange_timestamp=self.exchange_timestamp,
        )

    def _is_contiguous(self, delta: BookDelta) -> bool:
        if self.sequence is None:
            return False
        if delta.previous_sequence is not None:
            return delta.previous_sequence == self.sequence
        next_sequence = self.sequence + 1
        return delta.first_sequence <= next_sequence <= delta.last_sequence

    def _refresh_quality(self) -> None:
        if not self._bids or not self._asks:
            self.quality = DataQuality.INVALID
            self.recovery_reason = "empty_book_side"
        elif (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid >= self.best_ask
        ):
            self.quality = DataQuality.CROSSED
            self.recovery_reason = "crossed_book"
        else:
            self.quality = DataQuality.VALID
            self.recovery_reason = None

    def _checksum_valid(self, snapshot: BookSnapshot, checksum: str | None) -> bool:
        if checksum is None or self.checksum_validator is None:
            return True
        return self.checksum_validator(snapshot, checksum)

    def _trim(self, levels: dict[Decimal, Decimal], *, reverse: bool) -> dict[Decimal, Decimal]:
        prices = sorted(levels, reverse=reverse)[: self.max_depth]
        return {price: levels[price] for price in prices}

    def _gap(self, reason: str) -> BookApplyResult:
        self.quality = DataQuality.GAP
        self.recovery_reason = reason
        return BookApplyResult(
            status=BookApplyStatus.GAP,
            quality=self.quality,
            sequence=self.sequence,
            reason=reason,
        )

    def _reject(self, reason: str) -> BookApplyResult:
        return BookApplyResult(
            status=BookApplyStatus.REJECTED,
            quality=self.quality,
            sequence=self.sequence,
            reason=reason,
        )
