"""Deterministic local L2 reconstruction with fail-closed quality transitions."""

from __future__ import annotations

import hashlib
import heapq
from collections import OrderedDict
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
        # Built on demand and dropped on every commit; see snapshot().
        self._snapshot: BookSnapshot | None = None
        self.quality = DataQuality.RECOVERING
        self.recovery_reason: str | None = "snapshot_required"
        self._delta_fingerprints: OrderedDict[
            tuple[int, int, int | None, datetime], str
        ] = OrderedDict()
        self._delta_history_limit = max(128, max_depth * 4)

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
        if (
            self.exchange_timestamp is not None
            and snapshot.exchange_timestamp < self.exchange_timestamp
        ):
            return self._reject("snapshot_timestamp_regressed", DataQuality.INVALID)
        if (
            self.sequence is not None
            and snapshot.sequence == self.sequence
            and snapshot.exchange_timestamp == self.exchange_timestamp
        ):
            candidate = self._candidate_snapshot(
                bids=self._trim(
                    {level.price: level.quantity for level in snapshot.bids},
                    reverse=True,
                ),
                asks=self._trim(
                    {level.price: level.quantity for level in snapshot.asks},
                    reverse=False,
                ),
                sequence=snapshot.sequence,
                exchange_timestamp=snapshot.exchange_timestamp,
                checksum=snapshot.checksum,
            )
            if not self._checksum_valid(candidate, snapshot.checksum):
                return self._gap("snapshot_checksum_mismatch")
            if (
                candidate.model_dump(mode="json", exclude={"checksum"})
                != self.snapshot().model_dump(mode="json", exclude={"checksum"})
            ):
                return self._gap("snapshot_identity_collision")
            return BookApplyResult(
                status=BookApplyStatus.DUPLICATE,
                quality=self.quality,
                sequence=self.sequence,
                reason="already_applied",
            )
        candidate = self._candidate_snapshot(
            bids=self._trim(
                {level.price: level.quantity for level in snapshot.bids},
                reverse=True,
            ),
            asks=self._trim(
                {level.price: level.quantity for level in snapshot.asks},
                reverse=False,
            ),
            sequence=snapshot.sequence,
            exchange_timestamp=snapshot.exchange_timestamp,
            checksum=snapshot.checksum,
        )
        if not self._checksum_valid(candidate, snapshot.checksum):
            return self._gap("snapshot_checksum_mismatch")
        self._commit(candidate)
        self._delta_fingerprints.clear()
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
        identity = self._delta_identity(delta)
        fingerprint = self._delta_fingerprint(delta)
        previous_fingerprint = self._delta_fingerprints.get(identity)
        if previous_fingerprint is not None:
            if previous_fingerprint != fingerprint:
                return self._gap("delta_identity_collision")
            return BookApplyResult(
                status=BookApplyStatus.DUPLICATE,
                quality=self.quality,
                sequence=self.sequence,
                reason="already_applied",
            )
        sequence_reset = (
            delta.previous_sequence == self.sequence and delta.last_sequence < self.sequence
        )
        if delta.last_sequence <= self.sequence and not sequence_reset:
            if any(key[1] == delta.last_sequence for key in self._delta_fingerprints):
                return self._gap("delta_identity_collision")
            return BookApplyResult(
                status=BookApplyStatus.DUPLICATE,
                quality=self.quality,
                sequence=self.sequence,
                reason="already_applied",
            )
        if (
            self.exchange_timestamp is not None
            and delta.exchange_timestamp < self.exchange_timestamp
        ):
            return self._reject("delta_timestamp_regressed", DataQuality.INVALID)
        if not self._is_contiguous(delta):
            return self._gap("sequence_gap")
        bids = dict(self._bids)
        asks = dict(self._asks)
        for update in delta.updates:
            levels = bids if update.side is BookSide.BID else asks
            if update.action is BookDeltaAction.DELETE:
                levels.pop(update.price, None)
            else:
                levels[update.price] = update.quantity
        trimmed_bids = self._trim(bids, reverse=True)
        trimmed_asks = self._trim(asks, reverse=False)
        if delta.checksum is not None:
            # Only a venue checksum needs the candidate as a snapshot; without one
            # the levels commit directly (Binance diff depth sends none).
            candidate = self._candidate_snapshot(
                bids=trimmed_bids,
                asks=trimmed_asks,
                sequence=delta.last_sequence,
                exchange_timestamp=delta.exchange_timestamp,
                checksum=delta.checksum,
            )
            if not self._checksum_valid(candidate, delta.checksum):
                return self._gap("delta_checksum_mismatch")
        self._commit_levels(
            trimmed_bids,
            trimmed_asks,
            sequence=delta.last_sequence,
            exchange_timestamp=delta.exchange_timestamp,
        )
        self._remember_delta(identity, fingerprint)
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

    def snapshot(self, depth: int | None = None) -> BookSnapshot:
        if self.sequence is None or self.exchange_timestamp is None:
            raise RuntimeError("book has no authoritative snapshot")
        if depth is not None:
            # Only the best levels: rebuilding a 1000-level Binance book on every
            # delta for a 20-level consumer took a fifth of a core.
            return self._candidate_snapshot(
                bids=dict(heapq.nlargest(depth, self._bids.items())),
                asks=dict(heapq.nsmallest(depth, self._asks.items())),
                sequence=self.sequence,
                exchange_timestamp=self.exchange_timestamp,
                checksum=None,
            )
        if self._snapshot is None:
            self._snapshot = self._candidate_snapshot(
                bids=self._bids,
                asks=self._asks,
                sequence=self.sequence,
                exchange_timestamp=self.exchange_timestamp,
                checksum=None,
            )
        return self._snapshot

    def _candidate_snapshot(
        self,
        *,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        sequence: int,
        exchange_timestamp: datetime,
        checksum: str | None,
    ) -> BookSnapshot:
        """A snapshot of levels this book already validated, without re-validating.

        Every level comes from a validated snapshot or delta (positive price and
        size, one entry per price) and is sorted here, and the sequence and
        timestamp come from validated models too, so the BookSnapshot validators
        cannot fail. Running them anyway was a third of the canonical runtime's
        CPU on Binance diff-depth streams.
        """

        return BookSnapshot.model_construct(
            instrument=self.instrument,
            bids=tuple(
                BookLevel.model_construct(price=price, quantity=quantity)
                for price, quantity in sorted(bids.items(), reverse=True)
            ),
            asks=tuple(
                BookLevel.model_construct(price=price, quantity=quantity)
                for price, quantity in sorted(asks.items())
            ),
            sequence=sequence,
            checksum=checksum,
            exchange_timestamp=exchange_timestamp,
        )

    def _commit(self, snapshot: BookSnapshot) -> None:
        self._commit_levels(
            {level.price: level.quantity for level in snapshot.bids},
            {level.price: level.quantity for level in snapshot.asks},
            sequence=snapshot.sequence,
            exchange_timestamp=snapshot.exchange_timestamp,
        )

    def _commit_levels(
        self,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        *,
        sequence: int,
        exchange_timestamp: datetime,
    ) -> None:
        self._bids = bids
        self._asks = asks
        self.sequence = sequence
        self.exchange_timestamp = exchange_timestamp
        self._snapshot = None
        self._refresh_quality()

    @staticmethod
    def _delta_identity(delta: BookDelta) -> tuple[int, int, int | None, datetime]:
        return (
            delta.first_sequence,
            delta.last_sequence,
            delta.previous_sequence,
            delta.exchange_timestamp,
        )

    @staticmethod
    def _delta_fingerprint(delta: BookDelta) -> str:
        return hashlib.sha256(delta.model_dump_json().encode("utf-8")).hexdigest()

    def _remember_delta(
        self,
        identity: tuple[int, int, int | None, datetime],
        fingerprint: str,
    ) -> None:
        self._delta_fingerprints[identity] = fingerprint
        self._delta_fingerprints.move_to_end(identity)
        while len(self._delta_fingerprints) > self._delta_history_limit:
            self._delta_fingerprints.popitem(last=False)

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
        if checksum is None:
            return True
        if self.checksum_validator is None:
            return False
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

    def _reject(
        self,
        reason: str,
        quality: DataQuality | None = None,
    ) -> BookApplyResult:
        return BookApplyResult(
            status=BookApplyStatus.REJECTED,
            quality=quality or self.quality,
            sequence=self.sequence,
            reason=reason,
        )
