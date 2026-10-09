"""Incremental OFI, microprice, depth imbalance, trade imbalance, and CVD."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.events import (
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    Side,
    TradeTick,
)

ZERO = Decimal("0")
BPS = Decimal("10000")


class OrderFlowFeatureSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    mid_price: Decimal | None = Field(default=None, gt=0)
    microprice: Decimal | None = Field(default=None, gt=0)
    spread_bps: Decimal | None = Field(default=None, ge=0)
    ofi_1s: Decimal | None = None
    ofi_5s: Decimal | None = None
    ofi_30s: Decimal | None = None
    normalized_ofi_1s: Decimal | None = None
    normalized_ofi_5s: Decimal | None = None
    normalized_ofi_30s: Decimal | None = None
    ofi_zscore_5s: Decimal | None = None
    book_imbalance_l1: Decimal | None = Field(default=None, ge=-1, le=1)
    book_imbalance_l5: Decimal | None = Field(default=None, ge=-1, le=1)
    book_imbalance_l20: Decimal | None = Field(default=None, ge=-1, le=1)
    trade_imbalance_5s: Decimal | None = Field(default=None, ge=-1, le=1)
    cvd: Decimal

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class OrderFlowFeatureEngine:
    """Stateful single-instrument engine whose updates are deterministic in event order."""

    def __init__(self, instrument: InstrumentKey, *, zscore_history_seconds: int = 300) -> None:
        if zscore_history_seconds < 30:
            raise ValueError("zscore_history_seconds must be at least 30")
        self.instrument = instrument
        self.zscore_history = timedelta(seconds=zscore_history_seconds)
        self._book: BookSnapshot | None = None
        self._book_quality = DataQuality.UNAVAILABLE
        self._previous_top: tuple[Decimal, Decimal, Decimal, Decimal] | None = None
        self._ofi_events: deque[tuple[datetime, Decimal]] = deque()
        self._ofi_5s_history: deque[tuple[datetime, Decimal]] = deque()
        self._trade_events: deque[tuple[datetime, Decimal, Decimal]] = deque()
        self._cvd = ZERO
        self._last_book_timestamp: datetime | None = None
        self._last_trade_timestamp: datetime | None = None

    def on_book(
        self, book: BookSnapshot, *, quality: DataQuality = DataQuality.VALID
    ) -> OrderFlowFeatureSnapshot:
        self._require_instrument(book.instrument)
        self._require_monotonic(book.exchange_timestamp, self._last_book_timestamp, "book")
        self._last_book_timestamp = book.exchange_timestamp
        self._book = book
        self._book_quality = quality
        if quality is not DataQuality.VALID or not book.bids or not book.asks:
            self._previous_top = None
            return self.snapshot(book.exchange_timestamp)
        current_top = (
            book.bids[0].price,
            book.bids[0].quantity,
            book.asks[0].price,
            book.asks[0].quantity,
        )
        contribution = (
            self._ofi_contribution(self._previous_top, current_top)
            if self._previous_top is not None
            else ZERO
        )
        self._previous_top = current_top
        self._ofi_events.append((book.exchange_timestamp, contribution))
        self._purge(book.exchange_timestamp)
        normalized_5s = self._normalized_ofi(book.exchange_timestamp, 5)
        if normalized_5s is not None:
            self._ofi_5s_history.append((book.exchange_timestamp, normalized_5s))
        self._purge_zscore(book.exchange_timestamp)
        return self.snapshot(book.exchange_timestamp)

    def on_trade(self, trade: TradeTick) -> None:
        self._require_instrument(trade.instrument)
        self._require_monotonic(trade.exchange_timestamp, self._last_trade_timestamp, "trade")
        self._last_trade_timestamp = trade.exchange_timestamp
        signed = ZERO
        if trade.aggressor_side is Side.BUY:
            signed = trade.quantity
        elif trade.aggressor_side is Side.SELL:
            signed = -trade.quantity
        self._trade_events.append((trade.exchange_timestamp, signed, trade.quantity))
        self._cvd += signed
        self._purge(trade.exchange_timestamp)

    def snapshot(self, timestamp: datetime) -> OrderFlowFeatureSnapshot:
        now = (timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC)).astimezone(UTC)
        self._purge(now)
        book = self._book
        if book is None or self._book_quality is not DataQuality.VALID:
            return OrderFlowFeatureSnapshot(
                instrument=self.instrument,
                timestamp=now,
                data_quality=self._book_quality,
                cvd=self._cvd,
            )
        bid = book.bids[0]
        ask = book.asks[0]
        total_top_quantity = bid.quantity + ask.quantity
        mid = (bid.price + ask.price) / Decimal("2")
        microprice = (
            (ask.price * bid.quantity + bid.price * ask.quantity) / total_top_quantity
            if total_top_quantity > 0
            else None
        )
        spread_bps = (ask.price - bid.price) / mid * BPS
        return OrderFlowFeatureSnapshot(
            instrument=self.instrument,
            timestamp=now,
            data_quality=self._book_quality,
            mid_price=mid,
            microprice=microprice,
            spread_bps=spread_bps,
            ofi_1s=self._ofi(now, 1),
            ofi_5s=self._ofi(now, 5),
            ofi_30s=self._ofi(now, 30),
            normalized_ofi_1s=self._normalized_ofi(now, 1),
            normalized_ofi_5s=self._normalized_ofi(now, 5),
            normalized_ofi_30s=self._normalized_ofi(now, 30),
            ofi_zscore_5s=self._ofi_zscore(now),
            book_imbalance_l1=self._book_imbalance(1),
            book_imbalance_l5=self._book_imbalance(5),
            book_imbalance_l20=self._book_imbalance(20),
            trade_imbalance_5s=self._trade_imbalance(now, 5),
            cvd=self._cvd,
        )

    @staticmethod
    def _ofi_contribution(
        previous: tuple[Decimal, Decimal, Decimal, Decimal],
        current: tuple[Decimal, Decimal, Decimal, Decimal],
    ) -> Decimal:
        previous_bid_price, previous_bid_qty, previous_ask_price, previous_ask_qty = previous
        bid_price, bid_qty, ask_price, ask_qty = current
        value = ZERO
        if bid_price >= previous_bid_price:
            value += bid_qty
        if bid_price <= previous_bid_price:
            value -= previous_bid_qty
        if ask_price <= previous_ask_price:
            value -= ask_qty
        if ask_price >= previous_ask_price:
            value += previous_ask_qty
        return value

    def _ofi(self, now: datetime, seconds: int) -> Decimal:
        cutoff = now - timedelta(seconds=seconds)
        return sum(
            (value for timestamp, value in self._ofi_events if timestamp >= cutoff),
            ZERO,
        )

    def _normalized_ofi(self, now: datetime, seconds: int) -> Decimal | None:
        if self._book is None or not self._book.bids or not self._book.asks:
            return None
        local_depth = self._book.bids[0].quantity + self._book.asks[0].quantity
        return self._ofi(now, seconds) / local_depth if local_depth > 0 else None

    def _ofi_zscore(self, now: datetime) -> Decimal | None:
        self._purge_zscore(now)
        values = [value for _, value in self._ofi_5s_history]
        if len(values) < 2:
            return ZERO
        mean = sum(values, ZERO) / Decimal(len(values))
        variance = sum(((value - mean) ** 2 for value in values), ZERO) / Decimal(len(values))
        standard_deviation = variance.sqrt()
        if standard_deviation == 0:
            return ZERO
        return (values[-1] - mean) / standard_deviation

    def _book_imbalance(self, levels: int) -> Decimal | None:
        if self._book is None:
            return None
        bid_quantity = sum((level.quantity for level in self._book.bids[:levels]), ZERO)
        ask_quantity = sum((level.quantity for level in self._book.asks[:levels]), ZERO)
        total = bid_quantity + ask_quantity
        return (bid_quantity - ask_quantity) / total if total > 0 else None

    def _trade_imbalance(self, now: datetime, seconds: int) -> Decimal | None:
        cutoff = now - timedelta(seconds=seconds)
        relevant = [event for event in self._trade_events if event[0] >= cutoff]
        total = sum((quantity for _, _, quantity in relevant), ZERO)
        if total == 0:
            return None
        signed = sum((signed_quantity for _, signed_quantity, _ in relevant), ZERO)
        return signed / total

    def _purge(self, now: datetime) -> None:
        ofi_cutoff = now - timedelta(seconds=30)
        while self._ofi_events and self._ofi_events[0][0] < ofi_cutoff:
            self._ofi_events.popleft()
        trade_cutoff = now - timedelta(seconds=60)
        while self._trade_events and self._trade_events[0][0] < trade_cutoff:
            self._trade_events.popleft()

    def _purge_zscore(self, now: datetime) -> None:
        cutoff = now - self.zscore_history
        while self._ofi_5s_history and self._ofi_5s_history[0][0] < cutoff:
            self._ofi_5s_history.popleft()

    def _require_instrument(self, instrument: InstrumentKey) -> None:
        if instrument != self.instrument:
            raise ValueError("feature event instrument mismatch")

    @staticmethod
    def _require_monotonic(
        timestamp: datetime, previous: datetime | None, stream: str
    ) -> None:
        if previous is not None and timestamp < previous:
            raise ValueError(f"out-of-order {stream} event")
