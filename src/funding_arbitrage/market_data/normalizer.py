"""Canonical market identifiers and validation helpers."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from funding_arbitrage.domain.events import BookLevel
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel


def decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise InvalidResponseError(f"invalid decimal in {field}: {value!r}") from exc
    if not result.is_finite():
        raise InvalidResponseError(f"non-finite decimal in {field}")
    return result


def validate_orderbook(orderbook: OrderBook) -> OrderBook:
    if orderbook.bids and orderbook.asks and orderbook.bids[0].price > orderbook.asks[0].price:
        raise InvalidResponseError("best bid exceeds best ask")
    return orderbook


class LegacyBookLevels:
    """One side's legacy levels, reusing those built for the previous book.

    A streamed book is converted after every delta, and validating each of its
    levels again was a fifth of an OKX delta's cost. The local book reuses a
    BookLevel while its price and size are unchanged, so a level the previous
    book also held maps to the OrderBookLevel built for it then. The map is by
    identity, so an equal level with another Decimal spelling gets its own.
    OrderBookLevel is not frozen; nothing assigns to a book's levels.
    """

    __slots__ = ("_previous",)

    def __init__(self) -> None:
        self._previous: dict[int, tuple[BookLevel, OrderBookLevel]] = {}

    def __call__(self, levels: Iterable[BookLevel]) -> tuple[OrderBookLevel, ...]:
        previous = self._previous
        current: dict[int, tuple[BookLevel, OrderBookLevel]] = {}
        converted: list[OrderBookLevel] = []
        for level in levels:
            entry = previous.get(id(level))
            # The entry holds its BookLevel, so that id cannot be reused meanwhile.
            if entry is None or entry[0] is not level:
                entry = (level, OrderBookLevel(price=level.price, quantity=level.quantity))
            current[id(level)] = entry
            converted.append(entry[1])
        self._previous = current
        return tuple(converted)
