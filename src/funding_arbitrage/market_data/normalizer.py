"""Canonical market identifiers and validation helpers."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import OrderBook


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
