"""Dynamic venue capability, precision, fee, rate-limit, and clock metadata."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from funding_arbitrage.domain.events import InstrumentKey, InstrumentType

logger = logging.getLogger(__name__)


class VenueCapabilityStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    EMULATED = "EMULATED"


@dataclass(frozen=True)
class VenueInstrumentMetadata:
    instrument: InstrumentKey
    active: bool
    contract_size: Decimal
    price_precision: Decimal | None
    amount_precision: Decimal | None
    minimum_amount: Decimal | None
    minimum_cost: Decimal | None
    maker_fee: Decimal | None
    taker_fee: Decimal | None
    precision_mode: int | None


@dataclass(frozen=True)
class VenueMetadataSnapshot:
    venue: str
    account: str
    capabilities: tuple[tuple[str, VenueCapabilityStatus], ...]
    rate_limit_ms: Decimal
    clock_offset_ms: int | None
    instruments: tuple[VenueInstrumentMetadata, ...]
    observed_at: datetime
    revision: str

    def capability_status(self, name: str) -> VenueCapabilityStatus:
        return dict(self.capabilities).get(
            name, VenueCapabilityStatus.UNSUPPORTED
        )

    def capability(self, name: str) -> bool:
        return self.capability_status(name) is not VenueCapabilityStatus.UNSUPPORTED


class VenueMetadataError(ValueError):
    """Venue metadata is malformed or has ambiguous units."""


class VenueMetadataRegistry:
    """Atomically replace immutable per-account metadata snapshots."""

    def __init__(self) -> None:
        self._snapshots: dict[tuple[str, str], VenueMetadataSnapshot] = {}

    def update_from_ccxt(
        self,
        *,
        venue: str,
        account: str,
        exchange: Any,
        expected_type: InstrumentType,
        observed_at: datetime,
        server_time_ms: int | None,
    ) -> VenueMetadataSnapshot:
        current = _utc(observed_at)
        local_time_ms = int(current.timestamp() * 1000)
        clock_offset_ms = (
            server_time_ms - local_time_ms if server_time_ms is not None else None
        )
        rate_limit = _required_non_negative(
            getattr(exchange, "rateLimit", None), "rate limit"
        )
        raw_has = getattr(exchange, "has", None)
        if not isinstance(raw_has, dict):
            raise VenueMetadataError("exchange capabilities are unavailable")
        capabilities = tuple(
            sorted(
                (str(name), _capability_status(value))
                for name, value in raw_has.items()
                if value is not None
            )
        )
        raw_markets = getattr(exchange, "markets", None)
        if not isinstance(raw_markets, dict):
            raise VenueMetadataError("exchange markets are unavailable")
        expected_markets = tuple(
            market
            for market in raw_markets.values()
            if isinstance(market, dict)
            and _market_type(market) is expected_type
        )
        valid_markets = tuple(
            market
            for market in expected_markets
            if _has_complete_market_identity(market)
        )
        if expected_markets and not valid_markets:
            raise VenueMetadataError(
                "all expected markets have incomplete identity"
            )
        dropped_markets = len(expected_markets) - len(valid_markets)
        if dropped_markets:
            logger.warning(
                "venue_metadata_incomplete_markets_dropped",
                extra={"venue": venue.lower(), "count": dropped_markets},
            )
        instruments = tuple(
            sorted(
                (
                    _instrument_metadata(
                        venue,
                        market,
                        expected_type,
                        getattr(exchange, "precisionMode", None),
                    )
                    for market in valid_markets
                ),
                key=lambda item: item.instrument.canonical_id,
            )
        )
        canonical = {
            "account": account,
            "capabilities": [(name, status.value) for name, status in capabilities],
            "clock_offset_ms": clock_offset_ms,
            "instruments": [
                {
                    "id": item.instrument.canonical_id,
                    "active": item.active,
                    "contract_size": str(item.contract_size),
                    "price_precision": _string(item.price_precision),
                    "amount_precision": _string(item.amount_precision),
                    "minimum_amount": _string(item.minimum_amount),
                    "minimum_cost": _string(item.minimum_cost),
                    "maker_fee": _string(item.maker_fee),
                    "taker_fee": _string(item.taker_fee),
                    "precision_mode": item.precision_mode,
                }
                for item in instruments
            ],
            "rate_limit_ms": str(rate_limit),
            "venue": venue.lower(),
        }
        revision = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        snapshot = VenueMetadataSnapshot(
            venue=venue.lower(),
            account=account.lower(),
            capabilities=capabilities,
            rate_limit_ms=rate_limit,
            clock_offset_ms=clock_offset_ms,
            instruments=instruments,
            observed_at=current,
            revision=revision,
        )
        self._snapshots[(snapshot.venue, snapshot.account)] = snapshot
        return snapshot

    def get(self, venue: str, account: str) -> VenueMetadataSnapshot | None:
        return self._snapshots.get((venue.lower(), account.lower()))

    def snapshots(self) -> tuple[VenueMetadataSnapshot, ...]:
        return tuple(self._snapshots[key] for key in sorted(self._snapshots))


def _capability_status(value: object) -> VenueCapabilityStatus:
    if value is True:
        return VenueCapabilityStatus.SUPPORTED
    if isinstance(value, str) and value.strip().lower() == "emulated":
        return VenueCapabilityStatus.EMULATED
    return VenueCapabilityStatus.UNSUPPORTED


def _instrument_metadata(
    venue: str,
    market: dict[str, Any],
    expected_type: InstrumentType,
    precision_mode: object,
) -> VenueInstrumentMetadata:
    instrument_type = _market_type(market)
    if instrument_type is not expected_type:
        raise VenueMetadataError("market type changed during metadata refresh")
    market_id = _text(market.get("id"))
    base = _text(market.get("base"))
    quote = _text(market.get("quote"))
    if not market_id or not base or not quote:
        raise VenueMetadataError("market identity is incomplete")
    precision = market.get("precision")
    limits = market.get("limits")
    precision_map = precision if isinstance(precision, dict) else {}
    limits_map = limits if isinstance(limits, dict) else {}
    amount_limit = limits_map.get("amount")
    cost_limit = limits_map.get("cost")
    amount_map = amount_limit if isinstance(amount_limit, dict) else {}
    cost_map = cost_limit if isinstance(cost_limit, dict) else {}
    expiry = market.get("expiry")
    return VenueInstrumentMetadata(
        instrument=InstrumentKey(
            venue=venue,
            exchange_symbol=market_id,
            base_asset=base,
            quote_asset=quote,
            instrument_type=instrument_type,
            settlement_asset=_text(market.get("settle")) or None,
            expiry=_milliseconds(expiry) if expiry is not None else None,
        ),
        active=bool(market.get("active", True)),
        contract_size=_positive_or_default(market.get("contractSize"), Decimal("1")),
        price_precision=_optional_non_negative(precision_map.get("price")),
        amount_precision=_optional_non_negative(precision_map.get("amount")),
        minimum_amount=_optional_non_negative(amount_map.get("min")),
        minimum_cost=_optional_non_negative(cost_map.get("min")),
        maker_fee=_optional_non_negative(market.get("maker")),
        taker_fee=_optional_non_negative(market.get("taker")),
        precision_mode=int(precision_mode) if isinstance(precision_mode, int) else None,
    )


def _market_type(market: dict[str, Any]) -> InstrumentType | None:
    if market.get("spot"):
        return InstrumentType.SPOT
    if market.get("swap"):
        return InstrumentType.PERPETUAL
    if market.get("future"):
        return InstrumentType.FUTURE
    return None


def _has_complete_market_identity(market: dict[str, Any]) -> bool:
    return all(_text(market.get(field)) for field in ("id", "base", "quote"))


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise VenueMetadataError(f"{label} is invalid") from exc
    if not parsed.is_finite():
        raise VenueMetadataError(f"{label} is not finite")
    return parsed


def _required_non_negative(value: object, label: str) -> Decimal:
    if value is None or value == "":
        raise VenueMetadataError(f"{label} is missing")
    parsed = _decimal(value, label)
    if parsed < 0:
        raise VenueMetadataError(f"{label} is negative")
    return parsed


def _optional_non_negative(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    parsed = _decimal(value, "metadata value")
    if parsed < 0:
        raise VenueMetadataError("metadata value is negative")
    return parsed


def _positive_or_default(value: object, default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    parsed = _decimal(value, "contract size")
    if parsed <= 0:
        raise VenueMetadataError("contract size is not positive")
    return parsed


def _milliseconds(value: object) -> datetime:
    parsed = _decimal(value, "expiry")
    if parsed <= 0:
        raise VenueMetadataError("expiry is not positive")
    return datetime.fromtimestamp(float(parsed / 1000), tz=UTC)


def _text(value: object | None) -> str:
    return str(value).strip() if value is not None else ""


def _string(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
