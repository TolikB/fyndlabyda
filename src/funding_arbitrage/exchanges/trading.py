"""CCXT-backed authenticated adapters with venue-specific execution safeguards."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
    VenuePosition,
)

logger = logging.getLogger(__name__)


def _decimal(value: object | None, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "":
        return default
    return Decimal(str(value))


class CcxtTradingAdapter(TradingAdapter):
    """Normalize private order APIs without leaking CCXT payloads into strategy code."""

    def __init__(
        self,
        name: str,
        exchange: Any,
        *,
        margin_mode: str,
    ) -> None:
        self.name = name
        self.exchange = exchange
        self.margin_mode = margin_mode
        self._markets_loaded = False
        self._configured_derivatives: set[tuple[str, InstrumentType, int, str]] = set()
        self._taker_fee_cache: dict[tuple[str, InstrumentType], Decimal] = {}

    async def initialize(self) -> None:
        self.exchange.check_required_credentials()
        await self.exchange.load_markets(reload=True)
        self._markets_loaded = True

    async def close(self) -> None:
        await self.exchange.close()

    async def preflight(self) -> dict[str, object]:
        if not self._markets_loaded:
            await self.initialize()
        balance, positions, orders = await asyncio.gather(
            self.fetch_balance(), self.fetch_positions(), self.fetch_open_orders()
        )
        return {
            "exchange": self.name,
            "currencies": sorted(balance.total),
            "open_positions": len(positions),
            "open_orders": len(orders),
        }

    async def fetch_balance(self) -> VenueBalance:
        profiles: tuple[dict[str, object], ...]
        if self.name == "binance":
            profiles = ({"type": "spot"}, {"type": "future"})
        elif self.name == "gate":
            profiles = ({"type": "spot"}, {"type": "swap", "settle": "usdt"})
        elif self.name == "hyperliquid":
            profiles = ({"type": "swap"}, {"type": "spot"})
        else:
            profiles = ({},)
        results = await asyncio.gather(
            *(self.exchange.fetch_balance(profile) for profile in profiles),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise RuntimeError(f"{self.name} balance request failed") from errors[0]
        successful = [result for result in results if isinstance(result, dict)]
        if not successful:
            error = next((item for item in results if isinstance(item, Exception)), None)
            raise RuntimeError(f"{self.name} balance request failed") from error
        free: dict[str, Decimal] = {}
        used: dict[str, Decimal] = {}
        total: dict[str, Decimal] = {}
        for result in successful:
            for target, field in ((free, "free"), (used, "used"), (total, "total")):
                values = result.get(field) or {}
                if not isinstance(values, dict):
                    continue
                for currency, value in values.items():
                    amount = _decimal(value)
                    if amount != 0:
                        target[str(currency).upper()] = target.get(
                            str(currency).upper(), Decimal("0")
                        ) + amount
        stable_free = sum(
            (
                free.get(currency, Decimal("0"))
                for currency in ("USD", "USDT", "USDC")
            ),
            Decimal("0"),
        )
        free_override = self._usd_free_override(successful)
        spot_free: dict[str, Decimal] | None = None
        derivative_free: Decimal | None = None
        if len(profiles) == 1:
            spot_free = dict(free)
            derivative_free = free_override if free_override is not None else stable_free
        else:
            for profile, raw_result in zip(profiles, results, strict=True):
                if not isinstance(raw_result, dict):
                    continue
                profile_free = self._currency_values(raw_result, "free")
                profile_stable = sum(
                    (
                        profile_free.get(currency, Decimal("0"))
                        for currency in ("USD", "USDT", "USDC")
                    ),
                    Decimal("0"),
                )
                if profile.get("type") == "spot":
                    spot_free = profile_free
                else:
                    derivative_free = profile_stable
        return VenueBalance(
            exchange=self.name,
            free=free,
            used=used,
            total=total,
            spot_free=spot_free,
            equity_usd=self._usd_equity_override(successful),
            free_collateral_usd=(
                free_override if free_override is not None else stable_free
            ),
            derivative_free_collateral_usd=derivative_free,
            unrealized_pnl_usd=self._usd_unrealized_adjustment(successful),
        )

    async def fetch_positions(self) -> list[VenuePosition]:
        if not self.exchange.has.get("fetchPositions"):
            return []
        rows = await self.exchange.fetch_positions()
        result: list[VenuePosition] = []
        for row in rows:
            contracts = _decimal(row.get("contracts"))
            if contracts == 0:
                continue
            market = row.get("info") and self._market_from_symbol(row.get("symbol"))
            market = market or self._market_from_symbol(row.get("symbol"))
            if market is None:
                logger.warning(
                    "live_position_market_unresolved",
                    extra={"exchange": self.name, "symbol": row.get("symbol")},
                )
                continue
            contract_size = _decimal(market.get("contractSize"), Decimal("1"))
            side = str(row.get("side") or "").upper()
            if side not in {"LONG", "SHORT"}:
                side = "LONG" if contracts > 0 else "SHORT"
            result.append(
                VenuePosition(
                    exchange=self.name,
                    exchange_symbol=str(market["id"]),
                    instrument_type=(
                        InstrumentType.FUTURE if market.get("future") else InstrumentType.PERPETUAL
                    ),
                    side=side,
                    base_quantity=abs(contracts) * contract_size,
                    entry_price=_optional_positive(row.get("entryPrice")),
                    mark_price=_optional_positive(row.get("markPrice")),
                    unrealized_pnl=_decimal(row.get("unrealizedPnl")),
                )
            )
        return result

    async def fetch_open_orders(self) -> list[TradingOrderResult]:
        if not self.exchange.has.get("fetchOpenOrders"):
            raise RuntimeError(f"{self.name} does not support open-order reconciliation")
        profiles: tuple[dict[str, object], ...]
        if self.name == "binance":
            profiles = ({"type": "spot"}, {"type": "future"})
        elif self.name == "gate":
            profiles = ({"type": "spot"}, {"type": "swap", "settle": "usdt"})
        elif self.name == "hyperliquid":
            profiles = ({"type": "spot"}, {"type": "swap"})
        elif self.name == "bybit":
            profiles = ({"category": "spot"}, {"category": "linear"})
        else:
            profiles = ({},)
        results = await asyncio.gather(
            *(self.exchange.fetch_open_orders(params=profile) for profile in profiles),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise RuntimeError(f"{self.name} open-order request failed") from errors[0]
        rows = [
            row
            for result in results
            if isinstance(result, list)
            for row in result
            if isinstance(row, dict)
        ]
        unique = {
            (str(row.get("id") or ""), str(row.get("clientOrderId") or "")): row
            for row in rows
        }
        return [self._parse_order(row) for row in unique.values()]

    async def fetch_funding_payments(
        self, since: datetime
    ) -> list[VenueFundingPayment]:
        if not self.exchange.has.get("fetchFundingHistory"):
            raise RuntimeError(f"{self.name} does not support funding-payment history")
        rows = await self.exchange.fetch_funding_history(
            None,
            int(since.timestamp() * 1000),
            100,
        )
        result: list[VenueFundingPayment] = []
        for row in rows:
            timestamp_ms = row.get("timestamp")
            if timestamp_ms is None:
                continue
            timestamp = datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=UTC)
            symbol = str(row.get("symbol") or row.get("code") or "UNKNOWN")
            market = self._market_from_symbol(row.get("symbol"))
            exchange_symbol = str(market["id"]) if market is not None else symbol
            amount = _decimal(row.get("amount"))
            currency = str(row.get("code") or "USDT").upper()
            source_id = str(row.get("id") or "")
            external_id = hashlib.sha256(
                (
                    f"{self.name}:{source_id}:{exchange_symbol}:"
                    f"{timestamp.isoformat()}:{amount}"
                ).encode()
            ).hexdigest()
            result.append(
                VenueFundingPayment(
                    exchange=self.name,
                    external_id=external_id,
                    exchange_symbol=exchange_symbol,
                    amount=amount,
                    currency=currency,
                    timestamp=timestamp,
                )
            )
        return result

    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal:
        key = (exchange_symbol, instrument_type)
        if key in self._taker_fee_cache:
            return self._taker_fee_cache[key]
        if not self.exchange.has.get("fetchTradingFee"):
            raise RuntimeError(f"{self.name} cannot verify the account taker fee")
        market = self._market(exchange_symbol, instrument_type)
        row = await self.exchange.fetch_trading_fee(market["symbol"])
        fee = _decimal(row.get("taker"))
        if fee < 0 or fee > Decimal("0.02"):
            raise RuntimeError(f"{self.name} returned an invalid taker fee")
        self._taker_fee_cache[key] = fee
        return fee

    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal:
        market = self._market(exchange_symbol, instrument_type)
        contract_size = self._contract_size(market)
        order_amount = base_quantity / contract_size
        precise_amount = _decimal(
            self.exchange.amount_to_precision(market["symbol"], float(order_amount))
        )
        minimum = _decimal((market.get("limits") or {}).get("amount", {}).get("min"))
        if precise_amount <= 0 or (minimum > 0 and precise_amount < minimum):
            return Decimal("0")
        return precise_amount * contract_size

    async def normalize_price(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        price: Decimal,
    ) -> Decimal:
        market = self._market(exchange_symbol, instrument_type)
        return _decimal(self.exchange.price_to_precision(market["symbol"], float(price)))

    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult:
        market = self._market(request.exchange_symbol, request.instrument_type)
        contract_size = self._contract_size(market)
        amount = request.base_quantity / contract_size
        precise_amount = _decimal(
            self.exchange.amount_to_precision(market["symbol"], float(amount))
        )
        price = _decimal(
            self.exchange.price_to_precision(market["symbol"], float(request.limit_price))
        )
        normalized_request = request.model_copy(
            update={
                "base_quantity": precise_amount * contract_size,
                "limit_price": price,
            }
        )
        self._validate_order_limits(market, precise_amount, price)
        params: dict[str, object] = {
            "timeInForce": "IOC",
            "clientOrderId": request.client_order_id,
        }
        if request.instrument_type is not InstrumentType.SPOT:
            params["reduceOnly"] = request.reduce_only
        if self.name == "okx":
            params["tdMode"] = (
                "cash"
                if request.instrument_type is InstrumentType.SPOT
                else self.margin_mode
            )
        elif self.name == "bybit" and request.instrument_type is not InstrumentType.SPOT:
            params["positionIdx"] = 0
        elif self.name == "gate":
            params["text"] = request.client_order_id
            if request.instrument_type is not InstrumentType.SPOT:
                params["settle"] = "usdt"
        try:
            row = await self.exchange.create_order(
                market["symbol"],
                "limit",
                request.side.lower(),
                float(precise_amount),
                float(price),
                params,
            )
        except Exception as exc:
            recovered = await self._find_by_client_id(
                request.client_order_id, market["symbol"]
            )
            if recovered is not None:
                return self._parse_order(
                    recovered, request=normalized_request, market=market
                )
            if self._is_definitive_rejection(exc):
                return TradingOrderResult(
                    exchange=self.name,
                    client_order_id=request.client_order_id,
                    exchange_symbol=request.exchange_symbol,
                    instrument_type=request.instrument_type,
                    side=request.side,
                    requested_base_quantity=normalized_request.base_quantity,
                    filled_base_quantity=Decimal("0"),
                    status=LiveOrderStatus.REJECTED,
                    reduce_only=request.reduce_only,
                    raw={"error_type": type(exc).__name__},
                )
            return TradingOrderResult(
                exchange=self.name,
                client_order_id=request.client_order_id,
                exchange_symbol=request.exchange_symbol,
                instrument_type=request.instrument_type,
                side=request.side,
                requested_base_quantity=normalized_request.base_quantity,
                filled_base_quantity=Decimal("0"),
                status=LiveOrderStatus.UNKNOWN,
                reduce_only=request.reduce_only,
                raw={"error_type": type(exc).__name__},
            )
        parsed = self._parse_order(row, request=normalized_request, market=market)
        if parsed.is_terminal:
            return parsed
        return await self._wait_for_terminal(
            parsed, market["symbol"], normalized_request, timeout_seconds
        )

    async def cancel_order(self, order: TradingOrderResult) -> TradingOrderResult:
        if not order.exchange_order_id:
            return order.model_copy(update={"status": LiveOrderStatus.UNKNOWN})
        market = self._market(order.exchange_symbol, order.instrument_type)
        try:
            await self.exchange.cancel_order(order.exchange_order_id, market["symbol"])
        except Exception:
            logger.exception(
                "live_cancel_failed",
                extra={"exchange": self.name, "order_id": order.exchange_order_id},
            )
        fetched = await self._fetch_order(order.exchange_order_id, market["symbol"])
        return self._parse_order(fetched, market=market) if fetched else order

    async def configure_derivative(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        leverage: int,
        margin_mode: str,
    ) -> None:
        if instrument_type is InstrumentType.SPOT:
            return
        key = (exchange_symbol, instrument_type, leverage, margin_mode)
        if key in self._configured_derivatives:
            return
        market = self._market(exchange_symbol, instrument_type)
        symbol = market["symbol"]
        if self.exchange.has.get("setPositionMode"):
            await self._ignore_unchanged(self.exchange.set_position_mode(False, symbol))
        if self.exchange.has.get("setMarginMode"):
            await self._ignore_unchanged(
                self.exchange.set_margin_mode(margin_mode, symbol, {"leverage": leverage})
            )
        if self.exchange.has.get("setLeverage"):
            await self._ignore_unchanged(
                self.exchange.set_leverage(leverage, symbol, {"marginMode": margin_mode})
            )
        self._configured_derivatives.add(key)

    async def _ignore_unchanged(self, awaitable: Any) -> None:
        try:
            await awaitable
        except Exception as exc:
            message = str(exc).lower()
            if not any(value in message for value in ("not modified", "already", "same")):
                raise

    async def _wait_for_terminal(
        self,
        initial: TradingOrderResult,
        symbol: str,
        request: TradingOrderRequest,
        timeout_seconds: float,
    ) -> TradingOrderResult:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        current = initial
        while asyncio.get_running_loop().time() < deadline:
            if current.is_terminal:
                return current
            await asyncio.sleep(0.25)
            row = await self._fetch_order(current.exchange_order_id, symbol)
            if row is None:
                row = await self._find_by_client_id(request.client_order_id, symbol)
            if row is not None:
                current = self._parse_order(row, request=request)
        if current.exchange_order_id:
            current = await self.cancel_order(current)
            if current.is_terminal:
                return current
        return current.model_copy(update={"status": LiveOrderStatus.UNKNOWN})

    async def _fetch_order(self, order_id: str | None, symbol: str) -> dict[str, Any] | None:
        if not order_id or not self.exchange.has.get("fetchOrder"):
            return None
        try:
            row = await self.exchange.fetch_order(order_id, symbol)
            return row if isinstance(row, dict) else None
        except Exception:
            return None

    async def _find_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> dict[str, Any] | None:
        methods = ("fetch_open_orders", "fetch_closed_orders")
        for method_name in methods:
            if not self.exchange.has.get(
                "fetchOpenOrders" if method_name == "fetch_open_orders" else "fetchClosedOrders"
            ):
                continue
            try:
                rows = await getattr(self.exchange, method_name)(symbol)
            except Exception:
                continue
            for row in rows:
                if str(row.get("clientOrderId") or "") == client_order_id:
                    return row
        return None

    def _parse_order(
        self,
        row: dict[str, Any],
        *,
        request: TradingOrderRequest | None = None,
        market: dict[str, Any] | None = None,
    ) -> TradingOrderResult:
        market = market or self._market_from_symbol(row.get("symbol"))
        if market is None and request is not None:
            market = self._market(request.exchange_symbol, request.instrument_type)
        if market is None:
            raise ValueError(f"cannot resolve market for {self.name} order")
        instrument_type = (
            request.instrument_type
            if request is not None
            else InstrumentType.SPOT
            if market.get("spot")
            else InstrumentType.FUTURE
            if market.get("future")
            else InstrumentType.PERPETUAL
        )
        contract_size = self._contract_size(market)
        requested = (
            request.base_quantity
            if request is not None
            else _decimal(row.get("amount")) * contract_size
        )
        filled = _decimal(row.get("filled")) * contract_size
        status = self._status(row.get("status"), filled, requested)
        fee, fee_currency = self._fee(row)
        raw_info = row.get("info")
        return TradingOrderResult(
            exchange=self.name,
            exchange_order_id=str(row["id"]) if row.get("id") is not None else None,
            client_order_id=str(
                row.get("clientOrderId")
                or (request.client_order_id if request is not None else "")
            ),
            exchange_symbol=str(market["id"]),
            instrument_type=instrument_type,
            side=str(row.get("side") or (request.side if request else "")).upper(),
            requested_base_quantity=requested,
            filled_base_quantity=filled,
            average_price=_optional_positive(row.get("average") or row.get("price")),
            fee=fee,
            fee_currency=fee_currency,
            status=status,
            reduce_only=bool(row.get("reduceOnly") or (request and request.reduce_only)),
            raw={"info": raw_info} if isinstance(raw_info, dict) else {},
        )

    def _market(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> dict[str, Any]:
        candidates = self.exchange.markets_by_id.get(exchange_symbol)
        if isinstance(candidates, dict):
            candidates = [candidates]
        if not candidates:
            candidates = [
                market
                for market in self.exchange.markets.values()
                if str(market.get("id", "")).upper() == exchange_symbol.upper()
            ]
        for market in candidates or []:
            if instrument_type is InstrumentType.SPOT and market.get("spot"):
                return market
            if instrument_type is InstrumentType.PERPETUAL and market.get("swap"):
                return market
            if instrument_type is InstrumentType.FUTURE and market.get("future"):
                return market
        raise ValueError(
            f"{self.name} market not found: {exchange_symbol} {instrument_type.value}"
        )

    def _market_from_symbol(self, symbol: object) -> dict[str, Any] | None:
        if symbol is None:
            return None
        market = self.exchange.markets.get(str(symbol))
        return market if isinstance(market, dict) else None

    @staticmethod
    def _contract_size(market: dict[str, Any]) -> Decimal:
        return _decimal(market.get("contractSize"), Decimal("1"))

    @staticmethod
    def _validate_order_limits(
        market: dict[str, Any], amount: Decimal, price: Decimal
    ) -> None:
        limits = market.get("limits") or {}
        minimum_amount = _decimal((limits.get("amount") or {}).get("min"))
        minimum_cost = _decimal((limits.get("cost") or {}).get("min"))
        if amount <= 0 or (minimum_amount > 0 and amount < minimum_amount):
            raise ValueError("order amount is below venue minimum")
        if minimum_cost > 0 and amount * price * _decimal(
            market.get("contractSize"), Decimal("1")
        ) < minimum_cost:
            raise ValueError("order notional is below venue minimum")

    @staticmethod
    def _status(
        raw_status: object, filled: Decimal, requested: Decimal
    ) -> LiveOrderStatus:
        status = str(raw_status or "").lower()
        if status == "open":
            return LiveOrderStatus.OPEN
        if status == "closed":
            return LiveOrderStatus.FILLED if filled >= requested else LiveOrderStatus.PARTIAL
        if status in {"canceled", "cancelled", "expired"}:
            return LiveOrderStatus.PARTIAL if filled > 0 else LiveOrderStatus.CANCELED
        if status == "rejected":
            return LiveOrderStatus.REJECTED
        return LiveOrderStatus.UNKNOWN

    @staticmethod
    def _fee(row: dict[str, Any]) -> tuple[Decimal, str | None]:
        fees = row.get("fees") or ([] if row.get("fee") is None else [row["fee"]])
        total = Decimal("0")
        currency: str | None = None
        for fee in fees:
            if not isinstance(fee, dict):
                continue
            total += abs(_decimal(fee.get("cost")))
            currency = currency or (str(fee["currency"]) if fee.get("currency") else None)
        return total, currency

    @staticmethod
    def _is_definitive_rejection(exc: Exception) -> bool:
        return type(exc).__name__ in {
            "BadRequest",
            "InsufficientFunds",
            "InvalidOrder",
            "NotSupported",
            "PermissionDenied",
        }

    def _usd_equity_override(self, results: list[dict[str, Any]]) -> Decimal | None:
        return self._bybit_account_total(results, "totalEquity")

    def _usd_free_override(self, results: list[dict[str, Any]]) -> Decimal | None:
        return self._bybit_account_total(results, "totalAvailableBalance")

    def _usd_unrealized_adjustment(self, results: list[dict[str, Any]]) -> Decimal:
        if self.name != "gate":
            return Decimal("0")
        adjustment = Decimal("0")
        for result in results:
            info = result.get("info") or {}
            if not isinstance(info, dict):
                continue
            adjustment += _decimal(info.get("unrealised_pnl"))
        return adjustment

    @staticmethod
    def _currency_values(result: dict[str, Any], field: str) -> dict[str, Decimal]:
        values = result.get(field) or {}
        if not isinstance(values, dict):
            return {}
        return {
            str(currency).upper(): _decimal(value)
            for currency, value in values.items()
            if _decimal(value) != 0
        }

    def _bybit_account_total(
        self, results: list[dict[str, Any]], field: str
    ) -> Decimal | None:
        if self.name != "bybit":
            return None
        for result in results:
            info = result.get("info") or {}
            payload = info.get("result") if isinstance(info, dict) else None
            rows = payload.get("list") if isinstance(payload, dict) else None
            first = rows[0] if isinstance(rows, list) and rows else None
            if isinstance(first, dict) and field in first:
                return _decimal(first.get(field))
        return None


def _optional_positive(value: object | None) -> Decimal | None:
    parsed = _decimal(value)
    return parsed if parsed > 0 else None


def create_trading_adapters(settings: Settings) -> dict[str, TradingAdapter]:
    """Create authenticated clients only after live-mode validation succeeded."""

    import ccxt.async_support as ccxt  # type: ignore[import-untyped]

    from funding_arbitrage.exchanges.mexc.trading import MexcTradingAdapter

    classes: dict[str, Any] = {
        "bybit": ccxt.bybit,
        "gate": ccxt.gate,
        "okx": ccxt.okx,
        "binance": ccxt.binance,
        "hyperliquid": ccxt.hyperliquid,
    }
    result: dict[str, TradingAdapter] = {}
    for venue in settings.live_venue_values:
        if venue == "mexc":
            credentials = settings.live_credentials(venue)
            result[venue] = MexcTradingAdapter(
                api_key=credentials["apiKey"],
                api_secret=credentials["secret"],
                spot_base_url=settings.mexc_base_url,
                futures_base_url=settings.mexc_futures_base_url,
                timeout_seconds=settings.request_timeout_seconds,
                margin_mode=settings.live_margin_mode,
                leverage=settings.live_leverage,
            )
            continue
        config: dict[str, object] = {
            **settings.live_credentials(venue),
            "enableRateLimit": True,
            "timeout": int(settings.request_timeout_seconds * 1000),
        }
        if venue == "binance":
            config["options"] = {"defaultType": "future", "adjustForTimeDifference": True}
        elif venue == "gate":
            config["options"] = {"defaultType": "swap"}
        elif venue == "bybit":
            config["options"] = {"defaultType": "swap", "adjustForTimeDifference": True}
        elif venue == "okx":
            config["options"] = {"defaultType": "swap", "adjustForTimeDifference": True}
        exchange = classes[venue](config)
        if settings.live_sandbox:
            exchange.set_sandbox_mode(True)
        result[venue] = CcxtTradingAdapter(
            venue, exchange, margin_mode=settings.live_margin_mode
        )
    return result
