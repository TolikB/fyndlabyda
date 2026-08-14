"""Native authenticated MEXC spot and futures execution adapter.

The 2026 futures API has a signature and contract model that differs from the
spot V3 API.  Keeping both implementations here makes the signed bytes,
quantity conversion, idempotency lookup, and timeout recovery explicit.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any
from urllib.parse import urlencode

import httpx

from funding_arbitrage.exchanges.base.models import InstrumentType, NormalizedInstrument
from funding_arbitrage.exchanges.mexc.client import MexcPublicAdapter
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
    VenuePosition,
)


class MexcPrivateError(RuntimeError):
    def __init__(self, message: str, *, definitive: bool) -> None:
        super().__init__(message)
        self.definitive = definitive


def sign_mexc_spot(
    secret: str,
    parameters: Sequence[tuple[str, str | int | float | bool | None]],
) -> str:
    """Sign the exact URL-encoded spot parameter sequence sent on the wire."""

    payload = urlencode(parameters)
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def sign_mexc_futures(
    api_key: str,
    secret: str,
    timestamp_ms: int,
    parameter_string: str,
) -> str:
    payload = f"{api_key}{timestamp_ms}{parameter_string}"
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


class MexcTradingAdapter(TradingAdapter):
    name = "mexc"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        spot_base_url: str = "https://api.mexc.com",
        futures_base_url: str = "https://api.mexc.com",
        timeout_seconds: float = 15.0,
        margin_mode: str = "isolated",
        leverage: int = 1,
        recv_window_ms: int = 5000,
        http_client: httpx.AsyncClient | None = None,
        public_adapter: MexcPublicAdapter | None = None,
        clock_ms: Any | None = None,
        base_url: str | None = None,
    ) -> None:
        # ``base_url`` is retained only for deterministic single-transport tests.
        if base_url is not None:
            spot_base_url = base_url
            futures_base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.margin_mode = margin_mode
        self.leverage = leverage
        if not 1 <= recv_window_ms <= 30_000:
            raise ValueError("MEXC recv_window_ms must be between 1 and 30000")
        self.recv_window_ms = recv_window_ms
        self._futures_recv_window_seconds = max(1, (recv_window_ms + 999) // 1000)
        self._spot_http = http_client
        self._futures_http = http_client
        self._owns_http = http_client is None
        self._public = public_adapter or MexcPublicAdapter(
            spot_base_url=spot_base_url,
            futures_base_url=futures_base_url,
            timeout_seconds=timeout_seconds,
        )
        self._owns_public = public_adapter is None
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._clock_offset_ms = 0
        self._instruments: dict[tuple[str, InstrumentType], NormalizedInstrument] = {}
        self._configured: set[tuple[str, int, str]] = set()
        self._hedge_mode_configured = False

    async def _ensure_http(self, *, futures: bool) -> httpx.AsyncClient:
        if futures:
            if self._futures_http is None:
                self._futures_http = httpx.AsyncClient(
                    base_url=self.futures_base_url, timeout=self.timeout_seconds
                )
            return self._futures_http
        if self._spot_http is None:
            self._spot_http = httpx.AsyncClient(
                base_url=self.spot_base_url, timeout=self.timeout_seconds
            )
        return self._spot_http

    async def initialize(self) -> None:
        if not self.api_key or not self.api_secret:
            raise ValueError("MEXC credentials are missing")
        instruments = await self._public.get_instruments()
        self._instruments = {
            (item.exchange_symbol, item.instrument_type): item
            for item in instruments
            if item.is_active
        }
        client = await self._ensure_http(futures=False)
        response = await client.get("/api/v3/time")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("serverTime") is None:
            raise MexcPrivateError("MEXC server time is missing", definitive=True)
        self._clock_offset_ms = int(payload["serverTime"]) - int(self._clock_ms())

    async def close(self) -> None:
        if self._owns_public:
            await self._public.close()
        if self._owns_http:
            clients = {
                id(client): client
                for client in (self._spot_http, self._futures_http)
                if client is not None
            }
            await asyncio.gather(*(client.aclose() for client in clients.values()))
        self._spot_http = None
        self._futures_http = None

    def _timestamp(self) -> int:
        return int(self._clock_ms()) + self._clock_offset_ms

    async def _spot_request(
        self,
        method: str,
        endpoint: str,
        parameters: dict[str, object] | None = None,
    ) -> Any:
        ordered: list[tuple[str, str | int | float | bool | None]] = [
            (key, str(value)) for key, value in (parameters or {}).items()
        ]
        ordered.extend(
            (("recvWindow", str(self.recv_window_ms)), ("timestamp", str(self._timestamp())))
        )
        ordered.append(("signature", sign_mexc_spot(self.api_secret, ordered)))
        client = await self._ensure_http(futures=False)
        try:
            response = await client.request(
                method,
                endpoint,
                params=ordered,
                headers={"X-MEXC-APIKEY": self.api_key},
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise MexcPrivateError(
                "MEXC spot request outcome is unknown", definitive=False
            ) from exc
        return self._decode_response(response, futures=False)

    async def _futures_request(
        self,
        method: str,
        endpoint: str,
        parameters: dict[str, object] | list[dict[str, object]] | None = None,
    ) -> Any:
        timestamp = self._timestamp()
        parameters = {} if parameters is None else parameters
        if method in {"GET", "DELETE"}:
            if not isinstance(parameters, dict):
                raise ValueError("MEXC futures query parameters must be an object")
            sorted_parameters = sorted((key, str(value)) for key, value in parameters.items())
            parameter_string = "&".join(f"{key}={value}" for key, value in sorted_parameters)
        else:
            parameter_string = json.dumps(parameters, separators=(",", ":"), ensure_ascii=False)
        headers = {
            "ApiKey": self.api_key,
            "Request-Time": str(timestamp),
            "Recv-Window": str(self._futures_recv_window_seconds),
            "Signature": sign_mexc_futures(
                self.api_key, self.api_secret, timestamp, parameter_string
            ),
            "Content-Type": "application/json",
        }
        client = await self._ensure_http(futures=True)
        try:
            if method in {"GET", "DELETE"}:
                query: list[tuple[str, str | int | float | bool | None]] = list(sorted_parameters)
                response = await client.request(method, endpoint, headers=headers, params=query)
            else:
                response = await client.request(
                    method, endpoint, headers=headers, content=parameter_string.encode()
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise MexcPrivateError(
                "MEXC futures request outcome is unknown", definitive=False
            ) from exc
        return self._decode_response(response, futures=True)

    @staticmethod
    def _decode_response(response: httpx.Response, *, futures: bool) -> Any:
        if response.status_code >= 500:
            raise MexcPrivateError(
                f"MEXC HTTP {response.status_code}; outcome unknown", definitive=False
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise MexcPrivateError("MEXC private response is not JSON", definitive=False) from exc
        if response.status_code >= 400:
            raise MexcPrivateError(
                f"MEXC HTTP {response.status_code}: {_safe_message(payload)}", definitive=True
            )
        if futures:
            if not isinstance(payload, dict) or payload.get("success") is not True:
                raise MexcPrivateError(
                    f"MEXC futures rejected: {_safe_message(payload)}", definitive=True
                )
            return payload.get("data")
        if isinstance(payload, dict) and payload.get("code") not in (
            None,
            0,
            200,
            "0",
            "200",
        ):
            raise MexcPrivateError(f"MEXC spot rejected: {_safe_message(payload)}", definitive=True)
        return payload

    async def preflight(self) -> dict[str, object]:
        if not self._instruments:
            await self.initialize()
        kyc, position_mode, balance, positions, orders = await asyncio.gather(
            self._spot_request("GET", "/api/v3/kyc/status"),
            self._futures_request("GET", "/api/v1/private/position/position_mode"),
            self.fetch_balance(),
            self.fetch_positions(),
            self.fetch_open_orders(),
        )
        kyc_status = int(kyc.get("status", 0)) if isinstance(kyc, dict) else 0
        if kyc_status < 2:
            raise MexcPrivateError("MEXC futures requires completed KYC", definitive=True)
        mode = _position_mode(position_mode)
        if mode != 1:
            if positions or orders:
                raise MexcPrivateError(
                    "MEXC Hedge Mode is required and cannot be changed with open risk",
                    definitive=True,
                )
            await self._futures_request(
                "POST",
                "/api/v1/private/position/change_position_mode",
                {"positionMode": 1},
            )
            mode = 1
        self._hedge_mode_configured = True
        return {
            "exchange": self.name,
            "kyc_status": kyc_status,
            "position_mode": mode,
            "currencies": sorted(balance.total),
            "open_positions": len(positions),
            "open_orders": len(orders),
        }

    async def fetch_balance(self) -> VenueBalance:
        spot, futures = await asyncio.gather(
            self._spot_request("GET", "/api/v3/account"),
            self._futures_request("GET", "/api/v1/private/account/assets"),
        )
        spot_rows = spot.get("balances", []) if isinstance(spot, dict) else []
        spot_free: dict[str, Decimal] = {}
        spot_used: dict[str, Decimal] = {}
        spot_total: dict[str, Decimal] = {}
        for row in spot_rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset") or "").upper()
            free_amount = _decimal(row.get("free"))
            locked_amount = _decimal(row.get("locked"))
            if asset and free_amount + locked_amount != 0:
                spot_free[asset] = free_amount
                spot_used[asset] = locked_amount
                spot_total[asset] = free_amount + locked_amount
        future_rows = _object_rows(futures)
        free = dict(spot_free)
        used = dict(spot_used)
        total = dict(spot_total)
        derivative_free = Decimal("0")
        unrealized = Decimal("0")
        for row in future_rows:
            currency = str(row.get("currency") or "").upper()
            available = _decimal(row.get("availableBalance"))
            reserved = _decimal(row.get("positionMargin")) + _decimal(row.get("frozenBalance"))
            cash_balance = _decimal(row.get("cashBalance"))
            if currency:
                free[currency] = free.get(currency, Decimal("0")) + available
                used[currency] = used.get(currency, Decimal("0")) + reserved
                total[currency] = total.get(currency, Decimal("0")) + cash_balance
            if currency in {"USD", "USDT", "USDC"}:
                derivative_free += available
                unrealized += _decimal(row.get("unrealized"))
        return VenueBalance(
            exchange=self.name,
            free=free,
            used=used,
            total=total,
            spot_free=spot_free,
            equity_usd=None,
            free_collateral_usd=derivative_free
            + sum(
                (spot_free.get(asset, Decimal("0")) for asset in ("USD", "USDT", "USDC")),
                Decimal("0"),
            ),
            derivative_free_collateral_usd=derivative_free,
            unrealized_pnl_usd=unrealized,
        )

    async def fetch_positions(self) -> list[VenuePosition]:
        payload = await self._futures_request("GET", "/api/v1/private/position/open_positions")
        result: list[VenuePosition] = []
        for row in _object_rows(payload):
            symbol = str(row.get("symbol") or "")
            contracts = _decimal(row.get("holdVol"))
            if not symbol or contracts <= 0:
                continue
            instrument = self._instrument(symbol, InstrumentType.PERPETUAL)
            result.append(
                VenuePosition(
                    exchange=self.name,
                    exchange_symbol=symbol,
                    instrument_type=InstrumentType.PERPETUAL,
                    side="LONG" if int(row.get("positionType", 1)) == 1 else "SHORT",
                    base_quantity=contracts * instrument.contract_size,
                    entry_price=_positive(row.get("holdAvgPrice")),
                    mark_price=None,
                    unrealized_pnl=_decimal(row.get("unRealizedPnl") or row.get("pnl")),
                )
            )
        return result

    async def fetch_open_orders(self) -> list[TradingOrderResult]:
        spot, futures = await asyncio.gather(
            self._spot_request("GET", "/api/v3/openOrders"),
            self._futures_request(
                "GET",
                "/api/v1/private/order/list/open_orders",
                {"page_num": 1, "page_size": 100},
            ),
        )
        results = [self._parse_spot_order(row) for row in _object_rows(spot)]
        future_rows = futures.get("resultList", []) if isinstance(futures, dict) else futures
        results.extend(self._parse_futures_order(row) for row in _object_rows(future_rows))
        return results

    async def fetch_funding_payments(self, since: datetime) -> list[VenueFundingPayment]:
        payload = await self._futures_request(
            "GET",
            "/api/v1/private/position/funding_records",
            {
                "page_num": 1,
                "page_size": 100,
                "start_time": int(since.astimezone(UTC).timestamp() * 1000),
                "end_time": self._timestamp(),
            },
        )
        rows = payload.get("resultList", []) if isinstance(payload, dict) else payload
        result: list[VenueFundingPayment] = []
        for row in _object_rows(rows):
            timestamp = _utc_from_ms(row.get("settleTime"))
            symbol = str(row.get("symbol") or "UNKNOWN")
            amount = _decimal(row.get("funding"))
            external_id = (
                str(row.get("id") or "")
                or hashlib.sha256(
                    f"mexc:{symbol}:{timestamp.isoformat()}:{amount}".encode()
                ).hexdigest()
            )
            result.append(
                VenueFundingPayment(
                    exchange=self.name,
                    external_id=external_id,
                    exchange_symbol=symbol,
                    amount=amount,
                    currency="USDT",
                    timestamp=timestamp,
                )
            )
        return result

    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal:
        if instrument_type is InstrumentType.SPOT:
            payload = await self._spot_request(
                "GET", "/api/v3/tradeFee", {"symbol": exchange_symbol}
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            fee = _decimal(data.get("takerCommission")) if isinstance(data, dict) else Decimal("-1")
        else:
            payload = await self._futures_request(
                "GET",
                "/api/v1/private/account/tiered_fee_rate/v2",
                {"symbol": exchange_symbol},
            )
            if isinstance(payload, dict) and payload.get("realTakerFee") is not None:
                fee = _decimal(payload["realTakerFee"])
            elif isinstance(payload, dict) and payload.get("takerFee") is not None:
                discount = _decimal(payload.get("takerFeeDiscount", 1))
                fee = _decimal(payload["takerFee"]) * discount
            else:
                fee = Decimal("-1")
        if fee < 0 or fee > Decimal("0.02"):
            raise MexcPrivateError("MEXC returned an invalid taker fee", definitive=True)
        return fee

    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal:
        instrument = self._instrument(exchange_symbol, instrument_type)
        normalized = _floor_step(base_quantity, instrument.step_size)
        return normalized if normalized >= instrument.min_order_size else Decimal("0")

    async def normalize_price(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        price: Decimal,
    ) -> Decimal:
        return _floor_step(price, self._instrument(exchange_symbol, instrument_type).tick_size)

    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult:
        normalized_quantity = await self.normalize_base_quantity(
            request.exchange_symbol, request.instrument_type, request.base_quantity
        )
        normalized_price = await self.normalize_price(
            request.exchange_symbol, request.instrument_type, request.limit_price
        )
        if normalized_quantity <= 0:
            return self._empty_result(request, LiveOrderStatus.REJECTED)
        normalized = request.model_copy(
            update={"base_quantity": normalized_quantity, "limit_price": normalized_price}
        )
        try:
            await asyncio.wait_for(self._place_order(normalized), timeout=timeout_seconds)
        except (TimeoutError, MexcPrivateError) as exc:
            try:
                recovered = await self._recover_order(normalized)
            except MexcPrivateError:
                recovered = None
            if recovered is not None:
                return recovered
            status = (
                LiveOrderStatus.REJECTED
                if isinstance(exc, MexcPrivateError) and exc.definitive
                else LiveOrderStatus.UNKNOWN
            )
            return self._empty_result(normalized, status, type(exc).__name__)
        try:
            result = await self._recover_order(normalized)
        except MexcPrivateError:
            result = None
        if result is None:
            return self._empty_result(normalized, LiveOrderStatus.UNKNOWN, "ack_without_order")
        if result.status in {LiveOrderStatus.OPEN, LiveOrderStatus.PARTIAL}:
            result = await self.cancel_order(result)
        return result

    async def _place_order(self, request: TradingOrderRequest) -> None:
        if request.instrument_type is InstrumentType.SPOT:
            await self._spot_request(
                "POST",
                "/api/v3/order",
                {
                    "symbol": request.exchange_symbol,
                    "side": request.side.upper(),
                    "type": "IMMEDIATE_OR_CANCEL",
                    "quantity": _format_decimal(request.base_quantity),
                    "price": _format_decimal(request.limit_price),
                    "newClientOrderId": request.client_order_id,
                },
            )
            return
        instrument = self._instrument(request.exchange_symbol, request.instrument_type)
        contracts = request.base_quantity / instrument.contract_size
        side = _futures_side(request.side, request.reduce_only)
        await self._futures_request(
            "POST",
            "/api/v1/private/order/create",
            {
                "symbol": request.exchange_symbol,
                "price": _format_decimal(request.limit_price),
                "vol": _format_decimal(contracts),
                "leverage": self.leverage,
                "side": side,
                "type": 3,
                "openType": 1 if self.margin_mode == "isolated" else 2,
                "externalOid": request.client_order_id,
                "positionMode": 1,
            },
        )

    async def _recover_order(self, request: TradingOrderRequest) -> TradingOrderResult | None:
        try:
            if request.instrument_type is InstrumentType.SPOT:
                row = await self._spot_request(
                    "GET",
                    "/api/v3/order",
                    {
                        "symbol": request.exchange_symbol,
                        "origClientOrderId": request.client_order_id,
                    },
                )
                return self._parse_spot_order(row, request)
            row = await self._futures_request(
                "GET",
                f"/api/v1/private/order/external/{request.exchange_symbol}/"
                f"{request.client_order_id}",
            )
            return self._parse_futures_order(row, request)
        except MexcPrivateError as exc:
            if exc.definitive:
                return None
            raise

    async def cancel_order(self, order: TradingOrderResult) -> TradingOrderResult:
        try:
            if order.instrument_type is InstrumentType.SPOT:
                await self._spot_request(
                    "DELETE",
                    "/api/v3/order",
                    {
                        "symbol": order.exchange_symbol,
                        "origClientOrderId": order.client_order_id,
                    },
                )
                row = await self._spot_request(
                    "GET",
                    "/api/v3/order",
                    {
                        "symbol": order.exchange_symbol,
                        "origClientOrderId": order.client_order_id,
                    },
                )
                return self._parse_spot_order(row)
            await self._futures_request(
                "POST",
                "/api/v1/private/order/cancel_with_external",
                [
                    {
                        "symbol": order.exchange_symbol,
                        "externalOid": order.client_order_id,
                    }
                ],
            )
            row = await self._futures_request(
                "GET",
                f"/api/v1/private/order/external/{order.exchange_symbol}/{order.client_order_id}",
            )
            return self._parse_futures_order(row)
        except MexcPrivateError:
            return order.model_copy(update={"status": LiveOrderStatus.UNKNOWN})

    async def configure_derivative(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        leverage: int,
        margin_mode: str,
    ) -> None:
        if instrument_type is InstrumentType.SPOT:
            return
        if not self._hedge_mode_configured:
            raise MexcPrivateError(
                "MEXC private preflight has not configured Hedge Mode",
                definitive=True,
            )
        key = (exchange_symbol, leverage, margin_mode)
        if key in self._configured:
            return
        for position_type in (1, 2):
            await self._futures_request(
                "POST",
                "/api/v1/private/position/change_leverage",
                {
                    "leverage": leverage,
                    "openType": 1 if margin_mode == "isolated" else 2,
                    "symbol": exchange_symbol,
                    "positionType": position_type,
                },
            )
        self._configured.add(key)

    def _parse_spot_order(
        self, row: object, request: TradingOrderRequest | None = None
    ) -> TradingOrderResult:
        if not isinstance(row, dict):
            raise MexcPrivateError("invalid MEXC spot order", definitive=False)
        requested = _decimal(row.get("origQty") or row.get("Qty"))
        if requested <= 0 and request is not None:
            requested = request.base_quantity
        filled = _decimal(row.get("executedQty"))
        quote = _decimal(row.get("cummulativeQuoteQty"))
        average = quote / filled if filled > 0 and quote > 0 else _positive(row.get("price"))
        status = _spot_status(str(row.get("status") or ""), filled, requested)
        return TradingOrderResult(
            exchange=self.name,
            exchange_order_id=str(row.get("orderId")) if row.get("orderId") is not None else None,
            client_order_id=str(
                row.get("clientOrderId")
                or row.get("origClientOrderId")
                or (request.client_order_id if request else "")
            ),
            exchange_symbol=str(row.get("symbol") or (request.exchange_symbol if request else "")),
            instrument_type=InstrumentType.SPOT,
            side=str(row.get("side") or (request.side if request else "")),
            requested_base_quantity=requested,
            filled_base_quantity=filled,
            average_price=average,
            fee=Decimal("0"),
            status=status,
            reduce_only=False,
            timestamp=_utc_from_ms(row.get("updateTime") or row.get("transactTime")),
            raw={"status": str(row.get("status") or "")},
        )

    def _parse_futures_order(
        self, row: object, request: TradingOrderRequest | None = None
    ) -> TradingOrderResult:
        if not isinstance(row, dict):
            raise MexcPrivateError("invalid MEXC futures order", definitive=False)
        symbol = str(row.get("symbol") or (request.exchange_symbol if request else ""))
        instrument_type = request.instrument_type if request else InstrumentType.PERPETUAL
        instrument = self._instrument(symbol, instrument_type)
        requested = _decimal(row.get("vol")) * instrument.contract_size
        if requested <= 0 and request is not None:
            requested = request.base_quantity
        filled = _decimal(row.get("dealVol")) * instrument.contract_size
        state = int(row.get("state") or 0)
        status = _futures_status(state, filled, requested)
        side_code = int(row.get("side") or 0)
        side = request.side if request else ("BUY" if side_code in {1, 2} else "SELL")
        reduce_only = request.reduce_only if request else side_code in {2, 4}
        return TradingOrderResult(
            exchange=self.name,
            exchange_order_id=str(row.get("orderId")) if row.get("orderId") is not None else None,
            client_order_id=str(
                row.get("externalOid") or (request.client_order_id if request else "")
            ),
            exchange_symbol=symbol,
            instrument_type=instrument_type,
            side=side,
            requested_base_quantity=requested,
            filled_base_quantity=filled,
            average_price=_positive(row.get("dealAvgPrice")),
            fee=abs(_decimal(row.get("takerFee"))) + abs(_decimal(row.get("makerFee"))),
            fee_currency=str(row.get("feeCurrency") or "USDT"),
            status=status,
            reduce_only=reduce_only,
            timestamp=_utc_from_ms(row.get("updateTime") or row.get("createTime")),
            raw={"state": state, "error_code": row.get("errorCode")},
        )

    def _empty_result(
        self,
        request: TradingOrderRequest,
        status: LiveOrderStatus,
        error_type: str | None = None,
    ) -> TradingOrderResult:
        return TradingOrderResult(
            exchange=self.name,
            client_order_id=request.client_order_id,
            exchange_symbol=request.exchange_symbol,
            instrument_type=request.instrument_type,
            side=request.side,
            requested_base_quantity=request.base_quantity,
            filled_base_quantity=Decimal("0"),
            status=status,
            reduce_only=request.reduce_only,
            raw={"error_type": error_type} if error_type else {},
        )

    def _instrument(self, symbol: str, instrument_type: InstrumentType) -> NormalizedInstrument:
        try:
            return self._instruments[(symbol, instrument_type)]
        except KeyError as exc:
            raise MexcPrivateError(
                f"MEXC instrument is not active: {symbol}:{instrument_type.value}",
                definitive=True,
            ) from exc


def _safe_message(payload: object) -> str:
    if not isinstance(payload, dict):
        return type(payload).__name__
    return str(payload.get("message") or payload.get("msg") or payload.get("code") or "error")[:200]


def _object_rows(payload: object) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    raise MexcPrivateError("MEXC private rows are malformed", definitive=False)


def _position_mode(payload: object) -> int:
    if isinstance(payload, dict) and payload.get("positionMode") is not None:
        mode = int(payload["positionMode"])
        if mode in {1, 2}:
            return mode
    raise MexcPrivateError("MEXC position mode response is malformed", definitive=True)


def _decimal(value: object) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    result = Decimal(str(value))
    if not result.is_finite():
        raise MexcPrivateError("non-finite MEXC decimal", definitive=False)
    return result


def _positive(value: object) -> Decimal | None:
    result = _decimal(value)
    return result if result > 0 else None


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise MexcPrivateError("invalid MEXC precision step", definitive=True)
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _format_decimal(value: Decimal) -> str:
    return format(value, "f")


def _utc_from_ms(value: object) -> datetime:
    milliseconds = _decimal(value)
    if milliseconds <= 0:
        return datetime.now(UTC)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _futures_side(side: str, reduce_only: bool) -> int:
    normalized = side.upper()
    if normalized not in {"BUY", "SELL"}:
        raise MexcPrivateError("MEXC order side must be BUY or SELL", definitive=True)
    if reduce_only:
        return 2 if normalized == "BUY" else 4
    return 1 if normalized == "BUY" else 3


def _spot_status(status: str, filled: Decimal, requested: Decimal) -> LiveOrderStatus:
    normalized = status.upper()
    if normalized == "FILLED":
        return LiveOrderStatus.FILLED
    if normalized in {"CANCELED", "PARTIALLY_CANCELED"}:
        return LiveOrderStatus.PARTIAL if filled > 0 else LiveOrderStatus.CANCELED
    if normalized == "PARTIALLY_FILLED" or (filled > 0 and filled < requested):
        return LiveOrderStatus.PARTIAL
    if normalized == "NEW":
        return LiveOrderStatus.OPEN
    return LiveOrderStatus.UNKNOWN


def _futures_status(state: int, filled: Decimal, requested: Decimal) -> LiveOrderStatus:
    if state == 3:
        return LiveOrderStatus.FILLED if filled >= requested else LiveOrderStatus.PARTIAL
    if state == 4:
        return LiveOrderStatus.PARTIAL if filled > 0 else LiveOrderStatus.CANCELED
    if state == 5:
        return LiveOrderStatus.REJECTED
    if state in {1, 2}:
        return LiveOrderStatus.PARTIAL if filled > 0 else LiveOrderStatus.OPEN
    return LiveOrderStatus.UNKNOWN
