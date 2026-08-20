from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.trading import CcxtTradingAdapter
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingOrderRequest,
    TradingOrderResult,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


class InvalidOrder(Exception):
    pass


class PermissionDenied(Exception):
    pass


class FakeCcxtExchange:
    def __init__(self) -> None:
        self.spot = {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "spot": True,
            "swap": False,
            "future": False,
            "contractSize": 1,
            "limits": {"amount": {"min": "0.001"}, "cost": {"min": "5"}},
        }
        self.perp = {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "spot": False,
            "swap": True,
            "future": False,
            "contractSize": "0.001",
            "limits": {"amount": {"min": "1"}, "cost": {"min": "5"}},
        }
        self.future = {
            "id": "BTC-20261231",
            "symbol": "BTC/USDT:USDT-261231",
            "spot": False,
            "swap": False,
            "future": True,
            "contractSize": "0.01",
            "limits": {"amount": {"min": "1"}, "cost": {"min": "5"}},
        }
        self.markets_by_id: dict[str, Any] = {
            "BTCUSDT": [self.spot, self.perp],
            "BTC-20261231": [self.future],
        }
        self.markets = {
            market["symbol"]: market for market in (self.spot, self.perp, self.future)
        }
        self.has = {
            "fetchPositions": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchOrder": True,
            "fetchFundingHistory": True,
            "fetchTradingFee": True,
            "setPositionMode": True,
            "setMarginMode": True,
            "setLeverage": True,
        }
        self.balance_profiles: list[dict[str, object]] = []
        self.open_order_profiles: list[object] = []
        self.positions: list[dict[str, Any]] = []
        self.open_orders: list[dict[str, Any]] = []
        self.closed_orders: list[dict[str, Any]] = []
        self.funding_rows: list[dict[str, Any]] = []
        self.created_row: dict[str, Any] | None = None
        self.fetched_row: dict[str, Any] | None = None
        self.create_error: Exception | None = None
        self.cancel_error: Exception | None = None
        self.open_order_error_type: str | None = None
        self.configure_error: Exception | None = None
        self.credentials_checked = 0
        self.markets_loaded = 0
        self.closed = 0
        self.last_create: tuple[object, ...] | None = None
        self.cancel_calls = 0
        self.configuration_calls: list[tuple[str, object]] = []

    def check_required_credentials(self) -> None:
        self.credentials_checked += 1

    async def load_markets(self, reload: bool = False) -> dict[str, Any]:
        assert reload is True
        self.markets_loaded += 1
        return self.markets

    async def close(self) -> None:
        self.closed += 1

    async def fetch_balance(self, profile: dict[str, object]) -> dict[str, Any]:
        self.balance_profiles.append(profile)
        profile_type = str(profile.get("type") or "combined")
        stable = "100" if profile_type in {"spot", "combined"} else "50"
        result: dict[str, Any] = {
            "free": {"USDT": stable, "BTC": "1" if profile_type == "spot" else "0"},
            "used": {"USDT": "2"},
            "total": {"USDT": stable, "BTC": "1" if profile_type == "spot" else "0"},
            "info": {"unrealised_pnl": "1.25"},
        }
        if profile_type == "combined":
            result["info"] = {
                "result": {
                    "list": [
                        {
                            "totalEquity": "123.5",
                            "totalAvailableBalance": "110.5",
                        }
                    ]
                }
            }
        return result

    async def fetch_positions(self, *, params: dict[str, object]) -> list[dict[str, Any]]:
        return self.positions

    async def fetch_open_orders(
        self,
        *args: object,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, Any]]:
        profile: object = params if params is not None else (args[0] if args else None)
        self.open_order_profiles.append(profile)
        if (
            isinstance(profile, dict)
            and self.open_order_error_type is not None
            and profile.get("type") == self.open_order_error_type
        ):
            raise RuntimeError("synthetic open order failure")
        return self.open_orders

    async def fetch_closed_orders(self, symbol: str) -> list[dict[str, Any]]:
        return self.closed_orders

    async def fetch_funding_history(
        self,
        symbol: str | None,
        since: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        assert symbol is None
        assert since == int(NOW.timestamp() * 1000)
        assert limit == 100
        return self.funding_rows

    async def fetch_trading_fee(self, symbol: str) -> dict[str, Any]:
        return {"taker": "0.0007"}

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        return f"{amount:.3f}"

    def price_to_precision(self, symbol: str, price: float) -> str:
        return f"{price:.2f}"

    async def create_order(self, *args: object) -> dict[str, Any]:
        self.last_create = args
        if self.create_error is not None:
            raise self.create_error
        assert self.created_row is not None
        return self.created_row

    async def cancel_order(self, order_id: str, symbol: str) -> None:
        self.cancel_calls += 1
        if self.cancel_error is not None:
            raise self.cancel_error

    async def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any] | None:
        return self.fetched_row

    async def set_position_mode(self, hedged: bool, symbol: str) -> None:
        self.configuration_calls.append(("position", hedged))
        if self.configure_error is not None:
            raise self.configure_error

    async def set_margin_mode(
        self,
        mode: str,
        symbol: str,
        params: dict[str, Any],
    ) -> None:
        self.configuration_calls.append(("margin", mode))

    async def set_leverage(
        self,
        leverage: int,
        symbol: str,
        params: dict[str, Any],
    ) -> None:
        self.configuration_calls.append(("leverage", leverage))


def _adapter(name: str = "bybit") -> tuple[CcxtTradingAdapter, FakeCcxtExchange]:
    exchange = FakeCcxtExchange()
    return CcxtTradingAdapter(name, exchange, margin_mode="cross"), exchange


def _row(
    *,
    status: str = "closed",
    filled: str = "1000",
    client_order_id: str = "client-1",
    symbol: str = "BTC/USDT:USDT",
) -> dict[str, Any]:
    return {
        "id": "venue-order-1",
        "clientOrderId": client_order_id,
        "symbol": symbol,
        "side": "buy",
        "amount": "1000",
        "filled": filled,
        "average": "100",
        "status": status,
        "reduceOnly": False,
        "fees": [
            {"cost": "-0.2", "currency": "USDT"},
            "ignored",
            {"cost": "0.1", "currency": "USDT"},
        ],
        "info": {"venue": "synthetic"},
    }


def _request(
    *,
    instrument_type: InstrumentType = InstrumentType.PERPETUAL,
    client_order_id: str = "client-1",
) -> TradingOrderRequest:
    return TradingOrderRequest(
        intent_id="intent-1",
        client_order_id=client_order_id,
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        instrument_type=instrument_type,
        side="BUY",
        base_quantity=Decimal("1"),
        limit_price=Decimal("100"),
        reduce_only=False,
    )


def test_fee_ttl_requires_positive_value() -> None:
    with pytest.raises(ValueError, match="TTL must be positive"):
        CcxtTradingAdapter(
            "bybit",
            FakeCcxtExchange(),
            margin_mode="cross",
            fee_cache_ttl_seconds=0,
        )


async def test_initialize_close_and_preflight_cover_authenticated_contract() -> None:
    adapter, exchange = _adapter()
    exchange.positions = [
        {
            "contracts": "2",
            "symbol": "BTC/USDT:USDT",
            "side": "short",
            "entryPrice": "100",
            "markPrice": "99",
            "unrealizedPnl": "2",
        }
    ]
    exchange.open_orders = [_row(status="open", filled="0")]

    result = await adapter.preflight()
    await adapter.close()

    assert result == {
        "exchange": "bybit",
        "currencies": ["USDT"],
        "open_positions": 1,
        "open_orders": 1,
    }
    assert exchange.credentials_checked == 1
    assert exchange.markets_loaded == 1
    assert exchange.closed == 1


@pytest.mark.parametrize(
    ("name", "expected_profiles"),
    [
        ("binance", 2),
        ("gate", 2),
        ("hyperliquid", 2),
        ("htx", 2),
        ("bybit", 1),
        ("okx", 1),
    ],
)
async def test_balance_profiles_preserve_spot_and_derivative_collateral(
    name: str,
    expected_profiles: int,
) -> None:
    adapter, exchange = _adapter(name)

    balance = await adapter.fetch_balance()

    assert len(exchange.balance_profiles) == expected_profiles
    assert balance.exchange == name
    assert balance.total["USDT"] >= Decimal("100")
    if expected_profiles == 2:
        assert balance.spot_free == {"USDT": Decimal("100"), "BTC": Decimal("1")}
        assert balance.derivative_free_collateral_usd == Decimal("50")
    else:
        assert balance.spot_free == balance.free
    if name == "bybit":
        assert balance.equity_usd == Decimal("123.5")
        assert balance.free_collateral_usd == Decimal("110.5")
    if name == "gate":
        assert balance.unrealized_pnl_usd == Decimal("2.5")


class BalanceFailureExchange(FakeCcxtExchange):
    async def fetch_balance(self, profile: dict[str, object]) -> dict[str, Any]:
        raise TimeoutError("synthetic balance timeout")


async def test_balance_and_open_order_failures_are_fail_closed() -> None:
    failing = BalanceFailureExchange()
    adapter = CcxtTradingAdapter("bybit", failing, margin_mode="cross")
    with pytest.raises(RuntimeError, match="balance request failed"):
        await adapter.fetch_balance()

    adapter, exchange = _adapter("gate")
    exchange.open_order_error_type = "spot"
    with pytest.raises(RuntimeError, match="open-order request failed"):
        await adapter.fetch_open_orders()

    exchange.has["fetchOpenOrders"] = False
    with pytest.raises(RuntimeError, match="does not support open-order reconciliation"):
        await adapter.fetch_open_orders()


async def test_positions_filter_zero_unresolved_and_normalize_contracts() -> None:
    adapter, exchange = _adapter("htx")
    exchange.positions = [
        {"contracts": "0", "symbol": "BTC/USDT:USDT"},
        {"contracts": "1", "symbol": "UNKNOWN", "side": "long"},
        {
            "contracts": "-2",
            "symbol": "BTC/USDT:USDT",
            "side": "unexpected",
            "entryPrice": "100",
            "markPrice": "",
            "unrealizedPnl": "-1",
        },
        {
            "contracts": "3",
            "symbol": "BTC/USDT:USDT-261231",
            "side": "LONG",
            "entryPrice": "101",
            "markPrice": "102",
        },
    ]

    positions = await adapter.fetch_positions()

    assert len(positions) == 2
    assert positions[0].side == "SHORT"
    assert positions[0].base_quantity == Decimal("0.002")
    assert positions[0].mark_price is None
    assert positions[1].instrument_type is InstrumentType.FUTURE
    assert positions[1].base_quantity == Decimal("0.03")

    exchange.has["fetchPositions"] = False
    assert await adapter.fetch_positions() == []


@pytest.mark.parametrize("name", ["binance", "gate", "hyperliquid", "bybit", "htx", "okx"])
async def test_open_order_profiles_are_deduplicated(name: str) -> None:
    adapter, exchange = _adapter(name)
    exchange.open_orders = [_row(status="open", filled="0")]

    orders = await adapter.fetch_open_orders()

    assert len(orders) == 1
    assert orders[0].status is LiveOrderStatus.OPEN
    assert orders[0].fee == Decimal("0.3")


async def test_funding_payments_require_history_and_normalize_source_identity() -> None:
    adapter, exchange = _adapter()
    exchange.funding_rows = [
        {"timestamp": None, "amount": "99"},
        {
            "timestamp": int(NOW.timestamp() * 1000),
            "symbol": "BTC/USDT:USDT",
            "amount": "1.25",
            "code": "usdt",
            "info": {"id": "funding-1"},
        },
        {
            "timestamp": int(NOW.timestamp() * 1000) + 1,
            "amount": "-0.5",
            "info": {"contract_code": "ETH-USDT", "currency": "usdc"},
        },
    ]

    payments = await adapter.fetch_funding_payments(NOW)

    assert len(payments) == 2
    assert payments[0].exchange_symbol == "BTCUSDT"
    assert payments[0].currency == "USDT"
    assert payments[1].exchange_symbol == "ETH-USDT"
    assert payments[1].currency == "USDC"
    assert payments[0].external_id != payments[1].external_id

    exchange.has["fetchFundingHistory"] = False
    with pytest.raises(RuntimeError, match="does not support funding-payment history"):
        await adapter.fetch_funding_payments(NOW)


async def test_fee_precision_market_and_minimum_guards() -> None:
    adapter, exchange = _adapter()

    assert await adapter.fetch_taker_fee(
        "BTCUSDT", InstrumentType.PERPETUAL
    ) == Decimal("0.0007")
    normalized = await adapter.normalize_base_quantity(
        "BTCUSDT",
        InstrumentType.PERPETUAL,
        Decimal("1"),
    )
    assert normalized == Decimal("1")
    assert await adapter.normalize_price(
        "BTCUSDT",
        InstrumentType.PERPETUAL,
        Decimal("100.123"),
    ) == Decimal("100.12")
    assert await adapter.normalize_base_quantity(
        "BTCUSDT",
        InstrumentType.PERPETUAL,
        Decimal("0.0001"),
    ) == Decimal("0")

    exchange.has["fetchTradingFee"] = False
    fresh, fresh_exchange = _adapter()
    fresh_exchange.has["fetchTradingFee"] = False
    with pytest.raises(RuntimeError, match="cannot verify"):
        await fresh.fetch_taker_fee("BTCUSDT", InstrumentType.PERPETUAL)

    invalid, invalid_exchange = _adapter()
    invalid_exchange.fetch_trading_fee = lambda symbol: _async_value({"taker": "0.5"})  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="invalid taker fee"):
        await invalid.fetch_taker_fee("BTCUSDT", InstrumentType.PERPETUAL)

    with pytest.raises(ValueError, match="market not found"):
        adapter._market("MISSING", InstrumentType.PERPETUAL)
    with pytest.raises(ValueError, match="amount is below"):
        adapter._validate_order_limits(exchange.perp, Decimal("0"), Decimal("100"))
    high_cost = exchange.perp | {"limits": {"amount": {"min": "1"}, "cost": {"min": "500"}}}
    with pytest.raises(ValueError, match="notional is below"):
        adapter._validate_order_limits(high_cost, Decimal("1"), Decimal("100"))


async def _async_value(value: Any) -> Any:
    return value


@pytest.mark.parametrize(
    ("name", "expected_key", "expected_value"),
    [
        ("okx", "tdMode", "cross"),
        ("bybit", "positionIdx", 0),
        ("gate", "settle", "usdt"),
        ("htx", "marginMode", "cross"),
    ],
)
async def test_submit_ioc_applies_venue_specific_bounded_parameters(
    name: str,
    expected_key: str,
    expected_value: object,
) -> None:
    adapter, exchange = _adapter(name)
    exchange.created_row = _row()

    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0)

    assert result.status is LiveOrderStatus.FILLED
    assert result.filled_base_quantity == Decimal("1")
    assert result.average_price == Decimal("100")
    assert exchange.last_create is not None
    params = exchange.last_create[-1]
    assert isinstance(params, dict)
    assert params["timeInForce"] == "IOC"
    assert params[expected_key] == expected_value


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (InvalidOrder("bad amount"), LiveOrderStatus.REJECTED),
        (PermissionDenied("no permission"), LiveOrderStatus.REJECTED),
        (TimeoutError("ambiguous"), LiveOrderStatus.UNKNOWN),
    ],
)
async def test_submit_failure_classifies_definitive_and_ambiguous_outcomes(
    error: Exception,
    expected: LiveOrderStatus,
) -> None:
    adapter, exchange = _adapter()
    exchange.create_error = error

    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0)

    assert result.status is expected
    assert result.raw == {"error_type": type(error).__name__}


async def test_ambiguous_submit_recovers_by_client_id_without_duplicate_order() -> None:
    adapter, exchange = _adapter()
    exchange.create_error = TimeoutError("ambiguous")
    exchange.open_orders = [_row(client_order_id="client-1")]

    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0)

    assert result.status is LiveOrderStatus.FILLED
    assert result.exchange_order_id == "venue-order-1"


async def test_wait_cancel_and_fetch_helpers_preserve_unknown_outcomes() -> None:
    adapter, exchange = _adapter()
    request = _request()
    initial = TradingOrderResult(
        exchange="bybit",
        exchange_order_id="venue-order-1",
        client_order_id=request.client_order_id,
        exchange_symbol=request.exchange_symbol,
        instrument_type=request.instrument_type,
        side=request.side,
        requested_base_quantity=request.base_quantity,
        filled_base_quantity=Decimal("0"),
        status=LiveOrderStatus.OPEN,
    )
    exchange.fetched_row = _row(status="closed")

    terminal = await adapter._wait_for_terminal(
        initial,
        "BTC/USDT:USDT",
        request,
        timeout_seconds=1,
    )
    assert terminal.status is LiveOrderStatus.FILLED

    no_id = initial.model_copy(update={"exchange_order_id": None})
    assert (await adapter.cancel_order(no_id)).status is LiveOrderStatus.UNKNOWN

    exchange.cancel_error = RuntimeError("cancel failed")
    exchange.fetched_row = None
    unchanged = await adapter.cancel_order(initial)
    assert unchanged == initial
    assert await adapter._fetch_order(None, "BTC/USDT:USDT") is None

    exchange.has["fetchOrder"] = False
    assert await adapter._fetch_order("venue-order-1", "BTC/USDT:USDT") is None


async def test_derivative_configuration_is_idempotent_and_tolerates_unchanged() -> None:
    adapter, exchange = _adapter()
    await adapter.configure_derivative(
        "BTCUSDT",
        InstrumentType.SPOT,
        2,
        "cross",
    )
    assert exchange.configuration_calls == []

    exchange.configure_error = RuntimeError("position mode already configured")
    await adapter.configure_derivative(
        "BTCUSDT",
        InstrumentType.PERPETUAL,
        2,
        "cross",
    )
    first_calls = list(exchange.configuration_calls)
    await adapter.configure_derivative(
        "BTCUSDT",
        InstrumentType.PERPETUAL,
        2,
        "cross",
    )
    assert exchange.configuration_calls == first_calls

    failing, failing_exchange = _adapter()
    failing_exchange.configure_error = RuntimeError("network down")
    with pytest.raises(RuntimeError, match="network down"):
        await failing.configure_derivative(
            "BTCUSDT",
            InstrumentType.PERPETUAL,
            2,
            "cross",
        )


def test_order_parsing_status_fee_market_and_client_id_helpers() -> None:
    adapter, exchange = _adapter("htx")
    assert adapter._venue_client_order_id("alpha").isdigit()
    assert adapter._venue_client_order_id("alpha") == adapter._venue_client_order_id("alpha")

    assert adapter._status("open", Decimal("0"), Decimal("1")) is LiveOrderStatus.OPEN
    assert adapter._status("closed", Decimal("1"), Decimal("1")) is LiveOrderStatus.FILLED
    assert adapter._status("closed", Decimal("0.5"), Decimal("1")) is LiveOrderStatus.PARTIAL
    assert adapter._status("expired", Decimal("0"), Decimal("1")) is LiveOrderStatus.CANCELED
    assert adapter._status("canceled", Decimal("0.5"), Decimal("1")) is LiveOrderStatus.PARTIAL
    assert adapter._status("rejected", Decimal("0"), Decimal("1")) is LiveOrderStatus.REJECTED
    assert adapter._status("mystery", Decimal("0"), Decimal("1")) is LiveOrderStatus.UNKNOWN

    assert adapter._fee({"fee": {"cost": "-1", "currency": "USDT"}}) == (
        Decimal("1"),
        "USDT",
    )
    assert adapter._fee({"fees": ["ignored"]}) == (Decimal("0"), None)
    assert adapter._currency_values({"free": "invalid"}, "free") == {}
    assert adapter._market_from_symbol(None) is None

    unresolved = _row(symbol="UNKNOWN")
    with pytest.raises(ValueError, match="cannot resolve market"):
        adapter._parse_order(unresolved)

    exchange.markets_by_id["BTCUSDT"] = exchange.perp
    assert adapter._market("BTCUSDT", InstrumentType.PERPETUAL) == exchange.perp

