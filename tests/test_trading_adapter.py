from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.trading import (
    CcxtTradingAdapter,
    create_trading_adapters,
)
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingOrderRequest,
)


class FakeCcxtExchange:
    def __init__(self, create_result: object) -> None:
        self.create_result = create_result
        self.has = {
            "fetchPositions": False,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchOrder": True,
            "fetchTradingFee": True,
        }
        market = {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "spot": False,
            "swap": True,
            "future": False,
            "contractSize": 1,
            "limits": {"amount": {"min": 0.001}, "cost": {"min": 5}},
        }
        self.markets = {market["symbol"]: market}
        self.markets_by_id = {market["id"]: [market]}

    def amount_to_precision(self, _symbol: str, amount: float) -> str:
        return f"{amount:.3f}"

    def price_to_precision(self, _symbol: str, price: float) -> str:
        return f"{price:.1f}"

    async def create_order(self, *args: object) -> dict[str, Any]:
        if isinstance(self.create_result, BaseException):
            raise self.create_result
        return dict(self.create_result)  # type: ignore[arg-type]

    async def fetch_open_orders(self, *_: object, **__: object) -> list[object]:
        return []

    async def fetch_closed_orders(self, *_: object, **__: object) -> list[object]:
        return []

    async def fetch_order(self, *_: object, **__: object) -> None:
        return None

    async def fetch_balance(self, _params: object) -> dict[str, object]:
        return {
            "free": {"USDT": 900},
            "used": {"USDT": 100},
            "total": {"USDT": 1000},
            "info": {
                "result": {
                    "list": [
                        {
                            "totalEquity": "1012.50",
                            "totalAvailableBalance": "900.25",
                        }
                    ]
                }
            },
        }

    async def fetch_funding_history(
        self, *_: object, **__: object
    ) -> list[dict[str, object]]:
        return [
            {
                "id": "funding-1",
                "symbol": "BTC/USDT:USDT",
                "code": "USDT",
                "amount": "1.25",
                "timestamp": 1786500000000,
            }
        ]

    async def fetch_trading_fee(self, _symbol: str) -> dict[str, object]:
        return {"maker": 0.0002, "taker": 0.0006}


def _request() -> TradingOrderRequest:
    return TradingOrderRequest(
        intent_id="intent",
        client_order_id="fa-client",
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="BUY",
        base_quantity=Decimal("0.0104"),
        limit_price=Decimal("60000.04"),
    )


@pytest.mark.asyncio
async def test_ccxt_adapter_normalizes_quantity_and_parses_terminal_fill() -> None:
    exchange = FakeCcxtExchange(
        {
            "id": "order-1",
            "clientOrderId": "fa-client",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "amount": 0.010,
            "filled": 0.010,
            "average": 60000,
            "status": "closed",
            "fee": {"cost": 0.3, "currency": "USDT"},
        }
    )
    adapter = CcxtTradingAdapter("bybit", exchange, margin_mode="isolated")

    normalized = await adapter.normalize_base_quantity(
        "BTCUSDT", InstrumentType.PERPETUAL, Decimal("0.0104")
    )
    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0.01)

    assert normalized == Decimal("0.010")
    assert result.status is LiveOrderStatus.FILLED
    assert result.filled_base_quantity == Decimal("0.010")
    assert result.fee == Decimal("0.3")


@pytest.mark.asyncio
async def test_ccxt_adapter_treats_unrecoverable_transport_error_as_unknown() -> None:
    class RequestTimeout(Exception):
        pass

    adapter = CcxtTradingAdapter(
        "bybit", FakeCcxtExchange(RequestTimeout("timed out")), margin_mode="isolated"
    )

    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0.01)

    assert result.status is LiveOrderStatus.UNKNOWN


@pytest.mark.asyncio
async def test_ccxt_adapter_preserves_canceled_partial_fill() -> None:
    exchange = FakeCcxtExchange(
        {
            "id": "order-2",
            "clientOrderId": "fa-client",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "amount": 0.010,
            "filled": 0.006,
            "average": 60000,
            "status": "canceled",
        }
    )
    adapter = CcxtTradingAdapter("bybit", exchange, margin_mode="isolated")

    result = await adapter.submit_ioc_order(_request(), timeout_seconds=0.01)

    assert result.status is LiveOrderStatus.PARTIAL
    assert result.filled_base_quantity == Decimal("0.006")


@pytest.mark.asyncio
async def test_bybit_balance_uses_account_equity_including_unrealized_pnl() -> None:
    adapter = CcxtTradingAdapter(
        "bybit", FakeCcxtExchange({}), margin_mode="isolated"
    )

    balance = await adapter.fetch_balance()

    assert balance.equity_usd == Decimal("1012.50")
    assert balance.free_collateral_usd == Decimal("900.25")
    assert balance.derivative_free_collateral_usd == Decimal("900.25")
    assert balance.spot_available("USDT") == Decimal("900")


@pytest.mark.asyncio
async def test_bybit_explicit_zero_available_collateral_never_falls_back_to_cash() -> None:
    class ZeroAvailableExchange(FakeCcxtExchange):
        async def fetch_balance(self, _params: object) -> dict[str, object]:
            result = await super().fetch_balance(_params)
            info = result["info"]
            assert isinstance(info, dict)
            payload = info["result"]
            assert isinstance(payload, dict)
            rows = payload["list"]
            assert isinstance(rows, list)
            rows[0]["totalAvailableBalance"] = "0"  # type: ignore[index]
            return result

    adapter = CcxtTradingAdapter(
        "bybit", ZeroAvailableExchange({}), margin_mode="isolated"
    )

    balance = await adapter.fetch_balance()

    assert balance.free_collateral_usd == Decimal("0")


@pytest.mark.asyncio
async def test_gate_balance_exposes_unrealized_pnl_as_equity_adjustment() -> None:
    class GateBalanceExchange(FakeCcxtExchange):
        async def fetch_balance(self, params: object) -> dict[str, object]:
            profile = params if isinstance(params, dict) else {}
            if profile.get("type") == "swap":
                return {
                    "free": {"USDT": 40},
                    "used": {"USDT": 10},
                    "total": {"USDT": 50},
                    "info": {"unrealised_pnl": "3.25"},
                }
            return {
                "free": {"USDT": 100},
                "used": {},
                "total": {"USDT": 100},
                "info": [],
            }

    adapter = CcxtTradingAdapter(
        "gate", GateBalanceExchange({}), margin_mode="isolated"
    )

    balance = await adapter.fetch_balance()

    assert balance.total["USDT"] == Decimal("150")
    assert balance.unrealized_pnl_usd == Decimal("3.25")
    assert balance.spot_available("USDT") == Decimal("100")
    assert balance.derivative_free_collateral_usd == Decimal("40")


@pytest.mark.asyncio
async def test_private_funding_payments_are_normalized_from_account_history() -> None:
    exchange = FakeCcxtExchange({})
    exchange.has["fetchFundingHistory"] = True
    adapter = CcxtTradingAdapter("bybit", exchange, margin_mode="isolated")

    payments = await adapter.fetch_funding_payments(
        datetime(2026, 8, 1, tzinfo=UTC)
    )

    assert len(payments) == 1
    assert len(payments[0].external_id) == 64
    assert payments[0].exchange_symbol == "BTCUSDT"
    assert payments[0].amount == Decimal("1.25")


@pytest.mark.asyncio
async def test_account_specific_taker_fee_is_loaded_and_cached() -> None:
    adapter = CcxtTradingAdapter(
        "bybit", FakeCcxtExchange({}), margin_mode="isolated"
    )

    fee = await adapter.fetch_taker_fee(
        "BTCUSDT", InstrumentType.PERPETUAL
    )

    assert fee == Decimal("0.0006")


@pytest.mark.asyncio
async def test_factory_builds_every_supported_private_adapter_without_network() -> None:
    settings = Settings(
        _env_file=None,
        RUN_MODE="live",
        MARKET_DATA_MODE="live_public",
        EXECUTION_MODE="live",
        LIVE_ARMED=True,
        LIVE_AUTOTRADE=True,
        LIVE_TRADING_CONFIRM="I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        LIVE_VENUES="bybit,gate,okx,binance,hyperliquid",
        BYBIT_API_KEY="key",
        BYBIT_API_SECRET="secret",
        GATE_API_KEY="key",
        GATE_API_SECRET="secret",
        OKX_API_KEY="key",
        OKX_API_SECRET="secret",
        OKX_API_PASSPHRASE="passphrase",
        BINANCE_API_KEY="key",
        BINANCE_API_SECRET="secret",
        HYPERLIQUID_WALLET_ADDRESS="0x0000000000000000000000000000000000000000",
        HYPERLIQUID_PRIVATE_KEY="0x" + "1" * 64,
    )

    adapters = create_trading_adapters(settings)

    assert set(adapters) == {"bybit", "gate", "okx", "binance", "hyperliquid"}
    assert all(isinstance(adapter, CcxtTradingAdapter) for adapter in adapters.values())
    for adapter in adapters.values():
        await adapter.close()
