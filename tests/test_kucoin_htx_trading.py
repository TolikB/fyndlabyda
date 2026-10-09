from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.htx.trading import HtxTradingAdapter
from funding_arbitrage.exchanges.kucoin.trading import KucoinTradingAdapter
from funding_arbitrage.exchanges.trading import create_trading_adapters
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingOrderRequest,
)
from tests.live_security import live_credential_policy_json


def _market(
    exchange_id: str,
    symbol: str,
    base: str,
    *,
    spot: bool = False,
    contract_size: str = "0.001",
) -> dict[str, Any]:
    return {
        "id": exchange_id,
        "symbol": symbol,
        "base": base,
        "quote": "USDT",
        "settle": "USDT",
        "spot": spot,
        "swap": not spot,
        "future": False,
        "linear": not spot,
        "contractSize": Decimal("1") if spot else Decimal(contract_size),
        "limits": {"amount": {"min": Decimal("1") if not spot else Decimal("0.00001")}},
    }


def _live_settings() -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="production",
        RELEASE_COMMIT_SHA="a" * 40,
        RUN_MODE="live",
        MARKET_DATA_MODE="live_public",
        EXECUTION_MODE="live",
        DATABASE_URL=(
            "postgresql+asyncpg://funding:"
            "database-secret-0123456789abcdef@postgres:5432/funding"
        ),
        REDIS_URL="rediss://redis:6379/0",
        REDIS_USERNAME="funding",
        REDIS_PASSWORD="redis-secret-0123456789abcdefabcd",
        INTERNAL_SERVICE_TLS_REQUIRED=True,
        INTERNAL_TLS_CA_FILE="/run/secrets/internal/ca.crt",
        INTERNAL_TLS_CLIENT_CERT_FILE="/run/secrets/internal/app.crt",
        INTERNAL_TLS_CLIENT_KEY_FILE="/run/secrets/internal/app.key",
        CONTROL_PLANE_SECURITY_ENABLED=True,
        CONTROL_PLANE_JWT_SECRET="0123456789abcdef0123456789abcdef",
        CONTROL_PLANE_MTLS_REQUIRED=True,
        CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED=True,
        CONTROL_PLANE_RATE_LIMIT_BACKEND="redis",
        CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS="a" * 64,
        LIVE_ARMED=True,
        LIVE_AUTOTRADE=False,
        LIVE_TRADING_CONFIRM="I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        LIVE_VENUES="kucoin,htx",
        LIVE_ALLOWED_ASSETS="BTC,ETH",
        LIVE_EXPECTED_EGRESS_IP="203.0.113.10",
        LIVE_CREDENTIAL_POLICY_JSON=live_credential_policy_json(
            {"kucoin": "kucoin-key", "htx": "htx-key"}
        ),
        KUCOIN_API_KEY="kucoin-key",
        KUCOIN_API_SECRET="kucoin-secret",
        KUCOIN_API_PASSPHRASE="kucoin-passphrase",
        HTX_API_KEY="htx-key",
        HTX_API_SECRET="htx-secret",
        TELEGRAM_ENABLED=True,
        TELEGRAM_BOT_TOKEN="telegram-secret",
        TELEGRAM_CHAT_ID="123",
    )


def test_live_factory_builds_separate_kucoin_accounts_and_official_htx_host() -> None:
    adapters = create_trading_adapters(_live_settings())

    assert set(adapters) == {"kucoin", "htx"}
    kucoin = adapters["kucoin"]
    htx = adapters["htx"]
    assert isinstance(kucoin, KucoinTradingAdapter)
    assert isinstance(htx, HtxTradingAdapter)
    assert kucoin.spot.exchange.id == "kucoin"
    assert kucoin.futures.exchange.id == "kucoinfutures"
    assert kucoin.spot.exchange.options["uta"] is False
    assert kucoin.futures.exchange.options["uta"] is False
    assert kucoin.allowed_assets == frozenset({"BTC", "ETH"})
    assert htx.exchange.urls["hostnames"]["contract"] == "api.hbdm.com"
    assert htx.margin_mode == "isolated"


class FundingExchange:
    def __init__(self, rows_by_symbol: dict[str, list[dict[str, Any]]]) -> None:
        self.markets = {
            "BTC/USDT:USDT": _market("BTC-USDT", "BTC/USDT:USDT", "BTC"),
            "DOGE/USDT:USDT": _market("DOGE-USDT", "DOGE/USDT:USDT", "DOGE"),
        }
        self.markets_by_id = {market["id"]: [market] for market in self.markets.values()}
        self.rows_by_symbol = rows_by_symbol
        self.calls: list[tuple[str, int, int, dict[str, object]]] = []

    async def fetch_funding_history(
        self,
        symbol: str,
        since: int,
        limit: int,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((symbol, since, limit, params or {}))
        return self.rows_by_symbol.get(symbol, [])


@pytest.mark.asyncio
async def test_kucoin_private_funding_is_polled_per_allowlisted_market() -> None:
    timestamp = 1_767_225_600_000
    futures = FundingExchange(
        {
            "BTC/USDT:USDT": [
                {
                    "id": 239471298749817.0,
                    "symbol": "BTC/USDT:USDT",
                    "code": "USDT",
                    "amount": "0.055",
                    "timestamp": timestamp,
                    "info": {
                        "id": 239471298749817,
                        "symbol": "BTC-USDT",
                    },
                }
            ]
        }
    )
    adapter = KucoinTradingAdapter(
        object(),
        futures,
        margin_mode="isolated",
        allowed_assets=frozenset({"BTC"}),
    )
    payments = await adapter.fetch_funding_payments(datetime(2026, 1, 1, tzinfo=UTC))

    assert [call[0] for call in futures.calls] == ["BTC/USDT:USDT"]
    assert len(payments) == 1
    assert payments[0].exchange == "kucoin"
    assert payments[0].exchange_symbol == "BTC-USDT"
    assert payments[0].amount == Decimal("0.055")
    assert payments[0].currency == "USDT"


class HtxPrivateExchange(FundingExchange):
    def __init__(
        self,
        rows_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
        *,
        recover_order: bool = True,
    ) -> None:
        super().__init__(rows_by_symbol or {})
        self.has = {
            "fetchOpenOrders": True,
            "fetchClosedOrders": False,
            "fetchOrder": False,
        }
        self.recover_order = recover_order
        self.create_calls = 0
        self.fee_calls = 0
        self.last_params: dict[str, object] = {}

    def amount_to_precision(self, _symbol: str, amount: float) -> str:
        return str(int(amount))

    def price_to_precision(self, _symbol: str, price: float) -> str:
        return f"{price:.1f}"

    async def request(
        self,
        path: str,
        api: list[str],
        method: str,
        params: dict[str, object],
    ) -> dict[str, object]:
        self.fee_calls += 1
        assert path == "linear-swap-api/v1/swap_fee"
        assert api == ["contract", "private"]
        assert method == "POST"
        assert params == {"contract_code": "BTC-USDT"}
        return {
            "status": "ok",
            "data": [
                {
                    "contract_code": "BTC-USDT",
                    "open_taker_fee": "0.0006",
                    "close_taker_fee": "0.0007",
                }
            ],
        }

    async def create_order(
        self,
        _symbol: str,
        _order_type: str,
        _side: str,
        _amount: float,
        _price: float,
        params: dict[str, object],
    ) -> dict[str, object]:
        self.create_calls += 1
        self.last_params = params
        raise TimeoutError("ambiguous submission")

    async def fetch_open_orders(self, _symbol: str) -> list[dict[str, object]]:
        if not self.recover_order:
            return []
        return [
            {
                "id": "htx-101",
                "clientOrderId": self.last_params["clientOrderId"],
                "symbol": "BTC/USDT:USDT",
                "amount": 2,
                "filled": 2,
                "average": 100000,
                "price": 100000,
                "status": "closed",
                "side": "sell",
                "reduceOnly": False,
                "fees": [{"cost": "0.12", "currency": "USDT"}],
            }
        ]


@pytest.mark.asyncio
async def test_htx_derivative_fee_uses_private_linear_swap_endpoint_and_cache() -> None:
    exchange = HtxPrivateExchange()
    adapter = HtxTradingAdapter(
        exchange,
        margin_mode="isolated",
        allowed_assets=frozenset({"BTC"}),
    )
    first = await adapter.fetch_taker_fee("BTC-USDT", InstrumentType.PERPETUAL)
    second = await adapter.fetch_taker_fee("BTC-USDT", InstrumentType.PERPETUAL)

    assert first == Decimal("0.0007")
    assert second == first
    assert exchange.fee_calls == 1


@pytest.mark.asyncio
async def test_htx_private_funding_uses_allowlist_and_isolated_margin() -> None:
    exchange = HtxPrivateExchange(
        {
            "BTC/USDT:USDT": [
                {
                    "id": "2194774775",
                    "symbol": "BTC/USDT:USDT",
                    "code": "USDT",
                    "amount": "0.000433",
                    "timestamp": 1_767_225_600_000,
                    "info": {
                        "id": "2194774775",
                        "contract_code": "BTC-USDT",
                        "type": "30",
                    },
                }
            ]
        }
    )
    adapter = HtxTradingAdapter(
        exchange,
        margin_mode="isolated",
        allowed_assets=frozenset({"BTC"}),
    )
    payments = await adapter.fetch_funding_payments(datetime(2026, 1, 1, tzinfo=UTC))

    assert [call[0] for call in exchange.calls] == ["BTC/USDT:USDT"]
    assert exchange.calls[0][3] == {"marginMode": "isolated"}
    assert len(payments) == 1
    assert payments[0].external_id
    assert payments[0].exchange_symbol == "BTC-USDT"


def _htx_order_request() -> TradingOrderRequest:
    return TradingOrderRequest(
        intent_id="intent-htx-1",
        client_order_id="fa-htx-BTC-long-open-1",
        exchange="htx",
        exchange_symbol="BTC-USDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="SELL",
        base_quantity=Decimal("0.0023"),
        limit_price=Decimal("100000.09"),
    )


@pytest.mark.asyncio
async def test_htx_timeout_recovers_numeric_client_id_without_duplicate_order() -> None:
    exchange = HtxPrivateExchange(recover_order=True)
    adapter = HtxTradingAdapter(
        exchange,
        margin_mode="isolated",
        allowed_assets=frozenset({"BTC"}),
    )
    request = _htx_order_request()
    result = await adapter.submit_ioc_order(request, 1)

    venue_id = str(exchange.last_params["clientOrderId"])
    assert venue_id.isdigit()
    assert 0 < int(venue_id) <= 9_000_000_000_000_000_000
    assert venue_id == adapter._venue_client_order_id(request.client_order_id)
    assert exchange.last_params["marginMode"] == "isolated"
    assert exchange.create_calls == 1
    assert result.client_order_id == request.client_order_id
    assert result.status is LiveOrderStatus.FILLED
    assert result.requested_base_quantity == Decimal("0.002")
    assert result.filled_base_quantity == Decimal("0.002")
    assert result.fee == Decimal("0.12")


@pytest.mark.asyncio
async def test_htx_unresolved_timeout_returns_unknown_without_resubmission() -> None:
    exchange = HtxPrivateExchange(recover_order=False)
    adapter = HtxTradingAdapter(
        exchange,
        margin_mode="isolated",
        allowed_assets=frozenset({"BTC"}),
    )
    result = await adapter.submit_ioc_order(_htx_order_request(), 1)

    assert exchange.create_calls == 1
    assert result.status is LiveOrderStatus.UNKNOWN
    assert result.filled_base_quantity == 0
