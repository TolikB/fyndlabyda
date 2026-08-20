from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qsl

import httpx
import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import (
    InstrumentType,
    NormalizedInstrument,
)
from funding_arbitrage.exchanges.mexc.trading import (
    MexcPrivateError,
    MexcTradingAdapter,
    sign_mexc_futures,
    sign_mexc_spot,
)
from funding_arbitrage.exchanges.trading import create_trading_adapters
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    TradingOrderRequest,
)
from tests.live_security import live_credential_policy_json


def _perpetual() -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange="mexc",
        exchange_symbol="BTC_USDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        contract_size=Decimal("0.0001"),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.0001"),
        min_order_size=Decimal("0.0002"),
        funding_interval=8,
    )


def _spot() -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange="mexc",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.SPOT,
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.00001"),
        min_order_size=Decimal("0.0001"),
    )


def _adapter(handler: httpx.MockTransport) -> tuple[MexcTradingAdapter, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler, base_url="https://test.invalid")
    adapter = MexcTradingAdapter(
        api_key="futures-key",
        api_secret="futures-secret",
        base_url="https://test.invalid",
        http_client=client,
        clock_ms=lambda: 1761887134000,
    )
    adapter._instruments = {
        ("BTC_USDT", InstrumentType.PERPETUAL): _perpetual(),
        ("BTCUSDT", InstrumentType.SPOT): _spot(),
    }
    adapter._hedge_mode_configured = True
    return adapter, client


def _future_request(*, reduce_only: bool = False) -> TradingOrderRequest:
    return TradingOrderRequest(
        intent_id="intent-1",
        client_order_id="fa123456789",
        exchange="mexc",
        exchange_symbol="BTC_USDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="BUY" if reduce_only else "SELL",
        base_quantity=Decimal("0.00109"),
        limit_price=Decimal("100000.09"),
        reduce_only=reduce_only,
    )


def _spot_request() -> TradingOrderRequest:
    return TradingOrderRequest(
        intent_id="intent-spot-1",
        client_order_id="faspot123456",
        exchange="mexc",
        exchange_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        side="BUY",
        base_quantity=Decimal("0.001009"),
        limit_price=Decimal("100000.019"),
        reduce_only=False,
    )


@pytest.mark.asyncio
async def test_mexc_private_transports_use_separate_official_clients() -> None:
    adapter = MexcTradingAdapter(api_key="key", api_secret="secret")
    spot = await adapter._ensure_http(futures=False)
    futures = await adapter._ensure_http(futures=True)

    assert str(spot.base_url) == "https://api.mexc.com"
    assert str(futures.base_url) == "https://api.mexc.com"
    assert spot is not futures
    await adapter.close()


def test_mexc_signatures_match_fixed_vectors() -> None:
    spot_parameters = [
        ("symbol", "BTCUSDT"),
        ("side", "BUY"),
        ("type", "LIMIT"),
        ("quantity", "1"),
        ("price", "11"),
        ("recvWindow", "5000"),
        ("timestamp", "1644489390087"),
    ]
    assert (
        sign_mexc_spot("45d0b3c26f2644f19bfb98b07741b2f5", spot_parameters)
        == "fd3e4e8543c5188531eb7279d68ae7d26a573d0fc5ab0d18eb692451654d837a"
    )
    assert (
        sign_mexc_futures(
            "futures-key",
            "futures-secret",
            1761887134000,
            "page_num=1&page_size=100",
        )
        == "17d536feb3d177e515030d24ae0eb051ef1f2a37a26aeefc74db0bf471e7db8a"
    )


def test_live_factory_uses_native_mexc_adapter_without_network_calls() -> None:
    settings = Settings(
        _env_file=None,
        APP_ENV="production",
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
        LIVE_AUTOTRADE=True,
        LIVE_TRADING_CONFIRM="I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        LIVE_VENUES="mexc",
        LIVE_EXPECTED_EGRESS_IP="203.0.113.10",
        LIVE_CREDENTIAL_POLICY_JSON=live_credential_policy_json({"mexc": "mexc-key"}),
        MEXC_API_KEY="mexc-key",
        MEXC_API_SECRET="mexc-secret",
        TELEGRAM_ENABLED=True,
        TELEGRAM_BOT_TOKEN="telegram-secret",
        TELEGRAM_CHAT_ID="123",
    )

    adapters = create_trading_adapters(settings)

    assert set(adapters) == {"mexc"}
    assert isinstance(adapters["mexc"], MexcTradingAdapter)
    assert adapters["mexc"].spot_base_url == "https://api.mexc.com"
    assert adapters["mexc"].futures_base_url == "https://api.mexc.com"


@pytest.mark.asyncio
async def test_mexc_spot_ioc_is_signed_once_and_recovers_the_fill() -> None:
    creates = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal creates
        parameters = parse_qsl(request.url.query.decode())
        unsigned = [(key, value) for key, value in parameters if key != "signature"]
        signature = dict(parameters)["signature"]
        assert signature == sign_mexc_spot("futures-secret", unsigned)
        if request.method == "POST":
            creates += 1
            sent = dict(parameters)
            assert sent["type"] == "IMMEDIATE_OR_CANCEL"
            assert sent["quantity"] == "0.00100"
            assert sent["price"] == "100000.01"
            assert sent["newClientOrderId"] == "faspot123456"
            return httpx.Response(
                200,
                json={
                    "symbol": "BTCUSDT",
                    "orderId": "spot-1",
                    "clientOrderId": "faspot123456",
                    "transactTime": 1761887134000,
                },
            )
        assert request.method == "GET"
        assert dict(parameters)["origClientOrderId"] == "faspot123456"
        return httpx.Response(
            200,
            json={
                "symbol": "BTCUSDT",
                "orderId": "spot-1",
                "clientOrderId": "faspot123456",
                "price": "100000.01",
                "origQty": "0.00100",
                "executedQty": "0.00100",
                "cummulativeQuoteQty": "100.00001",
                "status": "FILLED",
                "side": "BUY",
                "updateTime": 1761887134000,
            },
        )

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_spot_request(), 1)
    await client.aclose()

    assert creates == 1
    assert result.status is LiveOrderStatus.FILLED
    assert result.filled_base_quantity == Decimal("0.00100")
    assert result.average_price == Decimal("100000.01")


@pytest.mark.asyncio
async def test_mexc_spot_uses_authenticated_account_taker_fee() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/tradeFee"
        assert dict(parse_qsl(request.url.query.decode()))["symbol"] == "BTCUSDT"
        return httpx.Response(
            200,
            json={
                "data": {"makerCommission": "0.0005", "takerCommission": "0.0007"},
                "code": 0,
                "msg": "success",
            },
        )

    adapter, client = _adapter(httpx.MockTransport(handler))
    fee = await adapter.fetch_taker_fee("BTCUSDT", InstrumentType.SPOT)
    await client.aclose()

    assert fee == Decimal("0.0007")


@pytest.mark.asyncio
async def test_mexc_futures_full_fill_uses_contract_units_and_signed_exact_json() -> None:
    creates = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal creates
        if request.url.path == "/api/v1/private/order/create":
            creates += 1
            body = request.content.decode()
            payload = json.loads(body)
            assert payload["vol"] == "10"
            assert payload["price"] == "100000.0"
            assert payload["side"] == 3
            assert payload["type"] == 3
            expected = hmac.new(
                b"futures-secret",
                ("futures-key1761887134000" + body).encode(),
                hashlib.sha256,
            ).hexdigest()
            assert request.headers["Signature"] == expected
            assert request.headers["Recv-Window"] == "5"
            return httpx.Response(
                200,
                json={"success": True, "code": 0, "data": {"orderId": "99", "ts": 1761887134000}},
            )
        assert request.url.path.endswith("/BTC_USDT/fa123456789")
        return httpx.Response(
            200,
            json={
                "success": True,
                "code": 0,
                "data": {
                    "orderId": "99",
                    "externalOid": "fa123456789",
                    "symbol": "BTC_USDT",
                    "vol": "10",
                    "dealVol": "10",
                    "dealAvgPrice": "100000",
                    "side": 3,
                    "state": 3,
                    "takerFee": "0.08",
                    "feeCurrency": "USDT",
                    "updateTime": 1761887134000,
                },
            },
        )

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_future_request(), 1)
    await client.aclose()

    assert creates == 1
    assert result.status is LiveOrderStatus.FILLED
    assert result.requested_base_quantity == Decimal("0.0010")
    assert result.filled_base_quantity == Decimal("0.0010")
    assert result.average_price == Decimal("100000")
    assert result.fee == Decimal("0.08")


@pytest.mark.asyncio
async def test_mexc_partial_ioc_is_canceled_and_keeps_exact_filled_quantity() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/api/v1/private/order/create":
            return httpx.Response(
                200,
                json={"success": True, "code": 0, "data": {"orderId": "77", "ts": 1761887134000}},
            )
        if request.url.path.endswith("/BTC_USDT/fa123456789"):
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "code": 0,
                    "data": {
                        "orderId": "77",
                        "externalOid": "fa123456789",
                        "symbol": "BTC_USDT",
                        "vol": "10",
                        "dealVol": "4",
                        "dealAvgPrice": "100000",
                        "side": 3,
                        "state": 2,
                    },
                },
            )
        if request.url.path == "/api/v1/private/order/cancel_with_external":
            body = request.content.decode()
            assert json.loads(body) == [
                {
                    "symbol": "BTC_USDT",
                    "externalOid": "fa123456789",
                }
            ]
            expected = hmac.new(
                b"futures-secret",
                ("futures-key1761887134000" + body).encode(),
                hashlib.sha256,
            ).hexdigest()
            assert request.headers["Signature"] == expected
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "code": 0,
                    "data": [{"orderId": "77", "errorCode": 0, "errorMsg": "success"}],
                },
            )
        assert request.url.path.endswith("/BTC_USDT/fa123456789")
        return httpx.Response(
            200,
            json={
                "success": True,
                "code": 0,
                "data": {
                    "orderId": "77",
                    "externalOid": "fa123456789",
                    "symbol": "BTC_USDT",
                    "vol": "10",
                    "dealVol": "4",
                    "dealAvgPrice": "100000",
                    "side": 3,
                    "state": 4,
                },
            },
        )

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_future_request(), 1)
    await client.aclose()

    assert result.status is LiveOrderStatus.PARTIAL
    assert result.filled_base_quantity == Decimal("0.0004")
    assert paths.count("/api/v1/private/order/create") == 1
    assert "/api/v1/private/order/cancel_with_external" in paths


@pytest.mark.asyncio
async def test_mexc_timeout_recovers_by_external_id_without_duplicate_submission() -> None:
    creates = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal creates
        if request.url.path == "/api/v1/private/order/create":
            creates += 1
            raise httpx.ReadTimeout("ambiguous timeout", request=request)
        return httpx.Response(
            200,
            json={
                "success": True,
                "code": 0,
                "data": {
                    "orderId": "101",
                    "externalOid": "fa123456789",
                    "symbol": "BTC_USDT",
                    "vol": "10",
                    "dealVol": "10",
                    "dealAvgPrice": "100000",
                    "side": 3,
                    "state": 3,
                },
            },
        )

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_future_request(), 1)
    await client.aclose()

    assert creates == 1
    assert result.status is LiveOrderStatus.FILLED
    assert result.exchange_order_id == "101"


@pytest.mark.asyncio
async def test_mexc_timeout_and_failed_recovery_return_unknown() -> None:
    creates = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal creates
        if request.url.path == "/api/v1/private/order/create":
            creates += 1
        raise httpx.ReadTimeout("network unavailable", request=request)

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_future_request(), 1)
    await client.aclose()

    assert creates == 1
    assert result.status is LiveOrderStatus.UNKNOWN
    assert result.filled_base_quantity == 0


@pytest.mark.asyncio
async def test_mexc_definitive_rejection_is_not_retried() -> None:
    creates = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal creates
        if request.url.path == "/api/v1/private/order/create":
            creates += 1
            return httpx.Response(400, json={"success": False, "code": 1002})
        return httpx.Response(404, json={"success": False, "code": 2006})

    adapter, client = _adapter(httpx.MockTransport(handler))
    result = await adapter.submit_ioc_order(_future_request(), 1)
    await client.aclose()

    assert creates == 1
    assert result.status is LiveOrderStatus.REJECTED


@pytest.mark.asyncio
async def test_mexc_private_account_position_funding_and_fee_are_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/account":
            return httpx.Response(
                200,
                json={
                    "balances": [
                        {"asset": "USDT", "free": "95", "locked": "5"},
                        {"asset": "BTC", "free": "0.49", "locked": "0.01"},
                    ]
                },
            )
        if request.url.path == "/api/v1/private/account/assets":
            return _future_response(
                [
                    {
                        "currency": "USDT",
                        "availableBalance": "32",
                        "positionMargin": "3",
                        "frozenBalance": "1",
                        "cashBalance": "30",
                        "equity": "32",
                        "unrealized": "2",
                    }
                ]
            )
        if request.url.path == "/api/v1/private/position/open_positions":
            return _future_response(
                [
                    {
                        "symbol": "BTC_USDT",
                        "holdVol": "5",
                        "positionType": 2,
                        "holdAvgPrice": "100000",
                        "unRealizedPnl": "1.25",
                    }
                ]
            )
        if request.url.path == "/api/v1/private/position/funding_records":
            query = dict(parse_qsl(request.url.query.decode()))
            assert query["page_num"] == "1"
            return _future_response(
                {
                    "resultList": [
                        {
                            "id": "funding-1",
                            "symbol": "BTC_USDT",
                            "funding": "1.25",
                            "settleTime": 1761887134000,
                        }
                    ]
                }
            )
        assert request.url.path == "/api/v1/private/account/tiered_fee_rate/v2"
        return _future_response({"realTakerFee": "0.0006"})

    adapter, client = _adapter(httpx.MockTransport(handler))
    balance = await adapter.fetch_balance()
    positions = await adapter.fetch_positions()
    funding = await adapter.fetch_funding_payments(datetime(2026, 10, 30, tzinfo=UTC))
    fee = await adapter.fetch_taker_fee("BTC_USDT", InstrumentType.PERPETUAL)
    await client.aclose()

    assert balance.total == {"USDT": Decimal("130"), "BTC": Decimal("0.50")}
    assert balance.used == {"USDT": Decimal("9"), "BTC": Decimal("0.01")}
    assert balance.equity_usd is None
    assert balance.free_collateral_usd == Decimal("127")
    assert balance.derivative_free_collateral_usd == Decimal("32")
    assert balance.unrealized_pnl_usd == Decimal("2")
    assert positions[0].side == "SHORT"
    assert positions[0].base_quantity == Decimal("0.0005")
    assert funding[0].amount == Decimal("1.25")
    assert funding[0].external_id == "funding-1"
    assert fee == Decimal("0.0006")


@pytest.mark.asyncio
async def test_mexc_preflight_enforces_kyc_and_configures_hedge_mode() -> None:
    changed: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/kyc/status":
            return httpx.Response(200, json={"status": 2})
        if request.url.path == "/api/v3/account":
            return httpx.Response(200, json={"balances": []})
        if request.url.path == "/api/v3/openOrders":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/private/position/position_mode":
            return _future_response({"positionMode": 2})
        if request.url.path in {
            "/api/v1/private/account/assets",
            "/api/v1/private/position/open_positions",
        }:
            return _future_response([])
        if request.url.path == "/api/v1/private/order/list/open_orders":
            return _future_response({"resultList": []})
        assert request.url.path == "/api/v1/private/position/change_position_mode"
        changed.append(json.loads(request.content))
        return _future_response(None)

    adapter, client = _adapter(httpx.MockTransport(handler))
    adapter._hedge_mode_configured = False
    result = await adapter.preflight()
    await client.aclose()

    assert result["position_mode"] == 1
    assert changed == [{"positionMode": 1}]
    assert adapter._hedge_mode_configured is True


@pytest.mark.asyncio
async def test_mexc_preflight_rejects_missing_kyc_without_secret_leak() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/kyc/status":
            return httpx.Response(200, json={"status": 1})
        if request.url.path == "/api/v3/account":
            return httpx.Response(200, json={"balances": []})
        if request.url.path == "/api/v3/openOrders":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/private/position/position_mode":
            return _future_response({"positionMode": 1})
        if request.url.path in {
            "/api/v1/private/account/assets",
            "/api/v1/private/position/open_positions",
        }:
            return _future_response([])
        return _future_response({"resultList": []})

    adapter, client = _adapter(httpx.MockTransport(handler))
    with pytest.raises(MexcPrivateError) as caught:
        await adapter.preflight()
    await client.aclose()

    assert caught.value.definitive is True
    assert "futures-secret" not in str(caught.value)


def _future_response(data: object) -> httpx.Response:
    return httpx.Response(200, json={"success": True, "code": 0, "data": data})
