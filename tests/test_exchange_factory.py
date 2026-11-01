from pathlib import Path

import yaml

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter


def test_public_exchange_registry_contains_bybit_and_gate() -> None:
    adapters = create_public_adapters(Settings())

    assert isinstance(adapters["bybit"], BybitPublicAdapter)
    assert isinstance(adapters["gate"], GatePublicAdapter)
    assert isinstance(adapters["okx"], OkxPublicAdapter)
    assert isinstance(adapters["binance"], BinancePublicAdapter)
    assert isinstance(adapters["hyperliquid"], HyperliquidPublicAdapter)


def test_paper_test_registry_is_mock_only() -> None:
    adapters = create_public_adapters(
        Settings(run_mode="paper_test", market_data_mode="mock", paper_autotrade=True)
    )

    assert set(adapters) == {"bybit", "gate", "okx", "binance", "hyperliquid"}
    assert all(isinstance(adapter, MockExchangeAdapter) for adapter in adapters.values())


def test_okx_websocket_uses_port_443() -> None:
    # OKX notice 64010 retires port 8443; its docs list the public URL without a port.
    url = "wss://ws.okx.com/ws/v5/public"

    assert Settings().okx_ws_url == url
    assert OkxPublicAdapter().websocket_url == url
    adapter = create_public_adapters(Settings())["okx"]
    assert isinstance(adapter, OkxPublicAdapter)
    assert adapter.websocket_url == url
    raw = yaml.safe_load(Path("config/default.yaml").read_text(encoding="utf-8"))
    assert raw["okx"]["websocket_url"] == url
