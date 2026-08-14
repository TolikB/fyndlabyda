from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter


def test_public_exchange_registry_contains_bybit_and_gate() -> None:
    adapters = create_public_adapters(Settings())

    assert isinstance(adapters["bybit"], BybitPublicAdapter)
    assert isinstance(adapters["gate"], GatePublicAdapter)
    assert isinstance(adapters["okx"], OkxPublicAdapter)
    assert isinstance(adapters["binance"], BinancePublicAdapter)
    assert isinstance(adapters["hyperliquid"], HyperliquidPublicAdapter)
    assert isinstance(adapters["mexc"], MexcPublicAdapter)


def test_paper_test_registry_is_mock_only() -> None:
    adapters = create_public_adapters(
        Settings(run_mode="paper_test", market_data_mode="mock", paper_autotrade=True)
    )

    assert set(adapters) == {"bybit", "gate", "okx", "binance", "hyperliquid", "mexc"}
    assert all(isinstance(adapter, MockExchangeAdapter) for adapter in adapters.values())
