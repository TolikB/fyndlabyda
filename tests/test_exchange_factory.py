import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter
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
    assert adapters["mexc"].spot_base_url == "https://api.mexc.com"
    assert adapters["mexc"].futures_base_url == "https://api.mexc.com"
    assert isinstance(adapters["kucoin"], KucoinPublicAdapter)
    assert isinstance(adapters["htx"], HtxPublicAdapter)
    assert adapters["kucoin"].futures_base_url == "https://api-futures.kucoin.com"
    assert adapters["htx"].futures_base_url == "https://api.hbdm.com"


def test_canonical_book_event_sink_is_wired_only_to_supported_adapter() -> None:
    async def sink(_event: object) -> None:
        return None

    adapters = create_public_adapters(
        Settings(), canonical_book_event_sink=sink
    )

    assert adapters["bybit"].canonical_book_event_sink is sink


def test_paper_test_registry_is_mock_only() -> None:
    adapters = create_public_adapters(
        Settings(run_mode="paper_test", market_data_mode="mock", paper_autotrade=True)
    )

    assert set(adapters) == {
        "bybit",
        "gate",
        "okx",
        "binance",
        "hyperliquid",
        "mexc",
        "kucoin",
        "htx",
    }
    assert all(isinstance(adapter, MockExchangeAdapter) for adapter in adapters.values())


@pytest.mark.asyncio
async def test_all_paper_test_mock_venues_emit_funding() -> None:
    adapters = create_public_adapters(
        Settings(run_mode="paper_test", market_data_mode="mock", paper_autotrade=True)
    )

    funding = {
        venue: (await adapter.get_funding_rates())[0].funding_rate
        for venue, adapter in adapters.items()
    }

    assert set(funding) == set(adapters)
    assert all(rate != 0 for rate in funding.values())
