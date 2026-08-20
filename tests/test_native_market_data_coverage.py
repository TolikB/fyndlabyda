import asyncio
import inspect
from pathlib import Path

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.exchanges.public_events import public_event_profiles


async def test_all_eight_cex_have_native_websocket_and_rest_recovery_boundaries() -> None:
    adapters = create_public_adapters(Settings(MARKET_DATA_MODE="live_public"))

    assert set(adapters) == {
        "binance",
        "bybit",
        "gate",
        "okx",
        "hyperliquid",
        "mexc",
        "kucoin",
        "htx",
    }
    for venue, adapter in adapters.items():
        implementation = type(adapter)
        assert implementation.get_orderbook is not ExchangeAdapter.get_orderbook, venue
        assert implementation.stream_orderbooks is not ExchangeAdapter.stream_orderbooks, venue
        assert implementation.stream_tickers is not ExchangeAdapter.stream_tickers, venue
        assert implementation.get_candles is not ExchangeAdapter.get_candles, venue
        assert inspect.isasyncgenfunction(implementation._stream_orderbooks), venue
        assert public_event_profiles(venue)
    await asyncio.gather(*(adapter.close() for adapter in adapters.values()))


@pytest.mark.parametrize(
    ("module", "recovery_markers"),
    [
        ("binance.orderbook", ("snapshot", "delta", "sequence")),
        ("bybit.orderbook", ("snapshot", "delta", "sequence")),
        ("gate.orderbook", ("snapshot", "sequence")),
        ("okx.orderbook", ("snapshot", "delta", "previous_sequence")),
        ("hyperliquid.orderbook", ("snapshot", "sequence")),
        ("mexc.orderbook", ("snapshot", "delta", "version")),
    ],
)
def test_native_books_expose_protocol_specific_recovery_markers(
    module: str, recovery_markers: tuple[str, ...]
) -> None:
    path = (
        Path(__file__).parents[1]
        / "src"
        / "funding_arbitrage"
        / "exchanges"
        / Path(*module.split("."))
    ).with_suffix(".py")
    source = path.read_text(encoding="utf-8").lower()
    for marker in recovery_markers:
        assert marker in source
