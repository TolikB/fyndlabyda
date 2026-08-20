"""Configured public exchange adapter registry."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import BookEvent, InstrumentType
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.orderbook_protocols import orderbook_protocol


def create_public_adapters(
    settings: Settings,
    *,
    canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
) -> dict[str, ExchangeAdapter]:
    if settings.market_data_mode == "mock":
        adapters: dict[str, ExchangeAdapter] = {
            name: MockExchangeAdapter(name)
            for name in (
                "bybit",
                "gate",
                "okx",
                "binance",
                "hyperliquid",
                "mexc",
                "kucoin",
                "htx",
            )
        }
        _validate_orderbook_protocols(adapters)
        return adapters
    adapters = {
        "bybit": BybitPublicAdapter(
            base_url=settings.bybit_base_url,
            websocket_url=settings.bybit_ws_url,
            categories=settings.bybit_category_values,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "gate": GatePublicAdapter(
            base_url=settings.gate_base_url,
            websocket_url=settings.gate_ws_url,
            settle=settings.gate_settle,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "okx": OkxPublicAdapter(
            base_url=settings.okx_base_url,
            websocket_url=settings.okx_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            funding_symbol_limit=settings.okx_funding_symbol_limit,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "binance": BinancePublicAdapter(
            spot_base_url=settings.binance_spot_base_url,
            futures_base_url=settings.binance_futures_base_url,
            websocket_url=settings.binance_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "hyperliquid": HyperliquidPublicAdapter(
            base_url=settings.hyperliquid_base_url,
            websocket_url=settings.hyperliquid_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "mexc": MexcPublicAdapter(
            spot_base_url=settings.mexc_base_url,
            futures_base_url=settings.mexc_futures_base_url,
            futures_websocket_url=settings.mexc_futures_ws_url,
            spot_websocket_url=settings.mexc_spot_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "kucoin": KucoinPublicAdapter(
            spot_base_url=settings.kucoin_spot_base_url,
            futures_base_url=settings.kucoin_futures_base_url,
            spot_websocket_url=settings.kucoin_spot_ws_url,
            futures_websocket_url=settings.kucoin_futures_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
        "htx": HtxPublicAdapter(
            spot_base_url=settings.htx_spot_base_url,
            futures_base_url=settings.htx_futures_base_url,
            spot_websocket_url=settings.htx_spot_ws_url,
            futures_websocket_url=settings.htx_futures_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            funding_symbol_limit=settings.htx_funding_symbol_limit,
            canonical_book_event_sink=canonical_book_event_sink,
        ),
    }
    _validate_orderbook_protocols(adapters)
    return adapters


def _validate_orderbook_protocols(adapters: dict[str, ExchangeAdapter]) -> None:
    """Fail startup if a configured CEX lacks an explicit book recovery contract."""

    for venue in adapters:
        orderbook_protocol(venue, InstrumentType.SPOT)
        orderbook_protocol(venue, InstrumentType.PERPETUAL)
