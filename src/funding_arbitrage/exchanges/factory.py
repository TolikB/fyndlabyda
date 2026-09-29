"""Configured public exchange adapter registry."""

from __future__ import annotations

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter


def create_public_adapters(settings: Settings) -> dict[str, ExchangeAdapter]:
    venues = settings.enabled_venue_values
    if settings.market_data_mode == "mock":
        return {
            name: MockExchangeAdapter(
                name, funding_interval_seconds=settings.mock_funding_interval_seconds
            )
            for name in venues
        }
    timeout = settings.request_timeout_seconds
    rate = settings.rate_limit_requests_per_second
    burst = settings.rate_limit_burst
    builders: dict[str, ExchangeAdapter] = {}
    for name in venues:
        if name == "bybit":
            builders[name] = BybitPublicAdapter(
                base_url=settings.bybit_base_url,
                websocket_url=settings.bybit_ws_url,
                categories=settings.bybit_category_values,
                timeout_seconds=timeout,
                requests_per_second=rate,
                burst=burst,
            )
        elif name == "gate":
            builders[name] = GatePublicAdapter(
                base_url=settings.gate_base_url,
                websocket_url=settings.gate_ws_url,
                settle=settings.gate_settle,
                timeout_seconds=timeout,
                requests_per_second=rate,
                burst=burst,
            )
        elif name == "okx":
            builders[name] = OkxPublicAdapter(
                base_url=settings.okx_base_url,
                websocket_url=settings.okx_ws_url,
                funding_symbol_limit=settings.okx_funding_symbol_limit,
                timeout_seconds=timeout,
                requests_per_second=rate,
                burst=burst,
            )
        elif name == "binance":
            builders[name] = BinancePublicAdapter(
                spot_base_url=settings.binance_spot_base_url,
                futures_base_url=settings.binance_futures_base_url,
                websocket_url=settings.binance_ws_url,
                timeout_seconds=timeout,
                requests_per_second=rate,
                burst=burst,
            )
        elif name == "hyperliquid":
            builders[name] = HyperliquidPublicAdapter(
                base_url=settings.hyperliquid_base_url,
                websocket_url=settings.hyperliquid_ws_url,
                timeout_seconds=timeout,
                requests_per_second=rate,
                burst=burst,
            )
    return builders
