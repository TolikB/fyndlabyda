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
    if settings.market_data_mode == "mock":
        return {
            name: MockExchangeAdapter(name)
            for name in ("bybit", "gate", "okx", "binance", "hyperliquid")
        }
    return {
        "bybit": BybitPublicAdapter(
            base_url=settings.bybit_base_url,
            websocket_url=settings.bybit_ws_url,
            categories=settings.bybit_category_values,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
        ),
        "gate": GatePublicAdapter(
            base_url=settings.gate_base_url,
            websocket_url=settings.gate_ws_url,
            settle=settings.gate_settle,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
        ),
        "okx": OkxPublicAdapter(
            base_url=settings.okx_base_url,
            websocket_url=settings.okx_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
            funding_symbol_limit=settings.okx_funding_symbol_limit,
        ),
        "binance": BinancePublicAdapter(
            spot_base_url=settings.binance_spot_base_url,
            futures_base_url=settings.binance_futures_base_url,
            websocket_url=settings.binance_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
        ),
        "hyperliquid": HyperliquidPublicAdapter(
            base_url=settings.hyperliquid_base_url,
            websocket_url=settings.hyperliquid_ws_url,
            timeout_seconds=settings.request_timeout_seconds,
            requests_per_second=settings.rate_limit_requests_per_second,
            burst=settings.rate_limit_burst,
        ),
    }
