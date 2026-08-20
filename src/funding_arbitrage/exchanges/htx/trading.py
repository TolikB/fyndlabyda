"""Authenticated HTX adapter with linear-swap fee and funding reconciliation."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.trading import CcxtTradingAdapter, _decimal
from funding_arbitrage.execution.trading import VenueFundingPayment


class HtxTradingAdapter(CcxtTradingAdapter):
    """Keep HTX spot and isolated USDT-M private API semantics explicit."""

    def __init__(
        self,
        exchange: Any,
        *,
        margin_mode: str,
        allowed_assets: frozenset[str],
    ) -> None:
        super().__init__("htx", exchange, margin_mode=margin_mode)
        self.allowed_assets = allowed_assets

    def _allowed_perpetual_markets(self) -> list[dict[str, Any]]:
        return [
            market
            for market in self.exchange.markets.values()
            if market.get("swap")
            and market.get("linear")
            and str(market.get("base") or "").upper() in self.allowed_assets
        ]

    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal:
        if instrument_type is InstrumentType.SPOT:
            return await super().fetch_taker_fee(exchange_symbol, instrument_type)
        key = (exchange_symbol, instrument_type)
        now = self._clock()
        cached = self._taker_fee_cache.get(key)
        if cached is not None and now - cached[1] <= self.fee_cache_ttl_seconds:
            return cached[0]
        market = self._market(exchange_symbol, instrument_type)
        response = await self.exchange.request(
            "linear-swap-api/v1/swap_fee",
            ["contract", "private"],
            "POST",
            {"contract_code": market["id"]},
        )
        data = response.get("data") if isinstance(response, dict) else None
        if not isinstance(data, list):
            raise RuntimeError("htx derivative fee response is invalid")
        row = next(
            (
                item
                for item in data
                if isinstance(item, dict)
                and str(item.get("contract_code") or "").upper() == str(market["id"]).upper()
            ),
            None,
        )
        if row is None:
            raise RuntimeError("htx derivative fee market is missing")
        open_fee = _decimal(row.get("open_taker_fee"))
        close_fee = _decimal(row.get("close_taker_fee"))
        fee = max(open_fee, close_fee)
        if open_fee < 0 or close_fee < 0 or fee > Decimal("0.02"):
            raise RuntimeError("htx returned an invalid derivative taker fee")
        self._taker_fee_cache[key] = (fee, now)
        return fee

    async def fetch_funding_payments(self, since: datetime) -> list[VenueFundingPayment]:
        markets = self._allowed_perpetual_markets()
        if not markets:
            raise RuntimeError("htx has no allowlisted linear-swap markets")
        results = await asyncio.gather(
            *(
                self.exchange.fetch_funding_history(
                    market["symbol"],
                    int(since.timestamp() * 1000),
                    100,
                    {"marginMode": self.margin_mode},
                )
                for market in markets
            ),
            return_exceptions=True,
        )
        failure = next((row for row in results if isinstance(row, BaseException)), None)
        if failure is not None:
            raise RuntimeError("htx funding-payment request failed") from failure
        rows = [item for result in results if isinstance(result, list) for item in result]
        return self._funding_rows_to_payments(rows)
