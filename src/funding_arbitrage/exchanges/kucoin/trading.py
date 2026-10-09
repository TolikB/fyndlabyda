"""Authenticated KuCoin adapter spanning separate Classic spot and futures APIs."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.trading import CcxtTradingAdapter
from funding_arbitrage.execution.trading import (
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
    VenuePosition,
)


class KucoinTradingAdapter(TradingAdapter):
    """Route every operation to KuCoin's correct spot or futures account."""

    name = "kucoin"

    def __init__(
        self,
        spot_exchange: Any,
        futures_exchange: Any,
        *,
        margin_mode: str,
        allowed_assets: frozenset[str],
    ) -> None:
        self.spot = CcxtTradingAdapter(self.name, spot_exchange, margin_mode=margin_mode)
        self.futures = CcxtTradingAdapter(self.name, futures_exchange, margin_mode=margin_mode)
        self.allowed_assets = allowed_assets

    def _adapter(self, instrument_type: InstrumentType) -> CcxtTradingAdapter:
        return self.spot if instrument_type is InstrumentType.SPOT else self.futures

    async def initialize(self) -> None:
        await asyncio.gather(self.spot.initialize(), self.futures.initialize())

    async def close(self) -> None:
        await asyncio.gather(self.spot.close(), self.futures.close())

    async def preflight(self) -> dict[str, object]:
        balance, positions, orders = await asyncio.gather(
            self.fetch_balance(), self.fetch_positions(), self.fetch_open_orders()
        )
        return {
            "exchange": self.name,
            "accounts": ("spot", "futures"),
            "currencies": sorted(balance.total),
            "open_positions": len(positions),
            "open_orders": len(orders),
        }

    async def fetch_balance(self) -> VenueBalance:
        spot, futures = await asyncio.gather(
            self.spot.fetch_balance(), self.futures.fetch_balance()
        )

        def combine(field: str) -> dict[str, Decimal]:
            output: dict[str, Decimal] = {}
            for source in (getattr(spot, field), getattr(futures, field)):
                for currency, value in source.items():
                    output[currency] = output.get(currency, Decimal("0")) + value
            return output

        equity_values = [
            value for value in (spot.equity_usd, futures.equity_usd) if value is not None
        ]
        derivative_free = futures.free_collateral_usd
        return VenueBalance(
            exchange=self.name,
            free=combine("free"),
            used=combine("used"),
            total=combine("total"),
            spot_free=dict(spot.free),
            equity_usd=sum(equity_values, Decimal("0")) if equity_values else None,
            free_collateral_usd=(
                (spot.free_collateral_usd or Decimal("0"))
                + (futures.free_collateral_usd or Decimal("0"))
            ),
            derivative_free_collateral_usd=derivative_free,
            unrealized_pnl_usd=spot.unrealized_pnl_usd + futures.unrealized_pnl_usd,
        )

    async def fetch_positions(self) -> list[VenuePosition]:
        return await self.futures.fetch_positions()

    async def fetch_open_orders(self) -> list[TradingOrderResult]:
        spot, futures = await asyncio.gather(
            self.spot.fetch_open_orders(), self.futures.fetch_open_orders()
        )
        return spot + futures

    async def fetch_funding_payments(self, since: datetime) -> list[VenueFundingPayment]:
        markets = [
            market
            for market in self.futures.exchange.markets.values()
            if (market.get("swap") or market.get("future"))
            and str(market.get("base") or "").upper() in self.allowed_assets
        ]
        if not markets:
            raise RuntimeError("kucoin has no allowlisted futures markets")
        results = await asyncio.gather(
            *(
                self.futures.exchange.fetch_funding_history(
                    market["symbol"], int(since.timestamp() * 1000), 100
                )
                for market in markets
            ),
            return_exceptions=True,
        )
        failure = next((row for row in results if isinstance(row, BaseException)), None)
        if failure is not None:
            raise RuntimeError("kucoin funding-payment request failed") from failure
        rows = [item for result in results if isinstance(result, list) for item in result]
        return self.futures._funding_rows_to_payments(rows)

    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal:
        return await self._adapter(instrument_type).fetch_taker_fee(
            exchange_symbol, instrument_type
        )

    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal:
        return await self._adapter(instrument_type).normalize_base_quantity(
            exchange_symbol, instrument_type, base_quantity
        )

    async def normalize_price(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        price: Decimal,
    ) -> Decimal:
        return await self._adapter(instrument_type).normalize_price(
            exchange_symbol, instrument_type, price
        )

    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult:
        return await self._adapter(request.instrument_type).submit_ioc_order(
            request, timeout_seconds
        )

    async def cancel_order(self, order: TradingOrderResult) -> TradingOrderResult:
        return await self._adapter(order.instrument_type).cancel_order(order)

    async def configure_derivative(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        leverage: int,
        margin_mode: str,
    ) -> None:
        await self._adapter(instrument_type).configure_derivative(
            exchange_symbol, instrument_type, leverage, margin_mode
        )
