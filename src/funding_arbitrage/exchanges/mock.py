"""Deterministic public-market simulator used by the offline paper_test profile."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)

_ASSETS = {"BTC": Decimal("100"), "ETH": Decimal("50")}


class MockExchangeAdapter(ExchangeAdapter):
    """A repeatable venue with funding spreads, epoch-aligned settlements, and depth.

    Spot and perpetual markets deliberately share the exchange symbol
    (``BTCUSDT``) like Bybit and Binance do, so key collisions surface in tests.
    """

    def __init__(
        self,
        name: str,
        sleep: float = 0.01,
        funding_interval_seconds: int = 28_800,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.name = name
        self._step = 0
        self._sleep = sleep
        self.funding_interval_seconds = funding_interval_seconds
        self._clock = clock
        self._funding_by_venue = {
            "bybit": Decimal("0.0020"),
            "gate": Decimal("-0.0010"),
            "okx": Decimal("0.0015"),
            "binance": Decimal("-0.0008"),
            "hyperliquid": Decimal("0.0010"),
        }

    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    async def close(self) -> None:
        return None

    @property
    def _interval(self) -> timedelta:
        return timedelta(seconds=self.funding_interval_seconds)

    def _next_funding(self, now: datetime) -> datetime:
        seconds = self.funding_interval_seconds
        epoch = int(now.timestamp())
        return datetime.fromtimestamp((epoch // seconds + 1) * seconds, tz=UTC)

    def _rate(self, asset: str) -> Decimal:
        rate = self._funding_by_venue.get(self.name, Decimal("0.0005"))
        return rate if asset == "BTC" else rate / Decimal("2")

    def _price(self, asset: str, instrument_type: InstrumentType) -> Decimal:
        wave = Decimal(str((self._step % 10) - 5)) / Decimal("100")
        basis = Decimal("0.15") if instrument_type is InstrumentType.PERPETUAL else Decimal("0")
        venue_offset = Decimal(str((sum(ord(char) for char in self.name) % 7) - 3)) / Decimal("10")
        return _ASSETS[asset] + venue_offset + basis + wave

    async def get_instruments(self) -> list[NormalizedInstrument]:
        return [
            NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=f"{asset}USDT",
                base_asset=asset,
                quote_asset="USDT",
                instrument_type=instrument_type,
                settlement_asset="USDT",
                contract_size=Decimal("1"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                min_order_size=Decimal("0.001"),
                funding_interval=max(1, self.funding_interval_seconds // 3600)
                if instrument_type is InstrumentType.PERPETUAL
                else None,
            )
            for asset in _ASSETS
            for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
        ]

    async def get_tickers(self) -> list[Ticker]:
        self._step += 1
        timestamp = self._now()
        result: list[Ticker] = []
        for asset in _ASSETS:
            for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL):
                price = self._price(asset, instrument_type)
                result.append(
                    Ticker(
                        exchange=self.name,
                        symbol=f"{asset}USDT",
                        instrument_type=instrument_type,
                        last_price=price,
                        mark_price=price if instrument_type is InstrumentType.PERPETUAL else None,
                        index_price=self._price(asset, InstrumentType.SPOT),
                        best_bid=price - Decimal("0.02"),
                        best_ask=price + Decimal("0.02"),
                        volume_24h=Decimal("100000000"),
                        open_interest=Decimal("250000")
                        if instrument_type is InstrumentType.PERPETUAL
                        else None,
                        timestamp=timestamp,
                    )
                )
        return result

    async def get_orderbook(
        self,
        symbol: str,
        depth: int,
        instrument_type: InstrumentType = InstrumentType.PERPETUAL,
    ) -> OrderBook:
        asset = symbol.removesuffix("USDT")
        price = self._price(asset, instrument_type)
        levels = max(2, min(depth, 20))
        now = self._now()
        bids = tuple(
            OrderBookLevel(
                price=price - Decimal("0.02") - Decimal(index) * Decimal("0.01"),
                quantity=Decimal("20"),
            )
            for index in range(levels)
        )
        asks = tuple(
            OrderBookLevel(
                price=price + Decimal("0.02") + Decimal(index) * Decimal("0.01"),
                quantity=Decimal("20"),
            )
            for index in range(levels)
        )
        return OrderBook(
            exchange=self.name,
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=now,
            sequence=self._step,
            instrument_type=instrument_type,
            received_at=now,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        now = self._now()
        interval_hours = Decimal(self.funding_interval_seconds) / Decimal("3600")
        return [
            FundingSnapshot(
                exchange=self.name,
                symbol=f"{asset}USDT",
                funding_rate=self._rate(asset),
                funding_interval_hours=interval_hours,
                next_funding_time=self._next_funding(now),
                mark_price=self._price(asset, InstrumentType.PERPETUAL),
                index_price=self._price(asset, InstrumentType.SPOT),
                timestamp=now,
            )
            for asset in _ASSETS
        ]

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        asset = symbol.removesuffix("USDT")
        rate = self._rate(asset)
        seconds = self.funding_interval_seconds
        # Settlements happen on epoch-aligned boundaries; history covers [start, end].
        first = (int(start.timestamp()) + seconds - 1) // seconds * seconds
        last = int(min(end, self._now()).timestamp()) // seconds * seconds
        # Like the venues, return at most the latest 1000 settlements of the window.
        first = max(first, last - 999 * seconds)
        points: list[FundingHistoryPoint] = []
        moment = first
        while moment <= last:
            points.append(
                FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=rate,
                    funding_timestamp=datetime.fromtimestamp(moment, tz=UTC),
                    mark_price=self._price(asset, InstrumentType.PERPETUAL),
                )
            )
            moment += seconds
        return points

    def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        while True:
            for ticker in await self.get_tickers():
                if not symbols or ticker.symbol in symbols:
                    yield ticker
            await asyncio.sleep(self._sleep)
