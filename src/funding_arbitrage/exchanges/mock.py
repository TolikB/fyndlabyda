"""Deterministic public-market simulator used by the paper_test deployment."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)


class MockExchangeAdapter(ExchangeAdapter):
    """A repeatable venue adapter with funding spreads and bounded price drift."""

    def __init__(self, name: str, sleep: float = 0.01) -> None:
        self.name = name
        self._step = 0
        self._sleep = sleep
        self._funding_by_venue = {
            "bybit": Decimal("0.0020"),
            "gate": Decimal("-0.0010"),
            "okx": Decimal("0.0015"),
            "binance": Decimal("-0.0008"),
            "hyperliquid": Decimal("0.0010"),
        }

    async def close(self) -> None:
        return None

    def _symbols(self) -> tuple[str, str]:
        return "BTCUSDT", "BTCUSDT"

    def _price(self, instrument_type: InstrumentType) -> Decimal:
        wave = Decimal(str((self._step % 10) - 5)) / Decimal("100")
        basis = Decimal("0.15") if instrument_type is InstrumentType.PERPETUAL else Decimal("0")
        venue_offset = Decimal(str((sum(ord(char) for char in self.name) % 7) - 3)) / Decimal("10")
        return Decimal("100") + venue_offset + basis + wave

    async def get_instruments(self) -> list[NormalizedInstrument]:
        return [
            NormalizedInstrument(
                exchange=self.name,
                exchange_symbol="BTCUSDT",
                base_asset="BTC",
                quote_asset="USDT",
                instrument_type=instrument_type,
                settlement_asset="USDT",
                contract_size=Decimal("1"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                min_order_size=Decimal("0.001"),
                funding_interval=8 if instrument_type is InstrumentType.PERPETUAL else None,
            )
            for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
        ]

    async def get_tickers(self) -> list[Ticker]:
        self._step += 1
        timestamp = datetime.now(UTC)
        result: list[Ticker] = []
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL):
            price = self._price(instrument_type)
            result.append(
                Ticker(
                    exchange=self.name,
                    symbol="BTCUSDT",
                    instrument_type=instrument_type,
                    last_price=price,
                    mark_price=price if instrument_type is InstrumentType.PERPETUAL else None,
                    index_price=self._price(InstrumentType.SPOT),
                    best_bid=price - Decimal("0.02"),
                    best_ask=price + Decimal("0.02"),
                    volume_24h=Decimal("1000000"),
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
        price = self._price(instrument_type)
        levels = max(2, min(depth, 20))
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
            instrument_type=instrument_type,
            bids=bids,
            asks=asks,
            timestamp=datetime.now(UTC),
            sequence=self._step,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        now = datetime.now(UTC)
        rate = self._funding_by_venue[self.name]
        return [
            FundingSnapshot(
                exchange=self.name,
                symbol="BTCUSDT",
                funding_rate=rate,
                funding_interval_hours=Decimal("8"),
                next_funding_time=now + timedelta(hours=8),
                mark_price=self._price(InstrumentType.PERPETUAL),
                index_price=self._price(InstrumentType.SPOT),
                timestamp=now,
            )
        ]

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        del start
        rate = self._funding_by_venue[self.name]
        return [
            FundingHistoryPoint(
                exchange=self.name,
                symbol=symbol,
                funding_rate=rate,
                funding_timestamp=end - timedelta(hours=8 * index),
                mark_price=self._price(InstrumentType.PERPETUAL),
            )
            for index in range(1, 31)
        ]

    async def get_candles(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        start: datetime,
        end: datetime,
        interval_minutes: int = 60,
    ) -> list[Candle]:
        candles: list[Candle] = []
        cursor = start.astimezone(UTC)
        while cursor + timedelta(minutes=interval_minutes) <= end:
            price = self._price(instrument_type)
            candles.append(
                Candle(
                    exchange=self.name,
                    symbol=symbol,
                    instrument_type=instrument_type,
                    interval_minutes=interval_minutes,
                    open_time=cursor,
                    close_time=cursor + timedelta(minutes=interval_minutes),
                    open=price,
                    high=price + Decimal("0.05"),
                    low=price - Decimal("0.05"),
                    close=price,
                    volume=Decimal("1000"),
                )
            )
            cursor += timedelta(minutes=interval_minutes)
        return candles

    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        requested = set(symbols)
        while True:
            for ticker in await self.get_tickers():
                if not requested or (ticker.symbol, ticker.instrument_type) in requested:
                    yield ticker
            await asyncio.sleep(self._sleep)

    def stream_orderbooks(
        self,
        symbols: list[tuple[str, InstrumentType]],
        depth: int = 20,
    ) -> AsyncIterator[OrderBook]:
        return self._stream_orderbooks(symbols, depth)

    async def _stream_orderbooks(
        self,
        symbols: list[tuple[str, InstrumentType]],
        depth: int,
    ) -> AsyncIterator[OrderBook]:
        while True:
            for symbol, instrument_type in symbols:
                yield await self.get_orderbook(symbol, depth, instrument_type)
            await asyncio.sleep(self._sleep)
