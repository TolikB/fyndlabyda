"""Abstract read-only exchange contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime

from funding_arbitrage.domain.events import OptionQuoteSnapshot

from .models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)


class ExchangeAdapter(ABC):
    name: str

    async def close(self) -> None:
        """Release transport resources owned by the adapter."""
        return None

    @abstractmethod
    async def get_instruments(self) -> list[NormalizedInstrument]: ...

    @abstractmethod
    async def get_tickers(self) -> list[Ticker]: ...

    @abstractmethod
    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook: ...

    @abstractmethod
    async def get_funding_rates(self) -> list[FundingSnapshot]: ...

    @abstractmethod
    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]: ...

    @abstractmethod
    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]: ...

    def stream_orderbooks(
        self,
        symbols: list[tuple[str, InstrumentType]],
        depth: int = 20,
    ) -> AsyncIterator[OrderBook]:
        raise NotImplementedError("orderbook streaming is not implemented by this adapter")

    def stream_funding(self, symbols: list[str]) -> AsyncIterator[FundingSnapshot]:
        raise NotImplementedError("native funding streaming is unavailable for this adapter")

    async def get_candles(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        start: datetime,
        end: datetime,
        interval_minutes: int = 60,
    ) -> list[Candle]:
        raise NotImplementedError("historical candles are not implemented by this adapter")

    async def get_option_chain(
        self, base_assets: tuple[str, ...]
    ) -> list[OptionQuoteSnapshot]:
        """Return normalized executable option quotes when the venue supports them.

        Unsupported venues intentionally return an empty capability result. The
        method is public-data only and must never require account credentials.
        """

        del base_assets
        return []
