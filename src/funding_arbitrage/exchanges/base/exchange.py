"""Abstract read-only exchange contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime

from .models import (
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
    def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]: ...

    def stream_orderbooks(self, symbols: list[str]) -> AsyncIterator[OrderBook]:
        raise NotImplementedError("orderbook streaming is not part of PHASE 2")

    def stream_funding(self, symbols: list[str]) -> AsyncIterator[FundingSnapshot]:
        raise NotImplementedError("funding streaming is not part of PHASE 2")
