"""Restart-safe market-data collection coordinator."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.health import CircuitBreaker
from funding_arbitrage.monitoring.metrics import market_data_age_seconds


@dataclass(frozen=True)
class MarketSnapshot:
    """In-memory normalized snapshot used by scanner and paper components."""

    instruments: list[NormalizedInstrument]
    tickers: list[Ticker]
    funding: list[FundingSnapshot]
    orderbooks: dict[tuple[str, str], OrderBook]
    captured_at: datetime
    funding_history: dict[tuple[str, str], list[FundingHistoryPoint]] | None = None


class MarketDataCollector:
    def __init__(
        self, adapters: Iterable[ExchangeAdapter], orderbook_symbol_limit: int = 20
    ) -> None:
        self.adapters = tuple(adapters)
        if orderbook_symbol_limit <= 0:
            raise ValueError("orderbook_symbol_limit must be positive")
        self.orderbook_symbol_limit = orderbook_symbol_limit
        self.health = {adapter.name: CircuitBreaker() for adapter in self.adapters}
        self._funding_history_cache: dict[tuple[str, str], list[FundingHistoryPoint]] = {}

    async def collect_once(
        self,
        orderbook_symbols: dict[str, list[str]] | None = None,
        include_history: bool = False,
    ) -> MarketSnapshot:
        instruments: list[NormalizedInstrument] = []
        tickers: list[Ticker] = []
        funding: list[FundingSnapshot] = []
        orderbooks: dict[tuple[str, str], OrderBook] = {}
        funding_history: dict[tuple[str, str], list[FundingHistoryPoint]] = {}
        for adapter in self.adapters:
            breaker = self.health[adapter.name]
            if breaker.status.value == "OFFLINE":
                continue
            try:
                instruments.extend(await adapter.get_instruments())
                tickers.extend(await adapter.get_tickers())
                funding.extend(await adapter.get_funding_rates())
                symbols = (orderbook_symbols or {}).get(adapter.name)
                if symbols is None:
                    symbols = [
                        item.symbol
                        for item in sorted(
                            [ticker for ticker in tickers if ticker.exchange == adapter.name],
                            key=lambda ticker: ticker.volume_24h,
                            reverse=True,
                        )[: self.orderbook_symbol_limit]
                    ]
                for symbol in symbols:
                    ticker = next(
                        (
                            item
                            for item in tickers
                            if item.exchange == adapter.name and item.symbol == symbol
                        ),
                        None,
                    )
                    try:
                        orderbooks[(adapter.name, symbol)] = await adapter.get_orderbook(
                            symbol,
                            20,
                            ticker.instrument_type if ticker else InstrumentType.PERPETUAL,
                        )
                    except Exception:
                        continue
                if include_history:
                    end = datetime.now(UTC)
                    start = end - timedelta(days=30)
                    funding_symbols = {
                        item.symbol for item in funding if item.exchange == adapter.name
                    }
                    for symbol in sorted(funding_symbols)[:20]:
                        try:
                            funding_history[(adapter.name, symbol)] = (
                                await adapter.get_funding_history(symbol, start, end)
                            )
                        except Exception:
                            continue
                breaker.record_success()
                market_data_age_seconds.labels(adapter.name).set(0)
            except Exception:
                breaker.record_failure()
                market_data_age_seconds.labels(adapter.name).set(-1)
        self._funding_history_cache.update(funding_history)
        return MarketSnapshot(
            instruments=instruments,
            tickers=tickers,
            funding=funding,
            orderbooks=orderbooks,
            captured_at=datetime.now(UTC),
            funding_history=dict(self._funding_history_cache),
        )
