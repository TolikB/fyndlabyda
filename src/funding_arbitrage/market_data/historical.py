"""Idempotent historical funding and OHLCV backfill coordinator."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.market_data import (
    save_candles,
    save_funding_history,
    save_instruments,
)
from funding_arbitrage.exchanges.base.exceptions import RateLimitError
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    Ticker,
)


@dataclass
class HistoricalBackfillResult:
    start: datetime
    end: datetime
    assets: tuple[str, ...]
    interval_minutes: int
    candle_count: int = 0
    funding_count: int = 0
    instruments: dict[str, list[str]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class HistoricalBackfill:
    def __init__(
        self,
        adapters: dict[str, ExchangeAdapter],
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.adapters = adapters
        self.session_factory = session_factory

    async def discover_assets(self, limit: int = 12) -> tuple[str, ...]:
        if limit <= 0:
            raise ValueError("asset limit must be positive")
        responses = await asyncio.gather(
            *(
                self._discover_venue(adapter)
                for adapter in self.adapters.values()
            ),
            return_exceptions=True,
        )
        instruments: list[NormalizedInstrument] = []
        tickers: list[Ticker] = []
        funding: list[FundingSnapshot] = []
        for response in responses:
            if isinstance(response, BaseException):
                continue
            venue_instruments, venue_tickers, venue_funding = response
            instruments.extend(venue_instruments)
            tickers.extend(venue_tickers)
            funding.extend(venue_funding)
        if not instruments:
            raise RuntimeError("asset discovery failed for every venue")
        return _rank_research_assets(instruments, tickers, funding, limit)

    @staticmethod
    async def _discover_venue(
        adapter: ExchangeAdapter,
    ) -> tuple[list[NormalizedInstrument], list[Ticker], list[FundingSnapshot]]:
        instruments, tickers, funding = await asyncio.gather(
            adapter.get_instruments(),
            adapter.get_tickers(),
            adapter.get_funding_rates(),
        )
        return instruments, tickers, funding

    async def run(
        self,
        days: int = 90,
        assets: tuple[str, ...] = ("BTC", "ETH", "SOL"),
        interval_minutes: int = 60,
        end: datetime | None = None,
    ) -> HistoricalBackfillResult:
        if not 30 <= days <= 90:
            raise ValueError("historical backfill days must be between 30 and 90")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be positive")
        end_at = (end or datetime.now(UTC)).astimezone(UTC)
        end_at = end_at.replace(minute=0, second=0, microsecond=0)
        start_at = end_at - timedelta(days=days)
        result = HistoricalBackfillResult(
            start=start_at,
            end=end_at,
            assets=assets,
            interval_minutes=interval_minutes,
        )
        collections = await asyncio.gather(
            *(
                self._collect_venue(
                    venue,
                    adapter,
                    set(assets),
                    start_at,
                    end_at,
                    interval_minutes,
                )
                for venue, adapter in self.adapters.items()
            )
        )
        async with self.session_factory() as session:
            for venue, instruments, candles, funding, errors in collections:
                result.instruments[venue] = [
                    f"{item.exchange_symbol}:{item.instrument_type.value}"
                    for item in instruments
                ]
                result.errors.update(errors)
                await save_instruments(session, instruments)
                await save_candles(session, candles)
                await save_funding_history(session, funding)
                result.candle_count += len(candles)
                result.funding_count += len(funding)
        return result

    async def _collect_venue(
        self,
        venue: str,
        adapter: ExchangeAdapter,
        assets: set[str],
        start: datetime,
        end: datetime,
        interval_minutes: int,
    ) -> tuple[
        str,
        list[NormalizedInstrument],
        list[Candle],
        list[FundingHistoryPoint],
        dict[str, str],
    ]:
        instruments = self._select_instruments(await adapter.get_instruments(), assets)
        concurrency = 1 if venue == "okx" else 4
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            self._collect_instrument_with_retry(
                semaphore, adapter, item, start, end, interval_minutes
            )
            for item in instruments
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        candles: list[Candle] = []
        funding: list[FundingHistoryPoint] = []
        errors: dict[str, str] = {}
        for instrument, response in zip(instruments, responses, strict=True):
            key = f"{venue}:{instrument.exchange_symbol}:{instrument.instrument_type.value}"
            if isinstance(response, BaseException):
                errors[key] = f"{type(response).__name__}: {response}"
                continue
            instrument_candles, instrument_funding = response
            candles.extend(instrument_candles)
            funding.extend(instrument_funding)
        unique_funding = {
            (item.exchange, item.symbol, item.funding_timestamp): item
            for item in funding
        }
        return venue, instruments, candles, list(unique_funding.values()), errors

    @staticmethod
    async def _collect_instrument_with_retry(
        semaphore: asyncio.Semaphore,
        adapter: ExchangeAdapter,
        instrument: NormalizedInstrument,
        start: datetime,
        end: datetime,
        interval_minutes: int,
    ) -> tuple[list[Candle], list[FundingHistoryPoint]]:
        async with semaphore:
            for attempt in range(5):
                try:
                    return await HistoricalBackfill._collect_instrument(
                        adapter, instrument, start, end, interval_minutes
                    )
                except RateLimitError:
                    if attempt == 4:
                        raise
                    await asyncio.sleep(2**attempt)
        raise RuntimeError("historical backfill retry loop exhausted")

    @staticmethod
    async def _collect_instrument(
        adapter: ExchangeAdapter,
        instrument: NormalizedInstrument,
        start: datetime,
        end: datetime,
        interval_minutes: int,
    ) -> tuple[list[Candle], list[FundingHistoryPoint]]:
        candles_task = adapter.get_candles(
            instrument.exchange_symbol,
            instrument.instrument_type,
            start,
            end,
            interval_minutes,
        )
        if instrument.instrument_type is InstrumentType.PERPETUAL:
            candles, funding = await asyncio.gather(
                candles_task,
                adapter.get_funding_history(instrument.exchange_symbol, start, end),
            )
            return candles, funding
        return await candles_task, []

    @staticmethod
    def _select_instruments(
        instruments: list[NormalizedInstrument], assets: set[str]
    ) -> list[NormalizedInstrument]:
        quote_priority = {"USDT": 0, "USDC": 1, "USD": 2}
        selected: dict[tuple[str, InstrumentType], NormalizedInstrument] = {}
        for item in sorted(
            instruments,
            key=lambda value: (
                value.base_asset,
                value.instrument_type.value,
                quote_priority.get(value.quote_asset, 99),
                value.exchange_symbol,
            ),
        ):
            if (
                item.is_active
                and item.base_asset in assets
                and item.quote_asset in quote_priority
                and item.instrument_type in {InstrumentType.SPOT, InstrumentType.PERPETUAL}
            ):
                selected.setdefault((item.base_asset, item.instrument_type), item)
        return list(selected.values())


def _rank_research_assets(
    instruments: list[NormalizedInstrument],
    tickers: list[Ticker],
    funding: list[FundingSnapshot],
    limit: int,
) -> tuple[str, ...]:
    instrument_by_key = {
        (item.exchange, item.exchange_symbol, item.instrument_type): item
        for item in instruments
        if item.is_active
    }
    spot_venues: dict[str, set[str]] = {}
    perp_venues: dict[str, set[str]] = {}
    for item in instruments:
        if not item.is_active or item.quote_asset not in {"USDT", "USDC", "USD"}:
            continue
        target = (
            spot_venues
            if item.instrument_type is InstrumentType.SPOT
            else perp_venues
            if item.instrument_type is InstrumentType.PERPETUAL
            else None
        )
        if target is not None:
            target.setdefault(item.base_asset, set()).add(item.exchange)
    volume: dict[str, Decimal] = {}
    for ticker in tickers:
        matched = instrument_by_key.get(
            (ticker.exchange, ticker.symbol, ticker.instrument_type)
        )
        if matched is not None:
            volume[matched.base_asset] = max(
                volume.get(matched.base_asset, Decimal("0")), ticker.volume_24h
            )
    funding_potential: dict[str, Decimal] = {}
    for point in funding:
        matched = instrument_by_key.get(
            (point.exchange, point.symbol, InstrumentType.PERPETUAL)
        )
        if matched is not None:
            funding_potential[matched.base_asset] = max(
                funding_potential.get(matched.base_asset, Decimal("0")),
                abs(point.funding_rate_daily),
            )
    eligible = {
        asset
        for asset, venues in perp_venues.items()
        if len(venues) >= 2 or bool(venues & spot_venues.get(asset, set()))
    }
    funding_order = sorted(
        eligible,
        key=lambda asset: (funding_potential.get(asset, Decimal("0")), asset),
        reverse=True,
    )
    volume_order = sorted(
        eligible,
        key=lambda asset: (volume.get(asset, Decimal("0")), asset),
        reverse=True,
    )
    funding_rank = {asset: rank for rank, asset in enumerate(funding_order)}
    volume_rank = {asset: rank for rank, asset in enumerate(volume_order)}
    core = [asset for asset in ("BTC", "ETH", "SOL") if asset in eligible]
    ranked = sorted(
        eligible - set(core),
        key=lambda asset: (
            funding_rank[asset] * 2 + volume_rank[asset],
            -funding_potential.get(asset, Decimal("0")),
            -volume.get(asset, Decimal("0")),
            asset,
        ),
    )
    return tuple([*core, *ranked][:limit])
