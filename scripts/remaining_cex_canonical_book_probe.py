"""Probe canonical public L2 books for MEXC, KuCoin, and HTX."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import BookEvent
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter


async def _probe(
    adapter: ExchangeAdapter,
    events: list[BookEvent],
    select: Callable[[str, InstrumentType], bool],
    required_source: str | None = None,
) -> dict[str, object]:
    instruments = await adapter.get_instruments()
    instrument = next(
        item for item in instruments if select(item.exchange_symbol, item.instrument_type)
    )
    stream = adapter.stream_orderbooks(
        [(instrument.exchange_symbol, instrument.instrument_type)], depth=20
    )
    try:
        async with asyncio.timeout(30):
            while True:
                book = await anext(stream)
                event = next(
                    event
                    for event in reversed(events)
                    if event.payload.instrument.venue == adapter.name.upper()
                    and getattr(
                        event.payload,
                        "sequence",
                        getattr(event.payload, "last_sequence", None),
                    )
                    == book.sequence
                )
                if required_source is None or event.metadata.source == required_source:
                    break
        return {
            "exchange": adapter.name,
            "symbol": book.symbol,
            "book_sequence": book.sequence,
            "best_bid": str(book.bids[0].price),
            "best_ask": str(book.asks[0].price),
            "event_id": event.metadata.event_id,
            "event_source": event.metadata.source,
            "event_sequence": event.metadata.sequence_id,
            "exchange_timestamp": event.metadata.exchange_timestamp.isoformat(),
            "receive_timestamp": event.metadata.receive_timestamp.isoformat(),
            "quality": event.metadata.quality.value,
        }
    finally:
        await stream.aclose()


async def run() -> int:
    settings = Settings()
    events: list[BookEvent] = []

    async def capture(event: BookEvent) -> None:
        events.append(event)

    adapters: list[
        tuple[ExchangeAdapter, Callable[[str, InstrumentType], bool], str | None]
    ] = [
        (
            MexcPublicAdapter(
                spot_base_url=settings.mexc_base_url,
                futures_base_url=settings.mexc_futures_base_url,
                futures_websocket_url=settings.mexc_futures_ws_url,
                spot_websocket_url=settings.mexc_spot_ws_url,
                timeout_seconds=settings.request_timeout_seconds,
                canonical_book_event_sink=capture,
                max_reconnects=1,
            ),
            lambda symbol, kind: symbol == "BTC_USDT"
            and kind is InstrumentType.PERPETUAL,
            "MEXC.PUBLIC.FUTURES.DEPTH.INCREMENTAL",
        ),
        (
            MexcPublicAdapter(
                spot_base_url=settings.mexc_base_url,
                futures_base_url=settings.mexc_futures_base_url,
                futures_websocket_url=settings.mexc_futures_ws_url,
                spot_websocket_url=settings.mexc_spot_ws_url,
                timeout_seconds=settings.request_timeout_seconds,
                canonical_book_event_sink=capture,
                max_reconnects=1,
            ),
            lambda symbol, kind: symbol == "BTCUSDT" and kind is InstrumentType.SPOT,
            None,
        ),
        (
            KucoinPublicAdapter(
                spot_base_url=settings.kucoin_spot_base_url,
                futures_base_url=settings.kucoin_futures_base_url,
                spot_websocket_url=settings.kucoin_spot_ws_url,
                futures_websocket_url=settings.kucoin_futures_ws_url,
                timeout_seconds=settings.request_timeout_seconds,
                canonical_book_event_sink=capture,
                max_reconnects=1,
            ),
            lambda symbol, kind: symbol == "BTC-USDT" and kind is InstrumentType.SPOT,
            None,
        ),
        (
            HtxPublicAdapter(
                spot_base_url=settings.htx_spot_base_url,
                futures_base_url=settings.htx_futures_base_url,
                spot_websocket_url=settings.htx_spot_ws_url,
                futures_websocket_url=settings.htx_futures_ws_url,
                timeout_seconds=settings.request_timeout_seconds,
                canonical_book_event_sink=capture,
                max_reconnects=1,
            ),
            lambda symbol, kind: symbol == "BTC-USDT" and kind is InstrumentType.PERPETUAL,
            None,
        ),
    ]
    results: list[dict[str, object]] = []
    try:
        for adapter, selector, required_source in adapters:
            results.append(await _probe(adapter, events, selector, required_source))
    finally:
        await asyncio.gather(*(adapter.close() for adapter, _, _ in adapters))
    print(json.dumps({"status": "ok", "results": results}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
