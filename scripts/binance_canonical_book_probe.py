"""Read public Binance USD-M depth through REST/bootstrap canonical replay."""

from __future__ import annotations

import asyncio
import json

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import BookDelta, BookEvent, BookSnapshot, EventKind
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.binance import BinancePublicAdapter


def _matches(event: BookEvent, sequence: int | None) -> bool:
    if isinstance(event.payload, BookSnapshot):
        return event.payload.sequence == sequence
    return event.payload.last_sequence == sequence


async def run() -> int:
    settings = Settings()
    events: list[BookEvent] = []

    async def capture(event: BookEvent) -> None:
        events.append(event)

    adapter = BinancePublicAdapter(
        spot_base_url=settings.binance_spot_base_url,
        futures_base_url=settings.binance_futures_base_url,
        websocket_url=settings.binance_ws_url,
        timeout_seconds=settings.request_timeout_seconds,
        canonical_book_event_sink=capture,
        max_reconnects=1,
    )
    stream = None
    try:
        instruments = await adapter.get_instruments()
        instrument = next(
            item
            for item in instruments
            if item.exchange_symbol == "BTCUSDT"
            and item.instrument_type is InstrumentType.PERPETUAL
        )
        stream = adapter.stream_orderbooks(
            [(instrument.exchange_symbol, instrument.instrument_type)], depth=20
        )
        book = await asyncio.wait_for(anext(stream), timeout=25)
        event = next(
            event
            for event in reversed(events)
            if _matches(event, book.sequence) and event.kind is EventKind.BOOK_DELTA
        )
        if not isinstance(event.payload, BookDelta):
            raise RuntimeError("Binance canonical delta was not observed")
        print(
            json.dumps(
                {
                    "status": "ok",
                    "symbol": book.symbol,
                    "book_sequence": book.sequence,
                    "best_bid": str(book.bids[0].price),
                    "best_ask": str(book.asks[0].price),
                    "event_id": event.metadata.event_id,
                    "event_kind": event.kind.value,
                    "event_sequence": event.metadata.sequence_id,
                    "event_source": event.metadata.source,
                    "exchange_timestamp": event.metadata.exchange_timestamp.isoformat(),
                    "receive_timestamp": event.metadata.receive_timestamp.isoformat(),
                    "quality": event.metadata.quality.value,
                    "snapshot_events": sum(item.kind is EventKind.BOOK_SNAPSHOT for item in events),
                    "delta_events": sum(item.kind is EventKind.BOOK_DELTA for item in events),
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        if stream is not None:
            await stream.aclose()
        await adapter.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
