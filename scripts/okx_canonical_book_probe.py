"""Read public OKX books data through the canonical snapshot/delta pipeline."""

from __future__ import annotations

import asyncio
import json

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import BookDelta, BookEvent, BookSnapshot, EventKind
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.okx import OkxPublicAdapter


def _matches(event: BookEvent, sequence: int | None) -> bool:
    if isinstance(event.payload, BookSnapshot):
        return event.payload.sequence == sequence
    return event.payload.last_sequence == sequence


async def run() -> int:
    settings = Settings()
    events: list[BookEvent] = []

    async def capture(event: BookEvent) -> None:
        events.append(event)

    adapter = OkxPublicAdapter(
        base_url=settings.okx_base_url,
        websocket_url=settings.okx_ws_url,
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
            if item.exchange_symbol == "BTC-USDT-SWAP"
            and item.instrument_type is InstrumentType.PERPETUAL
        )
        stream = adapter.stream_orderbooks(
            [(instrument.exchange_symbol, instrument.instrument_type)], depth=20
        )
        selected_book = None
        selected_event = None
        async with asyncio.timeout(25):
            while selected_event is None or selected_event.kind is not EventKind.BOOK_DELTA:
                selected_book = await anext(stream)
                selected_event = next(
                    event for event in reversed(events) if _matches(event, selected_book.sequence)
                )
        if selected_book is None or not isinstance(selected_event.payload, BookDelta):
            raise RuntimeError("OKX incremental event was not observed")
        print(
            json.dumps(
                {
                    "status": "ok",
                    "symbol": selected_book.symbol,
                    "book_sequence": selected_book.sequence,
                    "best_bid": str(selected_book.bids[0].price),
                    "best_ask": str(selected_book.asks[0].price),
                    "event_id": selected_event.metadata.event_id,
                    "event_kind": selected_event.kind.value,
                    "event_sequence": selected_event.metadata.sequence_id,
                    "event_source": selected_event.metadata.source,
                    "exchange_timestamp": (selected_event.metadata.exchange_timestamp.isoformat()),
                    "receive_timestamp": (selected_event.metadata.receive_timestamp.isoformat()),
                    "quality": selected_event.metadata.quality.value,
                    "snapshot_events": sum(
                        event.kind is EventKind.BOOK_SNAPSHOT for event in events
                    ),
                    "delta_events": sum(event.kind is EventKind.BOOK_DELTA for event in events),
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
