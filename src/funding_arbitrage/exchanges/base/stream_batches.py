"""Batched intake for WebSocket streams that journal before they emit.

A book stream publishes each update to the canonical journal and waits for the
commit before it hands the book on. Reading one message per commit let busy
venues fall 15-20 seconds behind the exchange. Streams now take every message
that has already arrived, apply them in order, and journal them together.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from funding_arbitrage.domain.events import EventEnvelope

_MAX_BATCH = 500
_QUEUE_SIZE = 10_000


@dataclass(frozen=True)
class _Ended:
    error: BaseException | None


async def message_batches(
    socket: AsyncIterable[Any],
    *,
    max_batch: int = _MAX_BATCH,
    queue_size: int = _QUEUE_SIZE,
) -> AsyncGenerator[list[Any], None]:
    """Yield every message received so far, waiting only for the first one.

    A reader task keeps draining the socket, so keepalive frames are answered
    while the caller journals the previous batch. Messages keep their order, and
    a socket error is raised only after the messages received before it.
    Close the iterator (``contextlib.aclosing``) to stop the reader.
    """

    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)

    async def read() -> None:
        try:
            async for message in socket:
                await queue.put(message)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            await queue.put(_Ended(error))
        else:
            await queue.put(_Ended(None))

    reader = asyncio.create_task(read())
    try:
        while True:
            batch = [await queue.get()]
            while len(batch) < max_batch:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            messages: list[Any] = []
            ended: _Ended | None = None
            for item in batch:
                if isinstance(item, _Ended):
                    ended = item
                    break
                messages.append(item)
            if messages:
                yield messages
            if ended is not None:
                if ended.error is not None:
                    raise ended.error
                return
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


async def publish_events(
    sink: Callable[[EventEnvelope[Any]], Awaitable[None]],
    events: Sequence[EventEnvelope[Any]],
) -> None:
    """Publish in order; a sink that offers ``publish_many`` commits them together."""

    if not events:
        return
    publish_many = getattr(sink, "publish_many", None)
    if publish_many is not None:
        await publish_many(events)
        return
    for event in events:
        await sink(event)
