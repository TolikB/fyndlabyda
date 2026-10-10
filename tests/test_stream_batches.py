from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import Any

import pytest

from funding_arbitrage.exchanges.base.stream_batches import message_batches, publish_events


class _Socket:
    def __init__(self, messages: list[str], error: Exception | None = None) -> None:
        self.messages = messages
        self.error = error

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[str]:
        for message in self.messages:
            yield message
        if self.error is not None:
            raise self.error


async def test_messages_already_received_arrive_as_one_ordered_batch() -> None:
    async with aclosing(message_batches(_Socket(["a", "b", "c"]))) as batches:
        received = [batch async for batch in batches]

    assert received == [["a", "b", "c"]]


async def test_socket_error_is_raised_after_the_messages_before_it() -> None:
    received: list[list[str]] = []

    with pytest.raises(ConnectionError, match="closed"):
        async with aclosing(
            message_batches(_Socket(["a", "b"], ConnectionError("closed")))
        ) as batches:
            async for batch in batches:
                received.append(batch)

    assert received == [["a", "b"]]


async def test_publish_events_commits_together_when_the_sink_can() -> None:
    class BatchSink:
        def __init__(self) -> None:
            self.calls: list[list[Any]] = []

        async def __call__(self, event: Any) -> None:
            self.calls.append([event])

        async def publish_many(self, events: list[Any]) -> None:
            self.calls.append(list(events))

    plain: list[Any] = []

    async def plain_sink(event: Any) -> None:
        plain.append(event)

    sink = BatchSink()
    await publish_events(sink, ["e1", "e2"])  # type: ignore[list-item]
    await publish_events(plain_sink, ["e1", "e2"])  # type: ignore[list-item]

    assert sink.calls == [["e1", "e2"]]
    assert plain == ["e1", "e2"]
