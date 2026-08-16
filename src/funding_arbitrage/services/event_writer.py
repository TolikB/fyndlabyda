"""Bounded, fail-closed batch persistence for the canonical raw-event journal."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import EventEnvelope

EventBatch = Sequence[EventEnvelope[Any]]
AppendBatch = Callable[[AsyncSession, EventBatch], Awaitable[int]]
_STOP = object()


class EventWriterFailed(RuntimeError):
    """Raw-event durability failed; market-data consumers must fail closed."""


class CanonicalEventWriter:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        queue_size: int = 50_000,
        batch_size: int = 500,
        flush_interval_seconds: float = 0.10,
        append_batch: AppendBatch = append_events,
    ) -> None:
        if queue_size <= 0 or batch_size <= 0 or batch_size > queue_size:
            raise ValueError("event writer queue and batch sizes are invalid")
        if flush_interval_seconds <= 0:
            raise ValueError("event writer flush interval must be positive")
        self.session_factory = session_factory
        self.queue: asyncio.Queue[EventEnvelope[Any] | object] = asyncio.Queue(maxsize=queue_size)
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.append_batch = append_batch
        self._publish_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._accepting = False
        self._failure: BaseException | None = None
        self.persisted_events = 0

    @property
    def failed(self) -> bool:
        return self._failure is not None

    @property
    def failure_reason(self) -> str | None:
        if self._failure is None:
            return None
        return type(self._failure).__name__

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("canonical event writer already started")
        self._accepting = True
        self._task = asyncio.create_task(self._run(), name="canonical-event-writer")

    async def publish(self, event: EventEnvelope[Any]) -> None:
        async with self._publish_lock:
            self._raise_if_failed()
            task = self._task
            if not self._accepting or task is None:
                raise RuntimeError("canonical event writer is not accepting events")
            put_task = asyncio.create_task(self.queue.put(event))
            try:
                done, _ = await asyncio.wait({put_task, task}, return_when=asyncio.FIRST_COMPLETED)
                if task in done:
                    if not put_task.done():
                        put_task.cancel()
                        await asyncio.gather(put_task, return_exceptions=True)
                    self._raise_if_failed()
                    raise EventWriterFailed("canonical event writer stopped unexpectedly")
                await put_task
            except BaseException:
                if not put_task.done():
                    put_task.cancel()
                    await asyncio.gather(put_task, return_exceptions=True)
                raise
            self._raise_if_failed()

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        async with self._publish_lock:
            if self._accepting:
                self._accepting = False
                put_task = asyncio.create_task(self.queue.put(_STOP))
                done, _ = await asyncio.wait({put_task, task}, return_when=asyncio.FIRST_COMPLETED)
                if task in done:
                    if not put_task.done():
                        put_task.cancel()
                        await asyncio.gather(put_task, return_exceptions=True)
                    await task
                await put_task
        try:
            await task
        finally:
            self._task = None
        self._raise_if_failed()

    async def _run(self) -> None:
        try:
            stopping = False
            while not stopping:
                item = await self.queue.get()
                if item is _STOP:
                    break
                batch: list[EventEnvelope[Any]] = [cast(EventEnvelope[Any], item)]
                deadline = asyncio.get_running_loop().time() + self.flush_interval_seconds
                while len(batch) < self.batch_size:
                    timeout = deadline - asyncio.get_running_loop().time()
                    if timeout <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                    except TimeoutError:
                        break
                    if item is _STOP:
                        stopping = True
                        break
                    batch.append(cast(EventEnvelope[Any], item))
                async with self.session_factory() as session:
                    self.persisted_events += await self.append_batch(session, batch)
        except BaseException as exc:
            self._failure = exc
            self._accepting = False
            raise

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise EventWriterFailed(
                f"canonical event writer failed: {type(self._failure).__name__}"
            ) from self._failure
