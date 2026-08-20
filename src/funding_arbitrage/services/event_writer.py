"""Bounded, fail-closed batch persistence for the canonical raw-event journal."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import EventEnvelope

EventBatch = Sequence[EventEnvelope[Any]]
AppendBatch = Callable[[AsyncSession, EventBatch], Awaitable[int]]
_STOP = object()


class EventWriterFailed(RuntimeError):
    """Raw-event durability failed; market-data consumers must fail closed."""


@dataclass(frozen=True)
class _QueuedEvent:
    event: EventEnvelope[Any]
    durable: asyncio.Future[None]


class CanonicalEventWriter:
    """Batch events while acknowledging producers only after the DB commit."""

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
        self.queue: asyncio.Queue[_QueuedEvent | object] = asyncio.Queue(maxsize=queue_size)
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
        """Return only after the event is committed or an existing ID is verified."""

        loop = asyncio.get_running_loop()
        durable: asyncio.Future[None] = loop.create_future()
        queued = _QueuedEvent(event=event, durable=durable)
        async with self._publish_lock:
            self._raise_if_failed()
            task = self._task
            if not self._accepting or task is None:
                raise RuntimeError("canonical event writer is not accepting events")
            put_task = asyncio.create_task(self.queue.put(queued))
            try:
                done, _ = await asyncio.wait(
                    {put_task, task}, return_when=asyncio.FIRST_COMPLETED
                )
                if task in done:
                    await _cancel(put_task)
                    self._raise_if_failed()
                    raise EventWriterFailed("canonical event writer stopped unexpectedly")
                await put_task
            except BaseException:
                await _cancel(put_task)
                raise

        task = self._task
        if task is None:
            raise EventWriterFailed("canonical event writer stopped before durability ACK")
        durable_task = asyncio.create_task(_await_durable(durable))
        try:
            done, _ = await asyncio.wait(
                {durable_task, task}, return_when=asyncio.FIRST_COMPLETED
            )
        except BaseException:
            await _cancel(durable_task)
            raise
        if durable_task in done:
            try:
                await durable_task
            except asyncio.CancelledError:
                raise
            except BaseException:
                self._raise_if_failed()
                raise
            return
        await _cancel(durable_task)
        self._raise_if_failed()
        raise EventWriterFailed("canonical event writer stopped before durability ACK")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        async with self._publish_lock:
            if self._accepting:
                self._accepting = False
                put_task = asyncio.create_task(self.queue.put(_STOP))
                done, _ = await asyncio.wait(
                    {put_task, task}, return_when=asyncio.FIRST_COMPLETED
                )
                if task in done:
                    await _cancel(put_task)
                    await task
                await put_task
        try:
            await task
        finally:
            self._task = None
        self._raise_if_failed()

    async def _run(self) -> None:
        active: list[_QueuedEvent] = []
        try:
            stopping = False
            while not stopping:
                item = await self.queue.get()
                if item is _STOP:
                    break
                if not isinstance(item, _QueuedEvent):
                    raise RuntimeError("canonical event queue item is invalid")
                active = [item]
                deadline = (
                    asyncio.get_running_loop().time() + self.flush_interval_seconds
                )
                while len(active) < self.batch_size:
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
                    if not isinstance(item, _QueuedEvent):
                        raise RuntimeError("canonical event queue item is invalid")
                    active.append(item)
                async with self.session_factory() as session:
                    inserted = await self.append_batch(
                        session, [queued.event for queued in active]
                    )
                self.persisted_events += inserted
                for queued in active:
                    if not queued.durable.done():
                        queued.durable.set_result(None)
                active = []
        except BaseException as exc:
            self._failure = exc
            self._accepting = False
            self._fail_waiters(active, exc)
            self._drain_waiters(exc)
            raise

    @staticmethod
    def _fail_waiters(waiters: Sequence[_QueuedEvent], error: BaseException) -> None:
        for queued in waiters:
            if not queued.durable.done():
                queued.durable.set_exception(error)

    def _drain_waiters(self, error: BaseException) -> None:
        while True:
            try:
                item = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if isinstance(item, _QueuedEvent) and not item.durable.done():
                item.durable.set_exception(error)

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise EventWriterFailed(
                f"canonical event writer failed: {type(self._failure).__name__}"
            ) from self._failure


async def _await_durable(future: asyncio.Future[None]) -> None:
    await future


async def _cancel(task: asyncio.Task[object]) -> None:
    if task.done():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
