"""Bounded, fail-closed batch persistence for the canonical raw-event journal."""

from __future__ import annotations

import asyncio
import errno
import logging
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy.exc import DBAPIError, DisconnectionError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import EventEnvelope

EventBatch = Sequence[EventEnvelope[Any]]
AppendBatch = Callable[[AsyncSession, EventBatch], Awaitable[int]]
_STOP = object()
logger = logging.getLogger(__name__)

_RETRYABLE_ERRNOS = frozenset(
    {
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.EPIPE,
        errno.ETIMEDOUT,
    }
)
_RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",
        "40P01",
        "53300",
        "57P01",
        "57P02",
        "57P03",
    }
)


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
        retry_window_seconds: float = 20.0,
        retry_initial_seconds: float = 0.25,
        retry_max_seconds: float = 2.0,
        shutdown_timeout_seconds: float = 30.0,
        append_batch: AppendBatch = append_events,
    ) -> None:
        if queue_size <= 0 or batch_size <= 0 or batch_size > queue_size:
            raise ValueError("event writer queue and batch sizes are invalid")
        if flush_interval_seconds <= 0:
            raise ValueError("event writer flush interval must be positive")
        retry_values = (
            retry_window_seconds,
            retry_initial_seconds,
            retry_max_seconds,
            shutdown_timeout_seconds,
        )
        if not all(math.isfinite(value) for value in retry_values):
            raise ValueError("event writer retry values must be finite")
        if retry_window_seconds <= 0:
            raise ValueError("event writer retry window must be positive")
        if retry_initial_seconds <= 0 or retry_max_seconds <= 0:
            raise ValueError("event writer retry delays must be positive")
        if retry_initial_seconds > retry_max_seconds:
            raise ValueError("event writer initial retry exceeds maximum")
        if retry_max_seconds > retry_window_seconds:
            raise ValueError("event writer maximum retry exceeds retry window")
        if shutdown_timeout_seconds <= retry_window_seconds:
            raise ValueError("event writer shutdown timeout must exceed retry window")
        if shutdown_timeout_seconds > 45:
            raise ValueError("event writer shutdown timeout exceeds safe process budget")
        self.session_factory = session_factory
        self.queue: asyncio.Queue[_QueuedEvent | object] = asyncio.Queue(maxsize=queue_size)
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.retry_window_seconds = retry_window_seconds
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_max_seconds = retry_max_seconds
        self.shutdown_timeout_seconds = shutdown_timeout_seconds
        self.append_batch = append_batch
        self._publish_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._accepting = False
        self._failure: BaseException | None = None
        self._recovering = False
        self._recovery_error_type: str | None = None
        self.persisted_events = 0
        self.retries_total = 0

    @property
    def failed(self) -> bool:
        return self._failure is not None

    @property
    def recovering(self) -> bool:
        return self._recovering

    @property
    def recovery_reason(self) -> str | None:
        return self._recovery_error_type

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
                if not durable.cancelled():
                    self._raise_if_failed()
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
        operation = asyncio.create_task(
            self._stop_worker(task), name="canonical-event-writer-stop"
        )
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            raise
        finally:
            if operation.done():
                self._task = None
        self._raise_if_failed()

    async def _stop_worker(self, task: asyncio.Task[None]) -> None:
        put_task: asyncio.Task[None] | None = None
        try:
            async with asyncio.timeout(self.shutdown_timeout_seconds):
                async with self._publish_lock:
                    self._accepting = False
                    if not task.done():
                        put_task = asyncio.create_task(self.queue.put(_STOP))
                        done, _ = await asyncio.wait(
                            {put_task, task}, return_when=asyncio.FIRST_COMPLETED
                        )
                        if task in done:
                            await _cancel(put_task)
                            await task
                        await put_task
                await task
        except BaseException:
            if put_task is not None:
                await _cancel(put_task)
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

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
                inserted = await self._append_with_retry(
                    [queued.event for queued in active]
                )
                self.persisted_events += inserted
                for queued in active:
                    if not queued.durable.done():
                        queued.durable.set_result(None)
                active = []
        except BaseException as exc:
            self._recovering = False
            self._failure = exc
            self._accepting = False
            self._fail_waiters(active, exc)
            self._drain_waiters(exc)
            raise

    async def _append_with_retry(self, events: EventBatch) -> int:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.retry_window_seconds
        delay = self.retry_initial_seconds
        outage_retries = 0
        last_error: BaseException | None = None
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                if last_error is not None:
                    raise last_error
                raise TimeoutError("canonical event writer retry window expired")
            try:
                async with asyncio.timeout(remaining):
                    async with self.session_factory() as session:
                        inserted = await self.append_batch(session, events)
            except BaseException as error:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    if isinstance(error, TimeoutError) and last_error is not None:
                        raise last_error from error
                    raise
                if not _is_retryable_storage_error(error):
                    raise
                last_error = error
                self._recovering = True
                self._recovery_error_type = type(error).__name__
                self.retries_total += 1
                outage_retries += 1
                sleep_seconds = min(delay, remaining)
                logger.warning(
                    "canonical_event_writer_retrying",
                    extra={
                        "event": "canonical_event_writer_retry",
                        "error_type": type(error).__name__,
                        "retry_count": outage_retries,
                        "retry_delay_seconds": sleep_seconds,
                    },
                )
                await asyncio.sleep(sleep_seconds)
                delay = min(delay * 2, self.retry_max_seconds)
                continue
            if self._recovering:
                logger.info(
                    "canonical_event_writer_recovered",
                    extra={
                        "event": "canonical_event_writer_recovered",
                        "retry_count": outage_retries,
                    },
                )
            self._recovering = False
            self._recovery_error_type = None
            return inserted

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


def _is_retryable_storage_error(error: BaseException) -> bool:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, (DisconnectionError, SQLAlchemyTimeoutError)):
            return True
        if isinstance(current, DBAPIError):
            if current.connection_invalidated:
                return True
            original = current.orig
            if isinstance(original, BaseException):
                pending.append(original)
        if isinstance(current, (ConnectionError, TimeoutError)):
            return True
        if isinstance(current, OSError) and current.errno in _RETRYABLE_ERRNOS:
            return True
        sqlstate = getattr(current, "sqlstate", None) or getattr(current, "pgcode", None)
        if isinstance(sqlstate, str) and (
            sqlstate.startswith("08") or sqlstate in _RETRYABLE_SQLSTATES
        ):
            return True
        cause = current.__cause__
        context = current.__context__
        if isinstance(cause, BaseException):
            pending.append(cause)
        if isinstance(context, BaseException):
            pending.append(context)
    return False


async def _await_durable(future: asyncio.Future[None]) -> None:
    await future


async def _cancel(task: asyncio.Task[object]) -> None:
    if task.done():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
