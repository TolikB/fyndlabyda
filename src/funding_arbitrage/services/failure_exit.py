"""Stop a paper process whose durable pipeline has failed closed.

A failed canonical event writer or multi-regime runtime rejects every later
publish until the process restarts, so streams, polls and paper cycles all stop
with it. Paper stacks run under a restart policy: exiting lets the supervisor
start a fresh process that rebuilds its state from the durable journal.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
from collections.abc import Callable, Sequence
from typing import Protocol

logger = logging.getLogger(__name__)

_HARD_EXIT_STATUS = 70


class FailClosedComponent(Protocol):
    @property
    def failure_reason(self) -> str | None: ...


def request_process_exit(hard_exit_after_seconds: float = 120.0) -> None:
    """Shut down gracefully, and exit hard if the shutdown itself hangs."""

    timer = threading.Timer(hard_exit_after_seconds, os._exit, args=(_HARD_EXIT_STATUS,))
    timer.daemon = True
    timer.start()
    signal.raise_signal(signal.SIGTERM)


async def exit_when_failed(
    components: Sequence[tuple[str, FailClosedComponent]],
    *,
    poll_seconds: float = 5.0,
    request_exit: Callable[[], None] = request_process_exit,
) -> None:
    """Wait until any component fails closed, then ask the process to exit."""

    if poll_seconds <= 0:
        raise ValueError("failure poll interval must be positive")
    while True:
        for name, component in components:
            reason = component.failure_reason
            if reason is not None:
                logger.critical(
                    "pipeline_failed_closed_exiting",
                    extra={"component": name, "reason": reason},
                )
                request_exit()
                return
        await asyncio.sleep(poll_seconds)
