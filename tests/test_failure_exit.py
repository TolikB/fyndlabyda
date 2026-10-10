from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.services import failure_exit
from funding_arbitrage.services.failure_exit import exit_when_failed, request_process_exit


class _Component:
    def __init__(self, failure_reason: str | None = None) -> None:
        self.failure_reason = failure_reason


async def test_exit_is_requested_once_any_component_fails_closed() -> None:
    writer, runtime = _Component(), _Component()
    exits: list[str] = []
    watcher = asyncio.create_task(
        exit_when_failed(
            [("writer", writer), ("runtime", runtime)],
            poll_seconds=0.01,
            request_exit=lambda: exits.append("exit"),
        )
    )
    await asyncio.sleep(0.05)
    assert exits == []
    assert not watcher.done()

    runtime.failure_reason = "DBAPIError"
    await asyncio.wait_for(watcher, timeout=1)

    assert exits == ["exit"]


async def test_a_component_that_already_failed_exits_without_waiting() -> None:
    exits: list[str] = []

    await asyncio.wait_for(
        exit_when_failed(
            [("writer", _Component("CanonicalEventWriterError"))],
            poll_seconds=3600,
            request_exit=lambda: exits.append("exit"),
        ),
        timeout=1,
    )

    assert exits == ["exit"]


async def test_failure_poll_interval_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        await exit_when_failed([], poll_seconds=0, request_exit=lambda: None)


def test_process_exit_is_graceful_with_a_hard_exit_behind_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timers: list[_RecordingTimer] = []
    raised: list[int] = []

    class _RecordingTimer:
        def __init__(
            self, interval: float, function: Callable[..., Any], args: tuple[Any, ...]
        ) -> None:
            self.interval = interval
            self.function = function
            self.args = args
            self.daemon = False
            self.started = False
            timers.append(self)

        def start(self) -> None:
            self.started = True

    monkeypatch.setattr(failure_exit.threading, "Timer", _RecordingTimer)
    monkeypatch.setattr(failure_exit.signal, "raise_signal", raised.append)

    request_process_exit(hard_exit_after_seconds=5.0)

    assert raised == [signal.SIGTERM]
    [timer] = timers
    assert (timer.interval, timer.function, timer.args) == (5.0, os._exit, (70,))
    assert timer.daemon
    assert timer.started


def test_pipeline_failure_exit_is_allowed_only_for_paper_processes() -> None:
    with pytest.raises(ValidationError, match="PAPER_EXIT_ON_PIPELINE_FAILURE"):
        Settings(_env_file=None, PAPER_EXIT_ON_PIPELINE_FAILURE=True)

    settings = Settings(
        _env_file=None,
        RUN_MODE="paper_test",
        TRADING_MODE="PAPER",
        CANONICAL_HIGH_FREQUENCY_MARKET_EVENTS_ENABLED=False,
        MULTI_REGIME_ENABLED=False,
        MARKET_DATA_STREAMS_ENABLED=False,
        PAPER_EXIT_ON_PIPELINE_FAILURE=True,
    )

    assert settings.paper_exit_on_pipeline_failure
