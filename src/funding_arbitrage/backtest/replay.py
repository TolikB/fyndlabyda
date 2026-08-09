"""Deterministic event replay with dataset/config provenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable

from .events import BacktestEvent, sort_events


def config_hash(config: object) -> str:
    payload = json.dumps(config, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class EventReplay:
    def __init__(self, events: list[BacktestEvent]) -> None:
        self.events = sort_events(events)

    def run(self, handler: Callable[[BacktestEvent], None]) -> None:
        for event in self.events:
            handler(event)
