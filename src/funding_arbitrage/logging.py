"""Small structured JSON logger setup."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": "funding-arbitrage",
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "exchange",
            "symbol",
            "event",
            "latency_ms",
            "success",
            "error",
            "profile",
            "reason",
            "risk_reasons",
            "asset",
            "strategy",
            "venue_a",
            "venue_b",
            "opportunity_id",
            "position_id",
            "category",
            "exchanges",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
