"""Small structured JSON logger setup."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

_TELEGRAM_TOKEN_URL = re.compile(
    r"(?P<prefix>\bhttps?://[^\s\"']*?/bot)[^/\s?#\"']+(?=/[A-Za-z])",
    re.IGNORECASE,
)


def _redact_sensitive_value(value: Any) -> Any:
    """Remove credentials that third-party libraries can embed in log messages."""

    if isinstance(value, str):
        return _TELEGRAM_TOKEN_URL.sub(r"\g<prefix><redacted>", value)
    if isinstance(value, dict):
        return {key: _redact_sensitive_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_redact_sensitive_value(item) for item in value)
    if isinstance(value, list):
        return [_redact_sensitive_value(item) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": "funding-arbitrage",
            "logger": record.name,
            "message": _redact_sensitive_value(record.getMessage()),
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
                payload[key] = _redact_sensitive_value(getattr(record, key))
        if record.exc_info:
            payload["exception"] = _redact_sensitive_value(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    # httpx/httpcore INFO records include complete request URLs. Telegram Bot API
    # embeds its credential in the URL path, so successful requests must stay out
    # of routine production logs in addition to formatter-level redaction.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    root.setLevel(level.upper())
