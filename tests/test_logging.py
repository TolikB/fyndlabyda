from __future__ import annotations

import json
import logging
import sys

from funding_arbitrage.logging import JsonFormatter


def test_json_formatter_preserves_exception_traceback() -> None:
    try:
        raise RuntimeError("persistence failed")
    except RuntimeError:
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "paper_test_cycle_failed",
            (),
            sys.exc_info(),
        )

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "paper_test_cycle_failed"
    assert "RuntimeError: persistence failed" in payload["exception"]
