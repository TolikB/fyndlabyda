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


def test_json_formatter_preserves_paper_decision_context() -> None:
    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        1,
        "paper_trade_rejected",
        (),
        None,
    )
    record.event = "paper_trade_rejected"
    record.profile = "candidate"
    record.reason = "settlement_cost_coverage"
    record.risk_reasons = ()
    record.asset = "COTI"
    record.strategy = "cross_exchange_funding"
    record.venue_a = "gate"
    record.venue_b = "bybit"
    record.opportunity_id = "opportunity-1"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["profile"] == "candidate"
    assert payload["reason"] == "settlement_cost_coverage"
    assert payload["asset"] == "COTI"
    assert payload["venue_a"] == "gate"
    assert payload["venue_b"] == "bybit"
