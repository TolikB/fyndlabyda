from __future__ import annotations

import json
import logging
import sys

from funding_arbitrage.logging import JsonFormatter, configure_logging


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


def test_json_formatter_redacts_telegram_token_everywhere() -> None:
    token = "123456789:secret-telegram-token"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        raise RuntimeError(f"request failed for {url}")
    except RuntimeError:
        record = logging.LogRecord(
            "httpx",
            logging.ERROR,
            __file__,
            1,
            "HTTP Request: POST %s",
            (url,),
            sys.exc_info(),
        )
    record.error = {"request_url": url, "attempts": [url]}

    formatted = JsonFormatter().format(record)
    payload = json.loads(formatted)

    assert token not in formatted
    assert payload["message"].endswith("/bot<redacted>/sendMessage")
    assert "bot<redacted>/sendMessage" in payload["exception"]
    assert payload["error"]["request_url"].endswith("/bot<redacted>/sendMessage")
    assert payload["error"]["attempts"][0].endswith("/bot<redacted>/sendMessage")


def test_configure_logging_suppresses_http_client_request_urls() -> None:
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_root_level = root.level
    httpx_logger = logging.getLogger("httpx")
    httpcore_logger = logging.getLogger("httpcore")
    original_httpx_level = httpx_logger.level
    original_httpcore_level = httpcore_logger.level

    try:
        configure_logging("INFO")

        assert httpx_logger.getEffectiveLevel() == logging.WARNING
        assert httpcore_logger.getEffectiveLevel() == logging.WARNING
    finally:
        root.handlers[:] = original_handlers
        root.setLevel(original_root_level)
        httpx_logger.setLevel(original_httpx_level)
        httpcore_logger.setLevel(original_httpcore_level)
