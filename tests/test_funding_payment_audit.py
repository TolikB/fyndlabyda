from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from scripts import funding_payment_audit as audit_module
from scripts.funding_payment_audit import audit_funding_payments

from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
)


def test_funding_gate_requires_exact_versions() -> None:
    with pytest.raises(SystemExit):
        audit_module._parse_args(["--start", "2026-08-14T08:40:00Z"])

    args = audit_module._parse_args(
        [
            "--start",
            "2026-08-14T08:40:00Z",
            "--candidate-version",
            "v31-oos-candidate",
            "--baseline-version",
            "v31-oos-baseline",
            "--require-payments",
        ]
    )

    assert args.candidate_version == "v31-oos-candidate"
    assert args.baseline_version == "v31-oos-baseline"
    assert args.require_payments is True


def _history(exchange: str, symbol: str, at: datetime, rate: str) -> FundingHistoryRecord:
    return FundingHistoryRecord(
        exchange=exchange,
        symbol=symbol,
        funding_timestamp=at,
        funding_rate=Decimal(rate),
        mark_price=None,
    )


def _payment(
    position_id: str,
    exchange: str,
    symbol: str,
    at: datetime,
    rate: str,
    pnl: str,
) -> PaperFundingPaymentRecord:
    return PaperFundingPaymentRecord(
        position_id=position_id,
        exchange=exchange,
        symbol=symbol,
        funding_timestamp=at,
        funding_rate=Decimal(rate),
        notional=Decimal("250"),
        pnl=Decimal(pnl),
    )


def _position(
    opened: datetime,
    target: datetime,
    payments: list[PaperFundingPaymentRecord],
) -> PaperPositionRecord:
    latest = {
        f"{exchange}|{symbol}": max(
            payment.funding_timestamp
            for payment in payments
            if payment.exchange == exchange and payment.symbol == symbol
        ).isoformat()
        for exchange, symbol in (("bybit", "COTIUSDT"), ("gate", "COTI_USDT"))
    }
    return PaperPositionRecord(
        position_id="position-1",
        opportunity_id="opportunity-1",
        state="OPEN",
        asset="COTI",
        capital=Decimal("250"),
        simulation_version="v26-oos-baseline",
        opened_at=opened,
        closed_at=None,
        payload={
            "leg_a": {
                "exchange": "bybit",
                "symbol": "COTIUSDT",
                "instrument_type": "PERPETUAL",
                "side": "SELL",
            },
            "leg_b": {
                "exchange": "gate",
                "symbol": "COTI_USDT",
                "instrument_type": "PERPETUAL",
                "side": "BUY",
            },
            "target_funding_events": {
                "bybit|COTIUSDT": target.isoformat(),
                "gate|COTI_USDT": target.isoformat(),
            },
            "settled_funding_at": latest,
            "funding_events": len(payments),
            "pnl": {
                "funding_pnl": str(
                    sum((payment.pnl for payment in payments), start=Decimal("0"))
                )
            },
        },
    )


def test_audit_reconciles_exact_events_and_only_applies_target_delay_to_first_event() -> None:
    target = datetime(2026, 8, 14, tzinfo=UTC)
    opened = target - timedelta(minutes=45)
    later = target + timedelta(hours=8)
    payments = [
        _payment("position-1", "bybit", "COTIUSDT", target, "-0.0005", "-0.125"),
        _payment(
            "position-1",
            "gate",
            "COTI_USDT",
            target + timedelta(seconds=3),
            "-0.004",
            "1.000",
        ),
        _payment("position-1", "bybit", "COTIUSDT", later, "0.0002", "0.050"),
        _payment(
            "position-1",
            "gate",
            "COTI_USDT",
            later + timedelta(seconds=2),
            "0.0001",
            "-0.025",
        ),
    ]
    history = [
        _history("bybit", "COTIUSDT", target, "-0.0005"),
        _history("gate", "COTI_USDT", target + timedelta(seconds=3), "-0.004"),
        _history("bybit", "COTIUSDT", later, "0.0002"),
        _history("gate", "COTI_USDT", later + timedelta(seconds=2), "0.0001"),
    ]

    result = audit_funding_payments(
        [_position(opened, target, payments)],
        payments,
        history,
        observed_at=later + timedelta(minutes=10),
        require_payments=True,
    )

    assert result["ok"] is True
    assert result["payment_count"] == 4
    assert result["maximum_target_delay_seconds"] == "3.0"
    assert result["late_target_payment_count"] == 0
    assert result["missing_history_payment_count"] == 0


def test_audit_rejects_missing_raw_history_payment() -> None:
    target = datetime(2026, 8, 14, tzinfo=UTC)
    opened = target - timedelta(minutes=30)
    payments = [
        _payment("position-1", "bybit", "COTIUSDT", target, "-0.0005", "-0.125"),
        _payment(
            "position-1",
            "gate",
            "COTI_USDT",
            target + timedelta(seconds=3),
            "-0.004",
            "1.000",
        ),
    ]
    missing_at = target + timedelta(hours=8)
    history = [
        _history("bybit", "COTIUSDT", target, "-0.0005"),
        _history("gate", "COTI_USDT", target + timedelta(seconds=3), "-0.004"),
        _history("bybit", "COTIUSDT", missing_at, "0.0002"),
    ]

    result = audit_funding_payments(
        [_position(opened, target, payments)],
        payments,
        history,
        observed_at=missing_at + timedelta(minutes=5),
        require_payments=True,
    )

    assert result["ok"] is False
    assert result["missing_history_payment_count"] == 1
    assert result["checks"]["raw_history_events_fully_paid"] is False


def test_audit_rejects_wrong_signed_pnl_and_notional() -> None:
    target = datetime(2026, 8, 14, tzinfo=UTC)
    opened = target - timedelta(minutes=30)
    payments = [
        _payment("position-1", "bybit", "COTIUSDT", target, "-0.0005", "0.125"),
        _payment(
            "position-1",
            "gate",
            "COTI_USDT",
            target + timedelta(seconds=3),
            "-0.004",
            "1.000",
        ),
    ]
    payments[1].notional = Decimal("249")
    history = [
        _history("bybit", "COTIUSDT", target, "-0.0005"),
        _history("gate", "COTI_USDT", target + timedelta(seconds=3), "-0.004"),
    ]

    result = audit_funding_payments(
        [_position(opened, target, payments)],
        payments,
        history,
        observed_at=target + timedelta(minutes=10),
        require_payments=True,
    )

    assert result["ok"] is False
    assert result["payment_pnl_mismatch_count"] == 2
    assert result["notional_mismatch_count"] == 1
    assert result["checks"]["payment_pnl_exact"] is False
    assert result["checks"]["notionals_match_position_capital"] is False


def test_audit_can_require_at_least_one_real_payment() -> None:
    result = audit_funding_payments(
        [],
        [],
        [],
        observed_at=datetime(2026, 8, 14, tzinfo=UTC),
        require_payments=True,
    )

    assert result["ok"] is False
    assert result["checks"]["payments_observed_if_required"] is False
