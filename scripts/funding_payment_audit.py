"""Read-only reconciliation of paper funding payments against durable raw history."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select, tuple_

from funding_arbitrage.config import get_settings
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
)
from funding_arbitrage.database.session import create_database

PNL_TOLERANCE = Decimal("0.000000000001")


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed) if parsed.utcoffset() is not None else None


def _decimal(value: object) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _perpetual_legs(payload: dict[str, Any]) -> tuple[dict[tuple[str, str], str], int]:
    legs: dict[tuple[str, str], str] = {}
    errors = 0
    for name in ("leg_a", "leg_b"):
        leg = payload.get(name)
        if not isinstance(leg, dict):
            errors += 1
            continue
        if str(leg.get("instrument_type", "")).upper() != "PERPETUAL":
            continue
        exchange = leg.get("exchange")
        symbol = leg.get("symbol")
        side = str(leg.get("side", "")).upper()
        if not isinstance(exchange, str) or not isinstance(symbol, str) or side not in {
            "BUY",
            "SELL",
        }:
            errors += 1
            continue
        key = (exchange, symbol)
        if key in legs and legs[key] != side:
            errors += 1
            continue
        legs[key] = side
    return legs, errors


def audit_funding_payments(
    positions: Sequence[PaperPositionRecord],
    payments: Sequence[PaperFundingPaymentRecord],
    history: Sequence[FundingHistoryRecord],
    *,
    observed_at: datetime,
    maximum_target_delay_seconds: int = 300,
    require_payments: bool = False,
) -> dict[str, Any]:
    """Reconcile every scoped payment and every raw event inside a holding interval."""

    observed = _utc(observed_at)
    grace = timedelta(seconds=maximum_target_delay_seconds)
    position_by_id = {row.position_id: row for row in positions}
    legs_by_position: dict[str, dict[tuple[str, str], str]] = {}
    payload_error_count = 0
    missing_target_count = 0
    target_not_after_open_count = 0
    targets: dict[tuple[str, str, str], datetime] = {}

    for position in positions:
        payload = position.payload if isinstance(position.payload, dict) else {}
        legs, errors = _perpetual_legs(payload)
        legs_by_position[position.position_id] = legs
        payload_error_count += errors + (0 if isinstance(position.payload, dict) else 1)
        target_payload = payload.get("target_funding_events")
        target_payload = target_payload if isinstance(target_payload, dict) else {}
        opened = _utc(position.opened_at) if position.opened_at is not None else None
        if opened is None:
            payload_error_count += 1
        for exchange, symbol in legs:
            target = _parse_datetime(target_payload.get(f"{exchange}|{symbol}"))
            if target is None:
                missing_target_count += 1
                continue
            targets[(position.position_id, exchange, symbol)] = target
            if opened is not None and target <= opened:
                target_not_after_open_count += 1

    history_by_tuple = {
        (row.exchange, row.symbol, _utc(row.funding_timestamp), row.funding_rate)
        for row in history
    }
    history_events: dict[tuple[str, str], list[FundingHistoryRecord]] = defaultdict(list)
    for row in history:
        history_events[(row.exchange, row.symbol)].append(row)

    payment_identity_counts = Counter(
        (
            row.position_id,
            row.exchange,
            row.symbol,
            _utc(row.funding_timestamp),
        )
        for row in payments
    )
    duplicate_payment_count = sum(count - 1 for count in payment_identity_counts.values())
    payment_identities = set(payment_identity_counts)
    payments_by_position: dict[str, list[PaperFundingPaymentRecord]] = defaultdict(list)
    payments_by_leg: dict[tuple[str, str, str], list[PaperFundingPaymentRecord]] = defaultdict(list)

    orphan_payment_count = 0
    leg_match_mismatch_count = 0
    exact_history_mismatch_count = 0
    payment_before_open_count = 0
    payment_after_close_count = 0
    notional_mismatch_count = 0
    payment_pnl_mismatch_count = 0
    maximum_payment_pnl_error = Decimal("0")
    payment_details: list[dict[str, Any]] = []

    for payment in payments:
        matched_position = position_by_id.get(payment.position_id)
        actual_at = _utc(payment.funding_timestamp)
        if matched_position is None:
            orphan_payment_count += 1
            continue
        payments_by_position[payment.position_id].append(payment)
        payments_by_leg[(payment.position_id, payment.exchange, payment.symbol)].append(payment)
        side = legs_by_position.get(payment.position_id, {}).get(
            (payment.exchange, payment.symbol)
        )
        if side is None:
            leg_match_mismatch_count += 1
            expected_pnl = None
        else:
            expected_pnl = (
                -payment.notional * payment.funding_rate
                if side == "BUY"
                else payment.notional * payment.funding_rate
            )
            error = abs(payment.pnl - expected_pnl)
            maximum_payment_pnl_error = max(maximum_payment_pnl_error, error)
            if error > PNL_TOLERANCE:
                payment_pnl_mismatch_count += 1
        exact_history_match = (
            payment.exchange,
            payment.symbol,
            actual_at,
            payment.funding_rate,
        ) in history_by_tuple
        if not exact_history_match:
            exact_history_mismatch_count += 1
        if matched_position.opened_at is None or actual_at <= _utc(
            matched_position.opened_at
        ):
            payment_before_open_count += 1
        if matched_position.closed_at is not None and actual_at > _utc(
            matched_position.closed_at
        ):
            payment_after_close_count += 1
        if payment.notional != matched_position.capital:
            notional_mismatch_count += 1
        target = targets.get((payment.position_id, payment.exchange, payment.symbol))
        payment_details.append(
            {
                "simulation_version": matched_position.simulation_version,
                "position_id": payment.position_id,
                "exchange": payment.exchange,
                "symbol": payment.symbol,
                "side": side,
                "target_at": target.isoformat() if target is not None else None,
                "actual_at": actual_at.isoformat(),
                "target_delay_seconds": (
                    str((actual_at - target).total_seconds()) if target is not None else None
                ),
                "rate": str(payment.funding_rate),
                "notional": str(payment.notional),
                "pnl": str(payment.pnl),
                "expected_pnl": str(expected_pnl) if expected_pnl is not None else None,
                "exact_history_match": exact_history_match,
            }
        )

    missing_history_payment_count = 0
    missing_target_payment_count = 0
    late_target_payment_count = 0
    maximum_target_delay = Decimal("0")
    settled_marker_mismatch_count = 0
    funding_event_count_mismatch_count = 0
    position_funding_total_mismatch_count = 0
    maximum_position_funding_total_error = Decimal("0")

    for position in positions:
        payload = position.payload if isinstance(position.payload, dict) else {}
        opened = _utc(position.opened_at) if position.opened_at is not None else None
        effective_end = min(
            observed,
            _utc(position.closed_at) if position.closed_at is not None else observed,
        )
        if opened is not None:
            for exchange, symbol in legs_by_position.get(position.position_id, {}):
                for event in history_events.get((exchange, symbol), []):
                    event_at = _utc(event.funding_timestamp)
                    identity = (position.position_id, exchange, symbol, event_at)
                    if opened < event_at <= effective_end and identity not in payment_identities:
                        missing_history_payment_count += 1
                target = targets.get((position.position_id, exchange, symbol))
                if target is not None and opened < target <= effective_end - grace:
                    after_target = sorted(
                        (
                            row
                            for row in payments_by_leg.get(
                                (position.position_id, exchange, symbol), []
                            )
                            if _utc(row.funding_timestamp) >= target
                        ),
                        key=lambda row: row.funding_timestamp,
                    )
                    if not after_target:
                        missing_target_payment_count += 1
                    else:
                        delay = Decimal(
                            str((_utc(after_target[0].funding_timestamp) - target).total_seconds())
                        )
                        maximum_target_delay = max(maximum_target_delay, delay)
                        if delay > maximum_target_delay_seconds:
                            late_target_payment_count += 1

        position_payments = payments_by_position.get(position.position_id, [])
        expected_event_count = len(position_payments)
        persisted_event_count = payload.get("funding_events")
        if persisted_event_count != expected_event_count:
            funding_event_count_mismatch_count += 1

        settled_payload = payload.get("settled_funding_at")
        settled_payload = settled_payload if isinstance(settled_payload, dict) else {}
        for exchange, symbol in legs_by_position.get(position.position_id, {}):
            leg_payments = payments_by_leg.get((position.position_id, exchange, symbol), [])
            persisted_marker = _parse_datetime(settled_payload.get(f"{exchange}|{symbol}"))
            expected_marker = (
                max(_utc(row.funding_timestamp) for row in leg_payments)
                if leg_payments
                else None
            )
            if persisted_marker != expected_marker:
                settled_marker_mismatch_count += 1

        persisted_funding = _decimal(
            (payload.get("pnl") or {}).get("funding_pnl")
            if isinstance(payload.get("pnl"), dict)
            else None
        )
        calculated_funding = sum(
            (row.pnl for row in position_payments), start=Decimal("0")
        )
        if persisted_funding is None:
            position_funding_total_mismatch_count += 1
        else:
            error = abs(persisted_funding - calculated_funding)
            maximum_position_funding_total_error = max(
                maximum_position_funding_total_error, error
            )
            if error > PNL_TOLERANCE:
                position_funding_total_mismatch_count += 1

    checks = {
        "payments_observed_if_required": not require_payments or bool(payments),
        "position_payloads_valid": payload_error_count == 0,
        "targets_complete": missing_target_count == 0,
        "targets_after_open": target_not_after_open_count == 0,
        "orphan_payments_zero": orphan_payment_count == 0,
        "leg_matches_complete": leg_match_mismatch_count == 0,
        "exact_history_matches_complete": exact_history_mismatch_count == 0,
        "payments_inside_holding_interval": (
            payment_before_open_count == 0 and payment_after_close_count == 0
        ),
        "notionals_match_position_capital": notional_mismatch_count == 0,
        "payment_pnl_exact": payment_pnl_mismatch_count == 0,
        "raw_history_events_fully_paid": missing_history_payment_count == 0,
        "initial_targets_paid": missing_target_payment_count == 0,
        "initial_target_delays_within_limit": late_target_payment_count == 0,
        "settled_markers_exact": settled_marker_mismatch_count == 0,
        "funding_event_counts_exact": funding_event_count_mismatch_count == 0,
        "position_funding_totals_exact": position_funding_total_mismatch_count == 0,
        "payments_unique": duplicate_payment_count == 0,
    }
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "observed_at": observed.isoformat(),
        "maximum_allowed_target_delay_seconds": maximum_target_delay_seconds,
        "position_count": len(positions),
        "payment_count": len(payments),
        "history_event_count": len(history),
        "payload_error_count": payload_error_count,
        "missing_target_count": missing_target_count,
        "target_not_after_open_count": target_not_after_open_count,
        "orphan_payment_count": orphan_payment_count,
        "leg_match_mismatch_count": leg_match_mismatch_count,
        "exact_history_mismatch_count": exact_history_mismatch_count,
        "payment_before_open_count": payment_before_open_count,
        "payment_after_close_count": payment_after_close_count,
        "notional_mismatch_count": notional_mismatch_count,
        "payment_pnl_mismatch_count": payment_pnl_mismatch_count,
        "maximum_payment_pnl_error": str(maximum_payment_pnl_error),
        "missing_history_payment_count": missing_history_payment_count,
        "missing_target_payment_count": missing_target_payment_count,
        "late_target_payment_count": late_target_payment_count,
        "maximum_target_delay_seconds": str(maximum_target_delay),
        "settled_marker_mismatch_count": settled_marker_mismatch_count,
        "funding_event_count_mismatch_count": funding_event_count_mismatch_count,
        "position_funding_total_mismatch_count": position_funding_total_mismatch_count,
        "maximum_position_funding_total_error": str(
            maximum_position_funding_total_error
        ),
        "duplicate_payment_count": duplicate_payment_count,
        "payments": sorted(
            payment_details,
            key=lambda row: (str(row["actual_at"]), str(row["exchange"])),
        ),
    }


async def _load_and_audit(args: argparse.Namespace) -> dict[str, Any]:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    observed_at = _parse_datetime(args.observed_at) if args.observed_at else datetime.now(UTC)
    boundary = _parse_datetime(args.start)
    if boundary is None or observed_at is None:
        raise ValueError("--start and --observed-at must be timezone-aware ISO-8601 values")
    versions = (args.candidate_version, args.baseline_version)
    try:
        async with session_factory() as session:
            positions = list(
                (
                    await session.execute(
                        select(PaperPositionRecord).where(
                            PaperPositionRecord.simulation_version.in_(versions),
                            PaperPositionRecord.opened_at >= boundary,
                        )
                    )
                ).scalars()
            )
            position_ids = [row.position_id for row in positions]
            payments = (
                list(
                    (
                        await session.execute(
                            select(PaperFundingPaymentRecord).where(
                                PaperFundingPaymentRecord.position_id.in_(position_ids)
                            )
                        )
                    ).scalars()
                )
                if position_ids
                else []
            )
            pairs = {
                key
                for position in positions
                for key in _perpetual_legs(position.payload)[0]
            }
            history = (
                list(
                    (
                        await session.execute(
                            select(FundingHistoryRecord).where(
                                tuple_(
                                    FundingHistoryRecord.exchange,
                                    FundingHistoryRecord.symbol,
                                ).in_(pairs),
                                FundingHistoryRecord.funding_timestamp > boundary,
                                FundingHistoryRecord.funding_timestamp <= observed_at,
                            )
                        )
                    ).scalars()
                )
                if pairs
                else []
            )
            return audit_funding_payments(
                positions,
                payments,
                history,
                observed_at=observed_at,
                maximum_target_delay_seconds=args.maximum_target_delay_seconds,
                require_payments=args.require_payments,
            )
    finally:
        await engine.dispose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-version", default="v28-oos-candidate")
    parser.add_argument("--baseline-version", default="v28-oos-baseline")
    parser.add_argument("--start", required=True)
    parser.add_argument("--observed-at")
    parser.add_argument("--maximum-target-delay-seconds", type=int, default=300)
    parser.add_argument("--require-payments", action="store_true")
    return parser.parse_args()


def main() -> int:
    result = asyncio.run(_load_and_audit(_parse_args()))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
