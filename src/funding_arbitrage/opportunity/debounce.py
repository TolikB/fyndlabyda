"""Candidate/confirmed/expired opportunity state transitions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .models import Opportunity, OpportunityStatus


class OpportunityDebouncer:
    def __init__(self, confirmation_seconds: int = 30, expiry_seconds: int = 60) -> None:
        self.confirmation_seconds = confirmation_seconds
        self.expiry_seconds = expiry_seconds
        self._first_seen: dict[str, datetime] = {}
        self._last_seen: dict[str, datetime] = {}

    def observe(self, opportunity: Opportunity, now: datetime | None = None) -> Opportunity:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        key = self.key(opportunity)
        first = self._first_seen.setdefault(key, current)
        self._last_seen[key] = current
        opportunity.status = (
            OpportunityStatus.CONFIRMED
            if (current - first).total_seconds() >= self.confirmation_seconds
            else OpportunityStatus.CANDIDATE
        )
        return opportunity

    def expire(self, now: datetime | None = None) -> list[str]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        expired = [
            key
            for key, last_seen in self._last_seen.items()
            if current - last_seen >= timedelta(seconds=self.expiry_seconds)
        ]
        for key in expired:
            self._last_seen.pop(key, None)
            self._first_seen.pop(key, None)
        return expired

    @staticmethod
    def key(opportunity: Opportunity) -> str:
        return ":".join(
            [
                str(opportunity.strategy),
                opportunity.asset,
                opportunity.venue_a,
                opportunity.venue_b or "",
                opportunity.leg_a_type,
                opportunity.leg_b_type,
            ]
        )

    @staticmethod
    def exposure_key(opportunity: Opportunity) -> str:
        """Return an order- and side-independent key for the traded instruments."""

        return canonical_exposure_key(
            opportunity.asset,
            (
                opportunity.venue_a,
                opportunity.symbol_a or "",
                opportunity.leg_a_type,
            ),
            (
                opportunity.venue_b or opportunity.venue_a,
                opportunity.symbol_b or "",
                opportunity.leg_b_type,
            ),
        )


def canonical_exposure_key(
    asset: str,
    leg_a: tuple[str, str, str],
    leg_b: tuple[str, str, str],
) -> str:
    """Identify the same two-instrument exposure regardless of route direction."""

    legs = sorted(
        (
            tuple(str(value) for value in leg_a),
            tuple(str(value) for value in leg_b),
        )
    )
    return "|".join(
        (
            "exposure",
            asset.upper(),
            *(value for leg in legs for value in leg),
        )
    )
