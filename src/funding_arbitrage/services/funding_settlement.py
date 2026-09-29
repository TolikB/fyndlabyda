"""Exact funding settlement for open paper legs.

A funding payment is booked only for a settlement the venue actually made:
its timestamp and rate come from the venue's published funding history. The
live pre-settlement rate is a fallback used only after a grace period and only
when the live feed proves the settlement happened (its next funding time moved
past the one the leg was waiting for). Every payment records both sources.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import FundingHistoryPoint
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.monitoring.metrics import paper_funding_settlements_total
from funding_arbitrage.portfolio.funding import FundingPayment, PriceSource, RateSource
from funding_arbitrage.portfolio.portfolio import PaperAccount
from funding_arbitrage.portfolio.position import PaperPosition, PositionLeg

logger = logging.getLogger(__name__)

HistoryFetcher = Callable[[str, str, datetime, datetime], Awaitable[list[FundingHistoryPoint]]]


class FundingSettler:
    def __init__(
        self,
        fetch_history: HistoryFetcher,
        *,
        settle_delay_seconds: float = 15.0,
        grace_seconds: float = 900.0,
        poll_seconds: float = 3600.0,
        give_up_seconds: float = 6 * 3600.0,
    ) -> None:
        self.fetch_history = fetch_history
        self.settle_delay = timedelta(seconds=settle_delay_seconds)
        self.grace = timedelta(seconds=grace_seconds)
        self.poll = timedelta(seconds=poll_seconds)
        self.give_up = timedelta(seconds=give_up_seconds)

    # ------------------------------------------------------------------ marks
    def observe(self, position: PaperPosition, snapshot: MarketSnapshot, now: datetime) -> None:
        """Refresh marks and remember the last pre-settlement mark and rate."""

        for leg in position.legs:
            ticker = snapshot.ticker(leg.exchange, leg.symbol, leg.instrument_type)
            if ticker is not None and snapshot.venue_collected(leg.exchange):
                leg.mark_price = ticker.reference_price
                leg.mark_time = now
            if not leg.is_perpetual:
                continue
            funding = snapshot.funding_for(leg.exchange, leg.symbol)
            if funding is None or not snapshot.venue_collected(leg.exchange):
                continue
            leg.funding_interval_hours = funding.funding_interval_hours
            upcoming = funding.next_funding_time
            if (
                upcoming is None
                or upcoming <= position.opened_at
                or (leg.last_funding_time is not None and upcoming <= leg.last_funding_time)
            ):
                continue
            if leg.next_funding_time is None and upcoming <= now:
                # A feed still showing a passed settlement is not a new one to wait for;
                # past events are found by the history poll instead.
                continue
            # Until the awaited settlement passes, the live feed defines it; afterwards
            # it stays frozen until the payment is booked.
            if leg.next_funding_time is None or now < leg.next_funding_time:
                leg.next_funding_time = upcoming
            if now < leg.next_funding_time and upcoming == leg.next_funding_time:
                leg.pre_funding_for = leg.next_funding_time
                leg.pre_funding_rate = funding.funding_rate
                leg.pre_funding_mark = leg.mark_price

    # ------------------------------------------------------------- settlement
    async def settle(
        self,
        account: PaperAccount,
        position: PaperPosition,
        snapshot: MarketSnapshot,
        now: datetime,
    ) -> list[str]:
        """Book every settled funding event of the position; return incidents."""

        incidents: list[str] = []
        for index, leg in position.perpetual_legs:
            waiting = leg.funding_due(now)
            due = (
                waiting
                and leg.next_funding_time is not None
                and (now >= leg.next_funding_time + self.settle_delay)
            )
            poll = leg.last_history_check is None or now - leg.last_history_check >= self.poll
            if not due and not poll:
                continue
            lower = max(position.opened_at, leg.last_funding_time or position.opened_at)
            try:
                points = await self.fetch_history(leg.exchange, leg.symbol, lower, now)
            except Exception as exc:
                incidents.append(f"funding_history_unavailable:{leg.exchange}:{leg.symbol}")
                logger.warning(
                    "funding_history_unavailable",
                    extra={
                        "exchange": leg.exchange,
                        "symbol": leg.symbol,
                        "position_id": position.id,
                        "error": f"{type(exc).__name__}: {exc}"[:200],
                    },
                )
                continue
            leg.last_history_check = now
            for point in sorted(points, key=lambda item: item.funding_timestamp):
                timestamp = point.funding_timestamp
                if timestamp <= lower or timestamp > now:
                    continue
                if leg.last_funding_time is not None and timestamp <= leg.last_funding_time:
                    continue
                mark, price_source = self._settlement_price(leg, point)
                self._book(
                    account,
                    position,
                    index,
                    leg,
                    timestamp,
                    point.funding_rate,
                    mark,
                    RateSource.HISTORY,
                    price_source,
                )
            incidents.extend(self._fallback(account, position, index, leg, snapshot, now))
            self._advance(leg, snapshot)
        return incidents

    def _fallback(
        self,
        account: PaperAccount,
        position: PaperPosition,
        index: int,
        leg: PositionLeg,
        snapshot: MarketSnapshot,
        now: datetime,
    ) -> list[str]:
        expected = leg.next_funding_time
        if expected is None or not leg.funding_due(now) or now < expected + self.grace:
            return []
        funding = snapshot.funding_for(leg.exchange, leg.symbol)
        moved_on = (
            funding is not None
            and funding.next_funding_time is not None
            and funding.next_funding_time > expected
        )
        if moved_on and leg.pre_funding_for == expected and leg.pre_funding_rate is not None:
            mark = leg.pre_funding_mark or leg.mark_price or leg.entry_price
            source = PriceSource.PRE_FUNDING_MARK if leg.pre_funding_mark else PriceSource.ENTRY
            self._book(
                account,
                position,
                index,
                leg,
                expected,
                leg.pre_funding_rate,
                mark,
                RateSource.SNAPSHOT,
                source,
            )
            logger.warning(
                "funding_settled_from_snapshot",
                extra={
                    "exchange": leg.exchange,
                    "symbol": leg.symbol,
                    "position_id": position.id,
                    "funding_timestamp": expected,
                },
            )
            return [f"funding_settled_from_snapshot:{leg.exchange}:{leg.symbol}"]
        if now >= expected + self.give_up:
            logger.error(
                "funding_event_unresolved",
                extra={
                    "exchange": leg.exchange,
                    "symbol": leg.symbol,
                    "position_id": position.id,
                    "funding_timestamp": expected,
                },
            )
            # Stop waiting (the position may exit again); a late history record is still
            # booked by the next poll because it is newer than the last settled event.
            leg.next_funding_time = None
            return [f"funding_event_unresolved:{leg.exchange}:{leg.symbol}"]
        return []

    @staticmethod
    def _advance(leg: PositionLeg, snapshot: MarketSnapshot) -> None:
        if (
            leg.next_funding_time is None
            or leg.last_funding_time is None
            or leg.last_funding_time < leg.next_funding_time
        ):
            return
        funding = snapshot.funding_for(leg.exchange, leg.symbol)
        upcoming = funding.next_funding_time if funding is not None else None
        leg.next_funding_time = (
            upcoming if upcoming is not None and upcoming > leg.last_funding_time else None
        )

    @staticmethod
    def _settlement_price(
        leg: PositionLeg, point: FundingHistoryPoint
    ) -> tuple[Decimal, PriceSource]:
        if point.mark_price is not None and point.mark_price > 0:
            return point.mark_price, PriceSource.HISTORY
        if leg.pre_funding_for == point.funding_timestamp and leg.pre_funding_mark:
            return leg.pre_funding_mark, PriceSource.PRE_FUNDING_MARK
        if leg.mark_price:
            return leg.mark_price, PriceSource.CURRENT_MARK
        return leg.entry_price, PriceSource.ENTRY

    @staticmethod
    def _book(
        account: PaperAccount,
        position: PaperPosition,
        index: int,
        leg: PositionLeg,
        timestamp: datetime,
        rate: Decimal,
        mark: Decimal,
        rate_source: RateSource,
        price_source: PriceSource,
    ) -> None:
        notional = leg.quantity * mark
        payment = FundingPayment(
            series_id=position.series_id,
            position_id=position.id,
            leg_index=index,
            exchange=leg.exchange,
            symbol=leg.symbol,
            funding_timestamp=timestamp,
            funding_rate=rate,
            quantity=leg.quantity,
            mark_price=mark,
            notional=notional,
            # Positive funding: longs pay shorts.
            amount=-leg.direction * notional * rate,
            rate_source=rate_source,
            price_source=price_source,
        )
        account.settle_funding(position, payment)
        paper_funding_settlements_total.labels(position.series_id, rate_source.value).inc()
