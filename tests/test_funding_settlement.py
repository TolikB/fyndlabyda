"""Exact, idempotent funding settlement against venue history."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import FundingHistoryPoint
from funding_arbitrage.execution.paper import PaperExecutionSimulator
from funding_arbitrage.opportunity.models import FeeSchedule
from funding_arbitrage.portfolio.funding import PriceSource, RateSource
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE, PaperAccount
from funding_arbitrage.services.funding_settlement import FundingSettler
from tests.builders import history_point, spot_perp_market
from tests.test_paper_simulator import spot_perp_opportunity

D = Decimal
OPEN = datetime(2026, 10, 1, 7, 30, tzinfo=UTC)
T1 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 1, 16, 0, tzinfo=UTC)
T3 = datetime(2026, 10, 2, 0, 0, tzinfo=UTC)


class FakeHistory:
    def __init__(self, points: list[FundingHistoryPoint]) -> None:
        self.points = points
        self.calls: list[tuple[str, str, datetime, datetime]] = []
        self.fail = False

    async def __call__(
        self, exchange: str, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        self.calls.append((exchange, symbol, start, end))
        if self.fail:
            raise ConnectionError("history endpoint down")
        return [
            point
            for point in self.points
            if point.exchange == exchange
            and point.symbol == symbol
            and start <= point.funding_timestamp <= end
        ]


def opened_account() -> tuple[PaperAccount, object]:
    simulator = PaperExecutionSimulator(
        {"bybit": FeeSchedule(maker_fee=D("0"), taker_fee=D("0.0005"))}
    )
    account = PaperAccount("candidate", D("1000"))
    market = spot_perp_market(OPEN, next_time=T1)
    position, fills = simulator.open(
        spot_perp_opportunity(), D("50"), market, OPEN, series_id="candidate"
    )
    account.open_position(position, fills)
    return account, position


async def test_only_settlements_after_open_are_booked_with_history_rate() -> None:
    account, position = opened_account()
    history = FakeHistory(
        [
            history_point("bybit", "BTCUSDT", "0.009", OPEN - timedelta(hours=8)),
            history_point("bybit", "BTCUSDT", "0.0004", T1, mark="101"),
            history_point("bybit", "ETHUSDT", "0.05", T1),
        ]
    )
    settler = FundingSettler(history)
    now = T1 + timedelta(seconds=30)
    market = spot_perp_market(now, next_time=T2)
    settler.observe(position, market, now)
    incidents = await settler.settle(account, position, market, now)

    assert incidents == []
    payments = account.pending_funding
    assert len(payments) == 1
    payment = payments[0]
    perp = position.legs[1]
    assert payment.funding_timestamp == T1
    assert payment.symbol == "BTCUSDT"
    assert payment.rate_source is RateSource.HISTORY
    assert payment.price_source is PriceSource.HISTORY
    # Short perpetual receives positive funding on quantity x mark at settlement.
    assert payment.amount == perp.quantity * D("101") * D("0.0004")
    assert perp.last_funding_time == T1
    assert perp.next_funding_time == T2
    assert account.invariant_diff() <= INVARIANT_TOLERANCE


async def test_settlement_is_idempotent_across_cycles() -> None:
    account, position = opened_account()
    history = FakeHistory([history_point("bybit", "BTCUSDT", "0.0004", T1)])
    settler = FundingSettler(history, poll_seconds=1)
    for minutes in (1, 2, 3):
        now = T1 + timedelta(minutes=minutes)
        market = spot_perp_market(now, next_time=T2)
        settler.observe(position, market, now)
        await settler.settle(account, position, market, now)
    assert len(account.pending_funding) == 1


async def test_waits_for_history_then_falls_back_after_grace() -> None:
    account, position = opened_account()
    history = FakeHistory([])
    settler = FundingSettler(history, grace_seconds=900)

    before = T1 - timedelta(seconds=20)
    pre_market = spot_perp_market(before, perp_price="100.5", rate="0.0007", next_time=T1)
    settler.observe(position, pre_market, before)

    early = T1 + timedelta(minutes=5)
    market = spot_perp_market(early, next_time=T2)
    settler.observe(position, market, early)
    await settler.settle(account, position, market, early)
    assert account.pending_funding == []

    late = T1 + timedelta(minutes=16)
    market = spot_perp_market(late, next_time=T2)
    settler.observe(position, market, late)
    incidents = await settler.settle(account, position, market, late)

    assert incidents and incidents[0].startswith("funding_settled_from_snapshot")
    payment = account.pending_funding[0]
    assert payment.funding_timestamp == T1
    assert payment.rate_source is RateSource.SNAPSHOT
    assert payment.funding_rate == D("0.0007")
    assert payment.mark_price == D("100.5")
    assert payment.price_source is PriceSource.PRE_FUNDING_MARK


async def test_restart_catches_up_every_missed_settlement_once() -> None:
    account, position = opened_account()
    history = FakeHistory(
        [
            history_point("bybit", "BTCUSDT", "0.0004", T1),
            history_point("bybit", "BTCUSDT", "0.0003", T2),
            history_point("bybit", "BTCUSDT", "-0.0001", T3),
        ]
    )
    # The leg still waits for T1 because the process was down for a day.
    restarted = T3 + timedelta(minutes=10)
    settler = FundingSettler(history)
    market = spot_perp_market(restarted, next_time=T3 + timedelta(hours=8))
    settler.observe(position, market, restarted)
    await settler.settle(account, position, market, restarted)

    timestamps = [payment.funding_timestamp for payment in account.pending_funding]
    assert timestamps == [T1, T2, T3]
    perp = position.legs[1]
    assert perp.next_funding_time == T3 + timedelta(hours=8)
    assert account.pending_funding[2].amount < 0  # negative funding costs the short

    await settler.settle(account, position, market, restarted + timedelta(hours=2))
    assert len(account.pending_funding) == 3


async def test_history_outage_is_reported_not_guessed() -> None:
    account, position = opened_account()
    history = FakeHistory([history_point("bybit", "BTCUSDT", "0.0004", T1)])
    history.fail = True
    settler = FundingSettler(history)
    now = T1 + timedelta(minutes=1)
    market = spot_perp_market(now, next_time=T2)
    settler.observe(position, market, now)
    incidents = await settler.settle(account, position, market, now)
    assert incidents == ["funding_history_unavailable:bybit:BTCUSDT"]
    assert account.pending_funding == []
    assert position.funding_due(now)
