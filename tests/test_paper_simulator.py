"""Simulator v2: honest fills, ledger accounting, and invariant checks."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.paper import FillRejected, PaperExecutionSimulator
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity, StrategyName
from funding_arbitrage.portfolio.funding import FundingPayment, PriceSource, RateSource
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE, PaperAccount
from tests.builders import book, instrument, snapshot, spot_perp_market, ticker

D = Decimal
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
FEES = {
    "bybit": FeeSchedule(
        maker_fee=D("0.0002"),
        taker_fee=D("0.00055"),
        spot_maker_fee=D("0.001"),
        spot_taker_fee=D("0.001"),
    ),
    "gate": FeeSchedule(maker_fee=D("0.0002"), taker_fee=D("0.0005")),
}


def spot_perp_opportunity() -> Opportunity:
    return Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="BTC",
        venue_a="bybit",
        venue_b="bybit",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type="SPOT",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=D("100"),
        price_b=D("100.1"),
        funding_rate_8h=D("0.0005"),
        gross_edge=D("0.0015"),
        net_edge=D("0.001"),
        expected_holding_hours=D("24"),
        net_apr=D("0.36"),
        available_liquidity=D("100"),
        risk_score=D("10"),
    )


def simulator() -> PaperExecutionSimulator:
    return PaperExecutionSimulator(FEES, max_book_age_seconds=10)


def test_open_uses_separate_spot_and_perp_books_for_shared_symbol() -> None:
    market = spot_perp_market(NOW, spot_price="100", perp_price="110")
    position, fills = simulator().open(
        spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate"
    )

    spot_fill, perp_fill = fills
    assert spot_fill.instrument_type is InstrumentType.SPOT
    assert perp_fill.instrument_type is InstrumentType.PERPETUAL
    # BUY walks the spot asks (100.02), SELL walks the perp bids (109.98).
    assert spot_fill.price == D("100.02")
    assert perp_fill.price == D("109.98")
    assert spot_fill.quantity == perp_fill.quantity == D("0.5")
    # Spot and perpetual taker fees differ per venue.
    assert spot_fill.fee == spot_fill.notional * D("0.001")
    assert perp_fill.fee == perp_fill.notional * D("0.00055")
    assert spot_fill.slippage == D("0.01")  # (100.02 - 100.00) * 0.5
    assert position.legs[0].collateral == spot_fill.notional
    assert position.legs[1].collateral == perp_fill.notional


def test_account_ledger_keeps_equity_invariant_through_open_funding_close() -> None:
    account = PaperAccount("candidate", D("1000"))
    market = spot_perp_market(NOW)
    sim = simulator()
    position, fills = sim.open(spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate")
    account.open_position(position, fills)

    entry_fees = sum((fill.fee for fill in fills), D("0"))
    opened = account.snapshot(NOW)
    assert opened.cash == D("1000") - position.capital - entry_fees
    assert opened.locked_capital == position.capital
    # Fees are a cost: total PnL right after entry equals minus fees plus MTM.
    assert opened.realized_pnl == -entry_fees
    assert opened.equity == opened.cash + opened.locked_capital + opened.unrealized_pnl
    assert opened.invariant_diff <= INVARIANT_TOLERANCE

    perp = position.legs[1]
    payment = FundingPayment(
        series_id="candidate",
        position_id=position.id,
        leg_index=1,
        exchange=perp.exchange,
        symbol=perp.symbol,
        funding_timestamp=NOW + timedelta(hours=1),
        funding_rate=D("0.0005"),
        quantity=perp.quantity,
        mark_price=D("100.1"),
        notional=perp.quantity * D("100.1"),
        amount=perp.quantity * D("100.1") * D("0.0005"),
        rate_source=RateSource.HISTORY,
        price_source=PriceSource.HISTORY,
    )
    account.settle_funding(position, payment)
    assert account.snapshot(NOW).funding_pnl == payment.amount

    later = NOW + timedelta(hours=2)
    close_market = spot_perp_market(later)
    close_fills = sim.close(position, close_market, later)
    booked = account.close_position(position, close_fills, "test", later)

    closed = account.snapshot(later)
    all_fees = entry_fees + sum((fill.fee for fill in close_fills), D("0"))
    assert closed.locked_capital == 0
    assert closed.open_positions == 0
    assert closed.fees == all_fees
    assert booked == position.realized_price_pnl + payment.amount - all_fees
    assert closed.equity == D("1000") + booked
    assert closed.cash == closed.equity
    assert closed.invariant_diff <= INVARIANT_TOLERANCE
    # Round trip at unchanged prices loses the spread and fees; funding offsets part of it.
    assert position.realized_price_pnl < 0


def test_close_fills_use_the_open_quantity() -> None:
    market = spot_perp_market(NOW)
    sim = simulator()
    position, _ = sim.open(spot_perp_opportunity(), D("50"), market, NOW, series_id="s")
    close_fills = sim.close(position, market, NOW)
    assert [fill.quantity for fill in close_fills] == [leg.quantity for leg in position.legs]
    assert all(fill.fee > 0 for fill in close_fills)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        ("no_books", "missing_book"),
        ("stale", "stale_book"),
        ("thin", "insufficient_depth"),
    ],
)
def test_fills_are_never_invented(mutate: str, reason: str) -> None:
    if mutate == "no_books":
        market = spot_perp_market(NOW, with_books=False)
    elif mutate == "stale":
        old = NOW - timedelta(seconds=30)
        market = snapshot(
            NOW,
            [
                instrument("bybit", "BTCUSDT", InstrumentType.SPOT),
                instrument("bybit", "BTCUSDT", InstrumentType.PERPETUAL),
            ],
            [
                ticker("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW),
                ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100.1", NOW),
            ],
            [],
            [
                book("bybit", "BTCUSDT", InstrumentType.SPOT, "100", old),
                book("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100.1", old),
            ],
        )
    else:
        market = snapshot(
            NOW,
            [
                instrument("bybit", "BTCUSDT", InstrumentType.SPOT),
                instrument("bybit", "BTCUSDT", InstrumentType.PERPETUAL),
            ],
            [
                ticker("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW),
                ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100.1", NOW),
            ],
            [],
            [
                book("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW, depth="0.01", levels=2),
                book("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100.1", NOW),
            ],
        )
    account = PaperAccount("candidate", D("1000"))
    with pytest.raises(FillRejected) as error:
        simulator().open(spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate")
    assert error.value.reason == reason
    assert account.snapshot(NOW).equity == D("1000")


def test_quantity_respects_lot_step_and_minimums() -> None:
    market = snapshot(
        NOW,
        [
            instrument("bybit", "BTCUSDT", InstrumentType.SPOT, step="0.0001"),
            instrument("bybit", "BTCUSDT", InstrumentType.PERPETUAL, step="0.01", min_notional="5"),
        ],
        [
            ticker("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW),
            ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100", NOW),
        ],
        [],
        [
            book("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW),
            book("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100", NOW),
        ],
    )
    sim = simulator()
    position, _ = sim.open(spot_perp_opportunity(), D("50.7"), market, NOW, series_id="s")
    assert position.legs[0].quantity == D("0.50")

    with pytest.raises(FillRejected) as error:
        sim.open(spot_perp_opportunity(), D("3"), market, NOW, series_id="s")
    assert error.value.reason == "below_min_notional"


def test_invariant_detects_position_ledger_drift() -> None:
    account = PaperAccount("candidate", D("1000"))
    market = spot_perp_market(NOW)
    position, fills = simulator().open(
        spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate"
    )
    account.open_position(position, fills)
    assert account.invariant_diff() <= INVARIANT_TOLERANCE

    # A funding amount that reaches the leg but not the ledger must be visible.
    position.legs[1].funding_pnl += D("0.05")
    assert account.invariant_diff() > INVARIANT_TOLERANCE


def test_funding_rejects_foreign_symbol_and_spot_leg() -> None:
    account = PaperAccount("candidate", D("1000"))
    market = spot_perp_market(NOW)
    position, fills = simulator().open(
        spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate"
    )
    account.open_position(position, fills)
    base = {
        "series_id": "candidate",
        "position_id": position.id,
        "funding_timestamp": NOW + timedelta(hours=1),
        "funding_rate": D("0.01"),
        "quantity": D("0.5"),
        "mark_price": D("100"),
        "notional": D("50"),
        "amount": D("0.5"),
        "rate_source": RateSource.HISTORY,
        "price_source": PriceSource.HISTORY,
    }
    with pytest.raises(ValueError):
        account.settle_funding(
            position,
            FundingPayment(leg_index=1, exchange="bybit", symbol="ETHUSDT", **base),
        )
    with pytest.raises(ValueError):
        account.settle_funding(
            position,
            FundingPayment(leg_index=0, exchange="bybit", symbol="BTCUSDT", **base),
        )


def test_insufficient_cash_is_rejected_before_any_booking() -> None:
    account = PaperAccount("candidate", D("40"))
    market = spot_perp_market(NOW)
    position, fills = simulator().open(
        spot_perp_opportunity(), D("50"), market, NOW, series_id="candidate"
    )
    with pytest.raises(ValueError):
        account.open_position(position, fills)
    assert account.snapshot(NOW).cash == D("40")
    assert not account.pending_ledger
