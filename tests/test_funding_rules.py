"""Rule replay over recorded funding: settlements, exits, cooldowns, costs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from funding_arbitrage.backtest.funding_rules import (
    FundingTape,
    ReplayLeg,
    Signal,
    load_inputs,
    replay_series,
    settled_edge,
    signal_from_payload,
)
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    FundingSnapshotRecord,
    OpportunityRecord,
)
from funding_arbitrage.services.series import SeriesConfig
from tests.conftest import Database

D = Decimal
T0 = datetime(2026, 10, 1, tzinfo=UTC)
SHORT = ReplayLeg("bybit", "XUSDT", -1, D("8"))
LONG = ReplayLeg("binance", "XUSDT", 1, D("8"))


def tape(turn_hours: int = 20, hours: int = 72) -> FundingTape:
    """The short leg shows and settles 0.1%/8h until ``turn_hours``, then -0.2%."""

    result = FundingTape()
    for minute in range(0, hours * 60, 15):
        at = T0 + timedelta(minutes=minute)
        rate = D("0.001") if minute < turn_hours * 60 else D("-0.002")
        settle = T0 + timedelta(hours=8 * (minute // 480 + 1))
        result.add_snapshot(SHORT.key, at, rate, D("8"), settle)
        result.add_snapshot(LONG.key, at, D("0"), D("8"), settle)
    return result.freeze()


def signal(at: datetime = T0, **overrides: Any) -> Signal:
    values: dict[str, Any] = {
        "at": at,
        "key": "cross:X",
        "asset": "X",
        "strategy": "cross_exchange_funding",
        "legs": (SHORT, LONG),
        "funding_rate_8h": D("0.001"),
        "net_apr": D("1"),
        "stability": D("95"),
        "persistence": D("100"),
    }
    values.update(overrides)
    return Signal(**values)


def config(**blocks: Any) -> SeriesConfig:
    return SeriesConfig.model_validate(
        {
            "name": "replay",
            "label": "replay-test",
            "initial_balance_usdt": "1000",
            "position_notional_usdt": "50",
            "max_total_notional_usdt": "400",
            "max_open_positions": 8,
            "strategies": ["cross_exchange_funding"],
            **blocks,
        }
    )


def test_settlements_from_snapshots_and_history_are_counted_once() -> None:
    recorded = FundingTape()
    settle = T0 + timedelta(hours=8)
    recorded.add_snapshot(SHORT.key, T0 + timedelta(hours=7), D("0.0009"), D("8"), settle)
    # History stamps the same settlement a millisecond later and wins.
    recorded.add_settlement(SHORT.key, settle + timedelta(milliseconds=1), D("0.001"))
    recorded.freeze()
    assert recorded.settled_between(SHORT.key, T0, T0 + timedelta(hours=9)) == [D("0.001")]
    assert settled_edge(recorded, signal(legs=(SHORT,)), T0, T0 + timedelta(hours=9)) == D("0.001")


def test_legacy_exit_leaves_soon_after_the_edge_turns() -> None:
    result = replay_series(
        config(exit={"min_hold_hours": "8", "exit_confirmations": 3}),
        [signal()],
        tape(),
        end=T0 + timedelta(hours=72),
    )
    (trade,) = result.trades
    assert trade.closed_at == T0 + timedelta(hours=20)
    # Settlements at 8h and 16h: two times 0.1% received on the short leg.
    assert trade.funding == D("0.002")
    assert trade.net == D("0.002") - D("0.00334")


def test_patient_exit_waits_for_a_negative_edge_and_the_confirmation() -> None:
    patient = config(
        exit={"min_hold_hours": "24", "exit_funding_rate_8h": "-0.0005"},
        holding={"exit_confirmation_minutes": "60"},
    )
    (trade,) = replay_series(patient, [signal()], tape(), end=T0 + timedelta(hours=72)).trades
    assert trade.closed_at == T0 + timedelta(hours=24)
    never_turns = replay_series(
        patient, [signal()], tape(turn_hours=100), end=T0 + timedelta(hours=72)
    )
    assert never_turns.trades[0].open_at_end


def test_selection_filters_and_cooldown() -> None:
    selective = config(
        exit={"min_hold_hours": "8"},
        selection={"min_persistence_score": "90", "reentry_cooldown_hours": "8"},
    )
    signals = [
        signal(persistence=D("50")),
        signal(T0 + timedelta(minutes=15)),
        signal(T0 + timedelta(hours=21)),  # one hour after the exit: cooling down
        signal(T0 + timedelta(hours=29), key="cross:X:reversed"),
    ]
    result = replay_series(selective, signals, tape(), end=T0 + timedelta(hours=72))
    assert [trade.opened_at for trade in result.trades] == [
        T0 + timedelta(minutes=15),
        T0 + timedelta(hours=29),
    ]


def test_forecast_rejects_a_spike_on_a_pair_that_settled_nothing() -> None:
    forecast = config(selection={"forecast": {"lookback_hours": "24", "min_history_points": 1}})
    spike = signal(T0 + timedelta(hours=30), funding_rate_8h=D("0.004"))
    assert not replay_series(forecast, [spike], tape(), end=T0 + timedelta(hours=72)).trades
    early = signal(T0 + timedelta(hours=17))
    assert replay_series(forecast, [early], tape(), end=T0 + timedelta(hours=72)).trades


def test_passive_execution_is_a_cost_discount() -> None:
    maker = config(execution={"maker_entry": True, "maker_exit": True})
    (trade,) = replay_series(maker, [signal()], tape(), end=T0 + timedelta(hours=72)).trades
    assert trade.cost == D("0.00334") - D("0.0006")


def test_signal_from_recorded_payload() -> None:
    payload = {
        "strategy": "cross_exchange_funding",
        "asset": "X",
        "venue_a": "bybit",
        "symbol_a": "XUSDT",
        "leg_a_type": "PERPETUAL",
        "leg_a_side": "SELL",
        "venue_b": "binance",
        "symbol_b": "XUSDT",
        "leg_b_type": "PERPETUAL",
        "leg_b_side": "BUY",
        "funding_interval_hours_a": "1",
        "funding_interval_hours_b": "8",
        "funding_rate_8h": "0.003",
        "net_apr": "2.5",
        "funding_stability_score": "93.5",
        "persistence_score": "100",
    }
    parsed = signal_from_payload(T0, payload)
    assert parsed is not None
    assert parsed.legs == (
        ReplayLeg("bybit", "XUSDT", -1, D("1")),
        ReplayLeg("binance", "XUSDT", 1, D("8")),
    )
    assert parsed.key.startswith("cross_exchange_funding:X:bybit:XUSDT:PERPETUAL:SELL")
    assert signal_from_payload(T0, {**payload, "symbol_b": None}) is None


async def test_inputs_load_from_the_recorded_tables(database: Database) -> None:
    payload = {
        "strategy": "cross_exchange_funding",
        "asset": "X",
        "venue_a": "bybit",
        "symbol_a": "XUSDT",
        "leg_a_type": "PERPETUAL",
        "leg_a_side": "SELL",
        "venue_b": "binance",
        "symbol_b": "XUSDT",
        "leg_b_type": "PERPETUAL",
        "leg_b_side": "BUY",
        "funding_interval_hours_a": "8",
        "funding_interval_hours_b": "8",
        "funding_rate_8h": "0.001",
        "net_apr": "1",
        "funding_stability_score": "95",
        "persistence_score": "100",
    }
    settle = T0 + timedelta(hours=8)
    async with database.session_factory() as session:
        session.add(
            OpportunityRecord(
                opportunity_id="o1",
                strategy="cross_exchange_funding",
                asset="X",
                venue_a="bybit",
                venue_b="binance",
                gross_edge=D("0"),
                net_edge=D("0"),
                net_apr=D("1"),
                opportunity_score=D("0"),
                status="confirmed",
                created_at=T0 + timedelta(hours=1),
                payload=payload,
            )
        )
        for venue, rate in (("bybit", "0.0009"), ("binance", "0.0001")):
            session.add(
                FundingSnapshotRecord(
                    exchange=venue,
                    symbol="XUSDT",
                    funding_rate=D(rate),
                    funding_interval_hours=D("8"),
                    next_funding_time=settle,
                    timestamp=T0 + timedelta(hours=7),
                )
            )
        session.add(
            FundingHistoryRecord(
                exchange="bybit", symbol="XUSDT", funding_rate=D("0.001"), funding_timestamp=settle
            )
        )
        await session.commit()
        signals, recorded = await load_inputs(
            session, T0, T0 + timedelta(hours=10), timedelta(hours=24)
        )
    (loaded,) = signals
    assert loaded.persistence == D("100")
    # History wins for bybit; binance falls back to its last snapshot before settlement.
    assert settled_edge(recorded, loaded, T0, T0 + timedelta(hours=9)) == D("0.001") - D("0.0001")
    assert recorded.shown_at(("bybit", "XUSDT"), T0 + timedelta(hours=8)) == D("0.0009")
