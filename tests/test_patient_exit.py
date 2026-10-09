"""Patient funding exits: the shared clock, the edge, settings, and replay parity."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.backtest.historical_replay import HistoricalDataset, HistoricalMarketReplay
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import MarketCandleRecord
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
)
from funding_arbitrage.opportunity.patient_exit import PatientExit, funding_edge_8h

D = Decimal
T0 = datetime(2026, 1, 2, tzinfo=UTC)


def funding_row(exchange: str, rate: str, interval: str) -> FundingSnapshot:
    return FundingSnapshot(
        exchange=exchange,
        symbol="XUSDT",
        funding_rate=D(rate),
        funding_interval_hours=D(interval),
        timestamp=T0,
    )


def test_edge_normalises_intervals_and_signs() -> None:
    rows = [funding_row("bybit", "0.0002", "1"), funding_row("binance", "0.0004", "8")]
    # Short the hourly venue (receives 8 x 0.02%), long the 8-hourly one (pays 0.04%).
    legs = [("bybit", "XUSDT", "SELL"), ("binance", "XUSDT", "BUY")]
    assert funding_edge_8h(legs, rows) == D("0.0016") - D("0.0004")
    assert funding_edge_8h([("okx", "XUSDT", "SELL")], rows) is None
    assert funding_edge_8h([], rows) is None


def test_clock_needs_min_hold_and_an_unbroken_low_edge() -> None:
    clock = PatientExit(D("-0.0005"), confirmation_seconds=3600, min_hold_seconds=86400)
    low, high = D("-0.001"), D("0.002")

    def at(hours: float, edge: Decimal | None) -> str | None:
        return clock.reason("p", T0, T0 + timedelta(hours=hours), edge)

    assert at(1, low) is None  # the clock starts, but the minimum hold has not passed
    assert at(23, low) is None
    assert at(23.5, high) is None  # recovered: the clock resets
    assert at(24, low) is None
    assert at(24.5, None) is None  # a feed gap neither starts nor resets it
    assert at(24.9, low) is None
    assert at(25, low) == "funding_edge_decay"
    clock.forget("p")
    assert at(25.1, low) is None


def test_patient_min_hold_must_fit_inside_max_hold() -> None:
    with pytest.raises(ValueError, match="PAPER_MIN_HOLD_SECONDS"):
        Settings(
            PAPER_EXIT_POLICY="patient",
            PAPER_MIN_HOLD_SECONDS=86400,
            PAPER_MAX_HOLD_SECONDS=3600,
        )
    settings = Settings(
        PAPER_EXIT_POLICY="patient", PAPER_MIN_HOLD_SECONDS=3600, PAPER_MAX_HOLD_SECONDS=86400
    )
    assert settings.paper_exit_policy == "patient"
    assert Settings().paper_exit_policy == "settlement"


def _dataset(hours: int, turn_hour: int, blip_hour: int | None = None) -> HistoricalDataset:
    """Funding pays 0.5%/h until ``turn_hour``, except one negative ``blip_hour``."""

    instruments = [
        NormalizedInstrument(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=D("0.1"),
            step_size=D("0.001"),
            min_order_size=D("0.001"),
            funding_interval=1 if instrument_type is InstrumentType.PERPETUAL else None,
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    candles = [
        MarketCandleRecord(
            exchange="bybit",
            symbol="BTCUSDT",
            instrument_type=instrument_type.value,
            interval_minutes=60,
            open_time=T0 + timedelta(hours=hour - 1),
            close_time=T0 + timedelta(hours=hour) - timedelta(milliseconds=1),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=D("10000"),
            is_closed=True,
        )
        for hour in range(hours)
        for instrument_type, price in (
            (InstrumentType.SPOT, D("100")),
            (InstrumentType.PERPETUAL, D("100.1")),
        )
    ]
    funding = [
        FundingHistoryPoint(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=D("0.005") if hour < turn_hour and hour != blip_hour else D("-0.005"),
            funding_timestamp=T0 + timedelta(hours=hour),
        )
        for hour in range(-24, hours + 1)
    ]
    return HistoricalDataset(
        instruments=instruments,
        candles=candles,
        funding=funding,
        dataset_version="patient-fixture",
        coverage={},
    )


def test_replay_patient_policy_holds_through_settlements() -> None:
    # One negative settlement at hour 10 makes the settlement policy leave and pay a
    # new round trip; the patient policy is still inside its minimum hold.
    dataset = _dataset(hours=48, turn_hour=30, blip_hour=10)
    common = {
        "SCANNER_MINIMUM_NET_APR": "0",
        "SCANNER_MINIMUM_LIQUIDITY_SCORE": "0",
        "SCANNER_MINIMUM_FUNDING_SAMPLES": 20,
        "PAPER_MIN_SETTLEMENT_COST_COVERAGE": "1.25",
        "PAPER_MAX_HOLD_SECONDS": 172800,
    }
    replay = HistoricalMarketReplay()
    settlement = replay.simulate(dataset, "candidate", D("15000"), Settings(**common))
    patient_settings = Settings(
        **common,
        PAPER_EXIT_POLICY="patient",
        PAPER_MIN_HOLD_SECONDS=43200,
        PAPER_PATIENT_EXIT_CONFIRMATION_SECONDS=3600,
    )
    patient = replay.simulate(dataset, "candidate", D("15000"), patient_settings)

    assert settlement.position_count > patient.position_count >= 1
    opened = {
        event.event_id.split(":")[-1]: event.timestamp
        for event in patient.events
        if event.event_type == "position" and ":open:" in event.event_id
    }
    closed = {
        event.event_id.split(":")[-1]: event.timestamp
        for event in patient.events
        if event.event_type == "position" and event.event_id.startswith("position-close:")
    }
    assert opened
    for position_id, closed_at in closed.items():
        # Held past the minimum, and left only after the edge turned (hour 30) + 1h.
        assert closed_at - opened[position_id] >= timedelta(hours=12)
        assert closed_at >= T0 + timedelta(hours=31)
    # Deterministic, like the settlement policy.
    again = replay.simulate(dataset, "candidate", D("15000"), patient_settings)
    assert [event.model_dump(mode="json") for event in patient.events] == [
        event.model_dump(mode="json") for event in again.events
    ]
