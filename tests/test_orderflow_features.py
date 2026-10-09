from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureEngine

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _book(
    timestamp: datetime,
    *,
    bid_price: str = "100",
    bid_quantity: str = "10",
    ask_price: str = "101",
    ask_quantity: str = "8",
    sequence: int = 1,
) -> BookSnapshot:
    return BookSnapshot(
        instrument=INSTRUMENT,
        bids=(
            BookLevel(price=Decimal(bid_price), quantity=Decimal(bid_quantity)),
            BookLevel(price=Decimal("99"), quantity=Decimal("4")),
        ),
        asks=(
            BookLevel(price=Decimal(ask_price), quantity=Decimal(ask_quantity)),
            BookLevel(price=Decimal("103"), quantity=Decimal("6")),
        ),
        sequence=sequence,
        exchange_timestamp=timestamp,
    )


def test_ofi_uses_restored_bid_ask_update_equation() -> None:
    engine = OrderFlowFeatureEngine(INSTRUMENT)
    engine.on_book(_book(NOW))
    features = engine.on_book(
        _book(
            NOW + timedelta(milliseconds=100),
            bid_quantity="12",
            ask_price="102",
            ask_quantity="7",
            sequence=2,
        )
    )

    # Bid unchanged: +12-10. Ask moved up: +8. Total OFI = +10.
    assert features.ofi_1s == Decimal("10")
    assert features.normalized_ofi_1s == Decimal("10") / Decimal("19")
    assert features.microprice == (Decimal("102") * 12 + Decimal("100") * 7) / 19
    assert features.book_imbalance_l1 == Decimal("5") / Decimal("19")


def test_trade_imbalance_and_cvd_are_incremental_and_windowed() -> None:
    engine = OrderFlowFeatureEngine(INSTRUMENT)
    engine.on_book(_book(NOW))
    engine.on_trade(
        TradeTick(
            instrument=INSTRUMENT,
            trade_id="buy",
            price=Decimal("100"),
            quantity=Decimal("2"),
            aggressor_side=Side.BUY,
            exchange_timestamp=NOW,
        )
    )
    engine.on_trade(
        TradeTick(
            instrument=INSTRUMENT,
            trade_id="sell",
            price=Decimal("100"),
            quantity=Decimal("1"),
            aggressor_side=Side.SELL,
            exchange_timestamp=NOW + timedelta(seconds=1),
        )
    )

    current = engine.snapshot(NOW + timedelta(seconds=1))
    expired = engine.snapshot(NOW + timedelta(seconds=7))

    assert current.trade_imbalance_5s == Decimal("1") / Decimal("3")
    assert current.cvd == Decimal("1")
    assert expired.trade_imbalance_5s is None
    assert expired.cvd == Decimal("1")


def test_invalid_book_returns_unavailable_features_instead_of_zero_signal() -> None:
    engine = OrderFlowFeatureEngine(INSTRUMENT)
    features = engine.on_book(_book(NOW), quality=DataQuality.GAP)

    assert features.data_quality is DataQuality.GAP
    assert features.mid_price is None
    assert features.microprice is None
    assert features.ofi_5s is None
    assert features.book_imbalance_l5 is None


def test_out_of_order_or_wrong_instrument_events_are_rejected() -> None:
    engine = OrderFlowFeatureEngine(INSTRUMENT)
    engine.on_book(_book(NOW))
    with pytest.raises(ValueError, match="out-of-order"):
        engine.on_book(_book(NOW - timedelta(milliseconds=1), sequence=2))

    other = INSTRUMENT.model_copy(update={"venue": "OKX"})
    with pytest.raises(ValueError, match="instrument mismatch"):
        engine.on_trade(
            TradeTick(
                instrument=other,
                trade_id="wrong",
                price=Decimal("100"),
                quantity=Decimal("1"),
                aggressor_side=Side.BUY,
                exchange_timestamp=NOW,
            )
        )
