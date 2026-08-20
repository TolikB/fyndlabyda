from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_arbitrage.domain.events import (
    BalanceSnapshot,
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookLevel,
    BookSide,
    BookSnapshot,
    Candle,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FillEvent,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OpenInterestSnapshot,
    OptionRight,
    OrderStatus,
    OrderType,
    OrderUpdate,
    PositionSnapshot,
    Side,
    TradeTick,
    deterministic_event_id,
)

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="bybit",
    exchange_symbol="BTCUSDT",
    base_asset="btc",
    quote_asset="usdt",
    settlement_asset="usdt",
    instrument_type=InstrumentType.PERPETUAL,
)


def _metadata(payload: TradeTick, *, kind: EventKind = EventKind.TRADE_TICK) -> EventMetadata:
    event_id = deterministic_event_id(
        source="bybit.public.trade",
        kind=kind,
        sequence_id="42",
        exchange_timestamp=payload.exchange_timestamp,
        payload=payload,
    )
    return EventMetadata(
        event_id=event_id,
        exchange_timestamp=payload.exchange_timestamp,
        receive_timestamp=NOW,
        monotonic_ns=123456,
        sequence_id="42",
        source="bybit.public.trade",
        correlation_id="market:BYBIT:BTCUSDT",
        payload_version=1,
    )


def test_event_id_and_serialization_are_deterministic() -> None:
    tick = TradeTick(
        instrument=INSTRUMENT,
        trade_id="trade-1",
        price=Decimal("62000.10"),
        quantity=Decimal("0.25"),
        aggressor_side=Side.BUY,
        exchange_timestamp=NOW,
    )
    first = EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=_metadata(tick),
        payload=tick,
    )
    second = EventEnvelope[TradeTick].model_validate_json(first.model_dump_json())

    assert first == second
    assert first.metadata.event_id == _metadata(tick).event_id
    assert first.payload.instrument.canonical_id == "BYBIT:BTC-USDT:PERPETUAL"


def test_event_id_tracks_logical_identity_not_payload_revision() -> None:
    original = TradeTick(
        instrument=INSTRUMENT,
        trade_id="trade-1",
        price=Decimal("62000.10"),
        quantity=Decimal("0.25"),
        exchange_timestamp=NOW,
    )
    corrected = original.model_copy(update={"price": Decimal("62000.20")})

    assert _metadata(original).event_id == _metadata(corrected).event_id

def test_envelope_rejects_kind_or_exchange_timestamp_mismatch() -> None:
    tick = TradeTick(
        instrument=INSTRUMENT,
        trade_id="trade-1",
        price=Decimal("1"),
        quantity=Decimal("1"),
        exchange_timestamp=NOW,
    )
    with pytest.raises(ValidationError, match="requires kind"):
        EventEnvelope[TradeTick](
            kind=EventKind.BOOK_DELTA,
            metadata=_metadata(tick, kind=EventKind.BOOK_DELTA),
            payload=tick,
        )
    with pytest.raises(ValidationError, match="timestamps must match"):
        EventEnvelope[TradeTick](
            kind=EventKind.TRADE_TICK,
            metadata=_metadata(tick).model_copy(
                update={"exchange_timestamp": datetime(2026, 8, 15, 13, tzinfo=UTC)}
            ),
            payload=tick,
        )


def test_book_contract_enforces_sorting_delta_actions_and_sequences() -> None:
    snapshot = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("2")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("3")),),
        sequence=10,
        exchange_timestamp=NOW,
    )
    assert snapshot.bids[0].price < snapshot.asks[0].price

    with pytest.raises(ValidationError, match="DELETE"):
        BookDeltaLevel(
            side=BookSide.BID,
            action=BookDeltaAction.DELETE,
            price=Decimal("100"),
            quantity=Decimal("1"),
        )
    with pytest.raises(ValidationError, match="last_sequence"):
        BookDelta(
            instrument=INSTRUMENT,
            updates=(
                BookDeltaLevel(
                    side=BookSide.ASK,
                    action=BookDeltaAction.UPSERT,
                    price=Decimal("102"),
                    quantity=Decimal("1"),
                ),
            ),
            first_sequence=12,
            last_sequence=11,
            exchange_timestamp=NOW,
        )


def test_all_specified_canonical_payloads_validate() -> None:
    spot = INSTRUMENT.model_copy(update={"instrument_type": InstrumentType.SPOT})
    payloads = (
        Candle(
            instrument=INSTRUMENT,
            interval_seconds=60,
            open_time=NOW,
            close_time=datetime(2026, 8, 15, 12, 1, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("102"),
            low=Decimal("99"),
            close=Decimal("101"),
            volume=Decimal("10"),
            exchange_timestamp=NOW,
        ),
        FundingSnapshot(
            instrument=INSTRUMENT,
            funding_rate=Decimal("0.0001"),
            funding_interval_seconds=28800,
            next_funding_time=datetime(2026, 8, 15, 16, tzinfo=UTC),
            mark_price=Decimal("62000"),
            index_price=Decimal("61999"),
            exchange_timestamp=NOW,
        ),
        OpenInterestSnapshot(
            instrument=INSTRUMENT,
            open_interest_quote=Decimal("1000000"),
            exchange_timestamp=NOW,
        ),
        OrderUpdate(
            instrument=spot,
            client_order_id="client-1",
            exchange_order_id="venue-1",
            status=OrderStatus.PARTIALLY_FILLED,
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("0.4"),
            limit_price=Decimal("62000"),
            average_fill_price=Decimal("61999"),
            exchange_timestamp=NOW,
        ),
        FillEvent(
            instrument=spot,
            fill_id="fill-1",
            client_order_id="client-1",
            exchange_order_id="venue-1",
            side=Side.BUY,
            price=Decimal("61999"),
            quantity=Decimal("0.4"),
            fee_amount=Decimal("0.2"),
            fee_asset="usdt",
            liquidity_role=LiquidityRole.TAKER,
            exchange_timestamp=NOW,
        ),
        PositionSnapshot(
            instrument=INSTRUMENT,
            signed_quantity=Decimal("-0.4"),
            entry_price=Decimal("62000"),
            mark_price=Decimal("61990"),
            unrealized_pnl=Decimal("4"),
            margin_used=Decimal("2500"),
            exchange_timestamp=NOW,
        ),
        BalanceSnapshot(
            venue="bybit",
            asset="usdt",
            total=Decimal("10000"),
            available=Decimal("7500"),
            locked=Decimal("2500"),
            exchange_timestamp=NOW,
        ),
    )

    assert len(payloads) == 7
    assert all(payload.exchange_timestamp.tzinfo is UTC for payload in payloads)


def test_option_identity_includes_expiry_strike_and_right() -> None:
    expiry = datetime(2026, 9, 25, 8, tzinfo=UTC)
    call = InstrumentKey(
        venue="deribit",
        exchange_symbol="BTC-25SEP26-60000-C",
        base_asset="btc",
        quote_asset="usd",
        settlement_asset="btc",
        instrument_type=InstrumentType.OPTION,
        expiry=expiry,
        strike_price=Decimal("60000.0"),
        option_right=OptionRight.CALL,
    )
    put = call.model_copy(
        update={
            "exchange_symbol": "BTC-25SEP26-60000-P",
            "option_right": OptionRight.PUT,
        }
    )

    assert call.canonical_id.endswith(":OPTION:2026-09-25T08:00:00+00:00:60000:CALL")
    assert put.canonical_id.endswith(":OPTION:2026-09-25T08:00:00+00:00:60000:PUT")
    assert call.canonical_id != put.canonical_id

    with pytest.raises(ValidationError, match="option identity requires"):
        InstrumentKey(
            venue="DERIBIT",
            exchange_symbol="BROKEN",
            base_asset="BTC",
            quote_asset="USD",
            instrument_type=InstrumentType.OPTION,
        )
