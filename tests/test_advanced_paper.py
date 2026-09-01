from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.backtest.fills import FillModelPolicy, SimulatedOrderState
from funding_arbitrage.database.models import (
    Base,
    ExecutionFillRecord,
    OMSOrderStateRecord,
    RiskDecisionRecord,
)
from funding_arbitrage.database.repositories.directional_paper import (
    DirectionalPaperEventProjection,
    load_advanced_paper_positions,
    save_directional_paper_page,
)
from funding_arbitrage.domain.decisions import (
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OptionQuoteSnapshot,
    OptionRight,
    Side,
    TradeTick,
    TradingMode,
)
from funding_arbitrage.execution.advanced_paper import (
    AdvancedPaperStatus,
    AdvancedStrategyPaperBroker,
)
from funding_arbitrage.services.strategy_execution import (
    AdvancedStrategyExecutionPlanner,
    InstrumentExecutionQuote,
    build_strategy_execution_snapshot,
)

NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)


def _instrument(venue: str) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _book(
    instrument: InstrumentKey,
    timestamp: datetime,
    *,
    bid: str = "100",
    ask: str = "101",
    quantity: str = "10",
) -> BookSnapshot:
    return BookSnapshot(
        instrument=instrument,
        bids=(BookLevel(price=Decimal(bid), quantity=Decimal(quantity)),),
        asks=(BookLevel(price=Decimal(ask), quantity=Decimal(quantity)),),
        sequence=int((timestamp - NOW).total_seconds() * 1000) + 1,
        exchange_timestamp=timestamp,
    )


def _intent(*, market_making: bool = False) -> SignalIntent:
    primary = _instrument("BYBIT")
    hedge = primary if market_making else _instrument("GATE")
    legs = (
        SignalLeg(
            instrument=primary,
            side=Side.BUY if market_making else Side.SELL,
            preferred_limit_price=Decimal("100") if market_making else None,
            post_only=market_making,
        ),
        SignalLeg(
            instrument=hedge,
            side=Side.SELL if market_making else Side.BUY,
            preferred_limit_price=Decimal("101") if market_making else None,
            post_only=market_making,
        ),
    )
    return SignalIntent(
        signal_id="mm-signal" if market_making else "stat-arb-signal",
        strategy_id="mm-v1" if market_making else "stat-arb-v1",
        mode=TradingMode.PAPER,
        signal_type=(
            SignalType.PASSIVE_MARKET_MAKING
            if market_making
            else SignalType.CROSS_EXCHANGE_STAT_ARB
        ),
        primary_instrument=primary,
        side=legs[0].side,
        legs=legs,
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        expected_holding_seconds=30,
        expected_move_bps=Decimal("20"),
        estimated_cost_bps=Decimal("5"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=5),
    )


def _authority(*, market_making: bool = False):
    intent = _intent(market_making=market_making)
    decision = RiskDecision(
        signal_id=intent.signal_id,
        decision_id="risk-mm" if market_making else "risk-stat-arb",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("10"),
        approved_quantity=Decimal("1"),
        approved_notional=Decimal("100"),
        max_slippage_bps=Decimal("20"),
        max_execution_seconds=5,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    instruments = {
        leg.instrument.canonical_id: leg.instrument for leg in intent.legs
    }
    quotes = tuple(
        InstrumentExecutionQuote(
            instrument=instrument,
            book=_book(instrument, NOW),
            data_quality=DataQuality.VALID,
            quantity_step=Decimal("0.001"),
            price_tick=Decimal("0.01"),
            minimum_quantity=Decimal("0.001"),
            maker_fee_bps=Decimal("1"),
            taker_fee_bps=Decimal("5"),
        )
        for instrument in instruments.values()
    )
    snapshot = build_strategy_execution_snapshot(
        intent=intent,
        source_event_id="source-event",
        captured_at=NOW,
        quotes=quotes,
    )
    plan = AdvancedStrategyExecutionPlanner().build(intent, decision, snapshot, NOW)
    return intent, decision, snapshot, plan


def _broker(*, participation: str = "1") -> AdvancedStrategyPaperBroker:
    policy = FillModelPolicy(
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("5"),
        order_latency_ms=0,
        cancel_latency_ms=0,
        maximum_participation_rate=Decimal(participation),
        passive_fill_ratio=Decimal("1"),
        impact_coefficient_bps=Decimal("0"),
    )
    return AdvancedStrategyPaperBroker(
        {"BYBIT": policy, "GATE": policy},
        simulation_version="advanced-paper-test-v1",
    )


def _event(
    payload: BookSnapshot | TradeTick | OptionQuoteSnapshot,
    sequence: int,
) -> EventEnvelope:
    timestamp = payload.exchange_timestamp
    return EventEnvelope(
        kind=(
            EventKind.BOOK_SNAPSHOT
            if isinstance(payload, BookSnapshot)
            else (
                EventKind.OPTION_QUOTE_SNAPSHOT
                if isinstance(payload, OptionQuoteSnapshot)
                else EventKind.TRADE_TICK
            )
        ),
        metadata=EventMetadata(
            event_id=f"advanced-paper-event-{sequence}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=1),
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            native_sequence=sequence,
            source=f"test:{payload.instrument.venue.lower()}",
            correlation_id="advanced-paper-test",
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=payload,
    )


def test_multi_leg_paper_entry_time_exit_and_restore_are_deterministic() -> None:
    intent, decision, snapshot, plan = _authority()
    broker = _broker()
    update = broker.submit_authorized(plan, intent, decision, snapshot)
    assert update is not None
    assert broker.submit_authorized(plan, intent, decision, snapshot) is None

    broker.advance(
        _event(_book(intent.legs[0].instrument, NOW + timedelta(seconds=1)), 1)
    )
    opened_updates = broker.advance(
        _event(_book(intent.legs[1].instrument, NOW + timedelta(seconds=2)), 2)
    )
    assert opened_updates[-1].position.status is AdvancedPaperStatus.OPEN

    broker.advance(
        _event(_book(intent.legs[0].instrument, NOW + timedelta(seconds=31)), 3)
    )
    closed_updates = broker.advance(
        _event(_book(intent.legs[1].instrument, NOW + timedelta(seconds=32)), 4)
    )
    closed = closed_updates[-1].position
    assert closed.status is AdvancedPaperStatus.CLOSED
    assert all(closed.net_quantity(index) == 0 for index in (0, 1))
    assert closed.total_fee > 0
    assert closed.net_pnl < 0
    assert broker.gross_exposure == 0

    restored = _broker()
    restored.restore(broker.positions)
    assert restored.positions == broker.positions
    assert restored.total_net_pnl == broker.total_net_pnl


def test_partial_entry_is_cancelled_and_compensated_without_orphan_exposure() -> None:
    intent, decision, snapshot, plan = _authority()
    broker = _broker(participation="0.5")
    broker.submit_authorized(plan, intent, decision, snapshot)

    first = broker.advance(
        _event(
            _book(
                intent.legs[0].instrument,
                NOW + timedelta(seconds=1),
                quantity="1",
            ),
            1,
        )
    )[-1].position
    assert first.entry_order(0).filled_quantity == Decimal("0.5")
    assert first.entry_order(0).state is SimulatedOrderState.PARTIALLY_FILLED

    compensated = broker.advance(
        _event(
            _book(
                intent.legs[0].instrument,
                NOW + timedelta(seconds=6),
                quantity="1",
            ),
            2,
        )
    )[-1].position
    assert compensated.status is AdvancedPaperStatus.COMPENSATED
    assert compensated.net_quantity(0) == 0
    assert compensated.net_quantity(1) == 0
    assert compensated.exit_reason is not None
    assert compensated.total_fee > 0
    assert broker.gross_exposure == 0


def test_post_only_quote_requires_trade_evidence_before_maker_fill() -> None:
    intent, decision, snapshot, plan = _authority(market_making=True)
    broker = _broker()
    broker.submit_authorized(plan, intent, decision, snapshot)

    book_only = broker.advance(
        _event(_book(intent.primary_instrument, NOW + timedelta(seconds=1)), 1)
    )[-1].position
    assert all(order.filled_quantity == 0 for order in book_only.entry_orders)

    trade = TradeTick(
        instrument=intent.primary_instrument,
        trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("10"),
        aggressor_side=Side.SELL,
        exchange_timestamp=NOW + timedelta(seconds=2),
    )
    traded = broker.advance(_event(trade, 2))[-1].position
    assert traded.entry_order(0).filled_quantity == Decimal("1")
    assert traded.entry_order(0).fills[0].liquidity_role is LiquidityRole.MAKER
    assert traded.entry_order(1).filled_quantity == 0

    compensated = broker.advance(
        _event(_book(intent.primary_instrument, NOW + timedelta(seconds=6)), 3)
    )[-1].position
    assert compensated.status is AdvancedPaperStatus.COMPENSATED
    assert all(compensated.net_quantity(index) == 0 for index in (0, 1))


def test_advanced_paper_rejects_tampered_post_only_instruction() -> None:
    intent, decision, snapshot, plan = _authority(market_making=True)
    instructions = (
        plan.instructions[0].model_copy(update={"post_only": False}),
        *plan.instructions[1:],
    )
    tampered = plan.model_copy(update={"instructions": instructions})

    with pytest.raises(
        ValueError,
        match="advanced paper instruction and intent mismatch",
    ):
        _broker().submit_authorized(tampered, intent, decision, snapshot)


def test_option_contract_multiplier_scales_fills_fees_pnl_and_exposure() -> None:
    expiry = NOW + timedelta(days=30)

    def option_instrument(right: OptionRight) -> InstrumentKey:
        code = "C" if right is OptionRight.CALL else "P"
        return InstrumentKey(
            venue="BYBIT",
            exchange_symbol=f"BTC-30SEP26-100-{code}",
            base_asset="BTC",
            quote_asset="USDT",
            settlement_asset="USDT",
            instrument_type=InstrumentType.OPTION,
            expiry=expiry,
            strike_price=Decimal("100"),
            option_right=right,
        )

    call = option_instrument(OptionRight.CALL)
    put = option_instrument(OptionRight.PUT)
    intent = SignalIntent(
        signal_id="option-straddle-signal",
        strategy_id="options-volatility-v1",
        mode=TradingMode.PAPER,
        signal_type=SignalType.OPTIONS_VOLATILITY,
        primary_instrument=call,
        side=Side.BUY,
        legs=(
            SignalLeg(instrument=call, side=Side.BUY),
            SignalLeg(instrument=put, side=Side.BUY),
        ),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        expected_holding_seconds=30,
        expected_move_bps=Decimal("100"),
        estimated_cost_bps=Decimal("10"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=120),
    )
    decision = RiskDecision(
        signal_id=intent.signal_id,
        decision_id="risk-option-straddle",
        decided_at=NOW,
        approved=True,
        approved_risk_usdt=Decimal("1"),
        approved_quantity=Decimal("1"),
        approved_notional=Decimal("1"),
        max_slippage_bps=Decimal("20"),
        max_execution_seconds=120,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )
    option_books = {
        call.canonical_id: _book(call, NOW, bid="5", ask="5.01"),
        put.canonical_id: _book(put, NOW, bid="4", ask="4.01"),
    }
    snapshot = build_strategy_execution_snapshot(
        intent=intent,
        source_event_id="source-option-event",
        captured_at=NOW,
        quotes=tuple(
            InstrumentExecutionQuote(
                instrument=instrument,
                book=option_books[instrument.canonical_id],
                data_quality=DataQuality.VALID,
                quantity_step=Decimal("1"),
                price_tick=Decimal("0.01"),
                minimum_quantity=Decimal("1"),
                contract_multiplier=Decimal("0.1"),
                maker_fee_bps=Decimal("0"),
                taker_fee_bps=Decimal("3"),
                option_underlying_price=Decimal("100"),
                option_fee_cap_rate=Decimal("0.07"),
            )
            for instrument in (call, put)
        ),
    )
    plan = AdvancedStrategyExecutionPlanner().build(
        intent,
        decision,
        snapshot,
        NOW,
    )
    broker = _broker()
    submitted = broker.submit_authorized(plan, intent, decision, snapshot)
    assert submitted is not None

    def option_event(
        instrument: InstrumentKey,
        timestamp: datetime,
        sequence: int,
        *,
        bid: str,
        ask: str,
        underlying: str = "100",
    ) -> EventEnvelope:
        quote = OptionQuoteSnapshot(
            instrument=instrument,
            underlying_price=Decimal(underlying),
            bid_price=Decimal(bid),
            bid_quantity=Decimal("10"),
            ask_price=Decimal(ask),
            ask_quantity=Decimal("10"),
            mark_implied_volatility=Decimal("0.2"),
            contract_multiplier=Decimal("0.1"),
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("1"),
            minimum_quantity=Decimal("1"),
            exchange_timestamp=timestamp,
        )
        return _event(quote, sequence)

    broker.advance(
        option_event(call, NOW + timedelta(seconds=1), 1, bid="5", ask="5.01")
    )
    opened = broker.advance(
        option_event(put, NOW + timedelta(seconds=2), 2, bid="4", ask="4.01")
    )[-1].position

    assert opened.status is AdvancedPaperStatus.OPEN
    assert opened.entry_order(0).fills[0].notional == Decimal("0.501")
    assert opened.entry_order(1).fills[0].notional == Decimal("0.401")
    assert opened.entry_order(0).fills[0].fee == Decimal("0.00300")
    assert opened.entry_order(1).fills[0].fee == Decimal("0.00300")
    assert opened.total_fee == Decimal("0.00600")
    assert opened.reserved_notional < Decimal("1")
    assert broker.gross_exposure == Decimal("0.9")
    assert broker.net_delta() == Decimal("20")

    broker.advance(
        option_event(
            call,
            NOW + timedelta(seconds=31),
            3,
            bid="5",
            ask="5.01",
            underlying="200",
        )
    )
    closed = broker.advance(
        option_event(
            put,
            NOW + timedelta(seconds=32),
            4,
            bid="4",
            ask="4.01",
            underlying="200",
        )
    )[-1].position

    assert closed.status is AdvancedPaperStatus.CLOSED
    assert closed.realized_gross_pnl == Decimal("-0.002")
    assert closed.total_fee == Decimal("0.01800")
    assert closed.net_pnl == Decimal("-0.02000")
    assert broker.gross_exposure == 0


@pytest.mark.asyncio
async def test_advanced_paper_projection_persists_orders_fills_and_restores() -> None:
    intent, decision, snapshot, plan = _authority()
    broker = _broker()
    submitted = broker.submit_authorized(plan, intent, decision, snapshot)
    assert submitted is not None
    event = _event(
        _book(intent.legs[0].instrument, NOW + timedelta(seconds=1)),
        1,
    )
    advanced = broker.advance(event)

    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    async with factory() as session:
        session.add(
            RiskDecisionRecord(
                decision_id=decision.decision_id,
                signal_id=decision.signal_id,
                approved=True,
                rejection_reason=None,
                approved_risk_usdt=decision.approved_risk_usdt,
                approved_quantity=decision.approved_quantity,
                approved_notional=decision.approved_notional,
                decided_at=decision.decided_at,
                payload=decision.model_dump(mode="json"),
            )
        )
        await session.commit()
        await save_directional_paper_page(
            session,
            (
                DirectionalPaperEventProjection(
                    event=event,
                    updates=(),
                    event_row_id=1,
                    advanced_updates=(submitted, *advanced),
                ),
            ),
            consumer_name="advanced-paper-test",
        )
        restored = await load_advanced_paper_positions(
            session,
            simulation_version=broker.simulation_version,
        )
        order_count = await session.scalar(select(func.count(OMSOrderStateRecord.id)))
        fill_count = await session.scalar(select(func.count(ExecutionFillRecord.id)))
    await database.dispose()

    assert restored == broker.positions
    assert order_count == 2
    assert fill_count == 1
