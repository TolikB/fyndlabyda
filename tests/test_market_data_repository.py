from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import (
    ExchangeRecord,
    FundingHistoryRecord,
    InstrumentRecord,
    MarketCandleRecord,
    OpportunityRecord,
    OrderBookSnapshotRecord,
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PaperRuntimeIncidentRecord,
    PortfolioSnapshotRecord,
    TickerSnapshotRecord,
)
from funding_arbitrage.database.repositories.market_data import (
    save_candles,
    save_funding_history,
    save_instruments,
    save_market_snapshot,
    save_opportunities,
    save_paper_fill,
    save_paper_funding_payment,
    save_paper_position,
    save_paper_runtime_incident,
    save_portfolio_snapshot,
)
from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.execution.base import FillStatus, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import Opportunity, OpportunityStatus, StrategyName
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot
from funding_arbitrage.portfolio.position import PaperPosition, PositionState

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _instrument(*, active: bool = True) -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        settlement_asset="USDT",
        contract_size=Decimal("0.001"),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
        funding_interval=8,
        is_active=active,
    )


def _ticker() -> Ticker:
    return Ticker(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        last_price=Decimal("60000"),
        mark_price=Decimal("60001"),
        index_price=Decimal("59999"),
        best_bid=Decimal("59999"),
        best_ask=Decimal("60001"),
        volume_24h=Decimal("1000000"),
        open_interest=Decimal("500000"),
        timestamp=NOW,
    )


def _funding() -> FundingSnapshot:
    return FundingSnapshot(
        exchange="bybit",
        symbol="BTCUSDT",
        funding_rate=Decimal("0.0001"),
        funding_interval_hours=Decimal("8"),
        next_funding_time=NOW + timedelta(hours=8),
        mark_price=Decimal("60001"),
        index_price=Decimal("59999"),
        timestamp=NOW,
    )


def _history(*, rate: str = "0.0001") -> FundingHistoryPoint:
    return FundingHistoryPoint(
        exchange="bybit",
        symbol="BTCUSDT",
        funding_rate=Decimal(rate),
        funding_timestamp=NOW,
        mark_price=Decimal("60000"),
    )


def _candle(*, close: str = "101") -> Candle:
    return Candle(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        interval_minutes=1,
        open_time=NOW,
        close_time=NOW + timedelta(minutes=1),
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal(close),
        volume=Decimal("12"),
    )


def _snapshot(*, active: bool = True, rate: str = "0.0001") -> MarketSnapshot:
    book = OrderBook(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        bids=(OrderBookLevel(price=Decimal("59999"), quantity=Decimal("2")),),
        asks=(OrderBookLevel(price=Decimal("60001"), quantity=Decimal("2")),),
        timestamp=NOW,
        sequence=7,
    )
    return MarketSnapshot(
        instruments=[_instrument(active=active)],
        tickers=[_ticker()],
        funding=[_funding()],
        orderbooks={
            ("bybit", "BTCUSDT", InstrumentType.PERPETUAL): book,
        },
        captured_at=NOW,
        funding_history={("bybit", "BTCUSDT"): [_history(rate=rate)]},
    )


def _opportunity(*, status: OpportunityStatus = OpportunityStatus.CONFIRMED) -> Opportunity:
    return Opportunity(
        id="opportunity-1",
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="BTC",
        venue_a="bybit",
        venue_b="gate",
        symbol_a="BTCUSDT",
        symbol_b="BTC_USDT",
        leg_a_type=InstrumentType.PERPETUAL.value,
        leg_b_type=InstrumentType.PERPETUAL.value,
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("60000"),
        price_b=Decimal("60000"),
        gross_edge=Decimal("0.001"),
        net_edge=Decimal("0.0005"),
        expected_holding_hours=Decimal("8"),
        net_apr=Decimal("0.2"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
        status=status,
        created_at=NOW,
    )


def _fill(*, fee: str = "0.1") -> PaperFill:
    return PaperFill(
        fill_id="fill-1",
        client_order_id="client-1",
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("60000"),
        reference_price=Decimal("60000"),
        fee=Decimal(fee),
        spread=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        timestamp=NOW,
    )


async def _count(session: AsyncSession, model: type[object]) -> int:
    return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_market_data_repository_full_idempotent_mapping(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, factory = database
    async with factory() as session:
        await save_market_snapshot(session, _snapshot(), include_history=True)
        await save_market_snapshot(
            session,
            _snapshot(active=False, rate="0.0002"),
            include_history=False,
        )
        await save_funding_history(session, [])
        await save_funding_history(
            session,
            [_history(rate="0.0002"), _history(rate="0.0003")],
        )
        await save_candles(session, [])
        await save_candles(session, [_candle(close="101")])
        await save_candles(session, [_candle(close="100.5")])

        await save_opportunities(session, [_opportunity()])
        await save_opportunities(
            session,
            [_opportunity(status=OpportunityStatus.EXPIRED)],
        )
        await save_portfolio_snapshot(
            session,
            PortfolioSnapshot(
                timestamp=NOW,
                simulation_version="repository-test",
                equity=Decimal("1001"),
                cash=Decimal("900"),
                locked_capital=Decimal("100"),
                total_pnl=Decimal("1"),
                funding_pnl=Decimal("2"),
                fees=Decimal("1"),
                balances={"bybit": Decimal("500"), "gate": Decimal("500")},
            ),
        )
        await save_paper_runtime_incident(
            session,
            ["repository-test", "repository-test", "baseline"],
            "market_data",
            "SyntheticError" * 20,
            NOW,
        )

        position = PaperPosition(
            id="position-1",
            opportunity_id="opportunity-1",
            asset="BTC",
            capital=Decimal("100"),
            strategy=StrategyName.CROSS_EXCHANGE_FUNDING.value,
            simulation_version="repository-test",
            state=PositionState.OPEN,
            leg_a=_fill(),
            opened_at=NOW,
            allocated_venues=("bybit", "gate"),
        )
        await save_paper_position(session, position)
        await save_paper_position(
            session,
            position.model_copy(update={"state": PositionState.CLOSING}),
        )
        await save_paper_fill(session, _fill(fee="0.2"), position.id)
        await save_paper_funding_payment(
            session,
            position.id,
            _funding(),
            Decimal("100"),
            Decimal("0.01"),
            history_event=_history(),
        )

        instrument = await session.scalar(select(InstrumentRecord))
        exchange = await session.scalar(select(ExchangeRecord))
        history = await session.scalar(select(FundingHistoryRecord))
        candle = await session.scalar(select(MarketCandleRecord))
        opportunity = await session.scalar(select(OpportunityRecord))
        fill = await session.scalar(select(PaperFillRecord))
        position_record = await session.scalar(select(PaperPositionRecord))

        assert instrument is not None and instrument.is_active is False
        assert exchange is not None and exchange.status == "ONLINE"
        assert history is not None and history.funding_rate == Decimal("0.0001")
        assert candle is not None and candle.close == Decimal("100.5")
        assert opportunity is not None and opportunity.status == "expired"
        assert fill is not None
        assert abs(fill.fee - Decimal("0.2")) < Decimal("1e-12")
        assert position_record is not None and position_record.state == "CLOSING"
        assert await _count(session, TickerSnapshotRecord) == 2
        assert await _count(session, OrderBookSnapshotRecord) == 2
        assert await _count(session, PortfolioSnapshotRecord) == 1
        assert await _count(session, PaperRuntimeIncidentRecord) == 2
        assert await _count(session, PaperFundingPaymentRecord) == 1


async def test_save_instruments_bulk_upsert_avoids_n_plus_one_queries(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, factory = database
    statements: list[str] = []

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    instruments = [
        _instrument().model_copy(
            update={
                "exchange_symbol": f"BTC{index}USDT",
                "is_active": True,
            }
        )
        for index in range(50)
    ]
    event.listen(engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        async with factory() as session:
            await save_instruments(session, instruments)
            await save_instruments(
                session,
                [instrument.model_copy(update={"is_active": False}) for instrument in instruments],
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record_statement)

    instrument_statements = [
        statement for statement in statements if "instruments" in statement.lower()
    ]
    assert (
        len(
            [
                statement
                for statement in instrument_statements
                if statement.lstrip().upper().startswith("INSERT")
            ]
        )
        == 2
    )
    assert not any(
        statement.lstrip().upper().startswith("SELECT") for statement in instrument_statements
    )
    async with factory() as session:
        assert await _count(session, InstrumentRecord) == 50
        assert (
            await session.scalar(
                select(func.count())
                .select_from(InstrumentRecord)
                .where(InstrumentRecord.is_active.is_(False))
            )
            == 50
        )


async def test_save_instruments_chunks_large_sqlite_batches(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, factory = database
    insert_statements = 0

    def count_inserts(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        nonlocal insert_statements
        if statement.lstrip().upper().startswith("INSERT INTO INSTRUMENTS"):
            insert_statements += 1

    instruments = [
        _instrument().model_copy(update={"exchange_symbol": f"ETH{index}USDT"})
        for index in range(2500)
    ]
    event.listen(engine.sync_engine, "before_cursor_execute", count_inserts)
    try:
        async with factory() as session:
            await save_instruments(session, instruments)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_inserts)

    assert insert_statements == 3
    async with factory() as session:
        assert await _count(session, InstrumentRecord) == 2500


async def test_save_instruments_refreshes_loaded_record_in_same_session(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, factory = database
    async with factory() as session:
        await save_instruments(session, [_instrument(active=True)])
        loaded = await session.scalar(select(InstrumentRecord))
        assert loaded is not None and loaded.is_active is True

        await save_instruments(session, [_instrument(active=False)])

        assert loaded.is_active is False
        assert loaded in session


async def test_duplicate_funding_payment_preserves_immutable_raw_event(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, factory = database
    funding = _funding()
    original_event = _history()
    async with factory() as session:
        original = await save_paper_funding_payment(
            session,
            "immutable-position",
            funding,
            Decimal("100"),
            Decimal("0.01"),
            history_event=original_event,
        )

    conflicting_funding = funding.model_copy(
        update={"funding_rate": Decimal("0.0009")}
    )
    conflicting_event = _history(rate="0.0009")
    async with factory() as session:
        durable = await save_paper_funding_payment(
            session,
            "immutable-position",
            conflicting_funding,
            Decimal("100"),
            Decimal("0.09"),
            history_event=conflicting_event,
        )

    async with factory() as session:
        history = await session.scalar(select(FundingHistoryRecord))
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id
                        == "immutable-position"
                    )
                )
            ).scalars()
        )
    assert Decimal(str(original.funding_rate)) == Decimal("0.0001")
    assert Decimal(str(durable.funding_rate)) == Decimal("0.0001")
    assert Decimal(str(durable.pnl)) == Decimal("0.01")
    assert history is not None
    assert Decimal(str(history.funding_rate)) == Decimal("0.0001")
    assert len(payments) == 1
