import asyncio
import gzip
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import funding_arbitrage.backtest.historical_replay as historical_replay_module
from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.historical_replay import (
    HistoricalDataset,
    HistoricalMarketReplay,
    _funding_points,
    _select_quote,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    FundingHistoryRecord,
    MarketCandleRecord,
    PaperFundingPaymentRecord,
)
from funding_arbitrage.database.repositories.market_data import (
    save_funding_history,
    save_paper_funding_payment,
)
from funding_arbitrage.exchanges.base.exceptions import RateLimitError
from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    Ticker,
)
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.historical import (
    HistoricalBackfill,
    _rank_research_assets,
)
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.risk.engine import RiskEngine


def test_candle_rejects_invalid_ohlc() -> None:
    with pytest.raises(ValueError):
        Candle(
            exchange="x",
            symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            interval_minutes=60,
            open_time=datetime(2026, 1, 1, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 1, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("99"),
            low=Decimal("98"),
            close=Decimal("100"),
            volume=Decimal("1"),
        )


def test_historical_baseline_does_not_fall_back_below_fixed_size() -> None:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    opportunity = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="COTI",
        venue_a="gate",
        venue_b="bybit",
        symbol_a="COTI_USDT",
        symbol_b="COTIUSDT",
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("0.011"),
        price_b=Decimal("0.011"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.002"),
        expected_holding_hours=Decimal("8"),
        net_apr=Decimal("0.2"),
        available_liquidity=Decimal("1000"),
        risk_score=Decimal("20"),
        size_quotes=[
            SizeQuote(
                capital=Decimal("100"),
                gross_profit=Decimal("1"),
                net_profit=Decimal("0.25"),
                net_return_percent=Decimal("0.0025"),
                net_apr=Decimal("0.2"),
                costs=CostBreakdown(
                    entry_fees=Decimal("0.1"),
                    exit_fees=Decimal("0.1"),
                    entry_spread=Decimal("0.1"),
                    exit_spread=Decimal("0.1"),
                    entry_slippage=Decimal("0.1"),
                    exit_slippage=Decimal("0.1"),
                    borrowing_cost=Decimal("0"),
                    network_cost=Decimal("0"),
                ),
            )
        ],
    )

    selected = _select_quote(
        opportunity,
        "baseline",
        Decimal("15000"),
        Decimal("15000"),
        {},
        MarketSnapshot([], [], [], {}, timestamp),
        timestamp,
        RiskEngine(),
        Settings(paper_position_size_usd=Decimal("250")),
        (),
    )

    assert selected is None


def test_portable_replay_dataset_preserves_digest_and_types(tmp_path: Path) -> None:
    files = {
        "candles.csv.gz": (
            "exchange,symbol,instrument_type,interval_minutes,open_time,close_time,"
            "open,high,low,close,volume,is_closed\n"
            "gate,BTC_USDT,PERPETUAL,60,2026-01-01 00:00:00+00,"
            "2026-01-01 01:00:00+00,100,110,90,105,12,t\n"
        ),
        "funding.csv.gz": (
            "exchange,symbol,funding_rate,funding_timestamp,mark_price\n"
            "gate,BTC_USDT,0.0001,2026-01-01 00:30:00+00,105\n"
        ),
        "instruments.csv.gz": (
            "exchange,exchange_symbol,base_asset,quote_asset,instrument_type,"
            "settlement_asset,contract_size,tick_size,step_size,min_order_size,"
            "funding_interval,expiry,is_active\n"
            "gate,BTC_USDT,BTC,USDT,PERPETUAL,USDT,1,0.1,0.001,0.001,8,,t\n"
        ),
    }
    for name, payload in files.items():
        with gzip.open(tmp_path / name, mode="wt", encoding="utf-8", newline="") as file:
            file.write(payload)

    replay = HistoricalMarketReplay()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)
    first = replay.load_portable(tmp_path, start, end)
    second = replay.load_portable(tmp_path, start, end)

    assert first.dataset_version == second.dataset_version
    assert first.coverage["candle_rows"] == 1
    assert first.coverage["funding_events"] == 1
    assert first.candles[0].close == Decimal("105")
    assert first.funding[0].funding_timestamp.tzinfo is UTC
    assert first.instruments[0].instrument_type is InstrumentType.PERPETUAL


def test_all_exchange_candle_contracts_are_normalized() -> None:
    timestamp_ms = 1735689600000
    timestamp_s = timestamp_ms // 1000
    rows = [
        (
            BybitPublicAdapter(),
            [timestamp_ms, "100", "110", "90", "105", "12"],
            InstrumentType.PERPETUAL,
        ),
        (
            BinancePublicAdapter(),
            [timestamp_ms, "100", "110", "90", "105", "12", timestamp_ms + 3600000],
            InstrumentType.SPOT,
        ),
        (
            OkxPublicAdapter(),
            [timestamp_ms, "100", "110", "90", "105", "12", "0", "0", "1"],
            InstrumentType.PERPETUAL,
        ),
        (
            GatePublicAdapter(),
            [timestamp_s, "1260", "105", "110", "90", "100", "12", "true"],
            InstrumentType.SPOT,
        ),
        (
            HyperliquidPublicAdapter(),
            {
                "t": timestamp_ms,
                "T": timestamp_ms + 3600000,
                "o": "100",
                "h": "110",
                "l": "90",
                "c": "105",
                "v": "12",
            },
            InstrumentType.PERPETUAL,
        ),
    ]
    for adapter, row, instrument_type in rows:
        candle = adapter._parse_candle(row, "BTCUSDT", instrument_type, 60)
        assert candle.open == Decimal("100")
        assert candle.high == Decimal("110")
        assert candle.low == Decimal("90")
        assert candle.close == Decimal("105")
        assert candle.volume == Decimal("12")
        assert candle.open_time.tzinfo is UTC


@pytest.mark.asyncio
async def test_binance_hourly_candles_use_exchange_interval_alias() -> None:
    timestamp_ms = 1735689600000

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["interval"] == "1h"
        return httpx.Response(
            200,
            json=[
                [
                    timestamp_ms,
                    "100",
                    "110",
                    "90",
                    "105",
                    "12",
                    timestamp_ms + 3600000,
                ]
            ],
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = BinancePublicAdapter(
        spot_base_url="https://test.invalid", http_client=client
    )
    candles = await adapter.get_candles(
        "BTCUSDT",
        InstrumentType.SPOT,
        datetime.fromtimestamp(timestamp_ms / 1000, UTC),
        datetime.fromtimestamp(timestamp_ms / 1000, UTC) + timedelta(hours=2),
        60,
    )
    await client.aclose()

    assert len(candles) == 1


def test_historical_selection_prefers_usdt_and_distinct_types() -> None:
    def instrument(quote: str, instrument_type: InstrumentType) -> NormalizedInstrument:
        return NormalizedInstrument(
            exchange="x",
            exchange_symbol=f"BTC{quote}{instrument_type.value}",
            base_asset="BTC",
            quote_asset=quote,
            instrument_type=instrument_type,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )

    selected = HistoricalBackfill._select_instruments(
        [
            instrument("USDC", InstrumentType.SPOT),
            instrument("USDT", InstrumentType.SPOT),
            instrument("USDT", InstrumentType.PERPETUAL),
        ],
        {"BTC"},
    )
    assert {(item.quote_asset, item.instrument_type) for item in selected} == {
        ("USDT", InstrumentType.SPOT),
        ("USDT", InstrumentType.PERPETUAL),
    }


@pytest.mark.asyncio
async def test_historical_backfill_retries_rate_limits(monkeypatch) -> None:
    calls = 0

    async def collect(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RateLimitError("retry")
        return [], []

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(HistoricalBackfill, "_collect_instrument", collect)
    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    instrument = NormalizedInstrument(
        exchange="okx",
        exchange_symbol="BTC-USDT-SWAP",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
    )

    candles, funding = await HistoricalBackfill._collect_instrument_with_retry(
        asyncio.Semaphore(1),
        object(),
        instrument,
        datetime(2026, 7, 1, tzinfo=UTC),
        datetime(2026, 8, 1, tzinfo=UTC),
        60,
    )

    assert candles == []
    assert funding == []
    assert calls == 3


def test_research_universe_combines_core_funding_and_liquidity() -> None:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    instruments: list[NormalizedInstrument] = []
    tickers: list[Ticker] = []
    funding: list[FundingSnapshot] = []
    for asset, funding_rate, volume in (
        ("BTC", "0.0001", "100000000"),
        ("TUT", "0.01", "1000000"),
        ("LOW", "0.0002", "100"),
    ):
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL):
            symbol = f"{asset}USDT"
            instruments.append(
                NormalizedInstrument(
                    exchange="gate",
                    exchange_symbol=symbol,
                    base_asset=asset,
                    quote_asset="USDT",
                    instrument_type=instrument_type,
                    tick_size=Decimal("0.01"),
                    step_size=Decimal("0.01"),
                    min_order_size=Decimal("0.01"),
                )
            )
            tickers.append(
                Ticker(
                    exchange="gate",
                    symbol=symbol,
                    instrument_type=instrument_type,
                    last_price=Decimal("1"),
                    volume_24h=Decimal(volume),
                    timestamp=timestamp,
                )
            )
        funding.append(
            FundingSnapshot(
                exchange="gate",
                symbol=f"{asset}USDT",
                funding_rate=Decimal(funding_rate),
                funding_interval_hours=Decimal("8"),
                timestamp=timestamp,
            )
        )

    ranked = _rank_research_assets(instruments, tickers, funding, limit=2)

    assert ranked == ("BTC", "TUT")


@pytest.mark.asyncio
async def test_funding_history_batch_deduplicates_before_flush(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    point = FundingHistoryPoint(
        exchange="hyperliquid",
        symbol="BTC",
        funding_rate=Decimal("0.0001"),
        funding_timestamp=datetime(2026, 8, 1, 15, tzinfo=UTC),
    )
    async with session_factory() as session:
        await save_funding_history(session, [point, point])

    async with session_factory() as session:
        records = list(
            (
                await session.execute(
                    select(FundingHistoryRecord).where(
                        FundingHistoryRecord.exchange == point.exchange,
                        FundingHistoryRecord.symbol == point.symbol,
                        FundingHistoryRecord.funding_timestamp
                        == point.funding_timestamp,
                    )
                )
            ).scalars()
        )
    assert len(records) == 1


@pytest.mark.asyncio
async def test_live_funding_payment_commits_raw_event_in_same_transaction(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    event = FundingHistoryPoint(
        exchange="bybit",
        symbol="COTIUSDT",
        funding_rate=Decimal("-0.00186723"),
        funding_timestamp=datetime(2026, 8, 14, 8, tzinfo=UTC),
    )
    funding = FundingSnapshot(
        exchange=event.exchange,
        symbol=event.symbol,
        funding_rate=event.funding_rate,
        funding_interval_hours=Decimal("8"),
        timestamp=event.funding_timestamp,
    )
    async with session_factory() as session:
        payment = await save_paper_funding_payment(
            session,
            "position-id",
            funding,
            Decimal("250"),
            Decimal("-0.4668075"),
            history_event=event,
        )

    async with session_factory() as session:
        raw_events = list(
            (
                await session.execute(
                    select(FundingHistoryRecord).where(
                        FundingHistoryRecord.exchange == event.exchange,
                        FundingHistoryRecord.symbol == event.symbol,
                        FundingHistoryRecord.funding_timestamp
                        == event.funding_timestamp,
                    )
                )
            ).scalars()
        )
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == "position-id"
                    )
                )
            ).scalars()
        )
    assert len(raw_events) == 1
    assert len(payments) == 1
    assert payment.id == payments[0].id


def test_replay_dataset_excludes_funding_outside_candle_universe() -> None:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    rows = [
        FundingHistoryRecord(
            exchange="bybit",
            symbol=symbol,
            funding_rate=Decimal("0.001"),
            funding_timestamp=timestamp,
        )
        for symbol in ("BTCUSDT", "DOGEUSDT")
    ]

    points = _funding_points(rows, {("bybit", "BTCUSDT")})

    assert [point.symbol for point in points] == ["BTCUSDT"]


def test_historical_strategy_replay_is_deterministic_and_no_lookahead() -> None:
    start = datetime(2026, 1, 2, tzinfo=UTC)
    instruments = [
        NormalizedInstrument(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            funding_interval=1 if instrument_type is InstrumentType.PERPETUAL else None,
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    candles: list[MarketCandleRecord] = []
    for hour in range(36):
        close_time = start + timedelta(hours=hour)
        for instrument_type, price in (
            (InstrumentType.SPOT, Decimal("100")),
            (InstrumentType.PERPETUAL, Decimal("100.1")),
        ):
            candles.append(
                MarketCandleRecord(
                    exchange="bybit",
                    symbol="BTCUSDT",
                    instrument_type=instrument_type.value,
                    interval_minutes=60,
                    open_time=close_time - timedelta(hours=1),
                    # Some venues encode the inclusive close one millisecond early.
                    close_time=close_time - timedelta(milliseconds=1),
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("10000"),
                    is_closed=True,
                )
            )
    funding = [
        FundingHistoryPoint(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.005"),
            funding_timestamp=start + timedelta(hours=hour),
        )
        for hour in range(-24, 37)
    ]
    dataset = HistoricalDataset(
        instruments=instruments,
        candles=candles,
        funding=funding,
        dataset_version="fixture-v1",
        coverage={},
    )
    settings = Settings(
        SCANNER_MINIMUM_NET_APR="0",
        SCANNER_MINIMUM_LIQUIDITY_SCORE="0",
        SCANNER_MINIMUM_FUNDING_SAMPLES=20,
        PAPER_MIN_SETTLEMENT_COST_COVERAGE="1.25",
    )
    replay = HistoricalMarketReplay()
    first = replay.simulate(dataset, "candidate", Decimal("15000"), settings)
    second = replay.simulate(dataset, "candidate", Decimal("15000"), settings)

    assert first.oms_event_count == second.oms_event_count
    assert first.oms_terminal_order_count == second.oms_terminal_order_count
    assert first.oms_terminal_order_count > 0
    assert first.oms_event_count >= first.oms_terminal_order_count * 3
    assert first.position_count > 0
    assert first.attribution["strategy"]
    entry_fills = [
        event
        for event in first.events
        if event.event_type == "fill" and event.event_id.startswith("candidate:entry:")
    ]
    assert entry_fills
    assert all(event.status == "FILLED" for event in entry_fills)
    assert all(event.fill_count == 2 for event in entry_fills)
    assert all(event.requested_notional is not None for event in entry_fills)
    assert all(event.latency_ms == settings.backtest_order_latency_ms for event in entry_fills)
    assert max(event.notional for event in entry_fills) <= Decimal("3000")
    event_ids = [event.event_id for event in first.events]
    assert len(event_ids) == len(set(event_ids))
    assert [event.model_dump(mode="json") for event in first.events] == [
        event.model_dump(mode="json") for event in second.events
    ]
    assert first.observation_start == start
    assert first.observation_end == start + timedelta(hours=35)
    assert first.snapshot_timestamps == tuple(
        start + timedelta(hours=hour) for hour in range(36)
    )
    assert first.snapshot_pnl_curve == second.snapshot_pnl_curve
    assert first.max_snapshot_gap_seconds == Decimal("3600")
    replay_result = BacktestEngine().run(
        first.events,
        Decimal("15000"),
        {"profile": "candidate"},
        first.dataset_version,
    )
    assert first.snapshot_pnl_delta == replay_result.metrics.net_profit_after_costs
    first_open = next(event for event in first.events if event.event_type == "position")
    assert first_open.timestamp >= start

    cutoff = start + timedelta(hours=12)
    changed_future = dataset.__class__(
        instruments=dataset.instruments,
        candles=dataset.candles,
        funding=[
            point.model_copy(update={"funding_rate": Decimal("-0.5")})
            if point.funding_timestamp >= cutoff
            else point
            for point in dataset.funding
        ],
        dataset_version="fixture-v2",
        coverage={},
    )
    future_changed = replay.simulate(
        changed_future, "candidate", Decimal("15000"), settings
    )
    assert [
        event.model_dump(mode="json") for event in first.events if event.timestamp < cutoff
    ] == [
        event.model_dump(mode="json")
        for event in future_changed.events
        if event.timestamp < cutoff
    ]


def test_historical_snapshots_mark_open_positions_and_reconcile_final_pnl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 8, 1, tzinfo=UTC)
    costs = CostBreakdown(
        entry_fees=Decimal("1"),
        exit_fees=Decimal("1"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("12"),
        network_cost=Decimal("0"),
    )
    opportunity = Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="BTC",
        venue_a="bybit",
        venue_b="bybit",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type=InstrumentType.SPOT,
        leg_b_type=InstrumentType.PERPETUAL,
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        gross_edge=Decimal("0.02"),
        net_edge=Decimal("0.01"),
        expected_holding_hours=Decimal("1"),
        net_apr=Decimal("100"),
        available_liquidity=Decimal("100000"),
        risk_score=Decimal("1"),
        size_quotes=[
            SizeQuote(
                capital=Decimal("250"),
                gross_profit=Decimal("5"),
                net_profit=Decimal("3"),
                net_return_percent=Decimal("0.012"),
                net_apr=Decimal("100"),
                costs=costs,
            )
        ],
    )

    class FixedEngine:
        def scan(self, snapshot: MarketSnapshot) -> list[Opportunity]:
            return [opportunity] if snapshot.captured_at == start else []

    monkeypatch.setattr(
        historical_replay_module,
        "_opportunity_engine",
        lambda _settings, _profile: FixedEngine(),
    )
    instruments = [
        NormalizedInstrument(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    candles: list[MarketCandleRecord] = []
    for index, spot_price in enumerate((Decimal("100"), Decimal("90"), Decimal("80"))):
        timestamp = start + timedelta(minutes=5 * index)
        for instrument_type, price in (
            (InstrumentType.SPOT, spot_price),
            (InstrumentType.PERPETUAL, Decimal("100")),
        ):
            candles.append(
                MarketCandleRecord(
                    exchange="bybit",
                    symbol="BTCUSDT",
                    instrument_type=instrument_type.value,
                    interval_minutes=5,
                    open_time=timestamp - timedelta(minutes=5),
                    close_time=timestamp,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("10000"),
                    is_closed=True,
                )
            )
    dataset = HistoricalDataset(
        instruments=instruments,
        candles=candles,
        funding=[],
        dataset_version="m2m-fixture-v1",
        coverage={},
    )

    replay = HistoricalMarketReplay().simulate(
        dataset,
        "baseline",
        Decimal("1000"),
        Settings(
            PAPER_POSITION_SIZE_USD="250",
            BACKTEST_FILL_MODEL_ENABLED=False,
        ),
    )
    event_result = BacktestEngine().run(
        replay.events,
        Decimal("1000"),
        {"profile": "baseline"},
        replay.dataset_version,
    )

    assert replay.snapshot_timestamps == tuple(
        start + timedelta(minutes=5 * index) for index in range(3)
    )
    assert replay.snapshot_pnl_curve[0][1] == Decimal("-1")
    # The interim mark includes +$25 price PnL and $1 of accrued borrow.
    assert replay.snapshot_pnl_curve[1][1] == Decimal("23")
    assert replay.snapshot_pnl_curve[-1][1] == Decimal("46")
    assert replay.snapshot_pnl_delta == event_result.metrics.net_profit_after_costs
    assert replay.max_snapshot_gap_seconds == Decimal("300")

    comparison = compare_paper_datasets(replay, replay, Decimal("1000"))
    assert comparison["snapshot_risk"]["source"] == "portfolio_snapshots"  # type: ignore[index]
    checks = comparison["checks"]
    assert isinstance(checks, dict)
    assert checks["snapshot_evidence_present"] is True
    assert checks["exact_shared_timestamps"] is True
    assert checks["maximum_gap_within_5_minutes"] is True
