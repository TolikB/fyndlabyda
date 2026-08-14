from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import LiveOrderRecord
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.execution.live import LiveTradingExecutor
from funding_arbitrage.execution.reconciliation import LiveReconciler
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    LivePositionState,
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
    VenuePosition,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    OpportunityStatus,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused


class FakeTradingAdapter(TradingAdapter):
    def __init__(self, name: str, outcomes: list[LiveOrderStatus]) -> None:
        self.name = name
        self.outcomes = deque(outcomes)
        self.requests: list[TradingOrderRequest] = []
        self.positions: list[VenuePosition] = []
        self.orders: list[TradingOrderResult] = []

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def preflight(self) -> dict[str, object]:
        return {"exchange": self.name}

    async def fetch_balance(self) -> VenueBalance:
        return VenueBalance(
            exchange=self.name,
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
        )

    async def fetch_positions(self) -> list[VenuePosition]:
        return list(self.positions)

    async def fetch_open_orders(self) -> list[TradingOrderResult]:
        return list(self.orders)

    async def fetch_funding_payments(
        self, since: datetime
    ) -> list[VenueFundingPayment]:
        return []

    async def fetch_taker_fee(
        self, exchange_symbol: str, instrument_type: InstrumentType
    ) -> Decimal:
        return Decimal("0.0005")

    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal:
        return base_quantity.quantize(Decimal("0.001"))

    async def normalize_price(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        price: Decimal,
    ) -> Decimal:
        return price.quantize(Decimal("0.1"))

    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult:
        self.requests.append(request)
        status = self.outcomes.popleft()
        filled = (
            request.base_quantity
            if status in {LiveOrderStatus.FILLED, LiveOrderStatus.PARTIAL}
            else Decimal("0")
        )
        if status is LiveOrderStatus.PARTIAL:
            filled /= 2
        return TradingOrderResult(
            exchange=self.name,
            exchange_order_id=f"{self.name}-{len(self.requests)}",
            client_order_id=request.client_order_id,
            exchange_symbol=request.exchange_symbol,
            instrument_type=request.instrument_type,
            side=request.side,
            requested_base_quantity=request.base_quantity,
            filled_base_quantity=filled,
            average_price=request.limit_price if filled > 0 else None,
            status=status,
            reduce_only=request.reduce_only,
        )

    async def cancel_order(self, order: TradingOrderResult) -> TradingOrderResult:
        return order

    async def configure_derivative(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        leverage: int,
        margin_mode: str,
    ) -> None:
        return None


class SpotFeeTradingAdapter(FakeTradingAdapter):
    """Model a spot buy whose base-asset commission is omitted from the order."""

    async def fetch_balance(self) -> VenueBalance:
        spot_bought = any(
            request.instrument_type is InstrumentType.SPOT and request.side == "BUY"
            for request in self.requests
        )
        spot_sold = any(
            request.instrument_type is InstrumentType.SPOT and request.side == "SELL"
            for request in self.requests
        )
        btc = (
            Decimal("0.0007")
            if spot_sold
            else Decimal("0.9987")
            if spot_bought
            else Decimal("0")
        )
        total = {"USDT": Decimal("1000")}
        free = {"USDT": Decimal("1000")}
        if btc:
            total["BTC"] = btc
            free["BTC"] = btc
        return VenueBalance(exchange=self.name, free=free, total=total)

    async def normalize_base_quantity(
        self,
        exchange_symbol: str,
        instrument_type: InstrumentType,
        base_quantity: Decimal,
    ) -> Decimal:
        if instrument_type is InstrumentType.SPOT:
            return base_quantity // Decimal("0.001") * Decimal("0.001")
        return await super().normalize_base_quantity(
            exchange_symbol, instrument_type, base_quantity
        )


def live_settings(tmp_path: Path) -> Settings:
    kill_switch = str(tmp_path / "LIVE_DISABLED")
    return Settings(
        _env_file=None,
        RUN_MODE="live",
        MARKET_DATA_MODE="live_public",
        EXECUTION_MODE="live",
        LIVE_ARMED=True,
        LIVE_AUTOTRADE=True,
        LIVE_TRADING_CONFIRM="I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        LIVE_VENUES="bybit,gate",
        LIVE_ALLOWED_ASSETS="BTC",
        BYBIT_API_KEY="key",
        BYBIT_API_SECRET="secret",
        GATE_API_KEY="key",
        GATE_API_SECRET="secret",
        LIVE_KILL_SWITCH_FILE=kill_switch,
    )


def opportunity() -> Opportunity:
    return Opportunity(
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
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        funding_a=Decimal("0.01"),
        funding_b=Decimal("0"),
        gross_edge=Decimal("0.02"),
        net_edge=Decimal("0.01"),
        expected_holding_hours=Decimal("1"),
        net_apr=Decimal("10"),
        available_liquidity=Decimal("100000"),
        risk_score=Decimal("10"),
        status=OpportunityStatus.CONFIRMED,
        size_quotes=[
            SizeQuote(
                capital=Decimal("100"),
                gross_profit=Decimal("2"),
                net_profit=Decimal("1"),
                net_return_percent=Decimal("0.01"),
                net_apr=Decimal("10"),
                costs=CostBreakdown(
                    entry_fees=Decimal("0"),
                    exit_fees=Decimal("0"),
                    entry_spread=Decimal("0"),
                    exit_spread=Decimal("0"),
                    entry_slippage=Decimal("0"),
                    exit_slippage=Decimal("0"),
                    borrowing_cost=Decimal("0"),
                    network_cost=Decimal("0"),
                ),
            )
        ],
    )


def spot_perp_opportunity() -> Opportunity:
    value = opportunity().model_copy(deep=True)
    value.strategy = StrategyName.SPOT_PERP
    value.venue_b = "bybit"
    value.symbol_b = "BTCUSDT"
    value.leg_a_type = InstrumentType.SPOT.value
    value.leg_b_type = InstrumentType.PERPETUAL.value
    value.leg_a_side = "BUY"
    value.leg_b_side = "SELL"
    return value


def market_snapshot(*, stale: bool = False) -> MarketSnapshot:
    now = datetime.now(UTC)
    timestamp = now - timedelta(minutes=10) if stale else now
    books = {
        ("bybit", "BTCUSDT", InstrumentType.PERPETUAL): OrderBook(
            exchange="bybit",
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.9"), quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            timestamp=timestamp,
        ),
        ("gate", "BTC_USDT", InstrumentType.PERPETUAL): OrderBook(
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.9"), quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            timestamp=timestamp,
        ),
    }
    tickers = [
        Ticker(
            exchange=exchange,
            symbol=symbol,
            instrument_type=InstrumentType.PERPETUAL,
            last_price=Decimal("100"),
            best_bid=Decimal("99.9"),
            best_ask=Decimal("100"),
            timestamp=timestamp,
        )
        for exchange, symbol in (("bybit", "BTCUSDT"), ("gate", "BTC_USDT"))
    ]
    funding = [
        FundingSnapshot(
            exchange=exchange,
            symbol=symbol,
            funding_rate=Decimal("0.01") if exchange == "bybit" else Decimal("0"),
            funding_interval_hours=Decimal("1"),
            next_funding_time=now + timedelta(minutes=30),
            timestamp=timestamp,
        )
        for exchange, symbol in (("bybit", "BTCUSDT"), ("gate", "BTC_USDT"))
    ]
    return MarketSnapshot(
        instruments=[],
        tickers=tickers,
        funding=funding,
        orderbooks=books,
        captured_at=now,
    )


def spot_perp_snapshot() -> MarketSnapshot:
    snapshot = market_snapshot()
    spot_book = OrderBook(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        bids=(OrderBookLevel(price=Decimal("99.9"), quantity=Decimal("10")),),
        asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
        timestamp=snapshot.captured_at,
    )
    spot_ticker = Ticker(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        last_price=Decimal("100"),
        best_bid=Decimal("99.9"),
        best_ask=Decimal("100"),
        timestamp=snapshot.captured_at,
    )
    return MarketSnapshot(
        instruments=snapshot.instruments,
        tickers=[*snapshot.tickers, spot_ticker],
        funding=snapshot.funding,
        orderbooks={
            **snapshot.orderbooks,
            ("bybit", "BTCUSDT", InstrumentType.SPOT): spot_book,
        },
        captured_at=snapshot.captured_at,
    )


def balances() -> dict[str, VenueBalance]:
    return {
        venue: VenueBalance(
            exchange=venue,
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
        )
        for venue in ("bybit", "gate")
    }


@pytest.mark.asyncio
async def test_live_open_and_close_use_exact_filled_base_quantities(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    bybit = FakeTradingAdapter(
        "bybit", [LiveOrderStatus.FILLED, LiveOrderStatus.FILLED]
    )
    gate = FakeTradingAdapter(
        "gate", [LiveOrderStatus.FILLED, LiveOrderStatus.FILLED]
    )
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
    )

    position = await executor.open_position(
        opportunity(), Decimal("100"), market_snapshot(), "key", balances(), Decimal("0")
    )
    assert position.state is LivePositionState.OPEN
    assert position.leg_a is not None and position.leg_b is not None

    closed = await executor.close_position(position, market_snapshot())

    assert closed.state is LivePositionState.CLOSED
    assert bybit.requests[-1].base_quantity == position.leg_a.filled_base_quantity
    assert gate.requests[-1].base_quantity == position.leg_b.filled_base_quantity
    assert bybit.requests[-1].reduce_only is True
    assert gate.requests[-1].reduce_only is True
    async with factory() as session:
        order_count = await session.scalar(select(func.count(LiveOrderRecord.id)))
    assert order_count == 4


@pytest.mark.asyncio
async def test_spot_base_fee_is_hedged_from_balance_delta_and_dust_is_reconciled(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    adapter = SpotFeeTradingAdapter(
        "bybit",
        [
            LiveOrderStatus.FILLED,
            LiveOrderStatus.FILLED,
            LiveOrderStatus.FILLED,
            LiveOrderStatus.FILLED,
        ],
    )
    risk = LiveRiskController(settings)
    executor = LiveTradingExecutor(settings, {"bybit": adapter}, factory, risk)

    position = await executor.open_position(
        spot_perp_opportunity(),
        Decimal("100"),
        spot_perp_snapshot(),
        "spot-key",
        {"bybit": VenueBalance(
            exchange="bybit",
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
        )},
        Decimal("0"),
    )

    assert position.state is LivePositionState.OPEN
    assert position.leg_a is not None and position.leg_b is not None
    assert position.leg_a.filled_base_quantity == Decimal("0.9987")
    assert position.leg_a.fee == Decimal("0.0013")
    assert position.leg_a.fee_currency == "BTC"
    assert adapter.requests[1].base_quantity == Decimal("0.999")

    closed = await executor.close_position(position, spot_perp_snapshot())

    assert closed.state is LivePositionState.CLOSED
    assert closed.leg_a is not None
    assert closed.leg_a.residual_base_quantity == Decimal("0.0007")
    reconciler = LiveReconciler(settings, {"bybit": adapter}, factory, risk)
    result = await reconciler.reconcile()
    assert result.passed
    assert result.details["tracked_spot_residuals"] == {"bybit:BTC": "0.0007"}


@pytest.mark.asyncio
async def test_second_leg_rejection_unwinds_first_leg(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    bybit = FakeTradingAdapter(
        "bybit", [LiveOrderStatus.FILLED, LiveOrderStatus.FILLED]
    )
    gate = FakeTradingAdapter("gate", [LiveOrderStatus.REJECTED])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
    )

    position = await executor.open_position(
        opportunity(), Decimal("100"), market_snapshot(), "key", balances(), Decimal("0")
    )

    assert position.state is LivePositionState.FAILED
    assert len(bybit.requests) == 2
    assert bybit.requests[1].side == "BUY"
    assert bybit.requests[1].reduce_only is True
    assert bybit.requests[1].base_quantity == bybit.requests[0].base_quantity


@pytest.mark.asyncio
async def test_unknown_order_state_trips_persistent_kill_switch(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    bybit = FakeTradingAdapter("bybit", [LiveOrderStatus.UNKNOWN])
    gate = FakeTradingAdapter("gate", [])
    executor = LiveTradingExecutor(
        settings, {"bybit": bybit, "gate": gate}, factory, risk
    )

    position = await executor.open_position(
        opportunity(), Decimal("100"), market_snapshot(), "key", balances(), Decimal("0")
    )

    assert position.state is LivePositionState.MANUAL_INTERVENTION
    assert risk.paused
    assert risk.kill_switch_path.read_text(encoding="utf-8").strip() == (
        "first_leg_order_state_unknown"
    )
    assert gate.requests == []


@pytest.mark.asyncio
async def test_stale_book_fails_before_any_exchange_submission(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    bybit = FakeTradingAdapter("bybit", [LiveOrderStatus.FILLED])
    gate = FakeTradingAdapter("gate", [LiveOrderStatus.FILLED])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
    )

    position = await executor.open_position(
        opportunity(), Decimal("100"), market_snapshot(stale=True), "key", balances(), Decimal("0")
    )

    assert position.state is LivePositionState.FAILED
    assert bybit.requests == []
    assert gate.requests == []


@pytest.mark.asyncio
async def test_spot_entry_rejects_funds_held_only_in_derivatives_wallet(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    adapter = SpotFeeTradingAdapter("bybit", [LiveOrderStatus.FILLED])
    executor = LiveTradingExecutor(
        settings, {"bybit": adapter}, factory, LiveRiskController(settings)
    )
    aggregate_only = VenueBalance(
        exchange="bybit",
        free={"USDT": Decimal("1000")},
        total={"USDT": Decimal("1000")},
        spot_free={},
        derivative_free_collateral_usd=Decimal("1000"),
    )

    position = await executor.open_position(
        spot_perp_opportunity(),
        Decimal("100"),
        spot_perp_snapshot(),
        "wallet-key",
        {"bybit": aggregate_only},
        Decimal("0"),
    )

    assert position.state is LivePositionState.FAILED
    assert position.failure_reason == "first_leg_rejected:insufficient spot quote inventory"
    assert adapter.requests == []


@pytest.mark.asyncio
async def test_reconciliation_passes_empty_dedicated_accounts_then_trips_on_position(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    reconciler = LiveReconciler(
        settings, {"bybit": bybit, "gate": gate}, factory, risk
    )

    result = await reconciler.reconcile(startup=True)

    assert result.passed
    bybit.positions.append(
        VenuePosition(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            side="SHORT",
            base_quantity=Decimal("1"),
        )
    )
    with pytest.raises(LiveTradingPaused, match="derivative_position_mismatch"):
        await reconciler.reconcile()
    assert risk.paused_reason is not None
    assert "reconciliation" in risk.paused_reason


def test_kill_switch_blocks_entries_but_permits_exact_risk_reduction(
    tmp_path: Path,
) -> None:
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    risk.trip("operator")

    with pytest.raises(LiveTradingPaused, match="operator"):
        risk.assert_can_open(
            order_notional=Decimal("100"),
            open_notional=Decimal("0"),
            free_collateral=Decimal("1000"),
        )
    risk.assert_can_reduce(order_notional=Decimal("100"))


def test_restored_daily_risk_baseline_cannot_be_reset_by_restart(
    tmp_path: Path,
) -> None:
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    now = datetime.now(UTC)
    risk.restore_baselines(
        starting_equity=Decimal("1000"),
        high_water_equity=Decimal("1100"),
        day_start_equity=Decimal("1050"),
        equity_day=now.astimezone(risk.timezone).date(),
    )

    risk.update_equity(Decimal("990"), now)

    assert risk.paused_reason == "daily_loss_limit"
