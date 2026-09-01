from __future__ import annotations

from collections import deque
from dataclasses import replace
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
from funding_arbitrage.database.models import LiveIntentRecord, LiveOrderRecord
from funding_arbitrage.database.repositories.live import (
    create_live_intent,
    save_live_position,
    save_pending_live_order,
)
from funding_arbitrage.domain.decisions import LiveExecutionApproval
from funding_arbitrage.domain.events import InstrumentType as DomainInstrumentType
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.execution.live import LiveExecutionError, LiveTradingExecutor
from funding_arbitrage.execution.reconciliation import LiveReconciler
from funding_arbitrage.execution.trading import (
    LiveLeg,
    LiveOrderStatus,
    LivePosition,
    LivePositionState,
    TradingAdapter,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
    VenuePosition,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.venue_metadata import VenueMetadataRegistry
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    OpportunityStatus,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused
from funding_arbitrage.services.decision_pipeline import FundingLiveDecisionService
from funding_arbitrage.services.live_runner import LiveTradingRunner
from tests.live_security import live_credential_policy_json

PRIVATE_RECONCILIATION_COVERAGE = {
    "bybit": frozenset({"SPOT", "PERPETUAL", "FUTURE"}),
    "gate": frozenset({"SPOT", "PERPETUAL"}),
}


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


class AcceptedThenRaisesAdapter(FakeTradingAdapter):
    """Model an exchange accepting a request before the client times out."""

    async def submit_ioc_order(
        self, request: TradingOrderRequest, timeout_seconds: float
    ) -> TradingOrderResult:
        self.requests.append(request)
        raise TimeoutError("simulated transport timeout after remote acceptance")


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
        APP_ENV="production",
        RUN_MODE="live",
        MARKET_DATA_MODE="live_public",
        EXECUTION_MODE="live",
        DATABASE_URL=(
            "postgresql+asyncpg://funding:"
            "database-secret-0123456789abcdef@postgres:5432/funding"
        ),
        REDIS_URL="rediss://redis:6379/0",
        REDIS_USERNAME="funding",
        REDIS_PASSWORD="redis-secret-0123456789abcdefabcd",
        INTERNAL_SERVICE_TLS_REQUIRED=True,
        INTERNAL_TLS_CA_FILE="/run/secrets/internal/ca.crt",
        INTERNAL_TLS_CLIENT_CERT_FILE="/run/secrets/internal/app.crt",
        INTERNAL_TLS_CLIENT_KEY_FILE="/run/secrets/internal/app.key",
        CONTROL_PLANE_SECURITY_ENABLED=True,
        CONTROL_PLANE_JWT_SECRET="0123456789abcdef0123456789abcdef",
        CONTROL_PLANE_MTLS_REQUIRED=True,
        CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED=True,
        CONTROL_PLANE_RATE_LIMIT_BACKEND="redis",
        CONTROL_PLANE_MTLS_CLIENT_FINGERPRINTS="a" * 64,
        LIVE_ARMED=True,
        LIVE_AUTOTRADE=True,
        LIVE_TRADING_CONFIRM="I_UNDERSTAND_THIS_SENDS_REAL_ORDERS",
        LIVE_VENUES="bybit,gate",
        LIVE_ALLOWED_ASSETS="BTC",
        LIVE_EXPECTED_EGRESS_IP="203.0.113.10",
        LIVE_CREDENTIAL_POLICY_JSON=live_credential_policy_json(
            {"bybit": "key", "gate": "key"}
        ),
        BYBIT_API_KEY="key",
        BYBIT_API_SECRET="secret",
        GATE_API_KEY="key",
        GATE_API_SECRET="secret",
        TELEGRAM_ENABLED=True,
        TELEGRAM_BOT_TOKEN="telegram-secret",
        TELEGRAM_CHAT_ID="123",
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
    instruments = [
        NormalizedInstrument(
            exchange=exchange,
            exchange_symbol=symbol,
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            settlement_asset="USDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            funding_interval=1,
        )
        for exchange, symbol in (("bybit", "BTCUSDT"), ("gate", "BTC_USDT"))
    ]
    return MarketSnapshot(
        instruments=instruments,
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
    spot_instrument = NormalizedInstrument(
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.SPOT,
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
    )
    return MarketSnapshot(
        instruments=[*snapshot.instruments, spot_instrument],
        tickers=[*snapshot.tickers, spot_ticker],
        funding=snapshot.funding,
        orderbooks={
            **snapshot.orderbooks,
            ("bybit", "BTCUSDT", InstrumentType.SPOT): spot_book,
        },
        captured_at=snapshot.captured_at,
    )


def live_approval(
    settings: Settings,
    candidate: Opportunity,
    snapshot: MarketSnapshot,
    key: str,
) -> LiveExecutionApproval:
    authority_settings = settings.model_copy(
        update={"live_armed": True, "live_autotrade": True}
    )
    service = FundingLiveDecisionService(
        authority_settings,
        LiveRiskController(authority_settings),
    )
    return service.approve(
        candidate,
        candidate.size_quotes[0],
        snapshot,
        key,
        now=snapshot.captured_at,
    )


async def open_approved(
    executor: LiveTradingExecutor,
    settings: Settings,
    candidate: Opportunity,
    snapshot: MarketSnapshot,
    key: str,
    venue_balances: dict[str, VenueBalance],
    open_notional: Decimal = Decimal("0"),
) -> LivePosition:
    return await executor.open_position(
        live_approval(settings, candidate, snapshot, key),
        snapshot,
        venue_balances,
        open_notional,
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


def _helper_executor(tmp_path: Path) -> LiveTradingExecutor:
    executor = LiveTradingExecutor.__new__(LiveTradingExecutor)
    executor.settings = live_settings(tmp_path)
    executor.adapters = {"bybit": FakeTradingAdapter("bybit", [])}
    executor.private_reconciliation_coverage = PRIVATE_RECONCILIATION_COVERAGE
    return executor


def test_venue_balance_collateral_prefers_wallet_specific_values() -> None:
    derivative_specific = VenueBalance(
        exchange="bybit",
        free={"USDT": Decimal("10")},
        free_collateral_usd=Decimal("20"),
        derivative_free_collateral_usd=Decimal("30"),
    )
    aggregate_collateral = VenueBalance(
        exchange="bybit",
        free={"USDT": Decimal("10")},
        free_collateral_usd=Decimal("20"),
    )

    assert derivative_specific.collateral_available(InstrumentType.PERPETUAL) == 30
    assert aggregate_collateral.collateral_available(InstrumentType.PERPETUAL) == 20


def test_live_executor_helper_boundaries_fail_closed(tmp_path: Path) -> None:
    executor = _helper_executor(tmp_path)
    with pytest.raises(LiveExecutionError, match="bounded limit"):
        executor._assert_planned_price(None, "BUY", Decimal("100"))
    with pytest.raises(LiveExecutionError, match="buy limit"):
        executor._assert_planned_price(Decimal("100"), "BUY", Decimal("100.1"))
    with pytest.raises(LiveExecutionError, match="sell limit"):
        executor._assert_planned_price(Decimal("100"), "SELL", Decimal("99.9"))
    with pytest.raises(LiveExecutionError, match="not enabled"):
        executor._adapter("mexc")
    assert executor._client_order_id("hyperliquid", "intent-1", "open_a").startswith(
        "0x"
    )

    empty_fill = TradingOrderResult(
        exchange="bybit",
        exchange_order_id="empty",
        client_order_id="empty",
        exchange_symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="BUY",
        requested_base_quantity=Decimal("1"),
        filled_base_quantity=Decimal("0"),
        status=LiveOrderStatus.REJECTED,
    )
    with pytest.raises(LiveExecutionError, match="empty fill"):
        executor._to_leg(empty_fill, Decimal("100"), "BTC")
    consumed_by_fee = empty_fill.model_copy(
        update={
            "instrument_type": InstrumentType.SPOT,
            "filled_base_quantity": Decimal("0.001"),
            "average_price": Decimal("100"),
            "status": LiveOrderStatus.FILLED,
            "fee": Decimal("0.001"),
            "fee_currency": "BTC",
        }
    )
    with pytest.raises(LiveExecutionError, match="fee consumed"):
        executor._to_leg(consumed_by_fee, Decimal("100"), "BTC")
    with pytest.raises(LiveExecutionError, match="spot base inventory"):
        executor._validate_spot_inventory(
            VenueBalance(exchange="bybit"),
            "BTC_USDT",
            InstrumentType.SPOT,
            "SELL",
            Decimal("1"),
            Decimal("100"),
        )

    now = datetime.now(UTC)
    empty_snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[],
        orderbooks={},
        captured_at=now,
    )
    with pytest.raises(LiveExecutionError, match="ticker and orderbook"):
        executor._fresh_book(
            empty_snapshot,
            "bybit",
            "BTCUSDT",
            InstrumentType.PERPETUAL,
        )
    shallow_book = OrderBook(
        exchange="bybit",
        symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        bids=(OrderBookLevel(price=Decimal("99.9"), quantity=Decimal("0.1")),),
        asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("0.1")),),
        timestamp=now,
    )
    with pytest.raises(LiveExecutionError, match="insufficient orderbook depth"):
        executor._ioc_limit_price(shallow_book, "BUY", Decimal("1"))
    high_slippage_book = shallow_book.model_copy(
        update={
            "asks": (
                OrderBookLevel(price=Decimal("100"), quantity=Decimal("0.5")),
                OrderBookLevel(price=Decimal("102"), quantity=Decimal("0.5")),
            )
        }
    )
    with pytest.raises(LiveExecutionError, match="slippage exceeds"):
        executor._ioc_limit_price(high_slippage_book, "BUY", Decimal("1"))


def test_live_approval_validation_rejects_expired_asset_and_strategy(
    tmp_path: Path,
) -> None:
    executor = _helper_executor(tmp_path)
    settings = executor.settings
    snapshot = market_snapshot()
    approval = live_approval(settings, opportunity(), snapshot, "approval-boundaries")
    expired = approval.model_copy(
        update={
            "plan": approval.plan.model_copy(
                update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
            )
        }
    )
    with pytest.raises(LiveExecutionError, match="approval expired"):
        executor._validate_approval(expired)
    with pytest.raises(LiveExecutionError, match="asset is not"):
        executor._validate_approval(approval.model_copy(update={"asset": "ETH"}))
    with pytest.raises(LiveExecutionError, match="strategy is not"):
        executor._validate_approval(
            approval.model_copy(update={"strategy": "unknown-strategy"})
        )


def test_live_approval_rejects_instrument_without_private_reconciliation(
    tmp_path: Path,
) -> None:
    executor = _helper_executor(tmp_path)
    executor.adapters["gate"] = FakeTradingAdapter("gate", [])
    executor.private_reconciliation_coverage = {
        "bybit": frozenset({"SPOT", "PERPETUAL"}),
        "gate": frozenset({"SPOT", "PERPETUAL"}),
    }
    snapshot = market_snapshot()
    approval = live_approval(
        executor.settings,
        opportunity(),
        snapshot,
        "unsupported-private-reconciliation",
    )
    instructions = list(approval.plan.instructions)
    bybit_instruction = instructions[0]
    instructions[0] = bybit_instruction.model_copy(
        update={
            "instrument": bybit_instruction.instrument.model_copy(
                update={"instrument_type": DomainInstrumentType.FUTURE}
            )
        }
    )
    unsupported = approval.model_copy(
        update={
            "plan": approval.plan.model_copy(
                update={"instructions": tuple(instructions)}
            )
        }
    )

    with pytest.raises(LiveExecutionError, match="private reconciliation coverage"):
        executor._validate_approval(unsupported)


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
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    position = await open_approved(
        executor, settings, opportunity(), market_snapshot(), "key", balances()
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
async def test_disarmed_autotrade_stops_before_intent_or_exchange_action(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path).model_copy(update={"live_autotrade": False})
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    with pytest.raises(LiveTradingPaused, match="live_autotrade_not_armed"):
        await open_approved(
            executor,
            settings,
            opportunity(),
            market_snapshot(),
            "disabled-key",
            balances(),
        )

    async with factory() as session:
        intent_count = await session.scalar(select(func.count(LiveIntentRecord.id)))
    assert intent_count == 0
    assert bybit.requests == []
    assert gate.requests == []


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
    executor = LiveTradingExecutor(
        settings,
        {"bybit": adapter},
        factory,
        risk,
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    position = await open_approved(
        executor,
        settings,
        spot_perp_opportunity(),
        spot_perp_snapshot(),
        "spot-key",
        {"bybit": VenueBalance(
            exchange="bybit",
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
        )},
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
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    position = await open_approved(
        executor, settings, opportunity(), market_snapshot(), "key", balances()
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
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        risk,
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    position = await open_approved(
        executor, settings, opportunity(), market_snapshot(), "key", balances()
    )

    assert position.state is LivePositionState.MANUAL_INTERVENTION
    assert risk.paused
    assert risk.kill_switch_path.read_text(encoding="utf-8").strip() == (
        "first_leg_order_state_unknown"
    )
    assert gate.requests == []


@pytest.mark.asyncio
async def test_submission_exception_is_durable_unknown_and_never_retried(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    bybit = AcceptedThenRaisesAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        risk,
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    candidate = opportunity()
    snapshot = market_snapshot()
    approval = live_approval(settings, candidate, snapshot, "timeout-key")
    position = await executor.open_position(
        approval, snapshot, balances(), Decimal("0")
    )

    assert position.state is LivePositionState.MANUAL_INTERVENTION
    assert position.failure_reason == "first_leg_order_state_unknown"
    assert risk.paused_reason == "order_submission_outcome_unknown"
    assert len(bybit.requests) == 1
    assert gate.requests == []
    async with factory() as session:
        order = await session.scalar(select(LiveOrderRecord))
    assert order is not None
    assert order.status == LiveOrderStatus.UNKNOWN.value
    assert order.payload["raw"] == {"submission_error_type": "TimeoutError"}

    with pytest.raises(LiveTradingPaused, match="order_submission_outcome_unknown"):
        await executor.open_position(
            approval, snapshot, balances(), Decimal("0")
        )
    assert len(bybit.requests) == 1
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
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    stale_snapshot = market_snapshot(stale=True)
    position = await open_approved(
        executor, settings, opportunity(), stale_snapshot, "key", balances()
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
        settings,
        {"bybit": adapter},
        factory,
        LiveRiskController(settings),
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )
    aggregate_only = VenueBalance(
        exchange="bybit",
        free={"USDT": Decimal("1000")},
        total={"USDT": Decimal("1000")},
        spot_free={},
        derivative_free_collateral_usd=Decimal("1000"),
    )

    position = await open_approved(
        executor,
        settings,
        spot_perp_opportunity(),
        spot_perp_snapshot(),
        "wallet-key",
        {"bybit": aggregate_only},
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


@pytest.mark.asyncio
async def test_startup_reconciliation_blocks_interrupted_intent_without_resubmission(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    pending_position = LivePosition(
        position_id="interrupted-position",
        intent_id="interrupted-intent",
        opportunity_id="interrupted-opportunity",
        opportunity_key="cross:BTC:bybit:gate",
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING.value,
        asset="BTC",
        capital_per_leg=Decimal("100"),
        state=LivePositionState.OPENING,
    )
    pending_request = TradingOrderRequest(
        intent_id=pending_position.intent_id,
        client_order_id="fa-interrupted-a",
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        side="SELL",
        base_quantity=Decimal("1"),
        limit_price=Decimal("100"),
        reduce_only=False,
    )
    async with factory() as session:
        await create_live_intent(
            session,
            pending_position.intent_id,
            opportunity(),
            pending_position.capital_per_leg,
        )
        await save_live_position(session, pending_position)
        await save_pending_live_order(
            session,
            pending_request,
            position_id=pending_position.position_id,
            leg="open_a",
        )

    reconciler = LiveReconciler(
        settings, {"bybit": bybit, "gate": gate}, factory, risk
    )
    with pytest.raises(LiveTradingPaused, match="interrupted_position_transition"):
        await reconciler.reconcile(startup=True)

    assert bybit.requests == []
    assert gate.requests == []
    assert risk.kill_switch_path.exists()


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
    assert risk.state.current_equity_observed_at == now


def test_live_equity_valuation_pauses_on_unpriced_non_stable_asset(
    tmp_path: Path,
) -> None:
    settings = live_settings(tmp_path)
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.settings = settings
    runner.risk = LiveRiskController(settings)
    balance = VenueBalance(
        exchange="bybit",
        free={"BTC": Decimal("1")},
        total={"BTC": Decimal("1")},
    )
    snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[],
        orderbooks={},
        captured_at=datetime.now(UTC),
    )

    with pytest.raises(LiveTradingPaused, match="unpriced_equity_asset:bybit:BTC"):
        runner._balance_equity(balance, snapshot)

    assert runner.risk.kill_switch_path.exists()


def test_live_equity_valuation_pauses_on_stale_spot_mark(tmp_path: Path) -> None:
    settings = live_settings(tmp_path)
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.settings = settings
    runner.risk = LiveRiskController(settings)
    balance = VenueBalance(
        exchange="bybit",
        free={"BTC": Decimal("1")},
        total={"BTC": Decimal("1")},
    )
    snapshot = market_snapshot()
    stale_snapshot = replace(
        snapshot,
        captured_at=snapshot.captured_at + timedelta(seconds=31),
        stale_after_seconds=30,
    )

    with pytest.raises(LiveTradingPaused, match="unpriced_equity_asset:bybit:BTC"):
        runner._balance_equity(balance, stale_snapshot)

    assert runner.risk.kill_switch_path.exists()


def test_live_runner_pauses_on_incomplete_market_snapshot(tmp_path: Path) -> None:
    settings = live_settings(tmp_path)
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.risk = LiveRiskController(settings)
    snapshot = replace(market_snapshot(), incomplete_venues=("gate",))

    with pytest.raises(LiveTradingPaused, match="market_snapshot_incomplete:gate"):
        runner._require_complete_market_snapshot(snapshot)

    assert runner.risk.kill_switch_path.exists()


@pytest.mark.asyncio
async def test_balance_refresh_pauses_on_venue_identity_mismatch(tmp_path: Path) -> None:
    settings = live_settings(tmp_path)
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.risk = LiveRiskController(settings)
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])

    async def wrong_gate_balance() -> VenueBalance:
        return VenueBalance(
            exchange="bybit",
            free={"USDT": Decimal("1000")},
            total={"USDT": Decimal("1000")},
        )

    gate.fetch_balance = wrong_gate_balance  # type: ignore[method-assign]
    runner.trading_adapters = {"bybit": bybit, "gate": gate}

    with pytest.raises(LiveTradingPaused, match="balance_identity_mismatch:gate"):
        await runner._fetch_fresh_balances()

    assert runner.risk.kill_switch_path.exists()

class _DynamicMetadataExchange:
    rateLimit = 50
    precisionMode = 4
    has = {"createOrder": True}

    def __init__(self, symbol: str, *, active: bool) -> None:
        market = {
            "id": symbol,
            "symbol": symbol,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "spot": False,
            "swap": True,
            "future": False,
            "active": active,
            "contractSize": "0.001",
            "precision": {"price": "0.1", "amount": "1"},
            "limits": {
                "amount": {"min": "1"},
                "cost": {"min": "5"},
            },
            "maker": "0.0002",
            "taker": "0.0005",
        }
        self.markets = {symbol: market}


@pytest.mark.asyncio
async def test_live_entry_consumes_dynamic_metadata_and_rejects_inactive_market(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]], tmp_path: Path
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    registry = VenueMetadataRegistry()
    observed_at = datetime.now(UTC)
    registry.update_from_ccxt(
        venue="bybit",
        account="linear",
        exchange=_DynamicMetadataExchange("BTCUSDT", active=True),
        expected_type=DomainInstrumentType.PERPETUAL,
        observed_at=observed_at,
        server_time_ms=None,
    )
    registry.update_from_ccxt(
        venue="gate",
        account="linear",
        exchange=_DynamicMetadataExchange("BTC_USDT", active=False),
        expected_type=DomainInstrumentType.PERPETUAL,
        observed_at=observed_at,
        server_time_ms=None,
    )
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
        metadata_registry=registry,
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )

    with pytest.raises(LiveExecutionError, match="metadata is inactive"):
        await open_approved(
            executor,
            settings,
            opportunity(),
            market_snapshot(),
            "metadata-key",
            balances(),
        )

    assert bybit.requests == []
    assert gate.requests == []

def test_live_decision_service_builds_bounded_immutable_authority(
    tmp_path: Path,
) -> None:
    settings = live_settings(tmp_path)
    snapshot = market_snapshot()
    authority = live_approval(settings, opportunity(), snapshot, "decision-key")

    assert authority.market_snapshot_at == snapshot.captured_at
    assert authority.risk_decision.approved_quantity == Decimal("1")
    assert authority.plan.instructions[0].side.value == "SELL"
    assert authority.plan.instructions[0].limit_price is not None
    assert authority.plan.instructions[0].limit_price < Decimal("100")
    assert authority.plan.instructions[1].side.value == "BUY"
    assert authority.plan.instructions[1].limit_price is not None
    assert authority.plan.instructions[1].limit_price > Decimal("100")

    tampered_instruction = authority.plan.instructions[1].model_copy(
        update={"side": authority.plan.instructions[0].side}
    )
    tampered_plan = authority.plan.model_copy(
        update={
            "instructions": (
                authority.plan.instructions[0],
                tampered_instruction,
            )
        }
    )
    with pytest.raises(ValueError, match="changed approved exposure"):
        LiveExecutionApproval.model_validate(
            authority.model_dump() | {"plan": tampered_plan.model_dump()}
        )


@pytest.mark.asyncio
async def test_live_executor_rejects_unapproved_snapshot_before_exchange_action(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    bybit = FakeTradingAdapter("bybit", [])
    gate = FakeTradingAdapter("gate", [])
    executor = LiveTradingExecutor(
        settings,
        {"bybit": bybit, "gate": gate},
        factory,
        LiveRiskController(settings),
        private_reconciliation_coverage=PRIVATE_RECONCILIATION_COVERAGE,
    )
    approved_snapshot = market_snapshot()
    authority = live_approval(
        settings,
        opportunity(),
        approved_snapshot,
        "snapshot-key",
    )
    different_snapshot = replace(
        approved_snapshot,
        captured_at=approved_snapshot.captured_at + timedelta(milliseconds=1),
    )

    with pytest.raises(LiveExecutionError, match="differs from approved snapshot"):
        await executor.open_position(
            authority,
            different_snapshot,
            balances(),
            Decimal("0"),
        )

    assert bybit.requests == []
    assert gate.requests == []

def test_live_decision_service_rejects_non_executable_inputs(tmp_path: Path) -> None:
    settings = live_settings(tmp_path)
    snapshot = market_snapshot()
    candidate = opportunity()
    quote = candidate.size_quotes[0]
    service = FundingLiveDecisionService(settings, LiveRiskController(settings))

    cases: tuple[tuple[str, Opportunity, SizeQuote], ...] = (
        (
            "only confirmed opportunities",
            candidate.model_copy(update={"status": OpportunityStatus.CANDIDATE}),
            quote,
        ),
        (
            "positive net edge after costs",
            candidate.model_copy(update={"net_edge": Decimal("0")}),
            quote,
        ),
        (
            "positive net edge after costs",
            candidate,
            quote.model_copy(update={"net_profit": Decimal("0")}),
        ),
        (
            "fully executable positive size",
            candidate,
            quote.model_copy(update={"capital": Decimal("0")}),
        ),
        (
            "fully executable positive size",
            candidate,
            quote.model_copy(update={"fully_filled": False}),
        ),
        (
            "asset is not live-allowlisted",
            candidate.model_copy(update={"asset": "DOGE"}),
            quote,
        ),
        (
            "strategy is not live-allowlisted",
            candidate.model_copy(update={"strategy": StrategyName.PERP_PERP}),
            quote,
        ),
    )
    for message, changed_candidate, changed_quote in cases:
        with pytest.raises(ValueError, match=message):
            service.approve(
                changed_candidate,
                changed_quote,
                snapshot,
                "invalid-input",
                now=snapshot.captured_at,
            )


def test_live_decision_service_rejects_mode_expiry_and_metadata_gaps(
    tmp_path: Path,
) -> None:
    settings = live_settings(tmp_path)
    snapshot = market_snapshot()
    candidate = opportunity()
    quote = candidate.size_quotes[0]

    paper_settings = settings.model_copy(update={"trading_mode": TradingMode.PAPER})
    paper_service = FundingLiveDecisionService(
        paper_settings,
        LiveRiskController(paper_settings),
    )
    with pytest.raises(ValueError, match="does not authorize exchange orders"):
        paper_service.approve(
            candidate,
            quote,
            snapshot,
            "paper-mode",
            now=snapshot.captured_at,
        )

    service = FundingLiveDecisionService(settings, LiveRiskController(settings))
    expired = candidate.model_copy(
        update={"expires_at": snapshot.captured_at - timedelta(microseconds=1)}
    )
    with pytest.raises(ValueError, match="expired before live decision"):
        service.approve(
            expired,
            quote,
            snapshot,
            "expired",
            now=snapshot.captured_at,
        )

    missing_symbol = candidate.model_copy(update={"symbol_a": None})
    with pytest.raises(ValueError, match="exact exchange symbols"):
        service.approve(
            missing_symbol,
            quote,
            snapshot,
            "missing-symbol",
            now=snapshot.captured_at,
        )

    without_instruments = replace(snapshot, instruments=[])
    with pytest.raises(ValueError, match="active typed instrument metadata"):
        service.approve(
            candidate,
            quote,
            without_instruments,
            "missing-metadata",
            now=snapshot.captured_at,
        )

class VenueIdentityMismatchAdapter(FakeTradingAdapter):
    async def fetch_balance(self) -> VenueBalance:
        return VenueBalance(exchange="wrong-venue")


class RichUnexpectedStateAdapter(FakeTradingAdapter):
    async def fetch_balance(self) -> VenueBalance:
        return VenueBalance(
            exchange=self.name,
            free={
                "USDT": Decimal("1000"),
                "BTC": Decimal("0.5"),
                "DOGE": Decimal("2"),
            },
            total={
                "USDT": Decimal("1000"),
                "BTC": Decimal("0.5"),
                "DOGE": Decimal("2"),
            },
        )


@pytest.mark.asyncio
async def test_reconciliation_treats_venue_identity_failure_as_private_api_outage(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    risk = LiveRiskController(settings)
    adapter = VenueIdentityMismatchAdapter("bybit", [])
    reconciler = LiveReconciler(settings, {"bybit": adapter}, factory, risk)

    with pytest.raises(LiveTradingPaused, match="private_api_unavailable"):
        await reconciler.reconcile(startup=True)

    assert reconciler.last_result is not None
    assert reconciler.last_result.details["api_errors"] == {"bybit": "ValueError"}


@pytest.mark.asyncio
async def test_reconciliation_blocks_manual_intervention_position(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    position = LivePosition(
        position_id="manual-position",
        intent_id="manual-intent",
        opportunity_id="manual-opportunity",
        opportunity_key="manual-key",
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING.value,
        asset="BTC",
        capital_per_leg=Decimal("100"),
        state=LivePositionState.MANUAL_INTERVENTION,
    )
    async with factory() as session:
        await create_live_intent(
            session,
            position.intent_id,
            opportunity(),
            position.capital_per_leg,
        )
        await save_live_position(session, position)

    reconciler = LiveReconciler(
        settings,
        {"bybit": FakeTradingAdapter("bybit", [])},
        factory,
        LiveRiskController(settings),
    )
    with pytest.raises(LiveTradingPaused, match="manual_intervention_position"):
        await reconciler.reconcile()


@pytest.mark.asyncio
async def test_reconciliation_detects_spot_derivative_balance_and_order_drift(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    tmp_path: Path,
) -> None:
    _, factory = database
    settings = live_settings(tmp_path)
    adapter = RichUnexpectedStateAdapter("bybit", [])
    adapter.orders.append(
        TradingOrderResult(
            exchange="bybit",
            exchange_order_id="venue-orphan",
            client_order_id="orphan-client-order",
            exchange_symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            side="SELL",
            requested_base_quantity=Decimal("1"),
            filled_base_quantity=Decimal("0"),
            status=LiveOrderStatus.OPEN,
        )
    )
    position = LivePosition(
        position_id="open-position",
        intent_id="open-intent",
        opportunity_id="open-opportunity",
        opportunity_key="open-key",
        strategy=StrategyName.SPOT_PERP.value,
        asset="BTC",
        capital_per_leg=Decimal("100"),
        state=LivePositionState.OPEN,
        leg_a=LiveLeg(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            side="BUY",
            requested_base_quantity=Decimal("1"),
            filled_base_quantity=Decimal("1"),
            average_price=Decimal("100"),
        ),
        leg_b=LiveLeg(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            side="SELL",
            requested_base_quantity=Decimal("1"),
            filled_base_quantity=Decimal("1"),
            average_price=Decimal("100"),
        ),
    )
    async with factory() as session:
        await create_live_intent(
            session,
            position.intent_id,
            opportunity(),
            position.capital_per_leg,
        )
        await save_live_position(session, position)

    reconciler = LiveReconciler(
        settings,
        {"bybit": adapter},
        factory,
        LiveRiskController(settings),
    )
    with pytest.raises(LiveTradingPaused) as exc_info:
        await reconciler.reconcile()

    reason = str(exc_info.value)
    assert "derivative_position_mismatch" in reason
    assert "spot_inventory_mismatch" in reason
    assert "unexpected_spot_balance" in reason
    assert "unexpected_open_order" in reason
    assert "non_terminal_live_order" in reason
