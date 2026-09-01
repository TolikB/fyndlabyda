from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.live import LiveExecutionError
from funding_arbitrage.execution.trading import (
    LiveLeg,
    LivePosition,
    LivePositionState,
    VenueBalance,
    VenueFundingPayment,
)
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.risk.live import LiveTradingPaused
from funding_arbitrage.services import live_runner as module
from funding_arbitrage.services.live_runner import LiveTradingRunner
from tests.test_live_executor import (
    FakeTradingAdapter,
    balances,
    live_settings,
    market_snapshot,
    opportunity,
    spot_perp_snapshot,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


class FakeRisk:
    def __init__(self, settings: object) -> None:
        self.settings = settings
        self.paused = False
        self.paused_reason: str | None = None
        self.verify_error: Exception | None = None
        self.timezone = UTC
        self.state = SimpleNamespace(
            starting_equity=None,
            high_water_equity=None,
            day_start_equity=None,
            current_equity=None,
            current_equity_observed_at=None,
        )
        self.restored: dict[str, object] | None = None

    def verify_interlock_storage(self) -> None:
        if self.verify_error is not None:
            raise self.verify_error

    def trip(self, reason: str) -> None:
        self.paused = True
        self.paused_reason = reason

    def update_equity(self, equity: Decimal, now: datetime) -> None:
        if self.state.starting_equity is None:
            self.state.starting_equity = equity
            self.state.high_water_equity = equity
        self.state.current_equity = equity
        self.state.current_equity_observed_at = now
        self.state.high_water_equity = max(self.state.high_water_equity, equity)

    def restore_baselines(self, **values: object) -> None:
        self.restored = values


class FakeExecutor:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.position: LivePosition | None = None
        self.open_error: Exception | None = None
        self.close_error: Exception | None = None

    async def open_position(self, *args: object, **kwargs: object) -> LivePosition:
        if self.open_error is not None:
            raise self.open_error
        assert self.position is not None
        return self.position

    async def close_position(
        self,
        position: LivePosition,
        snapshot: object,
    ) -> LivePosition:
        if self.close_error is not None:
            raise self.close_error
        return position.model_copy(update={"state": LivePositionState.CLOSED})


class FakeDecision:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.calls = 0

    def approve(self, *args: object, **kwargs: object) -> object:
        self.calls += 1
        return SimpleNamespace(approved=True)


class FakeReconciler:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.error: Exception | None = None
        self.result = SimpleNamespace(
            passed=True,
            reason=None,
            balances=balances(),
            positions=(),
            open_orders=(),
        )

    async def reconcile(
        self,
        *,
        startup: bool = False,
        raise_on_failure: bool = True,
    ) -> object:
        if self.error is not None:
            raise self.error
        return self.result

    @staticmethod
    def raise_if_failed(result: SimpleNamespace) -> None:
        if not result.passed:
            raise LiveTradingPaused(result.reason or "reconciliation_failed")


class FakeDailyReport:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.alerts: list[str] = []
        self.checks: list[datetime] = []
        self.closed = 0
        self.raise_alert = False

    async def send_safety_alert(self, reason: str) -> bool:
        if self.raise_alert:
            raise TimeoutError("synthetic alert failure")
        self.alerts.append(reason)
        return True

    async def check_and_send(self, now: datetime) -> bool:
        self.checks.append(now)
        return True

    async def close(self) -> None:
        self.closed += 1


class FakeCollector:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.snapshot = market_snapshot()
        self.calls: list[dict[str, object]] = []
        self.closed = 0

    async def collect_once(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        return self.snapshot

    async def close(self) -> None:
        self.closed += 1


class FakePrivateStreams:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.ingested = 0
        self.health_value = (True, None)

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def ingest_reconciliation(self, result: object, observed_at: datetime) -> None:
        self.ingested += 1

    def health(self) -> tuple[bool, str | None]:
        return self.health_value

    @staticmethod
    def reconciliation_coverage() -> dict[str, frozenset[str]]:
        return {
            "bybit": frozenset({"SPOT", "PERPETUAL"}),
            "gate": frozenset({"SPOT", "PERPETUAL"}),
        }


class FakePublicEvents:
    def __init__(self) -> None:
        self.metadata_registry = object()
        self.started = 0
        self.closed = 0
        self.observed = 0

    async def start(self) -> None:
        self.started += 1

    async def close(self) -> None:
        self.closed += 1

    async def observe_snapshot(self, snapshot: object) -> None:
        self.observed += 1


class FakeRuntime:
    def __init__(self) -> None:
        self.entry_health = lambda: (True, None)
        self.adapters = {"bybit": object(), "gate": object()}
        self.opportunities: list[Opportunity] = [opportunity()]
        self.opportunity_engine = SimpleNamespace(last_candidates=self.opportunities)
        self.last_completed_snapshot = None
        self.update_calls = 0

    def update_market(self, snapshot: object) -> list[Opportunity]:
        self.update_calls += 1
        return self.opportunities

    def entries_allowed(self) -> bool:
        return True


class NullSessionContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        return None


class NullSessionFactory:
    def __call__(self) -> NullSessionContext:
        return NullSessionContext()


def _build_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> tuple[
    LiveTradingRunner,
    FakeRuntime,
    dict[str, FakeTradingAdapter],
    FakePrivateStreams,
    FakePublicEvents,
]:
    monkeypatch.setattr(module, "MarketDataCollector", FakeCollector)
    monkeypatch.setattr(module, "LiveRiskController", FakeRisk)
    monkeypatch.setattr(module, "LiveTradingExecutor", FakeExecutor)
    monkeypatch.setattr(module, "FundingLiveDecisionService", FakeDecision)
    monkeypatch.setattr(module, "LiveReconciler", FakeReconciler)
    monkeypatch.setattr(module, "LiveDailyReportService", FakeDailyReport)

    settings = live_settings(tmp_path)
    runtime = FakeRuntime()
    adapters = {
        "bybit": FakeTradingAdapter("bybit", []),
        "gate": FakeTradingAdapter("gate", []),
    }
    private = FakePrivateStreams()
    public = FakePublicEvents()
    runner = LiveTradingRunner(
        settings,
        runtime,  # type: ignore[arg-type]
        NullSessionFactory(),  # type: ignore[arg-type]
        adapters,  # type: ignore[arg-type]
        private,  # type: ignore[arg-type]
        public,  # type: ignore[arg-type]
    )
    return runner, runtime, adapters, private, public


async def _noop(*args: object, **kwargs: object) -> None:
    return None


async def test_constructor_run_one_cycle_and_close_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, runtime, adapters, private, public = _build_runner(monkeypatch, tmp_path)
    runner._acquire_process_lock = _noop  # type: ignore[method-assign]
    runner._release_process_lock = _noop  # type: ignore[method-assign]
    runner._restore_positions = _noop  # type: ignore[method-assign]
    runner._restore_risk_baselines = _noop  # type: ignore[method-assign]
    runner._restore_funding_cursors = _noop  # type: ignore[method-assign]
    runner._poll_funding_payments = _noop  # type: ignore[method-assign]

    async def one_cycle() -> None:
        runner.stop_event.set()

    runner.cycle = one_cycle  # type: ignore[method-assign]

    await runner.run()

    assert runner.initialized is True
    assert private.started == 1
    assert private.ingested == 1
    assert public.started == 1
    assert runtime.entry_health == runner._entry_health
    assert set(runner._balances) == {"bybit", "gate"}

    await runner.close()
    assert runner.stop_event.is_set()
    assert runner.collector.closed == 1
    assert runner.daily_report.closed == 1
    assert private.stopped == 1
    assert public.closed == 1
    assert all(adapter.requests == [] for adapter in adapters.values())


@pytest.mark.parametrize(
    ("error", "startup_error", "trip_reason"),
    [
        (
            LiveTradingPaused("startup reconcile"),
            "live_startup_reconciliation_paused",
            None,
        ),
        (
            RuntimeError("startup failure"),
            "live_startup_failed",
            "live_startup_failed",
        ),
    ],
)
async def test_run_startup_failures_are_fail_closed_and_alerted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    error: Exception,
    startup_error: str,
    trip_reason: str | None,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    runner.risk.verify_error = error
    runner._acquire_process_lock = _noop  # type: ignore[method-assign]

    await runner.run()

    assert runner.startup_error == startup_error
    if trip_reason is not None:
        assert runner.risk.paused_reason == trip_reason
    if runner.risk.paused_reason:
        assert runner.daily_report.alerts == [runner.risk.paused_reason]


async def test_run_cycle_pause_is_caught_without_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    for name in (
        "_acquire_process_lock",
        "_restore_positions",
        "_restore_risk_baselines",
        "_restore_funding_cursors",
        "_poll_funding_payments",
    ):
        setattr(runner, name, _noop)

    async def paused_cycle() -> None:
        runner.risk.trip("cycle_pause")
        runner.stop_event.set()
        raise LiveTradingPaused("cycle_pause")

    runner.cycle = paused_cycle  # type: ignore[method-assign]
    await runner.run()

    assert runner.daily_report.alerts == ["cycle_pause"]


async def test_failed_reconciliation_is_journaled_before_pause_enforcement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, private, _ = _build_runner(monkeypatch, tmp_path)
    runner.reconciler.result = SimpleNamespace(
        passed=False,
        reason="non_terminal_live_order",
        balances=balances(),
        positions=(),
        open_orders=(object(),),
    )

    with pytest.raises(LiveTradingPaused, match="non_terminal_live_order"):
        await runner._reconcile_and_journal()

    assert private.ingested == 1


async def test_cycle_coordinates_snapshot_reconciliation_equity_and_entries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, runtime, _, private, public = _build_runner(monkeypatch, tmp_path)
    snapshot = market_snapshot()
    runner.collector.snapshot = snapshot
    calls: list[str] = []

    async def record(name: str, *args: object, **kwargs: object) -> None:
        calls.append(name)

    runner._persist_market_if_due = (  # type: ignore[method-assign]
        lambda *args, **kwargs: record("persist", *args, **kwargs)
    )
    runner._record_equity = (  # type: ignore[method-assign]
        lambda *args, **kwargs: record("equity", *args, **kwargs)
    )
    runner._poll_funding_payments = (  # type: ignore[method-assign]
        lambda *args, **kwargs: record("funding", *args, **kwargs)
    )
    runner._close_positions = (  # type: ignore[method-assign]
        lambda *args, **kwargs: record("close", *args, **kwargs)
    )
    runner._open_positions = (  # type: ignore[method-assign]
        lambda *args, **kwargs: record("open", *args, **kwargs)
    )

    await runner.cycle()

    assert runtime.update_calls == 1
    assert public.observed == 1
    assert private.ingested == 1
    assert calls == ["persist", "equity", "funding", "close", "open"]
    assert runner._last_reconciliation == snapshot.captured_at
    assert runner.daily_report.checks == [snapshot.captured_at]
    assert runtime.last_completed_snapshot is snapshot
    assert runner._candidate_books["bybit"]
    assert runner._candidate_history["gate"]


def test_entry_health_composes_base_and_private_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, runtime, _, private, _ = _build_runner(monkeypatch, tmp_path)
    assert runner._entry_health() == (True, None)

    private.health_value = (False, "private_stale")
    assert runner._entry_health() == (False, "private_stale")

    runner._base_entry_health = lambda: (False, "journal_failed")
    assert runner._entry_health() == (False, "journal_failed")

    runner.private_streams = None
    runner._base_entry_health = None
    assert runner._entry_health() == (True, None)
    assert runtime.entry_health is not None


class LockSession:
    def __init__(self, dialect: str, acquired: bool = True) -> None:
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name=dialect))
        self.acquired = acquired
        self.closed = 0
        self.executed = 0

    def get_bind(self) -> object:
        return self.bind

    async def scalar(self, statement: object) -> bool:
        return self.acquired

    async def execute(self, statement: object) -> None:
        self.executed += 1

    async def close(self) -> None:
        self.closed += 1


async def test_process_lock_handles_sqlite_postgres_and_contention() -> None:
    sqlite_runner = LiveTradingRunner.__new__(LiveTradingRunner)
    sqlite = LockSession("sqlite")
    sqlite_runner.session_factory = lambda: sqlite  # type: ignore[assignment]
    sqlite_runner._process_lock_session = None
    await sqlite_runner._acquire_process_lock()
    await sqlite_runner._release_process_lock()
    assert sqlite.closed == 1

    postgres_runner = LiveTradingRunner.__new__(LiveTradingRunner)
    postgres = LockSession("postgresql")
    postgres_runner.session_factory = lambda: postgres  # type: ignore[assignment]
    postgres_runner._process_lock_session = None
    await postgres_runner._acquire_process_lock()
    await postgres_runner._release_process_lock()
    assert postgres.executed == 1
    assert postgres.closed == 1

    contended_runner = LiveTradingRunner.__new__(LiveTradingRunner)
    contended = LockSession("postgresql", acquired=False)
    contended_runner.session_factory = lambda: contended  # type: ignore[assignment]
    contended_runner._process_lock_session = None
    with pytest.raises(RuntimeError, match="another live runner"):
        await contended_runner._acquire_process_lock()
    assert contended.closed == 1
    await contended_runner._release_process_lock()


class FundingAdapter:
    def __init__(
        self,
        name: str,
        *,
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.error = error
        self.since: datetime | None = None

    async def fetch_funding_payments(
        self,
        since: datetime,
    ) -> list[VenueFundingPayment]:
        self.since = since
        if self.error is not None:
            raise self.error
        return [
            VenueFundingPayment(
                exchange=self.name,
                external_id=f"{self.name}-funding",
                exchange_symbol="BTCUSDT",
                amount=Decimal("1"),
                currency="USDT",
                timestamp=NOW,
            )
        ]


async def test_funding_poll_uses_overlap_persists_and_pauses_on_partial_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inserted: list[int] = []

    async def save(session: object, rows: object) -> int:
        inserted.append(len(rows))  # type: ignore[arg-type]
        return len(rows)  # type: ignore[arg-type]

    monkeypatch.setattr(module, "save_live_funding_payments", save)
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.session_factory = NullSessionFactory()
    runner.risk = FakeRisk(SimpleNamespace())
    runner._funding_cursors = {"bybit": NOW - timedelta(hours=2)}
    runner._funding_floors = {"bybit": NOW - timedelta(hours=3)}
    bybit = FundingAdapter("bybit")
    runner.trading_adapters = {"bybit": bybit}

    await runner._poll_funding_payments(NOW)

    assert inserted == [1]
    assert bybit.since == NOW - timedelta(hours=3)
    assert runner._funding_cursors["bybit"] == NOW

    runner.risk = FakeRisk(SimpleNamespace())
    runner.trading_adapters = {
        "bybit": bybit,
        "gate": FundingAdapter("gate", error=TimeoutError("synthetic")),
    }
    runner._funding_cursors["gate"] = NOW
    runner._funding_floors["gate"] = NOW
    with pytest.raises(LiveTradingPaused, match="funding_history_poll_failed"):
        await runner._poll_funding_payments(NOW)


async def test_persist_market_obeys_interval_and_record_equity_uses_real_balances(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    snapshot = spot_perp_snapshot()
    saves: list[str] = []

    async def save_market(
        session: object,
        current: object,
        *,
        include_history: bool,
    ) -> None:
        saves.append(f"market:{include_history}")

    async def save_opportunities(session: object, values: object) -> None:
        saves.append("opportunities")

    async def save_accounts(
        session: object,
        values: object,
        captured_at: datetime,
    ) -> None:
        saves.append("accounts")

    monkeypatch.setattr(module, "save_market_snapshot", save_market)
    monkeypatch.setattr(module, "save_opportunities", save_opportunities)
    monkeypatch.setattr(module, "save_live_account_snapshots", save_accounts)
    runner._last_history_refresh = snapshot.captured_at

    await runner._persist_market_if_due(snapshot, [opportunity()])
    await runner._persist_market_if_due(snapshot, [opportunity()])
    assert saves[:2] == ["market:True", "opportunities"]

    runner._balances = {
        "bybit": VenueBalance(
            exchange="bybit",
            total={"USDT": Decimal("100"), "BTC": Decimal("1")},
            free={"USDT": Decimal("80")},
        ),
        "gate": VenueBalance(
            exchange="gate",
            total={"USDT": Decimal("120")},
            free={"USDT": Decimal("100")},
            equity_usd=Decimal("125"),
            free_collateral_usd=Decimal("90"),
        ),
    }
    await runner._record_equity(snapshot)

    assert "accounts" in saves
    assert runner.risk.state.current_equity == Decimal("325")
    assert runner.risk.state.current_equity_observed_at == snapshot.captured_at
    assert runner._last_account_snapshot == snapshot.captured_at

    higher_snapshot = replace(
        snapshot,
        captured_at=snapshot.captured_at + timedelta(seconds=10),
    )
    runner._balances["gate"] = runner._balances["gate"].model_copy(
        update={"equity_usd": Decimal("150")}
    )
    await runner._record_equity(higher_snapshot)

    lower_snapshot = replace(
        snapshot,
        captured_at=snapshot.captured_at + timedelta(seconds=20),
    )
    runner._balances["gate"] = runner._balances["gate"].model_copy(
        update={"equity_usd": Decimal("140")}
    )
    await runner._record_equity(lower_snapshot)

    assert saves.count("accounts") == 2
    assert runner._last_account_snapshot == higher_snapshot.captured_at


class BalanceAdapter:
    def __init__(self, value: VenueBalance | Exception) -> None:
        self.value = value

    async def fetch_balance(self) -> VenueBalance:
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


async def test_fresh_balance_refresh_rejects_failure_and_identity_mismatch() -> None:
    runner = LiveTradingRunner.__new__(LiveTradingRunner)
    runner.risk = FakeRisk(SimpleNamespace())

    runner.trading_adapters = {
        "bybit": BalanceAdapter(TimeoutError("synthetic")),
    }
    with pytest.raises(LiveTradingPaused, match="balance_refresh_failed"):
        await runner._fetch_fresh_balances()

    runner.risk = FakeRisk(SimpleNamespace())
    runner.trading_adapters = {
        "bybit": BalanceAdapter(VenueBalance(exchange="gate")),
    }
    with pytest.raises(LiveTradingPaused, match="balance_identity_mismatch"):
        await runner._fetch_fresh_balances()

    runner.risk = FakeRisk(SimpleNamespace())
    expected = VenueBalance(exchange="bybit", total={"USDT": Decimal("1")})
    runner.trading_adapters = {"bybit": BalanceAdapter(expected)}
    assert await runner._fetch_fresh_balances() == {"bybit": expected}


def _live_position(
    *,
    state: LivePositionState = LivePositionState.OPEN,
    opened_at: datetime | None = NOW - timedelta(hours=1),
) -> LivePosition:
    return LivePosition(
        position_id="position-1",
        intent_id="intent-1",
        opportunity_id="opportunity-1",
        opportunity_key=OpportunityDebouncer.key(opportunity()),
        strategy="cross_exchange_funding",
        asset="BTC",
        capital_per_leg=Decimal("100"),
        state=state,
        leg_a=LiveLeg(
            exchange="bybit",
            exchange_symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            side="SELL",
            requested_base_quantity=Decimal("1"),
            filled_base_quantity=Decimal("1"),
            average_price=Decimal("100"),
        ),
        leg_b=LiveLeg(
            exchange="gate",
            exchange_symbol="BTC_USDT",
            instrument_type=InstrumentType.PERPETUAL,
            side="BUY",
            requested_base_quantity=Decimal("1"),
            filled_base_quantity=Decimal("1"),
            average_price=Decimal("100"),
        ),
        opened_at=opened_at,
        target_settlements=(NOW - timedelta(minutes=1),),
    )


def test_size_concentration_books_funding_and_basis_helpers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    snapshot = market_snapshot()
    candidate = opportunity()
    quote = candidate.size_quotes[0]

    selected = runner._select_size(candidate, snapshot)
    assert selected == quote
    assert runner._select_size(
        candidate.model_copy(
            update={"size_quotes": [quote.model_copy(update={"fully_filled": False})]}
        ),
        snapshot,
    ) is None
    runner.settings = runner.settings.model_copy(
        update={"live_min_expected_profit_usd": Decimal("2")}
    )
    assert runner._select_size(candidate, snapshot) is None

    position = _live_position()
    runner.settings = live_settings(tmp_path).model_copy(
        update={"live_max_asset_notional_usd": Decimal("100")}
    )
    assert runner._concentration_rejection(candidate, quote, []) == "asset_notional_limit"
    runner.settings = live_settings(tmp_path).model_copy(
        update={"live_max_strategy_notional_usd": Decimal("100")}
    )
    assert runner._concentration_rejection(candidate, quote, []) == "strategy_notional_limit"
    runner.settings = live_settings(tmp_path).model_copy(
        update={"live_max_venue_notional_usd": Decimal("50")}
    )
    assert runner._concentration_rejection(candidate, quote, []) == "venue_notional_limit"
    runner.settings = live_settings(tmp_path).model_copy(
        update={"live_max_correlated_notional_usd": Decimal("100")}
    )
    assert runner._concentration_rejection(candidate, quote, []) == "correlated_notional_limit"
    runner.settings = live_settings(tmp_path)
    assert runner._concentration_rejection(candidate, quote, []) is None

    runner.positions = {position.position_id: position}
    runner._candidate_books = {"bybit": {("ETHUSDT", InstrumentType.PERPETUAL)}}
    books = runner._required_books()
    assert ("BTCUSDT", InstrumentType.PERPETUAL) in books["bybit"]
    assert ("BTC_USDT", InstrumentType.PERPETUAL) in books["gate"]

    runner._remember_candidates(
        [
            candidate,
            candidate.model_copy(
                update={
                    "venue_a": "unsupported",
                    "symbol_a": None,
                    "symbol_b": None,
                }
            ),
        ]
    )
    assert runner._candidate_history["bybit"] == {"BTCUSDT"}
    assert runner._reconciliation_due(snapshot.captured_at)
    runner._last_reconciliation = snapshot.captured_at
    assert not runner._reconciliation_due(snapshot.captured_at)

    assert runner._funding_reversed(position, snapshot) is False
    reversed_snapshot = replace(
        snapshot,
        funding=[
            item.model_copy(update={"funding_rate": Decimal("0")})
            for item in snapshot.funding
        ],
    )
    assert runner._funding_reversed(position, reversed_snapshot) is True
    assert runner._adverse_basis(position, snapshot) is False

    adverse = position.model_copy(
        update={
            "leg_a": position.leg_a.model_copy(update={"average_price": Decimal("90")}),
            "leg_b": position.leg_b.model_copy(update={"average_price": Decimal("110")}),
        }
    )
    assert runner._adverse_basis(adverse, snapshot) is True


async def test_open_and_close_positions_update_keys_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    snapshot = market_snapshot()
    candidate = opportunity()
    position = _live_position(opened_at=snapshot.captured_at - timedelta(days=1))
    runner.executor.position = position
    runner._balances = balances()

    await runner._open_positions([candidate], snapshot)

    key = OpportunityDebouncer.key(candidate)
    assert runner.positions[position.position_id] == position
    assert runner._position_by_key[key] == position.position_id
    assert runner.decision_pipeline.calls == 1

    await runner._close_positions([], snapshot)

    assert runner.positions[position.position_id].state is LivePositionState.CLOSED
    assert key not in runner._position_by_key

    failing = _live_position(opened_at=snapshot.captured_at - timedelta(days=1))
    runner.positions = {failing.position_id: failing}
    runner._position_by_key = {failing.opportunity_key: failing.position_id}
    runner.executor.close_error = LiveExecutionError("synthetic close failure")
    await runner._close_positions([], snapshot)
    assert runner.risk.paused_reason == "live_close_failed"


async def test_alert_failure_is_contained(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runner, _, _, _, _ = _build_runner(monkeypatch, tmp_path)
    runner.risk.trip("test_pause")
    runner.daily_report.raise_alert = True

    await runner._alert_if_paused()

    runner.risk.paused_reason = None
    await runner._alert_if_paused()

