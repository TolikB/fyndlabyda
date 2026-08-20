from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from funding_arbitrage import cli


class FakeEngine:
    def __init__(self) -> None:
        self.disposed = 0

    async def dispose(self) -> None:
        self.disposed += 1


class FakeSessionContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeSessionFactory:
    def __call__(self) -> FakeSessionContext:
        return FakeSessionContext()


class FakeAdapter:
    def __init__(self, count: int) -> None:
        self.count = count
        self.closed = 0

    async def get_instruments(self) -> list[object]:
        return [object()] * self.count

    async def get_tickers(self) -> list[object]:
        return [object()] * (self.count + 1)

    async def get_funding_rates(self) -> list[object]:
        return [object()] * (self.count + 2)

    async def close(self) -> None:
        self.closed += 1


class JsonModel:
    def __init__(self, value: str) -> None:
        self.value = value

    def model_dump(self, *, mode: str) -> dict[str, str]:
        assert mode == "json"
        return {"value": self.value}


class FakeCollector:
    def __init__(self, adapters: object) -> None:
        self.adapters = tuple(adapters)

    async def collect_once(self, *, include_history: bool) -> object:
        assert include_history is True
        return SimpleNamespace(captured_at="snapshot")


class FakeRuntime:
    def __init__(self, settings: object, adapters: object) -> None:
        self.settings = settings
        self.adapters = adapters
        self.portfolio = SimpleNamespace(snapshot=lambda: JsonModel("portfolio"))

    def update_market(self, snapshot: object) -> list[JsonModel]:
        assert snapshot.captured_at == "snapshot"
        return [JsonModel("opportunity")]


async def _noop(*args: object, **kwargs: object) -> None:
    return None


def _patch_database_and_saves(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FakeEngine, FakeSessionFactory]:
    engine = FakeEngine()
    factory = FakeSessionFactory()
    monkeypatch.setattr(cli, "create_database", lambda settings: (engine, factory))
    monkeypatch.setattr(cli, "init_database", _noop)
    for name in (
        "save_instruments",
        "save_tickers",
        "save_funding_snapshots",
        "save_market_snapshot",
        "save_opportunities",
        "save_portfolio_snapshot",
    ):
        monkeypatch.setattr(cli, name, _noop)
    return engine, factory


def test_read_monthly_pnl_preserves_decimal_precision(tmp_path: Path) -> None:
    path = tmp_path / "monthly.json"
    path.write_text(
        json.dumps({"2026-01": "1.2300", "2026-02": -0.5}),
        encoding="utf-8",
    )

    assert cli.read_monthly_pnl(str(path)) == {
        "2026-01": Decimal("1.2300"),
        "2026-02": Decimal("-0.5"),
    }


async def test_collect_once_persists_all_venues_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine, _ = _patch_database_and_saves(monkeypatch)
    adapters = {"bybit": FakeAdapter(1), "gate": FakeAdapter(2)}
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "create_public_adapters", lambda settings: adapters)

    await cli.collect_once()

    output = capsys.readouterr().out
    assert "instruments=3" in output
    assert "tickers=5" in output
    assert "funding=7" in output
    assert "exchanges=bybit,gate" in output
    assert engine.disposed == 1
    assert all(adapter.closed == 1 for adapter in adapters.values())


async def test_scan_once_persists_snapshot_opportunities_and_portfolio(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine, _ = _patch_database_and_saves(monkeypatch)
    adapters = {"bybit": FakeAdapter(1)}
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "create_public_adapters", lambda settings: adapters)
    monkeypatch.setattr(cli, "RuntimeState", FakeRuntime)
    monkeypatch.setattr(cli, "MarketDataCollector", FakeCollector)

    await cli.scan_once()

    assert json.loads(capsys.readouterr().out) == [{"value": "opportunity"}]
    assert engine.disposed == 1
    assert adapters["bybit"].closed == 1


@pytest.mark.parametrize("with_monthly_file", [False, True])
async def test_backtest_once_supports_empty_and_monthly_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    with_monthly_file: bool,
) -> None:
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(paper_initial_balance_usd=Decimal("1000")),
    )
    path: Path | None = None
    if with_monthly_file:
        path = tmp_path / "monthly.json"
        path.write_text(
            json.dumps({"2026-02": "2", "2026-01": "1"}),
            encoding="utf-8",
        )

    await cli.backtest_once(str(path) if path else None)

    payload = json.loads(capsys.readouterr().out)
    assert "net_profit_after_costs" in payload
    assert payload["net_profit_after_costs"] == (
        "3" if with_monthly_file else "0"
    )


def test_paper_status_prints_runtime_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "RuntimeState", FakeRuntime)

    cli.paper_status()

    assert json.loads(capsys.readouterr().out) == {"value": "portfolio"}


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (["collect"], "collect"),
        (["scan"], "scan"),
        (["backtest", "--monthly-pnl", "monthly.json"], "backtest:monthly.json"),
        (["paper"], "paper"),
        (["api"], "api"),
        ([], "api"),
    ],
)
def test_main_routes_each_command_without_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    expected: str,
) -> None:
    calls: list[str] = []

    async def collect() -> None:
        calls.append("collect")

    async def scan() -> None:
        calls.append("scan")

    async def backtest(path: str | None) -> None:
        calls.append(f"backtest:{path}")

    def paper() -> None:
        calls.append("paper")

    def uvicorn_run(*args: object, **kwargs: object) -> None:
        assert args == ("funding_arbitrage.main:app",)
        assert kwargs == {"host": "0.0.0.0", "port": 8000, "reload": False}
        calls.append("api")

    monkeypatch.setattr(cli, "collect_once", collect)
    monkeypatch.setattr(cli, "scan_once", scan)
    monkeypatch.setattr(cli, "backtest_once", backtest)
    monkeypatch.setattr(cli, "paper_status", paper)
    monkeypatch.setattr(cli.uvicorn, "run", uvicorn_run)
    monkeypatch.setattr(sys, "argv", ["funding-arbitrage", *arguments])

    cli.main()

    assert calls == [expected]

