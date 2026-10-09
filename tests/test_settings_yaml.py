from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.config import get_settings
from funding_arbitrage.opportunity.calculator import CostEngine


def test_yaml_defaults_are_revalidated_into_declared_settings_types(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "default.yaml").write_text(
        """
scanner:
  borrowing_cost_daily: 0.0015
paper_portfolio:
  legging_move_percent: 0.0002
  max_open_positions: 7
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCANNER_BORROWING_COST_DAILY", raising=False)
    monkeypatch.delenv("PAPER_LEGGING_MOVE_PERCENT", raising=False)
    monkeypatch.delenv("PAPER_MAX_OPEN_POSITIONS", raising=False)
    get_settings.cache_clear()
    try:
        settings = get_settings()
        assert settings.scanner_borrowing_cost_daily == Decimal("0.0015")
        assert isinstance(settings.scanner_borrowing_cost_daily, Decimal)
        assert settings.paper_legging_move_percent == Decimal("0.0002")
        assert isinstance(settings.paper_legging_move_percent, Decimal)
        assert settings.paper_max_open_positions == 7
        costs = CostEngine(
            legging_cost_percent=settings.paper_legging_move_percent
        ).estimate(Decimal("250"), "gate", "bybit", Decimal("8"))
        assert costs.legging_cost == Decimal("0.05")
    finally:
        get_settings.cache_clear()
