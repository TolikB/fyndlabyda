from funding_arbitrage.config import Settings
from funding_arbitrage.services.runtime import RuntimeState


def test_runtime_entry_health_fails_closed_without_blocking_state_access() -> None:
    health = [True, None]
    runtime = RuntimeState(
        Settings(
            run_mode="paper_test",
            market_data_mode="mock",
            execution_mode="paper",
            trading_mode="PAPER",
        ),        {},
        entry_health=lambda: (bool(health[0]), health[1]),
    )

    assert runtime.entries_allowed() is True
    assert runtime.entry_block_reason() is None

    health[:] = [False, "canonical_event_journal:OSError"]
    assert runtime.entries_allowed() is False
    assert runtime.entry_block_reason() == "canonical_event_journal:OSError"
    assert runtime.portfolio_value() == runtime.portfolio.snapshot().equity
