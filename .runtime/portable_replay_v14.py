import hashlib
import json
import time
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.historical_replay import HistoricalMarketReplay
from funding_arbitrage.config import Settings


def event_digest(events: list[object]) -> str:
    payload = [event.model_dump(mode="json") for event in events]  # type: ignore[attr-defined]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def progress(stage: str, started: float, **values: object) -> None:
    print(
        json.dumps(
            {
                "stage": stage,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                **values,
            },
            default=str,
            sort_keys=True,
        ),
        flush=True,
    )


settings = Settings(
    run_mode="paper_test",
    market_data_mode="mock",
    execution_mode="paper",
    paper_initial_balance_usd="6250",
    paper_reserve_percent="20",
    paper_max_single_opportunity_percent="20",
    paper_max_single_asset_percent="30",
    paper_max_single_exchange_percent="40",
    paper_max_single_strategy_percent="60",
    paper_max_correlated_group_percent="50",
    paper_legging_move_percent="0.0002",
    paper_correlation_groups="BTC,ETH,SOL;DOGE,SHIB,PEPE,WIF,BONK,FLOKI,TUT",
    paper_max_open_positions=15,
    paper_max_hold_seconds=86400,
    paper_exit_edge_miss_cycles=2,
    paper_funding_horizon_hours="24",
    paper_entry_window_hours="2",
    paper_min_settlement_cost_coverage="2",
    paper_max_adverse_basis_percent="0.005",
    scanner_minimum_net_apr="0",
    scanner_minimum_liquidity_score="40",
    scanner_maximum_slippage_percent="0.0015",
    scanner_maximum_spread_percent="0.0020",
    scanner_minimum_funding_samples=20,
    scanner_allow_spot_short=False,
    scanner_borrowing_cost_daily="0",
    bybit_maker_fee="0.0002",
    bybit_taker_fee="0.00055",
    gate_maker_fee="0.00015",
    gate_taker_fee="0.0005",
    okx_maker_fee="0.0002",
    okx_taker_fee="0.0005",
    binance_maker_fee="0.0002",
    binance_taker_fee="0.0004",
    hyperliquid_maker_fee="0.00015",
    hyperliquid_taker_fee="0.00035",
)
started = time.perf_counter()
replay = HistoricalMarketReplay()
dataset = replay.load_portable(
    ".runtime/portable-v14",
    datetime(2026, 7, 12, tzinfo=UTC),
    datetime(2026, 8, 11, tzinfo=UTC),
)
progress(
    "loaded",
    started,
    dataset_version=dataset.dataset_version,
    candle_rows=len(dataset.candles),
    funding_events=len(dataset.funding),
    instruments=len(dataset.instruments),
)
baseline = replay.simulate(dataset, "baseline", Decimal("6250"), settings)
progress("baseline_complete", started, positions=baseline.position_count)
candidate = replay.simulate(dataset, "candidate", Decimal("6250"), settings)
progress("candidate_complete", started, positions=candidate.position_count)
first_digest = event_digest(candidate.events)
candidate_repeat = replay.simulate(dataset, "candidate", Decimal("6250"), settings)
second_digest = event_digest(candidate_repeat.events)
progress(
    "candidate_repeat_complete",
    started,
    positions=candidate_repeat.position_count,
    deterministic=first_digest == second_digest,
)
comparison = compare_paper_datasets(baseline, candidate, Decimal("6250"))
print(
    json.dumps(
        {
            "stage": "complete",
            "dataset_version": dataset.dataset_version,
            "coverage": {
                "start": dataset.coverage.get("start"),
                "end": dataset.coverage.get("end"),
                "candle_rows": dataset.coverage.get("candle_rows"),
                "funding_events": dataset.coverage.get("funding_events"),
                "series": len(dataset.coverage.get("series", {})),
            },
            "positions": {
                "baseline": baseline.position_count,
                "candidate": candidate.position_count,
            },
            "deterministic": first_digest == second_digest,
            "candidate_event_sha256": first_digest,
            "comparison": comparison,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        },
        default=str,
        sort_keys=True,
    ),
    flush=True,
)
