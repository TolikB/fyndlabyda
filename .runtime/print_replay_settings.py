import json

from funding_arbitrage.config import get_settings

FIELDS = (
    "paper_initial_balance_usd",
    "paper_reserve_percent",
    "paper_max_single_opportunity_percent",
    "paper_max_single_asset_percent",
    "paper_max_single_exchange_percent",
    "paper_max_single_strategy_percent",
    "paper_max_correlated_group_percent",
    "paper_legging_move_percent",
    "paper_correlation_groups",
    "paper_max_open_positions",
    "paper_max_hold_seconds",
    "paper_exit_edge_miss_cycles",
    "paper_funding_horizon_hours",
    "paper_entry_window_hours",
    "paper_min_settlement_cost_coverage",
    "paper_max_adverse_basis_percent",
    "scanner_minimum_net_apr",
    "scanner_minimum_liquidity_score",
    "scanner_maximum_slippage_percent",
    "scanner_maximum_spread_percent",
    "scanner_minimum_funding_samples",
    "scanner_allow_spot_short",
    "scanner_borrowing_cost_daily",
    "bybit_maker_fee",
    "bybit_taker_fee",
    "gate_maker_fee",
    "gate_taker_fee",
    "okx_maker_fee",
    "okx_taker_fee",
    "binance_maker_fee",
    "binance_taker_fee",
    "hyperliquid_maker_fee",
    "hyperliquid_taker_fee",
)


settings = get_settings()
print(json.dumps({name: getattr(settings, name) for name in FIELDS}, default=str, sort_keys=True))
