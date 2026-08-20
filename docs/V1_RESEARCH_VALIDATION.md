# V1 research validation

V1 research uses point-in-time records and deterministic validation. A result is
not promotion evidence merely because a single in-sample backtest is profitable.

## Leakage and universe controls

Each ResearchTrade records the decision time, feature-availability time, outcome
time, listing and delisting bounds, and the universe selection ID and timestamp.
Construction fails when features or universe membership come from the future,
the outcome is already known at decision time, or the instrument was not active.
The LiquidAltcoinUniverseSelector separately evaluates historical membership
as-of each rebalance and retains delisted assets in historical inputs.

## Walk-forward

Walk-forward folds use rolling training and non-overlapping validation windows.
A configurable embargo separates them. Training trades whose outcomes were not
known by the training cutoff are purged. Incomplete final windows and windows
without the configured minimum number of trades are excluded explicitly.

## Monte Carlo

Monte Carlo uses a seeded circular block bootstrap over chronologically ordered
realized net trade PnL. Blocks retain short-run dependence better than independent
trade shuffling. Reports include expected PnL, 5th/50th/95th percentiles,
probability of profit, 95th-percentile maximum drawdown, and a path digest.
Dataset, configuration, and seed therefore reproduce the same report.

## Stress

Stress scenarios reprice gross PnL, funding, fees, spread, slippage, borrowing,
other costs, and fixed per-trade shock losses independently. This supports
doubled-slippage, fee widening, funding reversal, outage, and gap scenarios
without hiding the affected PnL component.

## Promotion gates

ResearchGateReport is fail-closed without completed walk-forward folds. It checks
positive out-of-sample PnL, profit factor, expectancy measured against the
initial risk of every trade, annualized daily Sharpe, trade-path drawdown,
profitable validation-window percentage, maximum positive-PnL share of one
strategy, total cost share of positive gross alpha, and profitability after
doubling slippage. Threshold defaults match config/v1_acceptance.yaml.

These controls validate research mechanics. They do not themselves authorize
paper-to-live promotion; runtime, security, accounting, reconciliation, canary,
paper-duration, and explicit operator authorization gates must also pass.