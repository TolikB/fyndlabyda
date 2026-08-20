# V1 deterministic fill model

The backtest uses DeterministicFillModel by default. The model never assumes
that a missing or stale order book fills at zero cost.

## Execution contract

- Market and crossing limit orders consume visible side-specific depth and add
  deterministic spread, nonlinear impact, latency, and venue-specific taker fees.
- Passive limit and post-only orders require a price touch, consume queue-ahead
  first, respect participation and passive-fill ratios, and apply maker fees.
- Exchange minimum quantity/notional, post-only crossing, price-band, stale
  market, unavailable venue, and no-liquidity conditions reject explicitly.
- Partial market fills cancel the unfilled remainder. A partial multi-leg replay
  entry fails closed until an explicit unwind can be represented.
- Cancel races are deterministic and controlled by the documented tie policy.
- Funding cash flow uses signed exchange funding and the held position side:
  positive rates debit longs and credit shorts.
- Every replay fill records requested and filled notional, status, reason,
  fill count, latency, fees, spread, and modeled impact.

## Accounting and reproducibility

Replay entry and exit costs come from simulated fills rather than displayed
quotes. The final snapshot is reconciled through the same sorted event-ledger
aggregation used by BacktestEngine, including its Decimal rounding order.
Attempt IDs advance for every attempt, including rejected attempts, so event IDs
remain unique and deterministic.

BACKTEST_FILL_MODEL_ENABLED=false exists only for explicit legacy comparison.
Production-like research keeps it enabled.

## Known data boundary

The model can only be as accurate as the replay frames. Aggregated candle-derived
books use a conservative synthetic depth and impact model and must not be
presented as historical L2 reconstruction. Native L2 replay should supply the
same execution contract with real depth frames when those datasets are present.