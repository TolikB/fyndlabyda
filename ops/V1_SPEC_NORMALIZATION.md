# V1 Specification Normalization

This document resolves formatting loss and scope ambiguity in the approved
2026-08-15 specification. It does not reduce scope. The machine-readable source
of delivery status is `config/v1_acceptance.yaml`.

## One-release scope

- The original V1 list, original exclusions, and original V2 list are all required
  in this single V1.
- Milestones and release candidates may be deployed to Backtest, Replay, Shadow,
  or Paper independently, but no capability is deferred to a later product version.
- Existing funding support for Bybit, Gate, OKX, Binance, Hyperliquid, MEXC,
  KuCoin, and HTX remains in scope and migrates onto the canonical contracts.
- Cross-exchange lead-lag has two explicit consumers: a non-trading confidence
  filter and a separately risked executable statistical-arbitrage strategy.
- Dangerous capabilities are complete only when implemented and tested, but they
  remain disabled by default. Enabling live orders or money movement requires a
  separate operator authorization recorded in immutable audit.

## Restored equations

The pasted specification lost parts of several LaTeX blocks. The following
equations are the normative V1 forms. Any later change requires a manifest-linked
architecture decision record and deterministic regression fixtures.

### Order Flow Imbalance

For best bid `(P_b, Q_b)` and best ask `(P_a, Q_a)` update `n`:

```text
e_n = I(P_b[n] >= P_b[n-1]) * Q_b[n]
    - I(P_b[n] <= P_b[n-1]) * Q_b[n-1]
    - I(P_a[n] <= P_a[n-1]) * Q_a[n]
    + I(P_a[n] >= P_a[n-1]) * Q_a[n-1]

OFI_N = sum(e_n, n=1..N)
NormalizedOFI_N = OFI_N / max(Epsilon, LocalDepth_N)
```

The engine must maintain 1 s, 5 s, and 30 s windows and a leakage-safe rolling
Z-score. A gap or stale book invalidates the affected window until rebuilt.

### Microprice and book imbalance

```text
Microprice = (AskPrice * BidQty + BidPrice * AskQty) / (BidQty + AskQty)
BookImbalance_k = (sum(BidQty_k) - sum(AskQty_k))
                  / (sum(BidQty_k) + sum(AskQty_k))
```

Zero depth yields `UNAVAILABLE`, never a numeric zero signal.

### Directional risk and position size

```text
RiskCash = Equity
           * BaseRisk
           * QualityMultiplier
           * RegimeMultiplier
           * DrawdownMultiplier
           * CorrelationMultiplier

LinearQuantity = RiskCash / abs(EntryPrice - StopPrice)

FinalQuantity = floor_to_step(min(
    LinearQuantity,
    MarginBasedQuantity,
    LiquidityBasedQuantity,
    VenueLimitQuantity,
    PortfolioLimitQuantity
))
```

For a long directional position:

```text
StopDistance = max(EntryPrice - StructuralLow, ATRMultiplier * ATR)
```

The short formula is symmetric. Invalid or zero stop distance rejects the intent.

### Expected funding and basis carry

For every exact settlement event expected inside the holding horizon:

```text
ExpectedFundingIncome = sum(
    -PerpSignedNotional_i * ForecastFundingRate_i
)

ExpectedBasisPnL = ExpectedExitBasisValue - EntryBasisValue

ExpectedNetCarry = ExpectedFundingIncome
                   + ExpectedBasisPnL
                   - EntryFees
                   - ExitFees
                   - EntrySlippage
                   - ExitSlippage
                   - BorrowCost
                   - CapitalCost
                   - GasAndTransferCost
                   - OperationalRiskBuffer

CarryToCostRatio = ExpectedGrossCarry / max(Epsilon, ExpectedAllInCost)
```

Settlement timestamps and intervals come from each instrument and venue. Missing
funding history, borrow availability, book depth, or fee data fails closed.

### Lead-lag fair value

```text
Weight_i = LiquidityScore_i * DataQuality_i * Freshness_i
FairPrice = WeightedMedian(MidPrice_i, Microprice_i; Weight_i)
DeviationBps = 10_000 * (PrimaryMicroprice - FairPrice) / FairPrice
```

At least two independent valid reference venues are required. The executable
strategy has its own all-in-cost, inventory, transfer, and legging-risk gates.

### All-in cost and entry edge

```text
AllInCost = MakerTakerFees
            + Spread
            + ExpectedSlippage
            + ExpectedAdverseSelection
            + FundingCost
            + BorrowCost
            + GasAndTransferCost

ExpectedMove >= MinEdgeToCostRatio * AllInCost
```

The default `MinEdgeToCostRatio` remains 2.5 unless strategy-specific research
passes the same out-of-sample gates.

### System objective

```text
NetPnLAfterAllCosts = GrossTradingPnL
                      + FundingPnL
                      + BasisPnL
                      + OptionPnL
                      + MarketMakingPnL
                      + DEXAndMEVPnL
                      - Fees
                      - SpreadCost
                      - Slippage
                      - BorrowCost
                      - FundingCost
                      - GasAndTransferCost
                      - OperationalLosses

Objective = maximize risk-adjusted NetPnLAfterAllCosts
```

The optimization is constrained by drawdown, tail loss, liquidity, margin,
correlation, data quality, execution reliability, and all kill switches. Gross
PnL or displayed APR alone can never satisfy the objective.

## Low-latency boundary

The Python control plane owns configuration, strategy coordination, audit, and
portfolio risk. A sub-10 ms claim requires a measured native execution and market
data path, co-located infrastructure, synchronized clocks, exchange-qualified
feeds, and P99 telemetry. A normal VPS or Python-loop benchmark is not evidence.

## Evidence policy

`implemented` means code and focused tests. `validated` additionally requires
integration, replay, sandbox, or failure-injection evidence. `accepted` requires
all configured thresholds and elapsed-time gates. Presence of a class, endpoint,
dashboard, or green narrow test is not sufficient evidence for V1 completion.
