# Do Hyperliquid's best perp traders keep winning? (round 13, research only)

## Question

Every Hyperliquid account's PnL history and open positions are public. If the accounts that made the
most last month keep beating the rest next month, a small account could follow their positions.
Round 13 is the gate: does that persistence exist at all?

## Data

- **Leaderboard snapshot, 2026-10-31.** 48,241 accounts, of which 2,854 are worth at least $1M.
- **Portfolio download.** `download_portfolio.py` fetched the `portfolio` reply of all 2,854
  accounts, one call every 4 s, with no errors.
- **Sample.** The test drops:
  - the 10 largest accounts;
  - 1,217 accounts whose perp PnL is zero throughout (spot only);
  - 44 market makers.

  That leaves 1,583 accounts, 1,441 of them with at least 90 days of perp history.

## Rules

The rules are in the `test_persist.py` docstring. They were written while the download was still
running, before any PnL was read.

- **Dates.** Every Wednesday.
- **Ranking.** Accounts are ranked by past 30-day perp ROI. The test compares the top decile's next
  30-day ROI with the median.
- **Pass bar.** The mean must be at least +2 pp a month, and a block-bootstrap 95% CI must exclude
  zero.

`selftest_persist.py` caught one trap before the real run. Interpolating cumulative PnL splits one
step between two adjacent windows and fakes persistence. PnL is therefore read at observed points.

## Result: PASS (gate only)

| Measure | Value |
|---|---|
| Dates | 122, from 2024-06-05 to 2026-09-30 |
| Eligible accounts per date (median) | 761 |
| Top decile minus median, next 30 days | **+4.86 pp**, 95% CI [+1.02, +9.88] |
| Bottom decile minus median | −1.19 pp |

## What the pass is made of

`diag_persist.py` → `diag_persist.txt`. These checks are descriptive and add no trial.

**Not a few outliers.** The median-based statistic is +4.68 pp (CI [+1.42, +9.27]), and the 10%
trimmed one is +5.34 pp. Dropping only the single best account per date leaves +2.80 pp, whose CI
includes zero.

**Absolute numbers.** The top decile's past month averaged +47%. Its next month averaged +4.86%,
and 57% of those accounts made money then, against 34% of all accounts.

**High churn.** Only 32% of the top decile is still in it four weeks later.

**Not market direction.** The correlation with BTC over the outcome month is −0.08. The edge was
larger when BTC fell (+8.50 pp) than when it rose (+2.03 pp).

**The edge fades toward the present:**

| Half-year | Dates | Top − median | Eligible |
|---|---|---|---|
| 2024 H2 | 26 | +7.60 pp | 127 |
| 2025 H1 | 26 | +6.80 pp | 610 |
| 2025 H2 | 27 | +5.63 pp | 813 |
| 2026 H1 | 25 | +0.99 pp | 1,051 |
| 2026 H2 | 14 | +2.08 pp | 1,315 |
| Last 6 months before the snapshot | 22 | **+1.43 pp** | |

**Why the fading matters.** The sample is the accounts worth at least $1M on 2026-10-31.

- **Old dates.** To appear on a 2024 date, an account had to survive until 2026-10. That is the
  strongest survivorship selection.
- **Recent dates.** These need only a few months of survival.

So the recent +1.4 pp is the least biased estimate, and it is below the +2 pp bar. It is also the
traders' own return, at their own leverage and with their own timing. Copying their positions once
a day can capture only part of it.

## Verdict

H passed, so the pre-registered next step follows: a forward test. The diagnostics say to expect
little from it. It is still cheap, and it is the only measurement free of survivorship.

## Forward series (`fwd_smart.py`, since 2026-11-01)

The rules are pre-registered in the `fwd_smart.py` docstring, written before any forward position
or price was looked at.

**Weekly ranking.** Every Wednesday the series re-downloads the `portfolio` replies of:
- the live ≥ $1M accounts;
- every member of the last five sets.

It ranks them by past 30-day perp ROI, using the same formula on the finer `perpMonth` series.

**Two sets** are frozen at each ranking:
- **smart:** the top decile;
- **control:** an equal-size random draw from the other eligible accounts.

**A. Persistence, measured forward.** Later rankings re-read each set member's PnL since its
ranking. The statistic is smart minus control.

**B. Copy ledgers.**
- **Inputs:** every member's `clearinghouseState`, polled every 15 minutes.
- **Score:** daily at 12:05 UTC, each coin is scored by the members' mean signed notional divided by
  account value.
- **Book:** the 5 coins with the largest |score| and at least 3 holders, 20% each (gross 1x).
- **Fills:** Binance USDT-M 1-minute open.
- **Costs:** 0.05% taker plus the `bt.slippage` tier, plus Binance funding.

**First ranking (2026-11-01, from the round 13 download).**
- 1,489 eligible accounts.
- Smart: 149 accounts with past ROI ≥ 22.9% a month (median 50%).
- Control: 149 random accounts.
- The first fill is 2026-11-01 12:05 UTC.

**Verdict on or after 2026-12-27.**
- The smart ledger at 1x must have a return ≥ 0 and a max drawdown ≤ 35%.
- It must beat the control.
- The leverage needed for 5% a month is reported, but not judged.

**Limits.** There is no development backtest of the copy rule, because no position history exists.
The drawdown is seen only at daily fills.

**Runs** in container `hl-smart-fwd` on the research VM. It reads public data only, places no orders,
and makes about 40 of the 1,200 Hyperliquid weight per minute, plus 300 during the Wednesday
download.

```bash
sh /opt/funding_arbitrage_research/hl_smart/run_fwd.sh         # selftest, (re)start
docker logs --tail 20 hl-smart-fwd
cat /opt/funding_arbitrage_research/hl_smart/fwd/report.json   # ledgers and persistence so far
```

## Registry

The registry stays at 572 trials (548 recorded plus 24 unrecorded). Round 13 tests a property of a
population, not a strategy with P&L. The smart ledger counts as one forward trial.

## Files

- **Download:** `download_portfolio.py`, `download.log`.
- **Test:** `test_persist.py`, `selftest_persist.py`.
- **Results:** `results_persist.json`, `persist_by_date.csv`, `persist.log`.
- **Diagnostics:** `diag_persist.py`, `diag_persist.txt`.
- **Forward:** `fwd_smart.py`, `selftest_fwd.py`, `run_fwd.sh`. Data: `fwd/` on the VM, holding
  `rank/<date>/`, `pos/<date>.jsonl`, `ledger_smart.jsonl`, `ledger_control.jsonl` and
  `report.json`.
