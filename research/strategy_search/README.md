# Directional strategy search and forward paper test

Research code, outside the application and its acceptance seal. It answers one question:
is there a Binance USDT-M strategy that earns 5–10% a month after costs?

It places no orders and holds no keys. It runs on the Contabo VM in a throwaway
container built from `Dockerfile` (`funding-research:py312`). Code, data and results
live in `/opt/funding_arbitrage_research/{code,data,fwd}`, and the container mounts
that directory as `/w`.

## Data

`download*.py` fetch the data.binance.vision archives: 1d and 1h klines, funding, and
daily metrics (OI, long/short ratios). The archives include delisted symbols, so there
is no survivorship bias.

Two traps:
- The archive keeps emitting flat zero-volume bars after a delisting. `bt.load` drops
  them.
- `download.py` missed the few symbols with non-ASCII names, such as 龙虾USDT.
  `fwd_update.py` fetches their full history from REST, because without it the
  listing sleeve takes them for new listings.

## Honesty rules (`bt.py`)

- **Timing.** The signal uses data up to the close of day t; the trade happens at the
  open of t+1 and earns the open-to-open return.
- **Universe.** Point in time: top N by 30-day median quote volume, at least 90 days
  of history.
- **Costs.** A 5 bp taker fee plus slippage tiered by liquidity (25/10/5/2 bp). Every
  run is repeated with double costs.
- **Funding.** The real funding history is used; the 00:00 settlement belongs to the
  previous day's position.
- **Delistings.** A delisted symbol is settled at its last close with a 2% penalty.
- **Holdout.** The last 6 months of the archive, 2026-03-30..09-30, opened once for a
  pre-chosen candidate.

`bt_selftest.py` and `evalx_selftest.py` check timing, costs, funding, delistings, stops,
the deflated Sharpe and PBO on synthetic data.

## Results so far (2026-10-26)

Rounds 1–5 ran 326 trials. `registry.py` gives a deflated Sharpe of 0.000 and a PBO of
0.51 over all of them.

The best development combination is `combo.py`, four sleeves:
- hedged short of new listings;
- top-20 momentum;
- top-20 funding carry;
- long after a pump.

| Run | Monthly mean | Max drawdown |
|---|---|---|
| Development, vol target 45% | +3.8% | 32% |
| Development, vol target 80% | +5.0% (median month +1.4%) | 42% |
| Holdout, vol target 45% | −0.7% | — |

Most of the development return comes from 2021; from 2022 on the result is roughly
flat. The combination has no proven edge.

## Forward paper test (since 2026-10-30)

The sleeves are frozen. Each day `fwd_update.py` appends the closed REST bars and
funding to the archive, then `fwd_run.py` rebuilds the sleeves. Two fresh paper
accounts start on 2026-10-01:

| Series | Vol target | Why this level |
|---|---|---|
| `combo5` | 80% | development mean of 5% a month |
| `combo45` | 45% | the candidate pre-registered for the holdout |

Execution is not simulated against order books: fills use the backtest cost model.

**Integrity**
- `fwd_check.py` proves the forward code reproduces the research sleeves bit for bit.
- A day's PnL is final once the next daily bar closes. `fwd/ledger.jsonl` keeps the
  first-reported value of every day and is never rewritten; a later recomputation that
  differs is logged as `REVISION`.
- October 2026 was computed retroactively on 2026-10-30, on data nobody had looked at.
- `fwd/ledger.discarded-archive-gap-20261030.jsonl` is a first run spoiled by the
  missing non-ASCII symbols, kept for the record.

**Running it**
- Daily: `fwd_daily.sh`.
- Schedule: `funding_arbitrage_research_forward.cron` goes into `/etc/cron.d`, at 00:20 UTC.
- Results: `fwd/report.json` (summary, book, trades) and `fwd/daily.log`.

## Round 6 (pre-registered 2026-10-30)

`round6.py` tests 8 trials:
- low volatility;
- top-trader and all-account positioning;
- taker flow;
- OI growth.

These use the daily metrics from `download_metrics.py`. The holdout is opened only for
trials that pass the development bar written in the module docstring. A trial that
passes the holdout joins the forward test as a new series, never inside `combo5` or
`combo45`.

**Data checks before the first run.** The daily metrics cover 2021-01..2026-09, 477
symbols. Two problems were fixed before any result was seen:
- Binance has no top-trader ratios for most of 2022. Each family is now gated on its
  own metric, not on the top-trader ratio.
- Zero values, such as OI = 0 on 193 rows, are treated as missing.

**Result (2026-10-30): no trial passes, and the holdout stays closed.**

| Trial | Monthly mean 1x / 2x | Sharpe 1x | Max DD 1x | Positive quarters |
|---|---|---|---|---|
| `crowd\|level` | +1.76% / +0.69% | 0.57 | 27.5% | 62% |
| `retail\|level` | +0.40% / −0.67% | 0.23 | 26.3% | 52% |
| `lowvol\|30` | +0.03% / −0.59% | 0.00 | 54.6% | 52% |
| `lowvol\|90` | −0.08% / −0.50% | −0.04 | 46.9% | 57% |
| `taker\|mom` | −2.12% / −4.16% | −1.21 | 80.0% | 19% |
| `taker\|rev` | −2.13% / −4.16% | −1.21 | 79.4% | 24% |
| `crowd\|chg7` | −2.85% / −5.50% | −1.46 | 91.6% | 33% |
| `oi\|chg7` | −3.80% / −6.91% | −2.19 | 92.3% | 10% |

- `crowd|level` is the best, but almost all of it is 2022 (+144%). That year the
  top-trader data covers only a handful of symbols. The other years compound to about −14%.
- The taker trials lose the same amount in both directions, so the signal is about zero
  and costs take about 2% a month.

The registry now holds 342 trials. The best is still the listing short: deflated Sharpe
0.000, PBO 0.51.

## Rounds 7–10: indicators and oscillators (2026-10-31)

Each round's trials and its development bar were written in the module docstring before
the run. The bar: Sharpe ≥1.0 at 1x and ≥0.7 at 2x costs, ≥60% positive quarters, and
no calendar year worse than −15%.

New tools:
- `features.py`: causal indicators — RSI, Stochastic, Williams %R, CCI, MACD, Bollinger,
  ATR/Keltner, Donchian, DI/ADX, OBV, MFI, taker share and funding z-score.
- `bt_hourly.py`: the `bt.run` rules on hourly bars. `bt_hourly_selftest.py` proves that
  a once-a-day book reproduces `bt.run` exactly.
- `Dockerfile.ml` (`funding-research:py312-ml`): LightGBM for round 10. The cron keeps
  `py312`.

| Round | What | Trials | Passed the bar |
|---|---|---|---|
| 7 | 14 daily indicators × time-series/cross-section × trend/reversal | 56 | 0 |
| 7b | cross-sectional trend oscillators with turnover control | 12 | 4 |
| 7c | those 4 with lookbacks halved and doubled | 8 | all 4 robust |
| 8 | hourly oscillators and BTC-residual reversal, 4h/8h rebalance | 16 | 0 |
| 9 | regime filters on the 4: BTC 200d, BTC vol, funding | 12 | 6 |
| 10 | walk-forward ridge and LightGBM over 22 features | 4 | 0 |

**Main finding.** The cross-sectional "trend" reading of range oscillators has a pre-cost
Sharpe of 1.5–2.2. Examples: Donchian20 (close position in its 20-day range) and
Bollinger z. Its correlation with `xsmom` is only 0.2–0.4. At daily rebalance on top-60,
turnover takes everything. On top-30, with weekly rebalance or a hysteresis band, the
development result is +2.2–2.7% a month at a 20% vol target, Sharpe 1.2–1.4.

The funding filter (the book is off while median top-30 funding is negative) raises it to
Sharpe 1.44–1.58, with drawdowns of 19–26%.

**Hourly.** The pre-cost signal is there (Sharpe up to 1.8), but turnover is 3–8 times
equity a day, and taker costs make every trial lose 10–27% a month.

**ML.** LightGBM with a 1-day horizon: Sharpe 1.29 at 1x costs, 0.46 at 2x.

**Holdout (`holdout2.py`, opened once on 2026-10-31).** The pool is the 4 funding-filtered
books; the rule was fixed before rounds 8–10 finished.

| Portfolio | Vol target (dev DD ≤35%) | Development | Holdout 2026-04..09 |
|---|---|---|---|
| `osc` (pool, equal risk) | 50% | +2.38%/mo, DD 31.7% | −2.26%/mo, Sharpe −1.79 |
| `combo5s` (osc + 4 old sleeves) | 50% | +4.49%/mo, median +0.35%, DD 30% | +0.05%/mo, DD 23.6% |

| Holdout, raw 1x PnL at vol 20% | 6 months |
|---|---|
| `comp8|weekly|fund` | −10.5% |
| `donch|buffer|fund` | +3.4% |
| `donch|weekly|fund` | −7.1% |
| `donch|ema5buf|fund` | −9.1% |
| old `carry` / `pumplong` / `xsmom` / `listshort` | +19.8% / +14.8% / +2.0% / −17.0% |

**Verdict: the success criterion (≥5% a month on the holdout with DD ≤35%) is not met.**
- The oscillator family, the best development result of 558 trials, lost on the holdout.
- On development, `osc` has a lower Sharpe (0.72) than its members because of the
  pre-registered drawdown brake. At 50% vol it cuts leverage to a quarter for long
  periods. The members' raw holdout PnL does not depend on that.
- The registry holds 558 trials: PBO 0.15, deflated Sharpe 0.000. The hourly trials'
  Sharpes (−5…−30) inflate the spread the deflation uses.
