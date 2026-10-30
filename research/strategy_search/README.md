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
