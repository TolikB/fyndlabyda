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
