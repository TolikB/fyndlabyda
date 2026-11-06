"""Round 32: hourly cross-sectional books at the costs of MEXC's zero-fee contracts (research only).

Pre-registered on 2026-11-06, before any series here was computed on these contracts. No orders are placed anywhere.

Why: round 8 found a pre-cost signal on hourly bars (Sharpe up to 1.8), and it died of costs: at about 3.8 bp gross per unit
of turnover it needs a cost of at most 1.5 bp, against the 5 bp fee plus slippage it paid. On MEXC's 0/0 contracts the cost
is half the spread. The venue is one the account holder may use; Lighter's terms name Ukraine.

Universe: the contracts at 0/0 with a median spread of at most 3 bp in ../xvenue/mexc_costs.json, mapped to Binance USDT-M
perps, whose hourly bars and funding stand in for MEXC's. A symbol trades once it has the history its score needs.
Signals, cross-sectional, as round 8: quintiles of the score, inverse-vol weights, 50/50 dollar-neutral (round6.xs_book),
signal at the close of hour h, trade at the open of h + 1, rebalance every 4 or 8 hours:
  donch20|trend  position in the 20-hour channel
  bb20|trend     20-hour Bollinger z-score
  cci20|trend    20-hour CCI
  samehour|trend mean return of the next K hours of the day over the past 14 days, K being the rebalance interval
Trials: 8 = 4 signals x {4, 8 hours}.

Vol target 40% a year, gross cap 3. Costs: no fee; half the contract's measured spread on every unit of turnover, 2x doubles
it. A third run adds a 2 bp taker fee, for contracts that move to 0/2, and is reported, not judged: the 0/0 list has moved
from 534 contracts to 105 and 144 in a month.

Periods: development 2021-01-01..2026-03-30; holdout 2026-03-30..2026-10-01, the last whole month of funding on file. The
daily pools of rounds 7 to 10 saw that calendar window, so a reader should weigh it as less fresh than an unseen one.

Bar on development: the bar of every round since 7 (Sharpe >= 1.0 at 1x, >= 0.7 at 2x, >= 60% of quarters positive, no calendar
year below -15%), plus a monthly mean >= 5% and a max drawdown <= 35% at 1x.
Holdout: monthly mean >= 5%, max drawdown <= 35%, mean > 0 at 2x, one look, only for trials that pass. A pass earns a forward
paper series on MEXC's own quotes.

Known limits: the universe is today's list, so it knows which coins lived. Today's spread stands in for every past year, and
spreads were wider in thin years. Binance prices stand in for MEXC's. Spread on an order for size is more than half the quote.

Usage (image funding-research:py312; /opt/funding_arbitrage_research mounted at /w; research/strategy_search is /w/code)
  python bt_hourly_mexc.py dev        results_hourly_mexc_dev.json, /w/pnl_round32.parquet
  python bt_hourly_mexc.py holdout    results_hourly_mexc_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
for path in ("/w/code", os.path.join(HERE, "..", "strategy_search")):
    if os.path.isdir(path):
        sys.path.insert(0, path)
_argv, sys.argv = sys.argv, sys.argv[:1]                # bt.py reads a data folder from argv at import
import bt  # noqa: E402
import bt_hourly as bh  # noqa: E402
import evalx  # noqa: E402
import features as f  # noqa: E402
from round6 import xs_book  # noqa: E402

sys.argv = _argv
COSTS = os.path.join(HERE, "..", "xvenue", "mexc_costs.json")
MAX_SPREAD = 3.0
ALIAS = {"TRUMPOFFICIAL": "TRUMP", "SILVER": "XAG"}
TARGET = 0.4
EVERY = (4, 8)
SAME_DAYS = 14
TAKER_STRESS = 2e-4
PERIODS = {"dev": ("2021-01-01", "2026-03-30"), "holdout": ("2026-03-30", "2026-10-01")}


def universe(costs, listed):
    """-> {Binance symbol: half spread as a fraction} for the 0/0 contracts under the spread cap."""
    out = {}
    for sym, c in costs.items():
        if c["taker_bp"] != 0 or c["maker_bp"] != 0 or c["spread_bp"] > MAX_SPREAD:
            continue
        base = sym[: -len("_USDT")]
        base = ALIAS.get(base, base)
        name = next((n for n in (f"{base}USDT", f"1000{base}USDT") if n in listed), None)
        if name:
            out[name] = c["spread_bp"] / 2 / 1e4
    return out


def same_hours(close, K, days=SAME_DAYS):
    """Mean log return of the K hours after this hour of the day, over the past `days` days."""
    lg = np.log(close)
    total = 0.0
    for d in range(1, days + 1):
        total = total + (lg.shift(24 * d - K) - lg.shift(24 * d))
    return total / days


def signals(px, every):
    h, lo, c = px["high"], px["low"], px["close"]
    return {"donch20|trend": f.donchian(h, lo, c, 20), "bb20|trend": f.bollinger_z(c, 20), "cci20|trend": f.cci(h, lo, c, 20),
            "samehour|trend": same_hours(c, every)}


def stats(pnl, lo, hi):
    x = pnl[(pnl.index >= lo) & (pnl.index < hi)]
    s = bt.stats(x)
    s["quarters_pos_pct"] = round(evalx.quarters_positive(x), 0)
    s["worst_year_pct"] = min(s["by_year_pct"].values())
    return s


def passes_dev(s1, s2):
    return bool(s1["sharpe"] >= 1.0 and s2["sharpe"] >= 0.7 and s1["quarters_pos_pct"] >= 60 and s1["worst_year_pct"] >= -15
                and s1["monthly_mean_pct"] >= 5 and s1["max_dd_pct"] <= 35)


def passes_holdout(s1, s2):
    return bool(s1["monthly_mean_pct"] >= 5 and s1["max_dd_pct"] <= 35 and s2["monthly_mean_pct"] > 0)


def main():
    np.seterr(all="ignore")
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    path = f"{HERE}/results_hourly_mexc_{mode}.json"
    keep = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_hourly_mexc_dev.json"))
        keep = {k for k, v in dev.items() if v["passes"]}
        if not keep:
            raise SystemExit("no trial passed development: the holdout stays closed")
    costs = json.load(open(COSTS))["contracts"]
    listed = set(pd.read_parquet(f"{bt.DATA}/klines_1h.parquet", columns=["symbol"])["symbol"].unique())
    half = universe(costs, listed)
    px = bh.load(("open", "high", "low", "close"), symbols=set(half))
    opn, close = px["open"], px["close"]
    cols = opn.columns
    live = opn.notna()
    funding = bh.funding_hourly(opn.index, cols)
    slip = pd.DataFrame(np.tile(pd.Series(half).reindex(cols).to_numpy(), (len(opn), 1)), index=opn.index, columns=cols)
    bt.FEE = 0.0
    lo, hi = (pd.Timestamp(x) for x in PERIODS[mode])
    print(f"{len(cols)} symbols, hours {opn.index.min()}..{opn.index.max()}, live at the start {int(live.loc[lo:].iloc[0].sum())}, "
          f"at the end {int(live.iloc[-1].sum())}, mean half spread {np.mean(list(half.values())) * 1e4:.2f} bp", flush=True)
    results, series = {}, {}
    for every in EVERY:
        for name, score in signals(px, every).items():
            key = f"{name}|h{every}"
            if keep is not None and key not in keep:
                continue
            w = bh.vol_target(bh.hold_every(xs_book(score, live, opn, long_high=True), every), opn, TARGET)
            runs = {}
            for tag, sl, mult in (("0x", slip, 0.0), ("1x", slip, 1.0), ("2x", slip, 2.0), ("taker", slip + TAKER_STRESS, 1.0)):
                pnl, parts = bh.run(w, opn, funding, sl, mult, close=close)
                runs[tag] = (bh.daily(pnl), parts)
            s = {tag: stats(runs[tag][0], lo, hi) for tag in runs}
            ok = passes_dev(s["1x"], s["2x"]) if keep is None else passes_holdout(s["1x"], s["2x"])
            turn = runs["1x"][1]["turnover_per_day"]
            results[key] = {"passes": ok, "turnover_per_day": round(float(turn), 2), **s}
            series[key] = runs["1x"][0]
            print(f"{key:<24} month {s['1x']['monthly_mean_pct']:>6}/{s['2x']['monthly_mean_pct']:>6}/{s['taker']['monthly_mean_pct']:>6} "
                  f"sh {s['0x']['sharpe']:>5}>{s['1x']['sharpe']:>5}/{s['2x']['sharpe']:>5} dd {s['1x']['max_dd_pct']:>5} "
                  f"q+ {s['1x']['quarters_pos_pct']:>3} worst {s['1x']['worst_year_pct']:>6} turn {turn:.2f} {'PASS' if ok else ''}", flush=True)
    with open(path, "w") as fh:
        json.dump(results, fh, indent=1, default=str)
    if mode == "dev":
        frame = pd.DataFrame(series)
        frame[(frame.index >= lo) & (frame.index < hi)].to_parquet("/w/pnl_round32.parquet")


if __name__ == "__main__":
    main()
