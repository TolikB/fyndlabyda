"""Round 33: cross-sectional reversal over minutes at the costs of MEXC's zero-fee contracts (research only).

Pre-registered on 2026-11-06, before these bars were downloaded. No orders are placed anywhere.

Why: rounds 8 and 26 tested reversal one coin at a time, or across a book every 4 hours. Here each period holds the coins that
moved most against the market, so the market's own move is out and a number of small bets add up. On a zero-fee contract
only the spread stands in the way. The minute-level holdout 2025-07..2026-10 has not been opened by any round.

Universe: bt_hourly_mexc.universe, today's 0/0 list under 3 bp, on Binance's minute bars.
Period: every H minutes on the UTC grid, H in {5, 15, 60}.
Score at the end of minute t: the coin's return over the past H minutes from closes, less the mean of all coins.
Trade: long the N lowest and short the N highest, N in {5, 10}, equal weight, with at least 2 x N coins on the grid.
Fills: VWAP of minute t + 1 in and of minute t + H + 1 out. A close can sit on the bid or the ask, so a score built from closes
would seem to reverse by itself; VWAP sits near the mid, so that bounce earns nothing (selftest_xs_minute.py).
Costs: every leg pays its half spread in and out, on every period, though a coin that stays in the book would not. 2x doubles
that. A third run adds a 2 bp taker fee a side and is reported, not judged. Funding is ignored over at most an hour.
Book: each side holds s / 2 of equity, s in {1, 2, 3}, flat, added up daily.

Trials: 6 = H {5, 15, 60} x N {5, 10}.

Periods: development 2024-01-01..2025-07-01; holdout 2025-07-01..2026-11-01.

Bar on development
  - mean daily return > 0 with t >= 2.5 at s = 1 and 1x costs
  - s* = the smallest s with 30 x the mean >= 5%; max drawdown at s* <= 35%
  - the mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass. A pass earns only a forward paper series.

Known limits: the list knows which coins lived, and today's spreads stand in for the past. Binance prices stand in for MEXC's.
A VWAP over a minute is not a fill at the start of that minute.

Usage (image funding-research:py312; /opt/funding_arbitrage_research mounted at /w)
  python download_minute.py          /w/data/minute_mexc/<SYMBOL>/<YYYY-MM>.parquet
  python bt_xs_minute.py dev         results_xs_minute_dev.json, /w/pnl_round33.parquet
  python bt_xs_minute.py holdout     results_xs_minute_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = "/w/data/minute_mexc"
COSTS = os.path.join(HERE, "..", "xvenue", "mexc_costs.json")
HS = {"h5": 5, "h15": 15, "h60": 60}
NS = {"n5": 5, "n10": 10}
SIZES = (1, 2, 3)
TAKER_STRESS = 2e-4
PERIODS = {"dev": ("2024-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-11-01")}
MIN_MS = 60_000


def load(lo, hi, symbols):
    """-> (minute stamps in ms, close, vwap as minutes x symbols, float32), NaN where a coin has no bar."""
    t0, t1 = (int(pd.Timestamp(x, tz="UTC").value // 1_000_000) for x in (lo, hi))
    n = (t1 - t0) // MIN_MS
    close, vwap = np.full((n, len(symbols)), np.nan, "float32"), np.full((n, len(symbols)), np.nan, "float32")
    months = {f"{m:%Y-%m}" for m in pd.date_range(lo, hi, freq="MS", inclusive="left")}
    for j, sym in enumerate(symbols):
        for path in sorted(glob.glob(f"{DATA}/{sym}/*.parquet")):
            if os.path.basename(path)[:7] not in months:
                continue
            d = pd.read_parquet(path)
            i = ((d["t"].to_numpy(np.int64) - t0) // MIN_MS)
            ok = (i >= 0) & (i < n)
            close[i[ok], j] = d["c"].to_numpy("float32")[ok]
            vwap[i[ok], j] = d["vw"].to_numpy("float32")[ok]
    return t0 + np.arange(n) * MIN_MS, close, vwap


def period_returns(close, vwap, half, H, N):
    """-> (row of each period's signal, gross per unit a side, spread cost per unit a side)."""
    idx = np.arange(H, len(close) - H - 1, H)
    past = close[idx] / close[idx - H] - 1
    ret = vwap[idx + H + 1] / vwap[idx + 1] - 1
    with np.errstate(all="ignore"):
        past = past - np.nanmean(past, axis=1, keepdims=True)
    valid = np.isfinite(past) & np.isfinite(ret)
    past = np.where(valid, past, np.nan)
    order = np.argsort(past, axis=1)                    # NaN goes last
    count = valid.sum(axis=1)
    keep = np.flatnonzero(count >= 2 * N)
    gross, cost = np.empty(len(keep)), np.empty(len(keep))
    for k, i in enumerate(keep):
        lows, highs = order[i, :N], order[i, count[i] - N:count[i]]
        gross[k] = ret[i, lows].mean() - ret[i, highs].mean()
        cost[k] = 2 * (half[lows].mean() + half[highs].mean())
    return idx[keep], gross, cost


def daily_book(stamp, idx, gross, cost, mult, extra, days):
    """-> daily return at s = 1: each side holds 0.5 of equity."""
    net = 0.5 * (gross - mult * cost - 4 * extra)
    day = pd.to_datetime(stamp[idx], unit="ms").normalize()
    return pd.Series(net).groupby(day).sum().reindex(days, fill_value=0.0)


def stats(d, s=1):
    x = d * s
    curve = 1 + x.cumsum()
    t = float(x.mean() / x.std() * math.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0
    return {"days": len(x), "month_pct": round(float(x.mean()) * 30 * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - curve / curve.cummax()).max()) * 100, 2)}


def passes(d1, d2, s=None):
    if not (d1.mean() > 0 and stats(d1)["t"] >= 2.5):
        return False, None
    if s is None:
        s = next((x for x in SIZES if stats(d1, x)["month_pct"] >= 5), None)
    if s is None:
        return False, None
    a = stats(d1, s)
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and d2.mean() > 0), s


def main():
    import bt_hourly_mexc as u                          # only here: it needs the strategy-search code

    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    path = f"{HERE}/results_xs_minute_{mode}.json"
    chosen = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_xs_minute_dev.json"))
        chosen = {k: v["s"] for k, v in dev.items() if v["passes"]}
        if not chosen:
            raise SystemExit("no trial passed development: the holdout stays closed")
    listed = {os.path.basename(p) for p in glob.glob(f"{DATA}/*")}
    half = u.universe(json.load(open(COSTS))["contracts"], listed)
    symbols = sorted(half)
    hs = np.array([half[s] for s in symbols])
    lo, hi = PERIODS[mode]
    stamp, close, vwap = load(lo, hi, symbols)
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    print(f"{len(symbols)} symbols, {len(stamp):,} minutes, median coins with a bar {int(np.median(np.isfinite(close).sum(axis=1)))}", flush=True)
    out, cols = {}, {}
    for hh, H in HS.items():
        for nn, N in NS.items():
            key = f"xs|{hh}|{nn}"
            if chosen is not None and key not in chosen:
                continue
            idx, gross, cost = period_returns(close, vwap, hs, H, N)
            d1, d2 = daily_book(stamp, idx, gross, cost, 1.0, 0.0, days), daily_book(stamp, idx, gross, cost, 2.0, 0.0, days)
            d3 = daily_book(stamp, idx, gross, cost, 1.0, TAKER_STRESS, days)
            ok, s = passes(d1, d2, None if chosen is None else chosen[key])
            out[key] = {"periods": len(idx), "gross_bp": round(float(gross.mean()) * 1e4, 2), "cost_bp": round(float(cost.mean()) * 1e4, 2),
                        "gross_pos": round(float((gross > 0).mean()), 3), "1x": {str(x): stats(d1, x) for x in SIZES},
                        "2x": {str(x): stats(d2, x) for x in SIZES}, "taker": stats(d3), "passes": ok, "s": s}
            cols[key] = d1
            print(key, json.dumps(out[key]), flush=True)
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode == "dev":
        pd.DataFrame(cols).to_parquet("/w/pnl_round33.parquet")


if __name__ == "__main__":
    main()
