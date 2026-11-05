"""Round 26: with fees near zero, is there a minute-scale edge on the tape? (research only)

Pre-registered on 2026-11-05, before any of these series was computed. selftest_tape.py checks this file on
synthetic minutes only. No orders are placed anywhere.

Idea: 593 trials paid 7.5 bp or more a unit of turnover. Lighter charges standard accounts nothing and quotes BTC
and ETH under 1 bp wide, and MEXC charges nothing on DOGE, LINK, ADA and ZEC. A very short signal too small to
matter at 7.5 bp may then be worth trading. Binance's 1-minute bars stand in for the price on those venues.

Assets (trades per asset do not overlap): BTC, ETH, SOL (Lighter); DOGE, LINK, ADA, ZEC (MEXC).
Signals at the close of minute t, over the last L = H minutes:
  ret    log return
  ofi    (2 x taker-buy quote volume - quote volume) / quote volume, summed over L minutes
  lead   BTC's log return over L minutes, for the other six assets
Each is scored against its own trailing std over 1,440 minutes, ending at t - 1. A bar with a time gap voids the
signal for L minutes.
Entry: when flat and |z| >= Z, in the direction of z (follow) or against it (fade), at the open of minute t + 1.
A real order would land about a second later; the 2x-cost run stands in for that.
Exit: at the open of minute t + 1 + H.
Costs: half the median quoted spread per side, from COST_BP (measured by xvenue/pilot.py on the listed venue),
which is also the whole cost because the fee is zero. Funding is ignored: holds are at most 15 minutes.
Size: each asset gets 1/7 of s x equity. s in {1, 2, 3, 5}.

Trials: 36 = {ret, ofi, lead} x {follow, fade} x H {1, 5, 15} x Z {2, 3}.

Periods: development 2022-01..2025-06, holdout 2025-07..2026-10. The holdout is opened once, only for trials that
pass, and it has not been used for any minute-level signal.

Bar on development, on daily sums of net returns:
  - mean > 0 with t >= 3 at s = 1 and 1x costs
  - s* = the smallest s with 30 x the daily mean >= 5%; the max drawdown at s* must be <= 35%
  - the daily mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development. A pass earns only a forward paper series on the real book.

Usage (image funding-research:py312, /opt/funding_arbitrage_research mounted at /w)
  python bt_tape.py dev        results_tape_dev.json, /w/pnl_round26.parquet
  python bt_tape.py holdout    results_tape_holdout.json (refuses a second time)
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
TAPE = "/w/data/tape_1m"
MIN, DAY = 60_000, 86_400_000
WINDOW = 1440
SIZES = (1, 2, 3, 5)
HS = (1, 5, 15)
ZS = (2.0, 3.0)
SIDES = {"follow": 1, "fade": -1}
SIGNALS = ("ret", "ofi", "lead")
ASSETS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "ADAUSDT", "ZECUSDT")
# half the median quoted spread per side, bp, floor 0.2, from xvenue/pilot.py on 2026-11-05 09:53 UTC:
# BTC 0.39, ETH 0.37, SOL 0.31 on Lighter; DOGE 1.30, LINK 0.86, ADA 5.02, ZEC 0.08 on MEXC
COST_BP = {"BTCUSDT": 0.2, "ETHUSDT": 0.2, "SOLUSDT": 0.2, "DOGEUSDT": 0.65, "LINKUSDT": 0.43, "ADAUSDT": 2.51,
           "ZECUSDT": 0.2}
PERIODS = {"dev": ("2022-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-11-01")}


def ms(day):
    return int(pd.Timestamp(day, tz="UTC").value // 1_000_000)


def raw_signal(f, name, L):
    """Signal value per bar, known at the bar's close."""
    if name == "ret":
        c = np.log(f["c"].to_numpy(float))
        out = np.full(len(c), np.nan)
        out[L:] = c[L:] - c[:-L]
        return out
    q = pd.Series(f["qv"].to_numpy(float))
    imb = pd.Series(f["tq"].to_numpy(float)) * 2 - q
    return (imb.rolling(L).sum() / q.rolling(L).sum()).to_numpy()


def zscore(x, window=WINDOW):
    """x over its trailing std; the std stops one bar before the bar scored."""
    sd = pd.Series(x).rolling(window, min_periods=window).std().shift(1).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        return x / sd


def continuous(t, L):
    """True where the last L bars, this one included, follow each other without a time gap."""
    ok = np.r_[True, np.diff(t) == MIN]
    bad = pd.Series(~ok).rolling(L, min_periods=1).sum().to_numpy()
    return bad == 0


def trades(z, Z, H, n):
    """-> (entry bars, exit bars, signs). z[t] is known at the close of t, so entry is bar t + 1."""
    idx = np.flatnonzero(np.abs(np.nan_to_num(z)) >= Z)
    ent, ext, sign = [], [], []
    free = 0
    for t in idx:
        i = t + 1
        if i < free or i + H >= n:
            continue
        ent.append(i)
        ext.append(i + H)
        sign.append(1 if z[t] > 0 else -1)
        free = i + H
    return np.array(ent, dtype=int), np.array(ext, dtype=int), np.array(sign, dtype=int)


def pnl_by_day(t, o, ent, ext, sign, side, cost):
    """Net return per UTC day, booked on the exit bar; a trade costs `cost` on each side."""
    if len(ent) == 0:
        return pd.Series(dtype=float)
    r = side * sign * (o[ext] / o[ent] - 1) - 2 * cost
    return pd.Series(r).groupby(t[ext] // DAY * DAY).sum()


def stats(d, s=1):
    d = d * s
    eq = (1 + d).cumprod()
    t = float(d.mean() / d.std() * math.sqrt(len(d))) if len(d) > 2 and d.std() > 0 else 0.0
    return {"days": len(d), "month_pct": round(float(d.mean()) * 30 * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - eq / eq.cummax()).max()) * 100, 2)}


def passes(d1, d2, size=None):
    if not (d1.mean() > 0 and stats(d1)["t"] >= 3):
        return False, None
    if size is None:
        size = next((s for s in SIZES if stats(d1, s)["month_pct"] >= 5), None)
    if size is None:
        return False, None
    a = stats(d1, size)
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and d2.mean() > 0), size


def load(sym):
    f = pd.concat([pd.read_parquet(p) for p in sorted(glob.glob(f"{TAPE}/{sym}/*.parquet"))], ignore_index=True)
    return f.drop_duplicates("t").sort_values("t").reset_index(drop=True)


def signal_z(f, name, L, btc):
    """z per bar of f; `lead` reads BTC's return at the same minute."""
    if name == "lead":
        x = pd.Series(btc[L], index=btc["t"]).reindex(f["t"].to_numpy()).to_numpy()
    else:
        x = raw_signal(f, name, L)
    z = zscore(x)
    z[~continuous(f["t"].to_numpy(), L + 1)] = np.nan
    return z


def asset_days(f, name, side, H, Z, cost, lo, hi, btc):
    t, o = f["t"].to_numpy(), f["o"].to_numpy(float)
    z = signal_z(f, name, H, btc)
    z[(t < lo) | (t >= hi - (H + 2) * MIN)] = np.nan
    return pnl_by_day(t, o, *trades(z, Z, H, len(t)), side, cost)


def run_period(period, keys=None, sizes=None):
    lo, hi = (ms(x) for x in PERIODS[period])
    frames = {s: load(s) for s in ASSETS}
    b = frames["BTCUSDT"]
    btc = {"t": b["t"].to_numpy(), **{H: raw_signal(b, "ret", H) for H in HS}}
    out, cols = {}, {}
    grid = pd.RangeIndex(lo, hi, DAY)
    for name in SIGNALS:
        for side_name, side in SIDES.items():
            for H in HS:
                for Z in ZS:
                    key = f"tape|{name}|{side_name}|h{H}|z{Z:g}"
                    if keys is not None and key not in keys:
                        continue
                    sums = {1: pd.Series(0.0, index=grid), 2: pd.Series(0.0, index=grid)}
                    n = 0
                    for sym in ASSETS:
                        if name == "lead" and sym == "BTCUSDT":
                            continue
                        for m in (1, 2):
                            d = asset_days(frames[sym], name, side, H, Z, COST_BP[sym] / 1e4 * m, lo, hi, btc)
                            sums[m] = sums[m].add(d / len(ASSETS), fill_value=0.0)
                    d1, d2 = sums[1], sums[2]
                    ok, size = passes(d1, d2, None if sizes is None else sizes[key])
                    out[key] = {"1x": stats(d1), "2x": stats(d2), "passes": ok, "size": size}
                    cols[key] = d1
                    print(key, out[key], flush=True)
    frame = pd.DataFrame(cols)
    frame.index = pd.to_datetime(frame.index, unit="ms")
    return out, frame


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl = run_period("dev")
        pnl.to_parquet("/w/pnl_round26.parquet")
        with open(f"{HERE}/results_tape_dev.json", "w") as fh:
            json.dump(out, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_tape_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_tape_dev.json"))
        ok = {k: v["size"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = run_period("holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
