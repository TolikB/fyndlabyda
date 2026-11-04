"""Round 23: do big Hyperliquid accounts lead small ones across coins? (research only)

Pre-registered on 2026-11-04. The collected positions have only been counted (status.json, folder
sizes); none has been read by coin, and this file was checked on synthetic data only
(selftest_skew.py). No orders are placed anywhere.

Idea: every account's positions are public. Where big accounts lean one way against small ones, the
big side may know more ("follow"), or may already have moved the price ("fade"). Both are tested.

Snapshots: every 4 h on the UTC clock. Coins: the 40 live perps with the highest day_ntl_vlm in the
latest ctx row at or before the snapshot.
Positions at t: each address's latest poll, kept by test_liqmap.live_observations (age <= 1 h for total
notional >= $1M, <= 6 h for >= $100k, <= 24 h otherwise). Big: total notional >= $1M at that poll.
Small: total notional < $100k. Middling accounts are left out.
Signal: X = (signed notional of big - signed notional of small) / open interest in USD (ctx oi * mark).
Book: top and bottom quintile of X (k = max(3, n // 5)), equal weights, each leg summing to 1 in absolute
value; follow buys the top and shorts the bottom, fade does the opposite. Flat with fewer than 15 coins.
Rebalance: H in {4 h, 24 h}; H24 rebalances at the 00:00 UTC snapshot.
Fill: the first 1-minute mid at or after t + 1 minute. A coin with no mid within 5 minutes of the fill
earns nothing over that period.
Costs: 7.5 bp on every unit of turnover (4.5 bp taker plus 3 bp spread), hourly funding from ctx on the
held book, and everything rerun at 2x costs.
Size: the book scaled by s in {1, 2, 3}.

Trials: 4 = {follow, fade} x {H4, H24}

Periods (by the snapshot that opens the position): development 2026-11-02..2026-11-14; holdout
2026-11-15..2026-11-28. Data before 11-02 is left out: the collector was still reaching every account.

Bar on development, daily sums of net returns
  - mean > 0 with t >= 2.5 over days, at s = 1 and 1x costs
  - s* = the smallest s with 30 x the daily mean >= 5%; the max drawdown at s* must be <= 35%
  - the daily mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass. Two weeks is short,
so a pass only earns a forward paper series, whatever the numbers.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research/hl_liqmap mounted at /w/hl_liqmap)
  python test_skew.py dev       after 2026-11-16 00:00 UTC: results_skew_dev.json, pnl_round23.parquet
  python test_skew.py holdout   after 2026-11-30 00:00 UTC: results_skew_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import os
import sys

import numpy as np
import pandas as pd

import test_liqmap as tl

HERE = os.path.dirname(os.path.abspath(__file__))
MIN, HOUR, DAY = tl.MIN, tl.HOUR, tl.DAY
STEP = 4 * HOUR
LAG = MIN
STALE = 5 * MIN
TOP = 40
BIG, SMALL = 1e6, 1e5
MIN_COINS = 15
COST = 0.00075
HS = (4, 24)
SIDES = {"follow": 1, "fade": -1}
SIZES = (1, 2, 3)
PERIODS = {"dev": ("2026-11-02", "2026-11-15"), "holdout": ("2026-11-15", "2026-11-29")}


def ms(day):
    return int(pd.Timestamp(day, tz="UTC").value // 1_000_000)


def signal(acct, pos, ctx, t):
    """Series of X by coin at snapshot t; NaN where open interest is unknown."""
    c = ctx[ctx["ts"] <= t]
    if c.empty:
        return pd.Series(dtype=float)
    top = c.sort_values("ts").groupby("coin").tail(1).nlargest(TOP, "day_ntl_vlm").set_index("coin")
    oi = (top["oi"] * top["mark"]).where(lambda v: v > 0)
    a = acct.merge(tl.live_observations(acct, t), on=["addr", "ts"]).drop_duplicates(["addr", "ts"])
    live = pos.merge(a[["addr", "ts", "total_ntl"]], on=["addr", "ts"])
    signed = np.sign(live["szi"]) * live["value"].abs()
    big = signed[live["total_ntl"] >= BIG].groupby(live["coin"]).sum().reindex(oi.index).fillna(0.0)
    small = signed[live["total_ntl"] < SMALL].groupby(live["coin"]).sum().reindex(oi.index).fillna(0.0)
    return (big - small) / oi


def weights(x, side):
    """Target weights by coin; empty when there are too few coins."""
    x = x.dropna()
    if len(x) < MIN_COINS:
        return pd.Series(dtype=float)
    k = max(3, len(x) // 5)
    ranked = x.sort_values(kind="stable")
    w = pd.Series(0.0, index=ranked.index)
    w[ranked.index[-k:]] = side / k
    w[ranked.index[:k]] = -side / k
    return w[w != 0]


def fill_px(mid_by, coin, t):
    """First mid at or after t + LAG, or None if it is stale."""
    if coin not in mid_by:
        return None
    m_t, m_px = mid_by[coin]
    i = np.searchsorted(m_t, t + LAG, "left")
    return float(m_px[i]) if i < len(m_t) and m_t[i] - (t + LAG) <= STALE else None


def funding_cost(w, ctx, t0, t1):
    """Funding paid over [t0, t1) by book w: mean hourly rate of each coin x hours held x weight."""
    if w.empty:
        return 0.0
    c = ctx[(ctx["ts"] >= t0) & (ctx["ts"] < t1) & ctx["coin"].isin(w.index)]
    rate = c.groupby("coin")["funding"].mean().reindex(w.index).fillna(0.0)
    return float((w * rate).sum() * (t1 - t0) / HOUR)


def period_pnl(sig, mid_by, ctx, H, side, cost=COST):
    """sig: {t: X}. -> Series of net return by opening snapshot; the last open period is dropped."""
    times = sorted(t for t in sig if H == 4 or t % DAY == 0)
    out, prev = {}, pd.Series(dtype=float)
    for t0, t1 in zip(times, times[1:]):
        w = weights(sig[t0], side)
        gross = 0.0
        for coin, wt in w.items():
            a, b = fill_px(mid_by, coin, t0), fill_px(mid_by, coin, t1)
            if a and b:
                gross += wt * (b / a - 1)
        turnover = float(w.reindex(w.index.union(prev.index), fill_value=0.0)
                         .sub(prev.reindex(w.index.union(prev.index), fill_value=0.0)).abs().sum())
        out[t0] = gross - funding_cost(w, ctx, t0, t1) - cost * turnover
        prev = w
    return pd.Series(out, dtype=float)


def daily(pnl):
    return pnl.groupby(pnl.index // DAY * DAY).sum()


def stats(d, s=1):
    d = d * s
    eq = (1 + d).cumprod()
    t = float(d.mean() / d.std() * math.sqrt(len(d))) if len(d) > 2 and d.std() > 0 else 0.0
    return {"days": len(d), "month_pct": round(float(d.mean()) * 30 * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - eq / eq.cummax()).max()) * 100, 2)}


def passes(d1, d2, size=None):
    if not (d1.mean() > 0 and stats(d1)["t"] >= 2.5):
        return False, None
    if size is None:
        size = next((s for s in SIZES if stats(d1, s)["month_pct"] >= 5), None)
    if size is None:
        return False, None
    a = stats(d1, size)
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and d2.mean() > 0), size


def collect(period):
    lo, hi = (ms(x) for x in PERIODS[period])
    end = hi + DAY                                      # the last positions need a closing snapshot
    mid_by = tl.mid_series(tl.read("mids", lo, end + HOUR))
    ctx = tl.read("ctx", lo - DAY, end + HOUR)
    sig = {}
    for d0 in range(lo, end, DAY):
        acct = tl.read("acct", d0 - DAY, d0 + DAY)
        pos = tl.read("pos", d0 - DAY, d0 + DAY)
        for t in range(d0, min(d0 + DAY, end), STEP):
            sig[t] = signal(acct, pos, ctx, t)
        print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d}", flush=True)
    return sig, mid_by, ctx, lo, hi


def evaluate(period, keys=None, sizes=None):
    sig, mid_by, ctx, lo, hi = collect(period)
    out, cols = {}, {}
    for side_name, side in SIDES.items():
        for H in HS:
            key = f"hlskew|{side_name}|h{H}"
            if keys is not None and key not in keys:
                continue
            p1, p2 = (period_pnl(sig, mid_by, ctx, H, side, COST * m) for m in (1, 2))
            d1, d2 = (daily(p[(p.index >= lo) & (p.index < hi)]) for p in (p1, p2))
            ok, size = passes(d1, d2, None if sizes is None else sizes[key])
            out[key] = {"1x": stats(d1), "2x": stats(d2), "passes": ok, "size": size}
            cols[key] = d1
    frame = pd.DataFrame(cols)
    frame.index = pd.to_datetime(frame.index, unit="ms")
    return out, frame


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl = evaluate("dev")
        pnl.to_parquet(f"{HERE}/pnl_round23.parquet")
        with open(f"{HERE}/results_skew_dev.json", "w") as fh:
            json.dump(out, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_skew_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_skew_dev.json"))
        ok = {k: v["size"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = evaluate("holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
