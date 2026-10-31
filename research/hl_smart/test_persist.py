"""Do Hyperliquid's best perp traders keep beating the rest? (round 13, a gate test, research only)

Pre-registered on 2026-10-31 while download_portfolio.py was still running. Only the timestamp
structure of the first 81 downloaded histories was looked at, never their PnL. Nothing is tuned
after the first run. No orders are placed anywhere.

If the answer is yes, a forward paper series follows the top accounts' live positions from the
hl-liqmap collector. If no, there is nothing to copy.

Sample
  the leaderboard snapshot of 2026-10-31 (download_portfolio.py), accounts with accountValue >= $1M,
  minus the 10 largest (system, HLP and spot whales), minus accounts whose perpAllTime PnL is zero
  throughout, minus market makers (month volume > 200x account value in the snapshot)

Series per account (from the `portfolio` reply)
  P     perpAllTime pnlHistory: cumulative perp PnL in USD, read at the observation nearest to each
        target time (within 7 days). Both windows share the observation at d, so their PnL increments
        never overlap. (Interpolating P would split one step between the two windows and fake
        persistence; selftest_persist.py caught this before any real run, and the rule was changed.)
  V     allTime accountValueHistory, interpolated linearly; usable only when the observations before
        and after t are each within 14 days of t

Dates: every Wednesday 00:00 UTC whose 30-day outcome window ends by the snapshot
  past ROI   (P(d) - P(d-30)) / mean(V(d-30), V(d)), with both values >= $100k
  next ROI   (P(d+30) - P(d)) / mean(V(d), V(d+30))
  Each is scaled to 30 days by the span between its two observations (at least 10 days), then clipped
  to [-1, +1]. A date needs at least 50 eligible accounts.
  stat(d)    mean next ROI of the top decile by past ROI, minus the median next ROI of all eligible

Hypothesis H: the mean of stat(d) over the dates is >= +2 percentage points, and the 95% CI of a
moving-block bootstrap over the dates excludes zero. Blocks are 5 dates long, because 30-day
windows overlap about 4 weeks; 10,000 resamples.
Minimum data: at least 300 sample accounts with >= 90 days of perp history and at least 20 dates.
Otherwise the verdict is "insufficient data", and persistence is measured forward on hl-liqmap.

Caveat, recorded before the run: the sample holds accounts worth >= $1M TODAY. Accounts that blew
up in the past are missing (survivorship), and this can fake or hide persistence. H is therefore
only a gate for a forward test, never evidence by itself.

  python test_persist.py      results_persist.json, persist_by_date.csv
"""

from __future__ import annotations

import glob
import gzip
import json
import os

import numpy as np
import pandas as pd

ROOT = "/w/data/hl_portfolio"
HERE = os.path.dirname(os.path.abspath(__file__))
DAY = 86_400_000
WINDOW = 30 * DAY
MAX_GAP = 14 * DAY
SNAP = 7 * DAY
MIN_SPAN = 10 * DAY
MIN_V = 1e5
TOP_DROP, MM_TURNOVER = 10, 200.0
MIN_ACCOUNTS, MIN_HISTORY_DAYS, MIN_DATES, MIN_PER_DATE = 300, 90, 20, 50
BLOCK, RESAMPLES = 5, 10_000


def series(points):
    a = np.array([(p[0], float(p[1])) for p in points], float)
    return (a[:, 0], a[:, 1]) if len(a) else (np.array([]), np.array([]))


def at(ts, vs, t):
    """Linear interpolation at t, or nan when no observation lies within MAX_GAP on both sides."""
    if len(ts) == 0 or t < ts[0] or t > ts[-1]:
        return np.nan
    j = np.searchsorted(ts, t)
    if ts[j] == t:
        return vs[j]
    if t - ts[j - 1] > MAX_GAP or ts[j] - t > MAX_GAP:
        return np.nan
    w = (t - ts[j - 1]) / (ts[j] - ts[j - 1])
    return vs[j - 1] + w * (vs[j] - vs[j - 1])


def snap(ts, t):
    """Index of the observation nearest to t, or None when none lies within SNAP."""
    if len(ts) == 0:
        return None
    j = int(np.clip(np.searchsorted(ts, t), 1, len(ts) - 1)) if len(ts) > 1 else 0
    i = min((j - 1, j), key=lambda k: abs(ts[k] - t)) if len(ts) > 1 else 0
    return i if abs(ts[i] - t) <= SNAP else None


def roi(P, V, t0, t1, need_v=True):
    i0, i1 = snap(P[0], t0), snap(P[0], t1)
    if i0 is None or i1 is None:
        return np.nan
    span = P[0][i1] - P[0][i0]
    v0, v1 = at(*V, t0), at(*V, t1)
    if span < MIN_SPAN or not np.all(np.isfinite([v0, v1])):
        return np.nan
    if need_v and (v0 < MIN_V or v1 < MIN_V):
        return np.nan
    base = (v0 + v1) / 2
    if base <= 0:
        return np.nan
    return float(np.clip((P[1][i1] - P[1][i0]) / base * WINDOW / span, -1, 1))


def sample(board_rows, replies):
    """board_rows: leaderboard rows; replies: {addr: portfolio dict} -> {addr: (P, V)} and a filter log."""
    rows = sorted(board_rows, key=lambda r: -float(r["accountValue"]))
    big = [r for r in rows if float(r["accountValue"]) >= 1e6]
    log = {"snapshot_1m": len(big), "downloaded": 0, "top_dropped": 0, "zero_perp": 0, "market_makers": 0}
    out = {}
    for n, r in enumerate(big):
        addr = r["ethAddress"]
        if addr not in replies:
            continue
        log["downloaded"] += 1
        if n < TOP_DROP:
            log["top_dropped"] += 1
            continue
        perf = dict(r["windowPerformances"])
        if float(perf["month"]["vlm"]) > MM_TURNOVER * float(r["accountValue"]):
            log["market_makers"] += 1
            continue
        d = replies[addr]
        P, V = series(d["perpAllTime"]["pnlHistory"]), series(d["allTime"]["accountValueHistory"])
        if len(P[1]) == 0 or not np.any(P[1] != 0):
            log["zero_perp"] += 1
            continue
        out[addr] = (P, V)
    return out, log


def stats_by_date(accts, end_ms):
    first = min(P[0][0] for P, _ in accts.values()) + WINDOW
    dates = pd.date_range(pd.Timestamp(first, unit="ms").ceil("D"), pd.Timestamp(end_ms - WINDOW, unit="ms"),
                          freq="W-WED")
    rows = []
    for d in dates:
        t = d.value // 1_000_000
        past, nxt = [], []
        for P, V in accts.values():
            a = roi(P, V, t - WINDOW, t)
            if not np.isfinite(a):
                continue
            b = roi(P, V, t, t + WINDOW, need_v=False)
            if np.isfinite(b):
                past.append(a)
                nxt.append(b)
        if len(past) < MIN_PER_DATE:
            continue
        past, nxt = np.array(past), np.array(nxt)
        cut_hi, cut_lo = np.quantile(past, 0.9), np.quantile(past, 0.1)
        med = float(np.median(nxt))
        rows.append({"date": d, "n": len(past), "stat": float(nxt[past >= cut_hi].mean() - med),
                     "bottom": float(nxt[past <= cut_lo].mean() - med), "median_next": med})
    return pd.DataFrame(rows)


def block_bootstrap(x, block=BLOCK, n=RESAMPLES, seed=0):
    x = np.asarray(x, float)
    rng = np.random.default_rng(seed)
    k = int(np.ceil(len(x) / block))
    starts = rng.integers(0, len(x) - block + 1, size=(n, k))
    idx = (starts[:, :, None] + np.arange(block)).reshape(n, -1)[:, :len(x)]
    means = x[idx].mean(axis=1)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def main():
    board = json.load(gzip.open(f"{ROOT}/leaderboard.json.gz"))["leaderboardRows"]
    replies = {os.path.basename(f)[:-8]: dict(json.load(gzip.open(f))) for f in glob.glob(f"{ROOT}/portfolio/*.json.gz")}
    accts, log = sample(board, replies)
    deep = sum((P[0][-1] - P[0][0]) >= MIN_HISTORY_DAYS * DAY for P, _ in accts.values())
    end_ms = max(P[0][-1] for P, _ in accts.values())
    by_date = stats_by_date(accts, end_ms)
    res = {"filters": log, "sample": len(accts), "deep_history": int(deep), "dates": int(len(by_date))}
    if deep < MIN_ACCOUNTS or len(by_date) < MIN_DATES:
        res["verdict"] = "insufficient data"
    else:
        lo, hi = block_bootstrap(by_date["stat"])
        mean = float(by_date["stat"].mean())
        res.update({"mean_top_minus_median_pp": round(mean * 100, 2), "ci95_pp": [round(lo * 100, 2), round(hi * 100, 2)],
                    "mean_bottom_minus_median_pp": round(float(by_date["bottom"].mean()) * 100, 2),
                    "median_eligible_per_date": int(by_date["n"].median()),
                    "first_date": str(by_date["date"].min().date()), "last_date": str(by_date["date"].max().date())})
        res["verdict"] = "PASS" if mean >= 0.02 and lo > 0 else "FAIL"
    text = json.dumps(res, indent=1, default=str)
    print(text)
    by_date.to_csv(f"{HERE}/persist_by_date.csv", index=False)
    with open(f"{HERE}/results_persist.json", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
