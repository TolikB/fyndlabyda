"""Round 35: does a Hyperliquid vault's past return say anything about its next month? (research only)

Pre-registered on 2026-11-06, before any vault's history was downloaded; only the number of vaults, their fields and the spacing of
one vault's points have been seen, and this file was checked on synthetic data only (selftest_vaults.py). No orders are placed.

Why: a follower needs no signal, no speed and no execution, only a choice of whom to follow. Round 13 found accounts on Hyperliquid
keep part of their edge for a month, but copying them means trading their positions. A vault pays the follower pro rata, less the
leader's share of profit, and third-party guides restrict only Russian-held regions of Ukraine there. Hyperliquid's own terms
answered 403, so they must be read before any recommendation.

Sample: every vault in stats-data.hyperliquid.xyz/Mainnet/vaults of relationship "normal" (HLP and its children are out) that ever
showed an absolute PnL of at least $1,000 or holds at least $10,000 now, closed ones included (download_vaults.py).

Series from `vaultDetails`, portfolio "allTime": points (t, V account value, P cumulative PnL), about a week apart.
Time-weighted return between two points: the product of (1 + (P[k+1] - P[k]) / V[k]) over the steps, so that deposits do not count as
profit. A step whose V[k] is under $1,000 counts as 0.
Window (t0, t1): from the point nearest t0 within 7 days to the point nearest t1 within 7 days, at least 10 days long, scaled to 30 days.
Next month only: a vault with no point near t1 is measured to its last point, unscaled, so a vault that closes or dies stays in.

Dates: every Wednesday 00:00 UTC from 2024-06-05 whose 30 days end by the snapshot.
Eligible: V at the date >= $50,000, first point at least 60 days old, and at least 20 eligible vaults that date.
Ranks, each needing a value above 0:
  r30   return over the past 30 days
  r90   return over the past 90 days
  sh90  mean / standard deviation of the step returns of the past 90 days, at least 5 steps
Trial: the top K by the rank, equal weight, held 30 days. Each Wednesday opens a quarter of the book. Net next return: a positive return
is cut by the vault's leaderCommission. A date with no pick earns 0.

Trials: 6 = rank {r30, r90, sh90} x K {5, 10}.

Periods, by date: development up to 2026-03-01; holdout from 2026-03-01.

Bar on development
  - mean net next return over the dates >= +5%
  - the 95% CI of a moving-block bootstrap (5 dates, 10,000 resamples) excludes zero
  - max drawdown <= 35%, the worst of the 4 books of every fourth date
Holdout: the same bar, one look, only for trials that pass. A pass earns only a forward paper series.

Known limits: V is the whole vault, so a late move by the leader's own money counts. Weekly points hide a drawdown inside a week. Vaults
may refuse deposits, and today's allowDeposits is the only record of that. Large vaults are few, so a pick may be one trader's luck.

Usage (image funding-research:py312; /opt/funding_arbitrage_research mounted at /w)
  python download_vaults.py    /w/data/hl_vaults/
  python test_vaults.py dev        results_vaults_dev.json, /w/pnl_round35.parquet
  python test_vaults.py holdout    results_vaults_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import gzip
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = "/w/data/hl_vaults"
DAY = 86_400_000
SNAP = 7 * DAY
MIN_SPAN = 10 * DAY
MONTH = 30 * DAY
MIN_V = 5e4
MIN_STEP_V = 1e3
MIN_AGE = 60 * DAY
MIN_PER_DATE = 20
MIN_STEPS = 5
BLOCK, RESAMPLES = 5, 10_000
RANKS = ("r30", "r90", "sh90")
KS = {"k5": 5, "k10": 10}
FIRST = "2024-06-05"
SPLIT = "2026-03-01"


def ms(x):
    return int(pd.Timestamp(x, tz="UTC").value // 1_000_000)


def nearest(ts, t):
    """Index of the point nearest t, or None when none lies within SNAP."""
    if len(ts) == 0:
        return None
    j = int(np.searchsorted(ts, t))
    near = [k for k in (j - 1, j) if 0 <= k < len(ts)]
    i = min(near, key=lambda k: abs(ts[k] - t))
    return i if abs(ts[i] - t) <= SNAP else None


def steps(v, i0, i1):
    """Step returns between points i0 and i1."""
    t, val, pnl = v
    base = val[i0:i1]
    d = pnl[i0 + 1:i1 + 1] - pnl[i0:i1]
    return np.where(base >= MIN_STEP_V, d / np.maximum(base, MIN_STEP_V), 0.0).clip(-1.0, None)


def twr(v, i0, i1):
    return float(np.prod(1 + steps(v, i0, i1)) - 1)


def past(v, t0, t1):
    """Scaled return of a past window, or nan."""
    i0, i1 = nearest(v[0], t0), nearest(v[0], t1)
    if i0 is None or i1 is None or v[0][i1] - v[0][i0] < MIN_SPAN:
        return np.nan
    return float((1 + twr(v, i0, i1)) ** (MONTH / (v[0][i1] - v[0][i0])) - 1)


def following(v, d):
    """Next month's return from date d, or nan. A vault that stops inside the month is measured to its last point, unscaled."""
    i0 = nearest(v[0], d)
    if i0 is None:
        return np.nan
    i1 = nearest(v[0], d + MONTH)
    if i1 is not None and v[0][i1] - v[0][i0] >= MIN_SPAN:
        return float((1 + twr(v, i0, i1)) ** (MONTH / (v[0][i1] - v[0][i0])) - 1)
    if v[0][-1] < d + MONTH and len(v[0]) - 1 > i0:
        return twr(v, i0, len(v[0]) - 1)
    return 0.0 if v[0][-1] < d + MONTH else np.nan


def sharpe(v, d):
    i0, i1 = nearest(v[0], d - 90 * DAY), nearest(v[0], d)
    if i0 is None or i1 is None or i1 - i0 < MIN_STEPS:
        return np.nan
    s = steps(v, i0, i1)
    return float(s.mean() / s.std()) if s.std() > 0 else np.nan


def frame(vaults, d):
    """vaults: {address: {"v": (t, V, P), "fee": share}}. -> eligible rows at date d."""
    rows = []
    for a, x in vaults.items():
        v = x["v"]
        i = nearest(v[0], d)
        if i is None or v[1][i] < MIN_V or d - v[0][0] < MIN_AGE:
            continue
        r = following(v, d)
        if not np.isfinite(r):
            continue
        rows.append({"a": a, "next": r, "net": r * (1 - x["fee"]) if r > 0 else r, "r30": past(v, d - MONTH, d),
                     "r90": past(v, d - 90 * DAY, d), "sh90": sharpe(v, d)})
    return pd.DataFrame(rows)


def picks(f, rank, K):
    x = f[f[rank] > 0]
    return x.nlargest(K, rank)


def bootstrap(x, seed=35):
    """95% CI of the mean, moving blocks of BLOCK dates."""
    rng = np.random.default_rng(seed)
    n = len(x)
    starts = rng.integers(0, max(n - BLOCK + 1, 1), size=(RESAMPLES, n // BLOCK + 1))
    idx = (starts[..., None] + np.arange(BLOCK)).reshape(RESAMPLES, -1)[:, :n]
    m = x[np.minimum(idx, n - 1)].mean(axis=1)
    return float(np.quantile(m, 0.025)), float(np.quantile(m, 0.975))


def drawdown(x):
    """The worst max drawdown of the four books that hold every fourth date."""
    worst = 0.0
    for off in range(4):
        c = np.cumprod(1 + x[off::4])
        if len(c):
            worst = max(worst, float((1 - c / np.maximum.accumulate(np.r_[1.0, c])[1:]).max()))
    return worst


def evaluate(x):
    lo, hi = bootstrap(x)
    return {"dates": len(x), "mean_pct": round(float(x.mean()) * 100, 2), "median_pct": round(float(np.median(x)) * 100, 2),
            "ci_pct": [round(lo * 100, 2), round(hi * 100, 2)], "max_dd_pct": round(drawdown(x) * 100, 2)}


def passes(s):
    return bool(s["mean_pct"] >= 5 and s["ci_pct"][0] > 0 and s["max_dd_pct"] <= 35)


def load():
    meta = {x["vaultAddress"]: x for x in json.load(gzip.open(f"{DATA}/summaries.json.gz"))}
    out = {}
    for path in glob.glob(f"{DATA}/details/*.json.gz"):
        a = os.path.basename(path)[:-8]
        d = json.load(gzip.open(path))
        if a not in meta or not d or not d.get("portfolio"):
            continue
        p = dict(d["portfolio"]).get("allTime") or {}
        val, pnl = p.get("accountValueHistory") or [], p.get("pnlHistory") or []
        if len(val) < 3 or len(val) != len(pnl):
            continue
        t = np.array([x[0] for x in val], np.int64)
        out[a] = {"v": (t, np.array([float(x[1]) for x in val]), np.array([float(x[1]) for x in pnl])),
                  "fee": float(d.get("leaderCommission") or 0.0), "name": meta[a].get("name", ""), "closed": bool(d.get("isClosed"))}
    return out


def dates(end):
    all_days = pd.date_range(FIRST, pd.Timestamp(end, unit="ms"), freq="W-WED", tz="UTC")
    return [int(x.value // 1_000_000) for x in all_days if int(x.value // 1_000_000) + MONTH <= end]


def run(mode, keys=None):
    vaults = load()
    end = max(int(v["v"][0][-1]) for v in vaults.values())
    ds = [d for d in dates(end) if (d < ms(SPLIT)) == (mode == "dev")]
    print(f"{len(vaults)} vaults with history, {sum(v['closed'] for v in vaults.values())} closed, {len(ds)} dates", flush=True)
    frames = {d: frame(vaults, d) for d in ds}
    frames = {d: f for d, f in frames.items() if len(f) >= MIN_PER_DATE}
    print("dates with enough vaults", len(frames), "median eligible", int(np.median([len(f) for f in frames.values()])) if frames else 0, flush=True)
    out, cols = {}, {}
    index = pd.to_datetime(list(frames), unit="ms")
    for rank in RANKS:
        for kk, K in KS.items():
            key = f"vault|{rank}|{kk}"
            if keys is not None and key not in keys:
                continue
            x = np.array([p["net"].mean() if len(p := picks(f, rank, K)) else 0.0 for f in frames.values()])
            med = np.array([f["net"].median() for f in frames.values()])
            s = evaluate(x)
            out[key] = {**s, "median_vault_pct": round(float(med.mean()) * 100, 2), "share_pos": round(float((x > 0).mean()), 3), "passes": passes(s)}
            cols[key] = pd.Series(x, index=index)
            print(key, json.dumps(out[key]), flush=True)
    return out, pd.DataFrame(cols)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    path = f"{HERE}/results_vaults_{mode}.json"
    keys = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        keys = {k for k, v in json.load(open(f"{HERE}/results_vaults_dev.json")).items() if v["passes"]}
        if not keys:
            raise SystemExit("no trial passed development: the holdout stays closed")
    out, pnl = run(mode, keys)
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode == "dev":
        pnl.to_parquet("/w/pnl_round35.parquet")


if __name__ == "__main__":
    main()
