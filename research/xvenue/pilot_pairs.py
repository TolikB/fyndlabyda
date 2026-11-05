"""First look at near-identical instruments on Lighter: do QQQ and US100 drift apart by more than the spread? (descriptive, no trial)

Both legs pay no fee on a standard account and take 300 ms to reach the book. A premium account pays 2.8 bp a side, so a
gap under about 5.6 bp is open to standard accounts only. The pilot hours are read so that thresholds and costs can be
fixed before any test; they are left out of one.

On a 1 s grid (last quote of the bin, carried at most 10 bins), for a pair (a, b):
  r       log(mid_a / mid_b)
  base    median of r over the past hour, lagged 60 s
  edge    the bigger of log(bid_a / ask_b) - base and log(bid_b / ask_a) + base, in bp: what crossing both spreads locks in
          if r goes back to base
Capture: edge >= THETA on a bin, both legs filled at the touch 1 bin later, out at the touch 1 bin after r is back at the
base or after 10 minutes. No overlap. A trade needs SLOT dollars at the touch of both legs on the filling bin.

  python pilot_pairs.py [hours]     prints the tables, writes pilot_pairs.txt
"""

from __future__ import annotations

import glob
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = f"{HERE}/data/lt"
PAIRS = [("QQQ", "US100"), ("SPY", "US500"), ("XAU", "PAXG"), ("SAMSUNG", "SAMSUNGUSD"), ("SKHYNIX", "SKHYNIXUSD"),
         ("HYUNDAI", "HYUNDAIUSD"), ("SPACEX", "SPCX")]
THETAS = (1.0, 2.0, 3.0, 5.0, 10.0)
BASE_S = 3600
LAG_S = 60
MAX_HOLD_S = 600
STALE_S = 10
SLOT = 1000.0
RTH = (14 * 3600 + 1800, 21 * 3600)     # regular US session in UTC seconds of the day, winter time


def load(hours):
    t0 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    coins = sorted({c for p in PAIRS for c in p})
    files = [p for p in sorted(glob.glob(f"{DATA}/*/*.parquet"))
             if pd.Timestamp(f"{os.path.basename(os.path.dirname(p))} {os.path.basename(p)[:6]}", tz="UTC") >= t0]
    cols = ["b", "coin", "bid", "ask", "bid_sz", "ask_sz"]
    d = pd.concat([pd.read_parquet(p, columns=cols, filters=[("coin", "in", coins)]) for p in files], ignore_index=True)
    return d.astype({"coin": "object"})


def grid(d, unit_ms):
    """-> {coin: frame on a 1 s index}."""
    d["sec"] = d["b"] * unit_ms // 1000
    out = {}
    for coin, g in d.groupby("coin"):
        q = g.sort_values("b").groupby("sec")[["bid", "ask", "bid_sz", "ask_sz"]].last()
        out[coin] = q
    return out


def pair(a, b):
    idx = pd.RangeIndex(max(a.index.min(), b.index.min()), min(a.index.max(), b.index.max()) + 1)
    a, b = a.reindex(idx).ffill(limit=STALE_S), b.reindex(idx).ffill(limit=STALE_S)
    x = pd.DataFrame({"ba": a["bid"], "aa": a["ask"], "bb": b["bid"], "ab": b["ask"],
                      "da": a["bid_sz"].clip(upper=a["ask_sz"]) * a["bid"], "db": b["bid_sz"].clip(upper=b["ask_sz"]) * b["bid"]})
    x = x.dropna()
    x = x.reindex(idx)
    r = np.log((x["ba"] + x["aa"]) / (x["bb"] + x["ab"]))
    x["r"] = r
    x["base"] = r.rolling(BASE_S, min_periods=600).median().shift(LAG_S)
    x["sell"] = (np.log(x["ba"] / x["ab"]) - x["base"]) * 1e4
    x["buy"] = (np.log(x["bb"] / x["aa"]) + x["base"]) * 1e4
    x["edge"] = x[["sell", "buy"]].max(axis=1)
    return x


def capture(x, theta):
    """-> (net bp on one leg's notional, second of entry) per trade: both legs in at the touch, both out at the touch."""
    v = x[["ba", "aa", "bb", "ab", "r", "base", "sell", "buy", "da", "db"]].to_numpy()
    sec = x.index.to_numpy()
    out, i, n = [], 0, len(v)
    while i < n - 2:
        s, b = v[i, 6], v[i, 7]
        if not (s >= theta or b >= theta) or np.isnan(v[i + 1, :4]).any() or min(v[i + 1, 8], v[i + 1, 9]) < SLOT:
            i += 1
            continue
        short_a = s >= b
        e = v[i + 1]
        j = i + 2
        while j < n - 1 and j - i < MAX_HOLD_S:
            w = v[j]
            if not np.isnan(w[4]) and not np.isnan(w[5]) and ((w[4] <= w[5]) if short_a else (w[4] >= w[5])):
                break
            j += 1
        j = min(j + 1, n - 1)
        w = v[j]
        if np.isnan(w[:4]).any():
            i = j + 1
            continue
        pa = (e[0] / w[1] - 1) if short_a else (w[0] / e[1] - 1)
        pb = (w[2] / e[3] - 1) if short_a else (e[2] / w[3] - 1)
        out.append(((pa + pb) * 1e4, sec[i]))
        i = j + 1
    return out


def line(c):
    if not c:
        return "    0 trades"
    x = np.array(c)
    return f"{len(x):4d} trades, mean {x.mean():6.2f} bp, median {np.median(x):6.2f}, won {(x > 0).mean():.2f}, sum {x.sum():7.1f} bp"


def main():
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
    d = load(hours)
    unit = 100 if d["b"].iloc[0] < 1e11 else 1
    q = grid(d, unit)
    lines = [f"{len(d):,} rows, unit {unit} ms, coins {sorted(q)}"]
    for a, b in PAIRS:
        if a not in q or b not in q:
            lines.append(f"{a}/{b}: no quotes")
            continue
        x = pair(q[a], q[b])
        y = x.dropna(subset=["edge"])
        if len(y) < 600:
            lines.append(f"{a}/{b}: only {len(y)} s")
            continue
        sa = ((y["aa"] / y["ba"] - 1) * 1e4).median()
        sb = ((y["ab"] / y["bb"] - 1) * 1e4).median()
        dev = (y["r"] - y["base"]) * 1e4
        lines.append(f"\n{a}/{b}: {len(y):,} s, spread {sa:.2f} / {sb:.2f} bp, touch ${y['da'].median():,.0f} / ${y['db'].median():,.0f}, "
                     f"r-base sd {dev.std():.2f} bp, |r-base| 99% {dev.abs().quantile(.99):.2f} bp")
        for th in THETAS:
            hot = y["edge"] >= th
            eps = int((hot & ~hot.shift(1, fill_value=False)).sum())
            c = capture(x, th)
            rth = [p for p, t in c if RTH[0] <= t % 86400 < RTH[1]]
            off = [p for p, t in c if not RTH[0] <= t % 86400 < RTH[1]]
            lines.append(f"  edge >= {th:4.1f} bp: {hot.mean() * 100:6.2f}% of s, {eps:5d} eps | session {line(rth)} | off {line(off)}")
    text = "\n".join(lines)
    print(text)
    with open(f"{HERE}/pilot_pairs.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
