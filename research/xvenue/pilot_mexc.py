"""First look at MEXC's zero-fee contracts quoted in two currencies: one coin in USDT and USDC or USD1 (descriptive, no trial).

The legs share a venue and pay no fee, so a gap only has to beat the spreads. On a 3 s grid, for coin X with contracts a, b
both at 0/0 and at least MIN_QV of 24 h quote volume:
  edge   the bigger of log(bid_a / ask_b) and log(bid_b / ask_a), less the median of log(mid_a / mid_b), in bp
Capture as in pilot_pairs.py: in at the touch on the next bin, out at the touch one bin after the mid gap is back at its
median or after 10 minutes. The median is the whole sample's, so this flatters the capture; it sizes the idea and no more.

  python pilot_mexc.py [hours]
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
DATA = f"{HERE}/data"
GRID_MS = 3000
MIN_QV = 100_000.0
MAX_HOLD = 200
THETAS = (2.0, 5.0, 10.0, 20.0)


def fees():
    d = pd.DataFrame(json.load(gzip.open(sorted(glob.glob(f"{DATA}/meta/*/mexc.json.gz"))[-1]))["data"])
    d["free"] = (d["makerFeeRate"].astype(float) == 0) & (d["takerFeeRate"].astype(float) == 0)
    return d


def quotes(hours, symbols):
    t0 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    files = [p for p in sorted(glob.glob(f"{DATA}/bbo/*/*.parquet"))
             if pd.Timestamp(f"{os.path.basename(os.path.dirname(p))} {os.path.basename(p)[:6]}", tz="UTC") >= t0]
    frames = [pd.read_parquet(p, columns=["t", "s", "bid", "ask"], filters=[("v", "==", "mexc"), ("s", "in", symbols)]) for p in files]
    d = pd.concat(frames, ignore_index=True).astype({"s": "object"})
    d["bin"] = d["t"] // GRID_MS
    return {s: g.groupby("bin")[["bid", "ask"]].last() for s, g in d.groupby("s")}


def capture(x, theta):
    v = x[["ba", "aa", "bb", "ab", "gap", "sell", "buy"]].to_numpy()
    out, i, n = [], 0, len(v)
    while i < n - 2:
        s, b = v[i, 5], v[i, 6]
        if not (s >= theta or b >= theta):
            i += 1
            continue
        short_a, e, j = s >= b, v[i + 1], i + 2
        while j < n - 1 and j - i < MAX_HOLD and not ((v[j, 4] <= 0) if short_a else (v[j, 4] >= 0)):
            j += 1
        j = min(j + 1, n - 1)
        w = v[j]
        pa = (e[0] / w[1] - 1) if short_a else (w[0] / e[1] - 1)
        pb = (w[2] / e[3] - 1) if short_a else (e[2] / w[3] - 1)
        out.append((pa + pb) * 1e4)
        i = j + 1
    return out


def main():
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
    d = fees()
    slow = pd.concat(pd.read_parquet(p) for p in sorted(glob.glob(f"{DATA}/slow/*/*.parquet"))[-12:]).astype({"v": "object", "s": "object"})
    qv = slow[slow["v"] == "mexc"].groupby("s")["qv"].last()
    d["qv"] = d["symbol"].map(qv).fillna(0.0)
    d["coin"] = d["symbol"].str.rsplit("_", n=1).str[0]
    print("top zero-fee contracts:", d[d["free"]].nlargest(25, "qv")["symbol"].tolist())
    free = d[d["free"] & (d["qv"] >= MIN_QV)]
    both = free.groupby("coin").filter(lambda g: len(g) >= 2)
    print(len(free), "free contracts over the floor;", both["coin"].nunique(), "coins in two currencies:", sorted(both["coin"].unique())[:40])
    if both.empty:
        return
    q = quotes(hours, both["symbol"].tolist())
    for coin, g in both.groupby("coin"):
        syms = sorted(g["symbol"], key=lambda s: not s.endswith("_USDT"))
        for other in syms[1:]:
            a, b = syms[0], other
            if a not in q or b not in q:
                continue
            x = pd.concat([q[a].add_suffix("_a"), q[b].add_suffix("_b")], axis=1).sort_index().ffill(limit=10).dropna()
            x.columns = ["ba", "aa", "bb", "ab"]
            r = np.log((x["ba"] + x["aa"]) / (x["bb"] + x["ab"]))
            med = r.median()
            x["gap"] = (r - med) * 1e4
            x["sell"] = (np.log(x["ba"] / x["ab"]) - med) * 1e4
            x["buy"] = (np.log(x["bb"] / x["aa"]) + med) * 1e4
            sa, sb = ((x["aa"] / x["ba"] - 1) * 1e4).median(), ((x["ab"] / x["bb"] - 1) * 1e4).median()
            print(f"\n{a}/{b}: {len(x):,} bins, qv {qv.get(a, 0):,.0f} / {qv.get(b, 0):,.0f}, spread {sa:.2f} / {sb:.2f} bp, "
                  f"median gap {med * 1e4:.1f} bp, gap sd {x['gap'].std():.1f} bp")
            for th in THETAS:
                c = np.array(capture(x, th))
                hot = (x[["sell", "buy"]].max(axis=1) >= th).mean() * 100
                if len(c):
                    print(f"  edge >= {th:4.1f}: {hot:6.2f}% of bins | {len(c):4d} trades, mean {c.mean():6.2f} bp, won {(c > 0).mean():.2f}, sum {c.sum():7.1f}")
                else:
                    print(f"  edge >= {th:4.1f}: {hot:6.2f}% of bins | 0 trades")


if __name__ == "__main__":
    main()
