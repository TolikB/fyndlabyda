"""Does the price give back the funding coupon? Minute-level descriptive study (research only).

A position held at a settlement instant T receives or pays the whole interval's funding, however
briefly it was held. If the perp behaved like a bond going ex-coupon, the receiving side would lose
about the rate in price between T-m and T+1 minute. This study measures that on development data
only (2022-01..2025-06); the 2025-07..2026-10 minute window stays untouched.

Events: every settlement of a symbol on a day it is in the wick-trap universe (ranks 30-150), with
gap-free minutes from T-60 to T+60.
  d        the receiving side: -1 (short) when the settled rate is positive, +1 when negative
  move_m   d * (close of minute T / open of minute T-m - 1), m in {1, 5, 15, 60}: entry m minutes
           before T, exit one minute after T
  snipe_m  move_m + |rate|, before fees
  prev     the previous settlement's rate, known long before T (a causal selector)
The settled rate is only known near T, so the `rate` buckets are a leakage-bounded upper estimate;
the `prev` buckets are causal. A taker round trip costs 10 bp plus twice the slippage tier.

  python study_snipe.py   -> study_snipe.txt, events_snipe.parquet
"""

from __future__ import annotations

import os
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/wick")
import bt_wick as b  # noqa: E402  (universe, funding, minute loader, slippage tiers)

HERE = os.path.dirname(os.path.abspath(__file__))
START, END = "2022-01-01", "2025-07-01"
MS = (1, 5, 15, 60)
G = {}


def init():
    live, slip = b.universe()
    G["live"], G["slip"], G["fund"] = live, slip, b.funding_events()
    G["start"], G["end"] = (int(pd.Timestamp(x).value // 1_000_000) for x in (START, END))


def one_symbol(sym):
    if sym not in G["fund"]:
        return None
    df = b.load_symbol(sym)
    if df is None or df.empty:
        return None
    t = df["t"].to_numpy(np.int64)
    o, c = df["o"].to_numpy(float), df["c"].to_numpy(float)
    ft, fr = G["fund"][sym]
    live_days = set(G["live"].index[G["live"][sym].to_numpy(dtype=bool)].asi8 // 1_000_000 // b.DAY)
    rows = []
    for n in range(1, len(ft)):
        T = (ft[n] + b.MIN // 2) // b.MIN * b.MIN  # settlement times carry a few ms of jitter
        if not (G["start"] <= T < G["end"]) or T // b.DAY not in live_days:
            continue
        i = np.searchsorted(t, T)
        if i < 60 or i + 60 >= len(t) or t[i] != T or t[i - 60] != T - 60 * b.MIN or t[i + 60] != T + 60 * b.MIN:
            continue
        rate, d = fr[n], (-1 if fr[n] > 0 else 1)
        row = {"sym": sym, "T": T, "rate": rate, "prev": fr[n - 1], "hours": round((ft[n] - ft[n - 1]) / 3.6e6),
               "d": d, "slip": float(G["slip"][sym].get(pd.Timestamp(T // b.DAY * b.DAY, unit="ms"), 0.0025))}
        for m in MS:
            row[f"move_{m}"] = d * (c[i] / o[i - m] - 1.0)
        row["after_60"] = d * (c[i + 60] / c[i] - 1.0)  # what the receiver would get by staying an hour
        rows.append(row)
    return pd.DataFrame(rows) if rows else None


def summary(ev, key, edges):
    out = []
    ev = ev.assign(bucket=pd.cut(ev[key].abs(), edges, right=False))
    for bucket, g in ev.groupby("bucket", observed=True):
        day = pd.to_datetime(g["T"], unit="ms").dt.normalize()
        line = f"  |{key}| {str(bucket):<18} n {len(g):>7}  |rate| {g['rate'].abs().mean() * 1e4:6.1f}"
        for m in MS:
            snipe = g[f"move_{m}"] + g["rate"].abs()
            per_day = snipe.groupby(day).mean()
            tt = per_day.mean() / per_day.std() * np.sqrt(len(per_day)) if len(per_day) > 2 else np.nan
            line += f"  m{m}: move {g[f'move_{m}'].mean() * 1e4:6.1f} snipe {snipe.mean() * 1e4:6.1f} (t {tt:5.1f})"
        line += f"  cost {(10 + 2e4 * g['slip']).mean():5.1f}  after60 {g['after_60'].mean() * 1e4:6.1f}"
        out.append(line)
    return out


def main():
    init()
    syms = sorted(s for s in G["live"].columns if G["live"][s].any() and os.path.isdir(f"{b.M1}/{s}"))
    with Pool(2) as pool:
        parts = [p for p in pool.imap_unordered(one_symbol, syms, chunksize=4) if p is not None]
    ev = pd.concat(parts, ignore_index=True)
    ev.to_parquet(f"{HERE}/events_snipe.parquet")
    edges = [0, 1e-4, 3e-4, 1e-3, 3e-3, 1.0]
    lines = [f"development {START}..{END}: {len(ev)} settlements, {ev['sym'].nunique()} symbols; basis points",
             "by the settled rate (leakage-bounded upper estimate):"]
    lines += summary(ev, "rate", edges)
    lines.append("by the previous settlement's rate (causal):")
    lines += summary(ev, "prev", edges)
    for hours, g in ev[ev["rate"].abs() >= 1e-3].groupby("hours"):
        lines.append(f"  |rate| >= 10 bp, {hours}h interval: n {len(g)}  snipe_5 {(g['move_5'] + g['rate'].abs()).mean() * 1e4:6.1f}")
    text = "\n".join(lines)
    print(text)
    with open(f"{HERE}/study_snipe.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
