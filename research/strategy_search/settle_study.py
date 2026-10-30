"""Price drift around funding settlements, from 1h bars (research only).

For every settlement T of a symbol in the top 60 by 30-day median quote volume,
measure open-to-open returns over [T-k, T) and [T, T+k), bucketed by the funding
rate of the previous settlement (known well before T, so no look-ahead) and by the
settled rate itself (known only near T; a leakage-bounded upper estimate).
Mean returns are in basis points; a taker round trip costs about 10-14 bps.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt

DATA = bt.DATA


def main():
    h = pd.read_parquet(f"{DATA}/klines_1h.parquet", columns=["symbol", "open_time", "open", "quote_volume"])
    h = h[(h["quote_volume"] > 0) & ~h["symbol"].isin(bt.EXCLUDE)]
    h["t"] = pd.to_datetime(h["open_time"], unit="ms")
    opn = h.pivot_table(index="t", columns="symbol", values="open", aggfunc="first")
    _, close, qvol, _ = bt.load()
    live, _ = bt.universe(close, qvol, 60)

    f = pd.read_parquet(f"{DATA}/fundingRate_all.parquet")
    f["T"] = pd.to_datetime(f["calc_time"], unit="ms").dt.round("h")
    f = f.sort_values(["symbol", "T"])
    f["prev_rate"] = f.groupby("symbol")["last_funding_rate"].shift(1)
    f = f[f["symbol"].isin(opn.columns)].dropna(subset=["prev_rate"])
    day = f["T"].dt.normalize() - pd.Timedelta(days=1)
    in_universe = live.stack()
    key = pd.MultiIndex.from_arrays([day, f["symbol"]])
    f = f[in_universe.reindex(key).fillna(False).to_numpy()]
    print(f"settlements in the top-60 universe: {len(f)}")

    pos = {t: i for i, t in enumerate(opn.index)}
    cols = {s: j for j, s in enumerate(opn.columns)}
    o = opn.to_numpy()
    rows = []
    for T, symbol, prev, rate, hours in zip(f["T"], f["symbol"], f["prev_rate"],
                                           f["last_funding_rate"], f["funding_interval_hours"]):
        i = pos.get(T)
        if i is None or i < 4 or i + 4 >= len(o):
            continue
        j = cols[symbol]
        p = o[i - 4: i + 5, j]
        if not np.isfinite(p).all():
            continue
        rows.append((T, symbol, prev, rate, hours,
                     (p[4] / p[3] - 1) * 1e4, (p[4] / p[2] - 1) * 1e4, (p[4] / p[0] - 1) * 1e4,
                     (p[5] / p[4] - 1) * 1e4, (p[6] / p[4] - 1) * 1e4, (p[8] / p[4] - 1) * 1e4))
    r = pd.DataFrame(rows, columns=["T", "symbol", "prev", "rate", "hours",
                                    "pre1", "pre2", "pre4", "post1", "post2", "post4"])
    bins = [-1, -0.001, -0.0003, 0, 0.0001, 0.0003, 0.001, 1]
    labels = ["<-10bp", "-10..-3", "-3..0", "0..1", "1..3", "3..10", ">10bp"]
    for basis in ("prev", "rate"):
        r["bucket"] = pd.cut(r[basis], bins, labels=labels)
        table = r.groupby("bucket", observed=True)[["pre1", "pre2", "pre4", "post1", "post2", "post4"]].mean()
        table["n"] = r.groupby("bucket", observed=True).size()
        print(f"\nmean return (bps) by funding bucket of `{basis}`:")
        print(table.round(1).to_string())
    r["year"] = r["T"].dt.year
    r["bucket"] = pd.cut(r["prev"], bins, labels=labels)
    hot = r[r["bucket"].isin(["3..10", ">10bp"])]
    print("\nprev >= 3bp: by year (bps)")
    print(hot.groupby("year")[["pre1", "pre2", "post1", "post2", "post4"]].mean().round(1).to_string())
    cold = r[r["bucket"].isin(["<-10bp", "-10..-3"])]
    print("\nprev <= -3bp: by year (bps)")
    print(cold.groupby("year")[["pre1", "pre2", "post1", "post2", "post4"]].mean().round(1).to_string())
    r.to_parquet(f"{DATA}/../settle_events.parquet")


if __name__ == "__main__":
    main()
