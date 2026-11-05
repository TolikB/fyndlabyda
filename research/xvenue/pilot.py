"""First look at the collected books: quoted spreads and gaps between venues (descriptive, no trial).

Only the pilot hours are read, before the development window of rounds 24 and 25 opens, so that thresholds and
cost figures can be fixed from it. Data from this window is left out of both tests.

Quotes are put on a 3 s grid (last quote of the bin, carried at most STALE_BINS bins). A pair is two venues
listing one coin, each with at least MIN_QV of 24 h quote volume. For a pair (a, b):
  mid gap   100 x 100 x (mid_b / mid_a - 1), in bp
  edge      the bigger of (bid_b / ask_a - 1) and (bid_a / ask_b - 1), minus the pair's median of the same-sided mid
            gap, so a steady basis is not counted; what a taker could lock in on both legs if the gap went
            back to its median
A pair whose median mids differ by more than 3% is two different tokens (or a contract multiplier not spotted
here), and is dropped.

  python pilot.py [hours]     prints the tables, writes pilot.txt
"""

from __future__ import annotations

import glob
import os
import re
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = f"{HERE}/data"
GRID = 3000
STALE_BINS = 10
MIN_QV = 1e6
PREFIX = re.compile(r"^(1000000|100000|10000|1000|100|1M|k)(?=[A-Z])")
SUFFIX = {"okx": "-USDT-SWAP", "gate": "_USDT", "mexc": "_USDT", "kucoin": "USDTM", "binance": "USDT", "bybit": "USDT",
          "bitget": "USDT", "lighter": ""}


def canon(venue, sym):
    """-> (coin, units of the coin in one quoted unit): 1000PEPEUSDT on binance -> ("PEPE", 1000)."""
    s = sym[: -len(SUFFIX[venue])] if SUFFIX[venue] and sym.endswith(SUFFIX[venue]) else sym
    if venue == "kucoin" and s == "XBT":
        s = "BTC"
    m = PREFIX.match(s)
    if not m:
        return s, 1.0
    p = m.group(1)
    return s[len(p):], 1e6 if p == "1M" else 1e3 if p == "k" else float(p)


def parts(table, t0):
    files = sorted(glob.glob(f"{DATA}/{table}/*/*.parquet"))
    frames = [pd.read_parquet(p) for p in files
              if pd.Timestamp(os.path.basename(os.path.dirname(p)), tz="UTC") >= t0.normalize()]
    return pd.concat([f.astype({c: "object" for c in f.columns if str(f[c].dtype) == "category"}) for f in frames],
                     ignore_index=True)


def load(hours):
    """-> (quotes long frame: bin, venue, coin, bid, ask per one coin unit; volumes by (venue, coin))."""
    t0 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    ms = t0.value // 1_000_000
    b = parts("bbo", t0)
    b = b[b["t"] >= ms][["t", "v", "s", "bid", "ask"]]
    lt = parts("lt", t0)
    lt = lt[lt["r_last"] >= ms].rename(columns={"r_last": "t", "coin": "s"})[["t", "s", "bid", "ask"]]
    lt["v"] = "lighter"
    q = pd.concat([b, lt], ignore_index=True)
    keys = q[["v", "s"]].drop_duplicates()
    names = [canon(v, s) for v, s in zip(keys["v"], keys["s"])]
    keys["coin"], keys["unit"] = [n[0] for n in names], [n[1] for n in names]
    q = q.merge(keys, on=["v", "s"])
    q["bid"], q["ask"] = q["bid"] / q["unit"], q["ask"] / q["unit"]
    q["bin"] = q["t"] // GRID
    q = q.sort_values("t").groupby(["bin", "v", "coin"], as_index=False)[["bid", "ask"]].last()
    s = parts("slow", t0).sort_values("t").groupby(["v", "s"], as_index=False)["qv"].last()
    names = [canon(v, x) for v, x in zip(s["v"], s["s"])]
    s["coin"] = [n[0] for n in names]
    return q, s.groupby(["v", "coin"])["qv"].max()


def wide(q, col):
    w = q.pivot_table(index="bin", columns=["v", "coin"], values=col, aggfunc="last")
    return w.reindex(range(int(w.index.min()), int(w.index.max()) + 1)).ffill(limit=STALE_BINS)


def spreads(bid, ask, vol):
    s = (ask / bid - 1) * 1e4
    rows = s.median().rename("spread_bp").to_frame().join(vol.rename("qv"), how="left").reset_index()
    return rows


def pair_stats(bid, ask, vol):
    venues = sorted({v for v, _ in bid.columns})
    out = []
    coins = {}
    for v, c in bid.columns:
        if vol.get((v, c), 0) >= MIN_QV:
            coins.setdefault(c, []).append(v)
    for coin, vs in coins.items():
        for i, a in enumerate(vs):
            for b in vs[i + 1:]:
                ma = (bid[a][coin] + ask[a][coin]) / 2
                mb = (bid[b][coin] + ask[b][coin]) / 2
                gap = (mb / ma - 1) * 1e4
                if gap.notna().sum() < 100 or abs(gap.median()) > 300:
                    continue
                med = gap.median()
                up = (bid[b][coin] / ask[a][coin] - 1) * 1e4 - med
                down = (bid[a][coin] / ask[b][coin] - 1) * 1e4 + med
                e = np.maximum(up, down)
                x = e.dropna()
                out.append({"coin": coin, "a": a, "b": b, "med_gap": med, "mad": (gap - med).abs().median(),
                            "e_p50": x.median(), "e_p99": x.quantile(0.99), "e_max": x.max(),
                            "h10": (x >= 10).mean(), "h20": (x >= 20).mean(), "h30": (x >= 30).mean(),
                            "h50": (x >= 50).mean(), "qv": min(vol.get((a, coin), 0), vol.get((b, coin), 0)), "n": len(x)})
    return pd.DataFrame(out), venues


def main():
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    q, vol = load(hours)
    bid, ask = wide(q, "bid"), wide(q, "ask")
    lines = [f"pilot over the last {hours:g} h: {len(bid)} bins of {GRID / 1000:g} s, {bid.shape[1]} venue-coins"]
    sp = spreads(bid, ask, vol)
    pd.set_option("display.width", 200, "display.max_rows", 300, "display.max_columns", 30)
    tops = ["BTC", "ETH", "SOL", "XRP", "DOGE", "LINK", "ADA", "HYPE", "ZEC", "SUI", "NEAR", "ENA", "XAU", "TSLA", "NVDA"]
    t = sp[sp["coin"].isin(tops)].pivot_table(index="coin", columns="v", values="spread_bp").reindex(tops).round(2)
    lines += ["", "median quoted spread, bp", t.to_string()]
    lines += ["", "median spread by venue and 24 h volume band, bp"]
    sp["band"] = pd.cut(sp["qv"], [0, 1e6, 1e7, 1e8, 1e12], labels=["<1M", "1-10M", "10-100M", ">100M"])
    b = sp.pivot_table(index="v", columns="band", values="spread_bp", aggfunc="median", observed=False).round(2)
    c = sp.pivot_table(index="v", columns="band", values="spread_bp", aggfunc="count", observed=False)
    lines += [b.to_string(), "", "markets in each band", c.to_string()]
    p, _ = pair_stats(bid, ask, vol)
    lines += ["", f"pairs with both legs >= {MIN_QV / 1e6:g}M: {len(p)}"]
    if len(p):
        lines += ["share of 3 s bins with edge >= X bp, median pair / mean pair",
                  p[["h10", "h20", "h30", "h50"]].agg(["median", "mean"]).round(4).to_string(), ""]
        by = p.groupby(["a", "b"]).agg(pairs=("coin", "size"), e_p99=("e_p99", "median"), h20=("h20", "mean"),
                                       h30=("h30", "mean"), h50=("h50", "mean")).round(4)
        lines += ["by venue pair", by.sort_values("h30", ascending=False).to_string(), "",
                  "top 25 pairs by share of bins with edge >= 30 bp",
                  p.sort_values("h30", ascending=False).head(25).round(3).to_string(index=False)]
    text = "\n".join(lines)
    print(text)
    with open(f"{HERE}/pilot.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
