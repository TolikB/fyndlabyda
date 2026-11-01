"""Do Bybit listing posts move the Binance perp? (descriptive, development events only, adds no trial)

Bybit's official announcement API gives two times per post, dateTimestamp and publishTime (median 97 s
apart), and most posts list a perp for a coin Binance already trades. Before any trial is spent, this
looks at the Binance perp's move in the minute after each time, for posts before 2025-07-01 whose coin
has a Binance perp at least 2 days old. Writes diag_bybit.txt.
"""

import json
import re

import numpy as np
import pandas as pd

import bt_spotlist as b
import diag_latency as dl

SPOT = [re.compile(r"\b([A-Z0-9]{2,})/USDT\b"), re.compile(r"Bybit to List ([A-Z0-9]{2,})\b")]
PERP = re.compile(r"\b([A-Z0-9]{2,})USDT Perpetual")


def kind_and_tickers(title):
    if "Pre-Market" in title or "TradFi" in title or "Convert" in title:
        return None, []
    if "Perpetual" in title:
        return "perp", list(dict.fromkeys(PERP.findall(title)))
    if "Spot" in title or "/USDT" in title:
        return "spot", list(dict.fromkeys(t for rx in SPOT for t in rx.findall(title)))
    return None, []


def move(w, T, L=60):
    t, p = w["t"].to_numpy(np.int64), w["p"].to_numpy(float)
    before = np.flatnonzero(t < T)
    i = int(np.searchsorted(t, T + L * 1000))
    if len(before) == 0 or i >= len(t):
        return np.nan
    return p[i] / p[before[-1]] - 1


def main():
    first = b.first_days()
    end = int(pd.Timestamp("2025-07-01").value // 1_000_000)
    rows = []
    for a in json.load(open(f"{b.DATA}/bybit_new.json", encoding="utf-8")):
        kind, tks = kind_and_tickers(a["title"])
        if not kind or a["publishTime"] >= end:
            continue
        for tk in tks:
            sym, _ = b.perp_for(tk, first, a["publishTime"])
            if sym is None:
                continue
            r = {"kind": kind, "sym": sym}
            for name in ("dateTimestamp", "publishTime"):
                T = int(a[name] or a["publishTime"])
                w = dl.window(sym, T)
                r[name] = move(w, T) if len(w) else np.nan
            rows.append(r)
    d = pd.DataFrame(rows)
    lines = [f"development posts with an old Binance perp: {len(d)}",
             "Binance perp move from the last trade before T to the first trade 60 s after T (%):"]
    for kind, g in d.groupby("kind"):
        for name in ("dateTimestamp", "publishTime"):
            x = g[name].dropna() * 100
            lines.append(f"  {kind:<5} T={name:<14} n {len(x):>3} mean {x.mean():6.2f} median {x.median():6.2f} "
                         f"share > +2% {(x > 2).mean():.0%}")
    text = "\n".join(lines)
    print(text)
    with open(f"{b.HERE}/diag_bybit.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
