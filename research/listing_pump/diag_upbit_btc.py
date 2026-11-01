"""Do Upbit BTC/USDT-market listings (no KRW) move the Binance perp? (descriptive, development only, no trial)

Round 16 used only KRW-market notices, so the BTC/USDT-only listing notices are untouched on both sides of
the 2025-07-01 split. This looks at development notices only: the Binance perp's price at T + L seconds
against the last trade before T, from aggTrades. Writes diag_upbit_btc.txt.
"""

import json

import numpy as np
import pandas as pd

import bt_spotlist as b
import diag_latency as dl

DELAYS = (1, 2, 3, 5, 10, 60)


def parse_btc_listing(title):
    """Tickers of a listing notice that opens BTC or USDT markets and no KRW market, else []."""
    if "KRW" in title or "종료" in title or not any(w in title for w in b.UPBIT_WORDS):
        return []
    if "BTC" not in title and "USDT" not in title:
        return []
    out = [x.strip() for g in b.GROUP.findall(title) for x in g.split(",")] + b.REVERSED.findall(title)
    return [x for x in dict.fromkeys(out) if x not in b.MARKETS]


def main():
    first = b.first_days()
    end = int(pd.Timestamp("2025-07-01").value // 1_000_000)
    rows, last = [], {}
    for n in json.load(open(f"{b.DATA}/upbit_notices.json", encoding="utf-8")):
        T = b.kst_ms(n["first_listed_at"])
        if T >= end:
            continue
        for tk in parse_btc_listing(n["title"]):
            sym, _ = b.perp_for(tk, first, T)
            if sym is None or T - last.get(sym, -10 ** 15) < b.DEDUPE:
                continue
            last[sym] = T
            w = dl.window(sym, T)
            if len(w) == 0:
                continue
            t, p = w["t"].to_numpy(np.int64), w["p"].to_numpy(float)
            before = np.flatnonzero(t < T)
            if len(before) == 0:
                continue
            r = {"sym": sym, "year": pd.Timestamp(T, unit="ms").year}
            for L in DELAYS:
                i = int(np.searchsorted(t, T + L * 1000))
                r[L] = p[i] / p[before[-1]] - 1 if i < len(t) else np.nan
            rows.append(r)
    d = pd.DataFrame(rows)
    lines = [f"development BTC/USDT-only listing notices with an old Binance perp: {len(d)}",
             "price at T + L vs the last trade before T, %:"]
    for L in DELAYS:
        x = d[L].dropna() * 100
        lines.append(f"  L={L:>3}s mean {x.mean():6.2f} median {x.median():6.2f} share > +2% {(x > 2).mean():.0%}")
    lines.append("left from 2 s to 60 s (median %): " + f"{((d[60] - d[2]) * 100).median():.2f}")
    lines.append("by year, median at 60 s: " + "; ".join(f"{y} n{len(g)} {g[60].median() * 100:.1f}"
                                                         for y, g in d.groupby("year")))
    text = "\n".join(lines)
    print(text)
    with open(f"{b.HERE}/diag_upbit_btc.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
