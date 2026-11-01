"""How many seconds does the listing jump take? (descriptive, development events only, adds no trial)

For every development event of bt_spotlist.py, the Binance USDT-M aggTrades archive gives every trade in
[T - 10 min, T + 10 min]. The script reports the price at fixed delays after T (the first trade at or
after T + L, against the last trade before T) and when the jump starts and completes. It answers one
question: could a bot that sees the notice a few seconds late still catch part of the move?

Output: diag_latency.txt; trade windows in /w/data/listing/agg/<SYM>_<T>.parquet.
"""

import io
import os
import tempfile
import zipfile

import numpy as np
import pandas as pd

import bt_spotlist as b

AGG = f"{b.DATA}/agg"
FILES = "https://data.binance.vision/data/futures/um/daily/aggTrades"
WINDOW = 10 * b.MIN
DELAYS = (-5, -2, -1, 0, 0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300)


def window(sym, T):
    path = f"{AGG}/{sym}_{T}.parquet"
    if os.path.exists(path):
        return pd.read_parquet(path)
    lo, hi = T - WINDOW, T + WINDOW
    days = sorted({pd.Timestamp(x // b.DAY * b.DAY, unit="ms").strftime("%Y-%m-%d") for x in (lo, hi)})
    parts = []
    for day in days:
        blob = b.fetch(f"{FILES}/{sym}/{sym}-aggTrades-{day}.zip")
        if blob is None:
            continue
        with zipfile.ZipFile(io.BytesIO(blob)) as archive, archive.open(archive.namelist()[0]) as raw:
            head = raw.peek(1)[:1] if hasattr(raw, "peek") else b""
            for chunk in pd.read_csv(raw, header=None if head.isdigit() else 0, usecols=[1, 2, 5, 6],
                                     chunksize=2_000_000):
                chunk.columns = ["p", "q", "t", "maker"]
                t = chunk["t"].astype("int64")
                if len(t) and t.max() > 10 ** 14:
                    t = t // 1000
                keep = (t >= lo) & (t <= hi)
                if keep.any():
                    parts.append(pd.DataFrame({"t": t[keep], "p": chunk["p"][keep].astype(float),
                                               "q": chunk["q"][keep].astype(float),
                                               "buy": ~chunk["maker"][keep].astype(str).str.lower().eq("true")}))
        del blob
    w = pd.concat(parts, ignore_index=True).sort_values("t", kind="stable") if parts else pd.DataFrame(
        columns=["t", "p", "q", "buy"])
    os.makedirs(AGG, exist_ok=True)
    w.to_parquet(path, index=False)
    return w


def describe(w, T):
    t, p = w["t"].to_numpy(np.int64), w["p"].to_numpy(float)
    before = np.flatnonzero(t < T)
    if len(before) == 0 or len(t) == 0:
        return None
    p0 = p[before[-1]]
    r = {}
    for L in DELAYS:
        i = int(np.searchsorted(t, T + int(L * 1000)))
        r[L] = p[i] / p0 - 1 if i < len(t) else np.nan
    move = r[60]
    r["move60"] = move
    if np.isfinite(move) and move >= 0.04:
        after = np.flatnonzero(t >= T - 5000)
        up = after[p[after] >= p0 * (1 + 0.25 * move)]
        half = after[p[after] >= p0 * (1 + 0.5 * move)]
        most = after[p[after] >= p0 * (1 + 0.8 * move)]
        r["t25"] = (t[up[0]] - T) / 1000 if len(up) else np.nan
        r["t50"] = (t[half[0]] - T) / 1000 if len(half) else np.nan
        r["t80"] = (t[most[0]] - T) / 1000 if len(most) else np.nan
    return r


def main():
    ev = b.period_events("dev")
    ev = ev[ev["sym"].notna()]
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        r = describe(window(e["sym"], T), T)
        if r is not None:
            rows.append({"src": e["src"], "sym": e["sym"], "T": T, **r})
            print(f"{e['sym']:<14} {pd.Timestamp(T, unit='ms'):%Y-%m-%d %H:%M:%S} move60 {r['move60'] * 100:6.1f}% "
                  f"t50 {r.get('t50', np.nan):6.1f}s", flush=True)
    d = pd.DataFrame(rows)
    d.to_parquet(f"{b.HERE}/latency_dev.parquet", index=False)
    lines = [f"events {len(d)}", "", "price vs the last trade before T, % (first trade at or after T + L)",
             f"{'L s':>6} {'mean':>8} {'median':>8} {'share of +60 s move (median)':>30}"]
    for L in DELAYS:
        x = d[L] * 100
        share = (d[L] / d["move60"]).where(d["move60"] >= 0.04)
        lines.append(f"{L:>6} {x.mean():8.2f} {x.median():8.2f} {share.median() * 100:29.0f}%")
    big = d[d["move60"] >= 0.04]
    lines += ["", f"events with a +60 s move >= 4%: {len(big)}",
              "seconds after T to reach 25% / 50% / 80% of that move (quartiles):"]
    for k in ("t25", "t50", "t80"):
        q = big[k].quantile([0.25, 0.5, 0.75]).round(1).tolist()
        lines.append(f"  {k}: {q}")
    lines += ["", "left to catch after a delay L (median of move60 - price at L, %):"]
    for L in (1, 2, 3, 5, 10):
        lines.append(f"  L={L:>2}s: {((d['move60'] - d[L]) * 100).median():.2f}")
    out = "\n".join(lines)
    print(out)
    with open(f"{b.HERE}/diag_latency.txt", "w") as fh:
        fh.write(out + "\n")


if __name__ == "__main__":
    main()
