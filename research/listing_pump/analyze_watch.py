"""Does Upbit's official market list show a new KRW market at the notice time? (forward step 0 for round 16b)

Inputs
  watch/markets.jsonl            upbit-watch's log of market-list changes (first-seen times, local clock)
  /w/data/listing/recent.json    recent Upbit notices (download_upbit.py --pages 2 on the workstation, copied in)
  aggTrades archive              the Binance perp around T (diag_latency.window, available the next day)

For each KRW market added after the watcher started: the KRW listing notice naming its ticker
(bt_spotlist.parse_upbit), its time T, the first-seen time, and when the Binance perp reached a quarter of
its +60 s move. Decision rule, fixed in the plan before any event: with at least 3 events, the route is
alive when the median (seen - T) is <= 1.5 s and the market shows up no later than the jump; it is dead
when markets appear only around trading open, hours after T. Writes watch/analysis.txt.

  python analyze_watch.py
"""

import json
import os

import numpy as np
import pandas as pd

import bt_spotlist as b
import diag_latency as dl

WATCH = os.path.join(b.HERE, "watch")


def first_seen():
    """-> {market: first time (ms) it was added after the baseline}."""
    seen, base = {}, None
    for line in open(f"{WATCH}/markets.jsonl"):
        r = json.loads(line)
        if r.get("baseline"):
            listed = set(r.get("markets") or r.get("added") or [])  # the first baseline used "added"
            base = listed if base is None else base | listed
            continue
        for m in r.get("added", []):
            if base is not None and m not in base and m not in seen:
                seen[m] = r["t"]
    return seen


def jump_t25(sym, T):
    w = dl.window(sym, T)
    if len(w) == 0:
        return np.nan, np.nan
    t, p = w["t"].to_numpy(np.int64), w["p"].to_numpy(float)
    before = np.flatnonzero(t < T)
    i60 = int(np.searchsorted(t, T + 60_000))
    if len(before) == 0 or i60 >= len(t):
        return np.nan, np.nan
    p0 = p[before[-1]]
    mv = p[i60] / p0 - 1
    after = np.flatnonzero(t >= T - 5000)
    hit = after[p[after] >= p0 * (1 + 0.25 * mv)] if mv > 0 else []
    return mv, ((t[hit[0]] - T) / 1000 if len(hit) else np.nan)


def main():
    seen = first_seen()
    notices = json.load(open(f"{b.DATA}/recent.json", encoding="utf-8"))
    first = b.first_days()
    rows = []
    for m, ts in sorted(seen.items(), key=lambda x: x[1]):
        if not m.startswith("KRW-"):
            continue
        tk = m[4:]
        cand = [n for n in notices if tk in b.parse_upbit(n["title"])]
        if not cand:
            rows.append({"market": m, "seen": ts, "T": None})
            continue
        T = min(b.kst_ms(n["first_listed_at"]) for n in cand)
        sym, _ = b.perp_for(tk, first, T)
        mv, t25 = jump_t25(sym, T) if sym else (np.nan, np.nan)
        rows.append({"market": m, "seen": ts, "T": T, "seen_minus_T_s": (ts - T) / 1000, "perp": sym,
                     "move60_pct": mv * 100, "jump_t25_s": t25})
    d = pd.DataFrame(rows)
    lines = [f"new KRW markets since the watcher started: {len(d)}"]
    if len(d):
        lines.append(d.to_string(index=False))
        x = d["seen_minus_T_s"].dropna() if "seen_minus_T_s" in d else pd.Series(dtype=float)
        if len(x) >= 3:
            alive = x.median() <= 1.5 and (d["seen_minus_T_s"] <= d["jump_t25_s"].fillna(np.inf)).mean() >= 0.5
            lines.append(f"median seen - T {x.median():.2f} s -> route {'ALIVE' if alive else 'DEAD'}")
        else:
            lines.append("fewer than 3 events with a notice: no verdict yet")
    text = "\n".join(lines)
    print(text)
    with open(f"{WATCH}/analysis.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
