"""Round 29: are underdogs underpriced on Polymarket shortly before they resolve? (research only)

Pre-registered on 2026-11-05. No price from 2026 has been read for this question. No orders are placed anywhere.

This idea comes from round 27's development calibration table: favourites priced 0.70..0.90 won about 3 points less often
than their price, 1 and 3 days before the scheduled end. Development entries were therefore SEEN before these rules were
written, and a pass there proves nothing. It only opens the holdout, the first look at 2026 entries.

Universe, entry time, cost formula and book are those of bt_bonds.py. At te = scheduled end - K take the favourite's price
p as there, and when LOW <= p < HIGH buy the OTHER outcome at u = 1 - p and hold to the actual close.

Book: as in round 27, but f in {1%, 2%, 5%}, because most of these trades lose their whole stake.

Trials: 6 = K {1, 3 days} x favourite's band [0.70, 0.80), [0.80, 0.90), [0.90, 0.95). K = 7 is left out: its gap was small.

Periods, by te: development 2024-01-01..2026-01-01; holdout 2026-01-01..2026-11-01.

Bar on development
  - mean daily return > 0 with t >= 2.5 at f = 1% and 1x costs
  - f* = the smallest f with 30 x the mean daily return >= 5%; the max drawdown at f* must be <= 35%
  - the mean at f* > 0 at 2x costs
Holdout: the same bar with f* from development, one look, only for trials that pass. A pass earns only a forward paper
series on the live book, whose real spreads this test cannot see.

Known limits: 1 cent of extra spread is 10% of the stake on a 0.10 share, so the 2x run matters more here than in round 27.
Hourly prices are not quotes, and the market's own favourite-side price stands in for the underdog's ask. Whether the
account holder may trade on Polymarket at all is not settled; see README.md.

Usage (image funding-research:py312, /opt/funding_arbitrage_research mounted at /w)
  python bt_underdog.py dev        results_underdog_dev.json, /w/pnl_round29.parquet
  python bt_underdog.py holdout    results_underdog_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

import bt_bonds as b

HERE = os.path.dirname(os.path.abspath(__file__))
KS = {"k1": 1, "k3": 3}
BANDS = {"f70": (0.70, 0.80), "f80": (0.80, 0.90), "f90": (0.90, 0.95)}
FRACTIONS = (0.01, 0.02, 0.05)


def signal(mk, t, p, K_days, low, high):
    """The favourite's signal turned round: the same market, the other outcome."""
    s = b.signal(mk, t, p, K_days, low, high)
    if s is None:
        return None
    return {**s, "yes": not s["yes"], "p": 1 - s["p"], "payout": 1 - s["payout"]}


def passes(by_f, by_f2, f=None):
    """by_f, by_f2: {f: daily returns} at 1x and 2x costs."""
    base = by_f[FRACTIONS[0]]
    if not (base.mean() > 0 and b.stats(base)["t"] >= 2.5):
        return False, None
    if f is None:
        f = next((x for x in FRACTIONS if b.stats(by_f[x])["month_pct"] >= 5), None)
    if f is None:
        return False, None
    a = b.stats(by_f[f])
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and by_f2[f].mean() > 0), f


def run_period(period, keys=None, fractions=None):
    lo, hi = (b.sec(x) for x in b.PERIODS[period])
    markets, px = b.load()
    print(f"{len(markets)} markets with history", flush=True)
    out, cols = {}, {}
    for kk, K in KS.items():
        for bb, (low, high) in BANDS.items():
            key = f"underdog|{kk}|{bb}"
            if keys is not None and key not in keys:
                continue
            trades = [s for mk in markets if (s := signal(mk, *px[mk["id"]], K, low, high)) and lo <= s["te"] < hi]
            trades.sort(key=lambda x: x["te"])
            r1, r2, n = {}, {}, {}
            for f in FRACTIONS:
                r1[f], n[f] = b.book(trades, px, f, 1.0, lo, hi)
                r2[f], _ = b.book(trades, px, f, 2.0, lo, hi)
            ok, chosen = passes(r1, r2, None if fractions is None else fractions[key])
            won = float(np.mean([t["payout"] for t in trades])) if trades else 0.0
            price = float(np.mean([t["p"] for t in trades])) if trades else 0.0
            out[key] = {"signals": len(trades), "taken": n, "won": round(won, 4), "price": round(price, 4),
                        "1x": {str(f): b.stats(r1[f]) for f in FRACTIONS}, "2x": {str(f): b.stats(r2[f]) for f in FRACTIONS},
                        "passes": ok, "f": chosen}
            cols[key] = r1[FRACTIONS[0]]
            print(key, json.dumps(out[key]), flush=True)
    frame = pd.DataFrame(cols)
    frame.index = pd.to_datetime(frame.index, unit="s")
    return out, frame


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl = run_period("dev")
        pnl.to_parquet("/w/pnl_round29.parquet")
        with open(f"{HERE}/results_underdog_dev.json", "w") as fh:
            json.dump(out, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_underdog_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_underdog_dev.json"))
        ok = {k: v["f"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = run_period("holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
