"""Round 27: are favourites underpriced on Polymarket shortly before they resolve? (research only)

Pre-registered on 2026-11-05, before any price history was downloaded; only market metadata has been counted, and
this file was checked on synthetic data only (selftest_bonds.py). No orders are placed anywhere.

Idea: crypto perps have been the wrong place for a 2,500 USDT account: every edge there is taken by bigger and faster
players. Prediction markets are full of retail flow, the positions a 2,500 USDT account needs are too small to
interest a fund, and nothing has to happen in seconds. Studies of such markets find that outcomes priced near 1
win more often than their price says. Buying one means holding to a payout of 1 or 0.

Universe (universe()): two-outcome resolved markets with total volume >= 50,000, scheduled life >= 2 days, closed on or
after 2022-01-01, and a final price of (1, 0) or (0.5, 0.5) for a voided market.
Entry: at te = scheduled end - K, while the market is open (start < te < actual close). Take the last hourly price
at or before te, no older than 2 h. The favourite is the outcome priced above 0.5, at price p.
Trade: when LOW <= p < HIGH, buy the favourite and hold to the actual close, paying the final price of that outcome.
Costs per share: half spread 0.5 cent, plus a taker fee of 0.05 x p x (1 - p), Polymarket's 2026 formula with the
coefficient of its dearest non-crypto category, on every trade even though older markets had no fee. Redeeming a
winner is free. The 2x run doubles both.
Book: capital 2,500 flat, no compounding. Each trade buys f x 2,500 worth, f in {5%, 10%, 20%}, if that much cash is
free and no market of the same event is open. Cash is locked until the actual close. Equity is marked daily at the
last price, and the daily return is the change in equity over 2,500.

Trials: 9 = K {1, 3, 7 days} x band [0.80, 0.90), [0.90, 0.95), [0.95, 0.985).

Periods, by te: development 2024-01-01..2026-01-01; holdout 2026-01-01..2026-11-01.

Bar on development
  - mean daily return > 0 with t >= 2.5 at f = 5% and 1x costs
  - f* = the smallest f with 30 x the mean daily return >= 5%; the max drawdown at f* must be <= 35%
  - the mean at f* > 0 at 2x costs
Holdout: the same bar with f* from development, one look, only for trials that pass. A pass earns only a forward paper
series on the live order book, whose real spreads this test cannot see.

Known limits: hourly prices are not quotes, so a thin book's real ask may be worse (hence the 2x run and the volume
floor). A scheduled end that was moved after te is seen at its final value. Marks later than 7 days after the scheduled
end keep the last price, so a late drawdown of a disputed market shows up only at its close.

Corrected on 2026-11-05 after the first run, whose rules stay as above: that run gave 0 signals because the downloader's
interval=max returns only 31 days at hourly fidelity (download_prices.py now asks for each market's range), and its
calibration table was not limited to the period. All its prices fell in the holdout, and the table was not read.

Usage (image funding-research:py312, /opt/funding_arbitrage_research mounted at /w)
  python bt_bonds.py dev        results_bonds_dev.json, /w/pnl_round27.parquet
  python bt_bonds.py holdout    results_bonds_holdout.json (refuses a second time)
"""

from __future__ import annotations

import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = "/w/data/polymarket"
HOUR, DAY = 3_600, 86_400
CAPITAL = 2_500.0
MIN_VOLUME = 50_000.0
MIN_LIFE = 2 * DAY
FIRST_CLOSE = "2022-01-01"
MAX_AGE = 2 * HOUR
HALF_SPREAD = 0.005
FEE_K = 0.05
KS = {"k1": 1, "k3": 3, "k7": 7}
BANDS = {"b80": (0.80, 0.90), "b90": (0.90, 0.95), "b95": (0.95, 0.985)}
FRACTIONS = (0.05, 0.10, 0.20)
PERIODS = {"dev": ("2024-01-01", "2026-01-01"), "holdout": ("2026-01-01", "2026-11-01")}


def sec(day):
    return int(pd.Timestamp(day, tz="UTC").timestamp())


def universe(m):
    """Markets the test may use; times stay in ms here."""
    final = m["outcome_yes"].isin([0.0, 0.5, 1.0])
    return m[final & (m["volume"] >= MIN_VOLUME) & (m["end"] - m["start"] >= MIN_LIFE * 1000)
             & (m["closed"] >= sec(FIRST_CLOSE) * 1000)]


def cost_per_share(p, mult=1.0):
    return p + mult * (HALF_SPREAD + FEE_K * p * (1 - p))


def last_price(t, p, at):
    """Price of the last point at or before `at`, or None when it is older than MAX_AGE."""
    i = int(np.searchsorted(t, at, "right")) - 1
    return None if i < 0 or at - t[i] > MAX_AGE else float(p[i])


def signal(mk, t, p, K_days, low, high):
    """mk: one market with times in s. -> dict for a trade, or None."""
    te = mk["end"] - K_days * DAY
    if not mk["start"] < te < mk["closed"]:
        return None
    py = last_price(t, p, te)
    if py is None:
        return None
    yes = py > 0.5
    fav = py if yes else 1 - py
    if not low <= fav < high:
        return None
    return {"te": te, "closed": mk["closed"], "m": mk["id"], "event": mk["event"], "yes": yes, "p": fav,
            "payout": mk["outcome_yes"] if yes else 1 - mk["outcome_yes"]}


def book(trades, px, f, mult, lo, hi):
    """trades: dicts sorted by te. px: {market: (t, p)}. -> (daily returns over [lo, hi), number taken)."""
    size = f * CAPITAL
    cash, opened, taken = CAPITAL, [], 0
    busy = set()
    days = np.arange(lo, hi, DAY)
    equity = np.empty(len(days))
    i = 0
    for n, d in enumerate(days):
        end_of_day = d + DAY
        while True:
            next_entry = trades[i]["te"] if i < len(trades) else None
            if next_entry is None or next_entry >= end_of_day:
                break
            tr = trades[i]
            i += 1
            for x in [o for o in opened if o["closed"] <= tr["te"]]:
                cash += x["shares"] * x["payout"]
                busy.discard(x["event"])
                opened.remove(x)
            if tr["event"] in busy or cash < size:
                continue
            shares = size / cost_per_share(tr["p"], mult)
            cash -= size
            busy.add(tr["event"])
            taken += 1
            opened.append({**tr, "shares": shares})
        for x in [o for o in opened if o["closed"] < end_of_day]:
            cash += x["shares"] * x["payout"]
            busy.discard(x["event"])
            opened.remove(x)
        marks = 0.0
        for x in opened:
            t, p = px[x["m"]]
            j = int(np.searchsorted(t, end_of_day, "left")) - 1
            py = float(p[j]) if j >= 0 else x["p"]
            marks += x["shares"] * (py if x["yes"] else 1 - py)
        equity[n] = cash + marks
    r = np.diff(np.r_[CAPITAL, equity]) / CAPITAL
    return pd.Series(r, index=days), taken


def stats(d):
    curve = 1 + d.cumsum()
    t = float(d.mean() / d.std() * math.sqrt(len(d))) if len(d) > 2 and d.std() > 0 else 0.0
    return {"days": len(d), "month_pct": round(float(d.mean()) * 30 * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - curve / curve.cummax()).max()) * 100, 2)}


def passes(by_f, by_f2, f=None):
    """by_f, by_f2: {f: daily returns} at 1x and 2x costs."""
    base = by_f[FRACTIONS[0]]
    if not (base.mean() > 0 and stats(base)["t"] >= 2.5):
        return False, None
    if f is None:
        f = next((x for x in FRACTIONS if stats(by_f[x])["month_pct"] >= 5), None)
    if f is None:
        return False, None
    a = stats(by_f[f])
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and by_f2[f].mean() > 0), f


def load():
    m = universe(pd.read_parquet(f"{DATA}/markets.parquet"))
    frames = [pd.read_parquet(p) for p in sorted(glob.glob(f"{DATA}/px/[0-9]*.parquet"))]
    h = pd.concat(frames, ignore_index=True).sort_values(["m", "t"])
    px = {int(k): (g["t"].to_numpy(np.int64), g["p"].to_numpy(float)) for k, g in h.groupby("m")}
    mk = m[m["id"].isin(px)].copy()
    for c in ("start", "end", "closed"):
        mk[c] = mk[c] // 1000
    return mk.to_dict("records"), px


def calibration(markets, px, K_days, lo, hi):
    """Descriptive: win rate by price band for entries inside [lo, hi), no book and no costs."""
    rows = []
    for mk in markets:
        s = signal(mk, *px[mk["id"]], K_days, 0.5, 1.01)
        if s and lo <= s["te"] < hi:
            rows.append(s)
    d = pd.DataFrame(rows)
    d["band"] = pd.cut(d["p"], [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.985, 1.01], right=False)
    g = d.groupby("band", observed=True).agg(n=("p", "size"), price=("p", "mean"), won=("payout", "mean"))
    return g.round(4).reset_index().astype({"band": str}).to_dict("records")


def run_period(period, keys=None, fractions=None):
    lo, hi = (sec(x) for x in PERIODS[period])
    markets, px = load()
    print(f"{len(markets)} markets with history", flush=True)
    out, cols = {}, {}
    for kk, K in KS.items():
        for bb, (low, high) in BANDS.items():
            key = f"bonds|{kk}|{bb}"
            if keys is not None and key not in keys:
                continue
            trades = [s for mk in markets if (s := signal(mk, *px[mk["id"]], K, low, high)) and lo <= s["te"] < hi]
            trades.sort(key=lambda x: x["te"])
            r1, r2, n = {}, {}, {}
            for f in FRACTIONS:
                r1[f], n[f] = book(trades, px, f, 1.0, lo, hi)
                r2[f], _ = book(trades, px, f, 2.0, lo, hi)
            ok, chosen = passes(r1, r2, None if fractions is None else fractions[key])
            won = float(np.mean([t["payout"] for t in trades])) if trades else 0.0
            price = float(np.mean([t["p"] for t in trades])) if trades else 0.0
            out[key] = {"signals": len(trades), "taken": n, "won": round(won, 4), "price": round(price, 4),
                        "1x": {str(f): stats(r1[f]) for f in FRACTIONS}, "2x": {str(f): stats(r2[f]) for f in FRACTIONS},
                        "passes": ok, "f": chosen}
            cols[key] = r1[FRACTIONS[0]]
            print(key, json.dumps(out[key]), flush=True)
    frame = pd.DataFrame(cols)
    frame.index = pd.to_datetime(frame.index, unit="s")
    return out, frame, {f"k{K}": calibration(markets, px, K, lo, hi) for K in KS.values()} if period == "dev" else {}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl, cal = run_period("dev")
        pnl.to_parquet("/w/pnl_round27.parquet")
        with open(f"{HERE}/results_bonds_dev.json", "w") as fh:
            json.dump({"trials": out, "calibration": cal}, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_bonds_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_bonds_dev.json"))["trials"]
        ok = {k: v["f"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _, _ = run_period("holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
