"""Round 28: does the weekend move of a stock or commodity perp on Lighter reverse once the underlying reopens? (research only)

Pre-registered on 2026-11-05, before any price was read; only candle counts, volumes and field names have been seen, and
this file was checked on synthetic data only (selftest_weekend.py). No orders are placed anywhere.

Idea: when the underlying is closed, Lighter prices these perps from its own book, and the only traders left are crypto
accounts. Their move may overshoot and give back when the reference returns, or it may be early and carry on. Fees are
0/0 on a standard account, so a weekly trade can afford a spread that killed the earlier books.

Weekends: one per Friday, in New York time.
  R  Friday 16:00, the reference price
  S  Sunday 16:00, signal and entry
  X1 Sunday 20:00, exit once futures and overnight venues are back
  X2 Monday 11:00, exit after the cash open
A Friday is skipped when it or the next Monday is an NYSE holiday or half day (SKIPPED).

Signal: w = P(S) / P(R) - 1. P(x) is the close of the last bar with volume ending at or before x, no older than 2 h; at
an exit there is no age limit. An instrument needs 3.5M USD of volume over the 7 days before S. Instruments with |w| >=
THETA are ranked by |w| and the top 5 are traded, sign -w for "fade" and +w for "follow". A slot left empty stays in cash.

Costs per leg: 10 bp a side for spread (the fee is 0) plus the absolute funding of each hour in (S, X], charged whichever
way it went, because the API's direction field has no documented sign. The 2x run doubles both.
Book: each of the 5 slots holds s x 2,500 / 5, s in {1, 3, 5}. A weekend's return is the sum over its legs, flat size,
no compounding. A weekend with no leg counts as 0.

Trials: 12 = direction {fade, follow} x exit {X1, X2} x THETA {0.5%, 1%, 2%}.

Periods, by Friday: development 2025-12-05..2026-08-01; holdout 2026-08-01..2026-11-01.

Bar on development
  - mean weekend return > 0 with t >= 2.5 at s = 1 and 1x costs
  - s* = the smallest s with 52 / 12 x the mean >= 5% a month; the max drawdown at s* must be <= 35%
  - the mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass. A pass earns only a forward paper
series on the live book.

Known limits: a candle close is a last trade, not a quote. Weekend spreads are unknown until xvenue records its first
weekend (2026-11-07), and any trial that passes must have them checked against the 10 bp. Drawdown is read at weekend ends,
so a move inside a trade does not show. The universe is a list made today, though the volume test uses past data only.

Usage (image funding-research:py312, /opt/funding_arbitrage_research mounted at /w)
  python download_candles.py     /w/data/lighter/candles_1h.parquet, fund_1h.parquet
  python bt_weekend.py dev       results_weekend_dev.json, /w/pnl_round28.parquet
  python bt_weekend.py holdout   results_weekend_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = "/w/data/lighter"
ET = "America/New_York"
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
CAPITAL = 2_500.0
MAX_AGE = 2 * HOUR_MS
MIN_VOLUME = 3.5e6
SLOTS = 5
COST = 0.0010
SIZES = (1, 3, 5)
WEEKENDS_A_MONTH = 52 / 12
THETAS = {"t05": 0.005, "t10": 0.01, "t20": 0.02}
EXITS = ("x1", "x2")
DIRECTIONS = {"fade": -1, "follow": 1}
PERIODS = {"dev": ("2025-12-05", "2026-08-01"), "holdout": ("2026-08-01", "2026-11-01")}
SKIPPED = {"2025-11-28", "2026-01-16", "2026-02-13", "2026-04-03", "2026-05-22", "2026-06-19", "2026-07-03", "2026-09-04"}
UNIVERSE = """AAOI AAPL ADI AMD AMZN ARM ASML AVGO AXTI BABA BE BMNR BOTZ BRENTOIL COIN CRCL CRWV DELL DIA EWY GEV GME GOOGL
HOOD IBM INTC IREN IWM LITE MAGS META MRNA MRVL MSFT MSTR MU NATGAS NBIS NOK NOW NVDA ORCL PLTR QCOM QQQ RKLB SNDK SOXX SPCX
SPY TSLA TSM TTWO URA US100 US500 WDC WHEAT WTI XAG XAU XCU XPD XPT""".split()


def clock(day, hour):
    """ms of `hour`:00 New York time on `day`."""
    return int(pd.Timestamp(f"{day} {hour:02d}:00").tz_localize(ET).timestamp() * 1000)


def weekend(friday):
    d = pd.Timestamp(friday)
    sunday, monday = (d + pd.Timedelta(days=n) for n in (2, 3))
    return {"R": clock(d.date(), 16), "S": clock(sunday.date(), 16), "x1": clock(sunday.date(), 20),
            "x2": clock(monday.date(), 11)}


def fridays(period):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, hi, freq="W-FRI", inclusive="left")
    return [str(d.date()) for d in days if str(d.date()) not in SKIPPED]


def last_close(ins, at, max_age=None):
    """Close of the last bar with volume that ends at or before `at`, or None. Bars are stamped at their start."""
    i = int(np.searchsorted(ins["t"], at - HOUR_MS, "right")) - 1
    if i < 0 or (max_age is not None and at - (ins["t"][i] + HOUR_MS) > max_age):
        return None
    return float(ins["c"][i])


def volume_before(ins, at):
    """Quote volume of the bars inside the 7 days before `at`."""
    return float(ins["V"][(ins["t"] >= at - 7 * DAY_MS) & (ins["t"] + HOUR_MS <= at)].sum())


def picks(data, tm, theta):
    """-> legs (symbol, w, entry price), largest |w| first, at most SLOTS."""
    rows = []
    for sym, ins in data.items():
        ref, ent = last_close(ins, tm["R"], MAX_AGE), last_close(ins, tm["S"], MAX_AGE)
        if ref is None or ent is None or volume_before(ins, tm["S"]) < MIN_VOLUME:
            continue
        w = ent / ref - 1
        if abs(w) >= theta:
            rows.append((sym, w, ent))
    return sorted(rows, key=lambda x: -abs(x[1]))[:SLOTS]


def funding(ins, a, b):
    """Sum of the absolute hourly funding in (a, b], as a fraction."""
    return float(ins["fr"][(ins["ft"] > a) & (ins["ft"] <= b)].sum())


def leg_return(sign, ent, ext, paid, mult):
    return sign * (ext / ent - 1) - mult * (2 * COST + paid)


def weekend_return(data, tm, direction, exit_key, theta, mult):
    """-> (return at s = 1, the legs' returns)."""
    legs = []
    for sym, w, ent in picks(data, tm, theta):
        ext = last_close(data[sym], tm[exit_key])
        sign = DIRECTIONS[direction] * (1 if w > 0 else -1)
        legs.append(leg_return(sign, ent, ext, funding(data[sym], tm["S"], tm[exit_key]), mult))
    return sum(legs) / SLOTS, legs


def stats(r, s=1):
    x = r * s
    curve = 1 + x.cumsum()
    t = float(x.mean() / x.std() * math.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0
    return {"n": len(x), "month_pct": round(float(x.mean()) * WEEKENDS_A_MONTH * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - curve / curve.cummax()).max()) * 100, 2)}


def passes(r1, r2, s=None):
    """r1, r2: weekend returns at s = 1 and 1x, 2x costs."""
    if not (r1.mean() > 0 and stats(r1)["t"] >= 2.5):
        return False, None
    if s is None:
        s = next((x for x in SIZES if stats(r1, x)["month_pct"] >= 5), None)
    if s is None:
        return False, None
    a = stats(r1, s)
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and r2.mean() > 0), s


def load():
    c = pd.read_parquet(f"{DATA}/candles_1h.parquet")
    f = pd.read_parquet(f"{DATA}/fund_1h.parquet")
    c = c[c["symbol"].isin(UNIVERSE) & (c["V"] > 0)].sort_values(["symbol", "t"])
    fund = {k: g for k, g in f.groupby("symbol")}
    data = {}
    for sym, g in c.groupby("symbol"):
        fr = fund.get(sym)
        data[sym] = {"t": g["t"].to_numpy(np.int64), "c": g["c"].to_numpy(float), "V": g["V"].to_numpy(float),
                     "ft": np.array([], np.int64) if fr is None else fr["t"].to_numpy(np.int64),
                     "fr": np.array([]) if fr is None else fr["rate"].abs().to_numpy(float) / 100}
    return data


def run_period(period, keys=None, sizes=None):
    data = load()
    days = fridays(period)
    print(f"{len(data)} instruments, {len(days)} weekends", flush=True)
    out, cols = {}, {}
    for dd in DIRECTIONS:
        for xx in EXITS:
            for tt, theta in THETAS.items():
                key = f"weekend|{dd}|{xx}|{tt}"
                if keys is not None and key not in keys:
                    continue
                r1, r2, legs = [], [], []
                for f in days:
                    tm = weekend(f)
                    a, leg = weekend_return(data, tm, dd, xx, theta, 1.0)
                    r1.append(a)
                    r2.append(weekend_return(data, tm, dd, xx, theta, 2.0)[0])
                    legs.extend(leg)
                r1, r2 = pd.Series(r1, index=pd.to_datetime(days)), pd.Series(r2, index=pd.to_datetime(days))
                ok, chosen = passes(r1, r2, None if sizes is None else sizes[key])
                out[key] = {"legs": len(legs), "active": int((r1 != 0).sum()),
                            "mean_leg_bp": round(float(np.mean(legs)) * 1e4, 1) if legs else 0.0,
                            "won": round(float(np.mean(np.array(legs) > 0)), 3) if legs else 0.0,
                            "1x": {str(s): stats(r1, s) for s in SIZES}, "2x": {str(s): stats(r2, s) for s in SIZES},
                            "passes": ok, "s": chosen}
                cols[key] = r1
                print(key, json.dumps(out[key]), flush=True)
    return out, pd.DataFrame(cols)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl = run_period("dev")
        pnl.to_parquet("/w/pnl_round28.parquet")
        with open(f"{HERE}/results_weekend_dev.json", "w") as fh:
            json.dump(out, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_weekend_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_weekend_dev.json"))
        ok = {k: v["s"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = run_period("holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
