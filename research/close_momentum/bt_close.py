"""Round 36: does the day's move carry on into the last half hour, and can Binance's 3x fund perps trade it? (research only)

Pre-registered on 2026-11-10, before any price of the funds was read. Seen so far: bar counts, bar start times and split
dates from Yahoo; listing dates, funding terms and one current quote of the perps; and, in a mechanics check, hourly
SOXLUSDT and SOXSUSDT bars from 2026-10-23 12:00 to 10-26 18:00 UTC plus the one-day moves of those two perps on
2026-11-05. This file was checked on synthetic data only (selftest_close.py). No orders are placed anywhere.

Idea: a leveraged fund must trade in the direction of the day's move before the close (a 3x fund 6x its assets times
the index return, a -3x fund 12x), and option hedgers do the same. Baltussen, Da, Lammers and Martens (2021) find that
the return from the previous close to 15:30 predicts the last half hour in 60 futures from 1974 to 2020. On an index
the effect is a few bp, less than a fee. Since May 2026 Binance lists perps on the 3x funds themselves, which carry
three times the move for one fee, around the clock, at 5 bp.

Sessions: NYSE sessions as Yahoo shows them for the fund. A session with a 15:30 bar is full, any other is a half day.
Times are New York.
  C  the previous session's close, a half day included
  E  15:30, signal and entry
  X  16:00, exit
Only days that are full for both funds are counted. On such a day a fund has no leg if it split that day (split), if
one of its three prices is missing (no_price), or if they give |w| > 75% or a half-hour move > 25% (bad: a data error,
not a market).

Signal: w = P(E) / C - 1 on the fund itself. A leg opens when |w| >= THETA: long if w > 0, short if w < 0.

Prices
  development: Yahoo 60-minute bars of SOXL and TQQQ. P(E) is the open of the 15:30 bar and P(X) its close. C is the
    previous session's P(X), or after a half day the close of its last bar, which must start at 12:30 or 13:00.
  holdout: Binance USDT-M 1-minute klines of SOXLUSDT and TQQQUSDT. P(E) is the open of the 15:30 bar and P(X) the
    close of the 16:00 bar, the first minute after the bell, so that the closing auction is in the price. C is the
    previous session's P(X), or after a half day the close of its 13:00 bar. A bar without volume gives no price.

Costs per leg: 7 bp a side, which is the 5 bp taker fee plus 2 bp for spread and depth (one tick was 1.8 bp on both
perps on 2026-11-10, with as little as 800 USD at the touch). The fee is the standard USDT-M one; Binance's launch
notice says these contracts share it, but that was read in secondary sources only. No funding: these perps settle at
00, 08 and 16 UTC and the hold is 19:30-20:01 or 20:30-21:01 UTC. The 2x run doubles the cost.

Book: two slots, one per fund, each s x 2,500 / 2 of perp notional, s in {1, 2, 3}. A day's return is the sum over its
legs, flat size, no compounding. A day without a leg counts as 0.

Trials: 3 = THETA {3%, 6%, 9%} on the fund, which is 1%, 2%, 3% on its index.

Periods, by session date: development 2023-12-13..2026-05-14, before either perp existed; holdout 2026-05-15..
2026-11-09, on the perps (SOXLUSDT listed 2026-05-15, TQQQUSDT 2026-06-29; a leg needs the perp's own C).

Bar on development
  - mean day return > 0 with t >= 2.5 at s = 1 and 1x costs
  - s* = the smallest s with 21 x the mean >= 5% a month; the max drawdown at s* must be <= 35%
  - the mean > 0 at 2x costs
Holdout: at s* from development, 21 x the mean >= 5% a month, max drawdown <= 35% and the mean > 0 at 2x costs; t is
reported. One look, only for trials that pass. A pass earns only a forward paper series on the live book.

Known limits: development reads the funds and the holdout reads the perps, so whether the perp follows the fund through
the close is part of what the holdout tests. Whether Yahoo's 15:30 bar ends on the auction print or on the last trade
before it is not known. The signal and the entry are the same print; a live book would decide a second earlier.
Development is two and a half years of one regime. Drawdown is read at day ends, so a move inside the half hour does
not show, and s = 3 on a 3x fund is 9x the index: a 4% index move against the book in that half hour costs 36% of the
account. A halt of the underlying would leave the perp trading alone. The published effect may have been traded away
since 2021; that is what this round asks.

Usage (image funding-research:py312, /opt/funding_arbitrage_research mounted at /w)
  python download_bars.py     /w/data/close_momentum/yahoo_60m_dev.parquet, sessions.json
  python bt_close.py dev      results_close_dev.json, /w/pnl_round36.parquet
  python bt_close.py holdout  results_close_holdout.json, perp_1m_holdout.parquet (refuses a second time)
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.request

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = "/w/data/close_momentum"
ET = "America/New_York"
MIN_MS = 60_000
CAPITAL = 2_500.0
FUNDS = {"SOXL": "SOXLUSDT", "TQQQ": "TQQQUSDT"}
SLOTS = len(FUNDS)
COST = 0.0007
SIZES = (1, 2, 3)
DAYS_A_MONTH = 21
THETAS = {"t3": 0.03, "t6": 0.06, "t9": 0.09}
PERIODS = {"dev": ("2023-12-13", "2026-05-15"), "holdout": ("2026-05-15", "2026-11-10")}
BAD_SIGNAL, BAD_MOVE = 0.75, 0.25
ENTRY_BAR = "15:30"
HALF_CLOSE_BARS = ("12:30", "13:00")


def clock(day, hour, minute=0):
    """ms of `hour`:`minute` New York time on `day`."""
    return int(pd.Timestamp(f"{day} {hour:02d}:{minute:02d}").tz_localize(ET).timestamp() * 1000)


def fund(cal, close, entry, exit_):
    """One fund's book: its session calendar and the prices C, P(E), P(X) keyed by session date."""
    sessions = sorted(set(cal["full"]) | set(cal["half"]))
    return {"sessions": sessions, "index": {d: i for i, d in enumerate(sessions)}, "full": set(cal["full"]),
            "splits": set(cal["splits"]), "close": close, "e": entry, "x": exit_}


def signal(f, day):
    """-> (w, entry, exit) for one fund on one session, or the reason there is none."""
    if day not in f["full"]:
        return "half"
    if day in f["splits"]:
        return "split"
    i = f["index"][day]
    c = f["close"].get(f["sessions"][i - 1]) if i else None
    e, x = f["e"].get(day), f["x"].get(day)
    if c is None or e is None or x is None:
        return "no_price"
    w = e / c - 1
    if abs(w) > BAD_SIGNAL or abs(x / e - 1) > BAD_MOVE:
        return "bad"
    return w, e, x


def day_return(book, day, theta, mult):
    """-> (return at s = 1, [(fund, leg return)])."""
    legs = []
    for sym, f in book.items():
        got = signal(f, day)
        if isinstance(got, str) or abs(got[0]) < theta:
            continue
        w, e, x = got
        legs.append((sym, (1 if w > 0 else -1) * (x / e - 1) - mult * 2 * COST))
    return sum(r for _, r in legs) / SLOTS, legs


def stats(r, s=1):
    x = r * s
    curve = 1 + x.cumsum()
    t = float(x.mean() / x.std() * math.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0
    return {"n": len(x), "month_pct": round(float(x.mean()) * DAYS_A_MONTH * 100, 2), "t": round(t, 2),
            "max_dd_pct": round(float((1 - curve / curve.cummax()).max()) * 100, 2)}


def passes(r1, r2, s=None):
    """r1, r2: day returns at s = 1 and 1x, 2x costs. Without s the development bar, with s the holdout bar at that size."""
    if s is None:
        if not (r1.mean() > 0 and stats(r1)["t"] >= 2.5):
            return False, None
        s = next((x for x in SIZES if stats(r1, x)["month_pct"] >= 5), None)
        if s is None:
            return False, None
    a = stats(r1, s)
    return bool(a["month_pct"] >= 5 and a["max_dd_pct"] <= 35 and r2.mean() > 0), s


def load_dev(bars, cal):
    """bars: symbol, day, hm, open, close (Yahoo, New York dates and bar starts); cal: {fund: {full, half, splits}}."""
    book = {}
    for sym in FUNDS:
        g = bars[bars["symbol"] == sym].dropna(subset=["open", "close"]).sort_values(["day", "hm"])
        full = set(cal[sym]["full"])
        b = g[(g["hm"] == ENTRY_BAR) & g["day"].isin(full)]
        entry, exit_ = dict(zip(b["day"], b["open"])), dict(zip(b["day"], b["close"]))
        last = g[~g["day"].isin(full)].groupby("day").tail(1)
        close = dict(exit_)
        close.update({d: c for d, hm, c in zip(last["day"], last["hm"], last["close"]) if hm in HALF_CLOSE_BARS})
        book[sym] = fund(cal[sym], close, entry, exit_)
    return book


def klines(symbol, start, limit):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&startTime={start}&limit={limit}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def perp_prices(rows, e_ms, x_ms):
    """-> (open of the bar at e_ms, close of the bar at x_ms), each None unless that bar exists and traded."""
    by = {int(r[0]): r for r in rows}
    a, b = by.get(e_ms), by.get(x_ms)
    return (float(a[1]) if a and float(a[5]) > 0 else None, float(b[4]) if b and float(b[5]) > 0 else None)


def load_holdout(cal, fetch=klines, pause=0.1):
    """-> (book, the 1-minute bars it was built from). One call per fund and session."""
    lo, hi = PERIODS["holdout"]
    book, raw = {}, []
    for sym, perp in FUNDS.items():
        f = fund(cal[sym], {}, {}, {})
        days = [d for d in f["sessions"] if d < hi]
        first = max(0, next(i for i, d in enumerate(days) if d >= lo) - 1)  # one session earlier, for C
        for d in days[first:]:
            if d in f["full"]:
                e_ms, x_ms = clock(d, 15, 30), clock(d, 16, 0)
            else:
                e_ms = x_ms = clock(d, 13, 0)
            rows = fetch(perp, e_ms, (x_ms - e_ms) // MIN_MS + 1)
            e, x = perp_prices(rows, e_ms, x_ms)
            if e is not None and d in f["full"]:
                f["e"][d] = e
            if x is not None:
                f["close"][d] = x
                if d in f["full"]:
                    f["x"][d] = x
            raw.extend([perp, d] + [float(v) for v in r[:6]] for r in rows if e_ms <= int(r[0]) <= x_ms)
            time.sleep(pause)
        book[sym] = f
    return book, pd.DataFrame(raw, columns=["perp", "day", "t", "o", "h", "l", "c", "v"])


def run_period(book, period, keys=None, sizes=None):
    lo, hi = PERIODS[period]
    cals = [f["sessions"] for f in book.values()]
    if any(c != cals[0] for c in cals):
        raise SystemExit("the funds' calendars differ")
    days = [d for d in cals[0] if lo <= d < hi and all(d in f["full"] for f in book.values())]
    if not days:
        raise SystemExit(f"{period}: no full session")
    skipped = {sym: {} for sym in book}
    for sym, f in book.items():
        for d in days:
            got = signal(f, d)
            if isinstance(got, str):
                skipped[sym][got] = skipped[sym].get(got, 0) + 1
    print(f"{period}: {len(days)} full sessions {days[0]}..{days[-1]}, skipped {json.dumps(skipped)}", flush=True)
    out, cols = {"_skipped": skipped, "_sessions": len(days)}, {}
    for tt, theta in THETAS.items():
        key = f"close|follow|{tt}"
        if keys is not None and key not in keys:
            continue
        r1, r2, legs = [], [], []
        for d in days:
            a, leg = day_return(book, d, theta, 1.0)
            r1.append(a)
            r2.append(day_return(book, d, theta, 2.0)[0])
            legs.extend(leg)
        r1, r2 = pd.Series(r1, index=pd.to_datetime(days)), pd.Series(r2, index=pd.to_datetime(days))
        ok, chosen = passes(r1, r2, None if sizes is None else sizes[key])
        rets = [r for _, r in legs]
        half = len(rets) // 2
        out[key] = {"legs": len(legs), "active": int((r1 != 0).sum()),
                    "mean_leg_bp": round(sum(rets) / len(rets) * 1e4, 1) if rets else 0.0,
                    "won": round(sum(r > 0 for r in rets) / len(rets), 3) if rets else 0.0,
                    "by_fund": {sym: {"legs": sum(s == sym for s, _ in legs),
                                      "mean_leg_bp": round(sum(r for s, r in legs if s == sym) /
                                                           max(1, sum(s == sym for s, _ in legs)) * 1e4, 1)}
                                for sym in book},
                    "halves_bp": [round(sum(h) / len(h) * 1e4, 1) if h else 0.0 for h in (rets[:half], rets[half:])],
                    "1x": {str(s): stats(r1, s) for s in SIZES}, "2x": {str(s): stats(r2, s) for s in SIZES},
                    "passes": ok, "s": chosen}
        cols[key] = r1
        print(key, json.dumps(out[key]), flush=True)
    return out, pd.DataFrame(cols)


def calendar():
    with open(f"{DATA}/sessions.json") as fh:
        return json.load(fh)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl = run_period(load_dev(pd.read_parquet(f"{DATA}/yahoo_60m_dev.parquet"), calendar()), "dev")
        pnl.to_parquet("/w/pnl_round36.parquet")
        with open(f"{HERE}/results_close_dev.json", "w") as fh:
            json.dump(out, fh, indent=1)
    elif mode == "holdout":
        path = f"{HERE}/results_close_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        with open(f"{HERE}/results_close_dev.json") as fh:
            dev = json.load(fh)
        ok = {k: v["s"] for k, v in dev.items() if not k.startswith("_") and v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        book, raw = load_holdout(calendar())  # prints nothing; a failed download is not a look
        raw.to_parquet(f"{DATA}/perp_1m_holdout.parquet")
        with open(path, "w") as fh:
            json.dump({"_opened": True}, fh)  # spent from this point on, whatever happens next
        out, _ = run_period(book, "holdout", set(ok), ok)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
