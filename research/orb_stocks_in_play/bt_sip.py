"""5-minute opening-range breakout on "stocks in play" (Zarattini, Barbon & Aziz 2024), research only.

Pre-registered on 2026-10-31, before any data was downloaded. The rules are the paper's, as
summarised by CXO Advisory and re-implemented on QuantConnect; nothing is tuned.

Data: Alpaca SIP (all US exchanges), 2016-01..2026-10. Daily bars raw and split-adjusted, the
first 5-minute bar of each day, 1-minute bars of the selected stocks. Universe: every US-listed
stock Alpaca knows on NYSE, NASDAQ, AMEX, ARCA or BATS, active and delisted, names that read
like an ETF, ETN or fund left out.

Each day, at 9:35 New York time:
  filters   today's open > $5; mean daily volume over the previous 14 sessions >= 1,000,000
            shares; ATR(14) over the previous 14 sessions > $0.50 (true range, simple mean);
            no split inside those 15 sessions (raw/split-adjusted close ratio unchanged)
  in play   relative volume = today's 9:30-9:35 volume / its mean over the previous 14 sessions
            (at least 12 of them present); >= 1; the 20 highest are traded
  direction the first 5-minute bar closed above its open -> buy stop at its high; below -> sell
            stop at its low; unchanged -> no trade
  entry     the first 1-minute bar from 9:35 on that reaches the level; fill at the level, or at
            that bar's open if it opened through it
  stop      10% of ATR(14) from the fill
  exit      a later bar opening through the stop -> that open; touching it -> the stop; else the
            close of the last regular-session minute. The entry bar counts as stopped when it
            touches the stop (pessimistic); the bound where it counts only when it CLOSES through
            the stop is reported beside it
  costs     3 bp a side (spread, slippage, fees; Alpaca charges no commission); rerun at 6 bp
  R         profit per share / (0.1 x ATR)
Portfolios (compounded daily from 1.0):
  slots     each trade gets L/20 of equity as notional; L = 1, 2, 4 (4 = the intraday buying
            power of a US margin day-trading account)
  paper     1% of equity at risk a trade, at most 4x equity a position, as the paper sizes it;
            reported for reference only: its gross exposure is not something a broker allows
  capped    the paper sizing with total gross notional <= 4x equity at each fill, first come first served
Periods: 2016-01..2023-12 is the paper's own sample; 2024-01..2026-10 came after publication.
Bar:
  edge      2024-01..2026-10: the day-clustered mean R > 0 with t >= 2 at 3 bp, and > 0 at 6 bp
  the goal  the slots or capped book, with the largest L in {1, 2, 4} whose 2016-2023 drawdown is
            <= 35%, makes >= 5% a month in 2024-01..2026-10 with drawdown <= 35%
Long-only and short-only rows are a breakdown of the same trades, not extra trials.
Anything else is reported as it is; nothing is re-tuned after the run.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import math
import os
import statistics
import sys
from collections import defaultdict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
DATA = "data"
LOOKBACK = 14
MIN_HIST = 12
TOP_N = 20
ATR_STOP = 0.10
MIN_OPEN = 5.0
MIN_AVG_VOL = 1_000_000
MIN_ATR = 0.50
SPLIT_TOL = 0.02
OOS_START = dt.date(2024, 1, 1)


def daily_candidates(rows):
    """rows: [(date, o, h, l, c, v, factor)] sorted -> {date: atr} for the days that pass."""
    out = {}
    for i in range(LOOKBACK + 1, len(rows)):
        d, o = rows[i][0], rows[i][1]
        if o is None or o <= MIN_OPEN:
            continue
        win = rows[i - LOOKBACK:i]
        avg_vol = sum(r[5] for r in win) / LOOKBACK
        if avg_vol < MIN_AVG_VOL:
            continue
        f = rows[i][6]
        if any(abs(r[6] / f - 1) > SPLIT_TOL for r in rows[i - LOOKBACK - 1:i]):
            continue
        trs = [max(rows[j][2], rows[j - 1][4]) - min(rows[j][3], rows[j - 1][4]) for j in range(i - LOOKBACK, i)]
        atr = sum(trs) / LOOKBACK
        if atr > MIN_ATR:
            out[d] = atr
    return out


def select(day_i, calendar, firstbars, cands):
    """The 20 stocks in play on calendar[day_i] -> [(sym, direction, high, low, atr, rvol)]."""
    day = calendar[day_i]
    today = firstbars.get(day, {})
    hist_days = calendar[max(0, day_i - LOOKBACK):day_i]
    if len(hist_days) < LOOKBACK:
        return []
    picks = []
    for sym, atr in cands.get(day, {}).items():
        fb = today.get(sym)
        if not fb:
            continue
        o, h, l, c, v = fb
        hist = [firstbars.get(d, {}).get(sym) for d in hist_days]
        vols = [x[4] for x in hist if x]
        if len(vols) < MIN_HIST or sum(vols) <= 0:
            continue
        rvol = v / (sum(vols) / len(vols))
        direction = 1 if c > o else -1 if c < o else 0
        if rvol >= 1 and direction:
            picks.append((sym, direction, h, l, atr, rvol))
    picks.sort(key=lambda p: -p[5])
    return picks[:TOP_N]


def simulate(bars, pick, cost, pessimistic=True):
    """bars: [[minute, o, h, l, c, v]] from 9:35 on, sorted. -> trade dict or None (never triggered)."""
    sym, d, hi, lo, atr, rvol = pick
    level = hi if d > 0 else lo
    risk = ATR_STOP * atr
    for k, (m, o, h, l, c, v) in enumerate(bars):
        if (d > 0 and h >= level) or (d < 0 and l <= level):
            break
    else:
        return None
    fill = max(o, level) if d > 0 else min(o, level)
    stop = fill - d * risk
    exit_px, exit_m = None, None
    m, o, h, l, c, v = bars[k]
    if (d > 0 and (l <= stop if pessimistic else c <= stop)) or (d < 0 and (h >= stop if pessimistic else c >= stop)):
        exit_px, exit_m = stop, m
    else:
        for m, o, h, l, c, v in bars[k + 1:]:
            if (d > 0 and o <= stop) or (d < 0 and o >= stop):
                exit_px, exit_m = o, m
            elif (d > 0 and l <= stop) or (d < 0 and h >= stop):
                exit_px, exit_m = stop, m
            if exit_px is not None:
                break
    if exit_px is None:
        exit_px, exit_m = bars[-1][4], bars[-1][0]
    pnl = d * (exit_px - fill) - cost * (fill + exit_px)
    return {"sym": sym, "dir": d, "entry_m": bars[k][0], "exit_m": exit_m, "fill": fill, "exit": exit_px,
            "atr": atr, "rvol": rvol, "r": pnl / risk, "ret": pnl / fill}


# ---------------------------------------------------------------- portfolios

def book_slots(trades_by_day, lev):
    return {d: sum(t["ret"] for t in ts) * lev / TOP_N for d, ts in trades_by_day.items()}


def book_paper(trades_by_day, cap_total=None):
    """Returns daily simple returns; sizes off the start-of-day equity, as fractions of it."""
    out = {}
    for d, ts in trades_by_day.items():
        gross, day_ret, live = 0.0, 0.0, []
        for t in sorted(ts, key=lambda t: t["entry_m"]):
            w = min(0.01 / (ATR_STOP * t["atr"] / t["fill"]), 4.0)  # notional as a multiple of equity
            if cap_total is not None:
                live = [x for x in live if x[0] >= t["entry_m"]]
                room = cap_total - sum(x[1] for x in live)
                w = min(w, max(room, 0.0))
                if w <= 0:
                    continue
                live.append((t["exit_m"], w))
            gross += w
            day_ret += w * t["ret"]
        out[d] = day_ret
    return out


def stats(daily, start=None, end=None):
    days = sorted(d for d in daily if (start is None or d >= start) and (end is None or d < end))
    if not days:
        return {}
    eq, peak, dd, months = 1.0, 1.0, 0.0, defaultdict(lambda: 1.0)
    rets = []
    for d in days:
        r = max(daily[d], -0.99)
        rets.append(r)
        eq *= 1 + r
        peak = max(peak, eq)
        dd = max(dd, 1 - eq / peak)
        months[d.strftime("%Y-%m")] *= 1 + r
    m = [v - 1 for v in months.values()]
    sd = statistics.stdev(rets) if len(rets) > 1 else 0
    return {"days": len(days), "month_mean_pct": round(statistics.mean(m) * 100, 2),
            "month_median_pct": round(statistics.median(m) * 100, 2),
            "pos_months_pct": round(100 * sum(x > 0 for x in m) / len(m), 1),
            "max_dd_pct": round(dd * 100, 1), "total_x": round(eq, 3),
            "sharpe": round(statistics.mean(rets) / sd * math.sqrt(252), 2) if sd else None,
            "worst_month_pct": round(min(m) * 100, 1)}


def edge(trades, start=None, end=None, side=None):
    per_day = defaultdict(list)
    for d, t in trades:
        if (start is None or d >= start) and (end is None or d < end) and (side is None or t["dir"] == side):
            per_day[d].append(t["r"])
    means = [statistics.mean(v) for v in per_day.values()]
    n = sum(len(v) for v in per_day.values())
    if len(means) < 3:
        return {"trades": n}
    m, sd = statistics.mean(means), statistics.stdev(means)
    allr = [r for v in per_day.values() for r in v]
    return {"trades": n, "days": len(means), "mean_r": round(statistics.mean(allr), 4),
            "day_mean_r": round(m, 4), "t": round(m / sd * math.sqrt(len(means)), 2),
            "win_pct": round(100 * sum(r > 0 for r in allr) / len(allr), 1)}


# ---------------------------------------------------------------- run

def load_json(path):
    with gzip.open(path, "rt") if path.endswith(".gz") else open(path) as fh:
        return json.load(fh)


def main():
    cal = [dt.date.fromisoformat(x) for x in load_json(os.path.join(DATA, "calendar.json"))]
    cands = {dt.date.fromisoformat(d): m
             for d, m in load_json(os.path.join(DATA, "candidates.json.gz")).items()}
    firstbars = {}
    for d in cal:
        p = os.path.join(DATA, "first", f"{d}.json.gz")
        if os.path.exists(p):
            firstbars[d] = load_json(p)
    results, trades = {}, {}
    for mult in (1, 2):
        for pess in (True, False):
            key = f"cost{mult}x|{'pess' if pess else 'bound'}"
            tr = []
            for i, d in enumerate(cal):
                p = os.path.join(DATA, "minute", f"{d}.json.gz")
                if not os.path.exists(p):
                    continue
                bars = load_json(p)
                for pick in select(i, cal, firstbars, cands):
                    b = bars.get(pick[0])
                    if b:
                        t = simulate(b, pick, 0.0003 * mult, pess)
                        if t:
                            tr.append((d, t))
            trades[key] = tr
            by_day = defaultdict(list)
            for d, t in tr:
                by_day[d].append(t)
            row = {"edge_all": edge(tr), "edge_ins": edge(tr, end=OOS_START), "edge_oos": edge(tr, start=OOS_START),
                   "edge_oos_long": edge(tr, start=OOS_START, side=1), "edge_oos_short": edge(tr, start=OOS_START, side=-1),
                   "edge_ins_long": edge(tr, end=OOS_START, side=1), "edge_ins_short": edge(tr, end=OOS_START, side=-1)}
            books = {f"slots_L{L}": book_slots(by_day, L) for L in (1, 2, 4)}
            books["paper"] = book_paper(by_day)
            books["capped4x"] = book_paper(by_day, cap_total=4.0)
            for name, daily in books.items():
                row[name] = {"ins": stats(daily, end=OOS_START), "oos": stats(daily, start=OOS_START)}
            by_year = defaultdict(list)
            for d, t in tr:
                by_year[d.year].append(t["r"])
            row["mean_r_by_year"] = {y: round(statistics.mean(v), 4) for y, v in sorted(by_year.items())}
            results[key] = row
            print(f"== {key}: all {row['edge_all']}\n   2016-23 {row['edge_ins']}\n   2024-26 {row['edge_oos']}\n"
                  f"   oos long {row['edge_oos_long']}\n   oos short {row['edge_oos_short']}\n"
                  f"   R by year {row['mean_r_by_year']}", flush=True)
            for name in books:
                print(f"   {name:<10} 2016-23 {row[name]['ins']}\n   {'':<10} 2024-26 {row[name]['oos']}", flush=True)
    with open("results_sip.json", "w") as fh:
        json.dump(results, fh, indent=1, default=str)


if __name__ == "__main__":
    sys.exit(main())
