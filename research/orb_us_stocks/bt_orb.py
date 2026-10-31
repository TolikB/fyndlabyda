"""Opening-range breakout from day-trader-skill, tested on history (research only).

Pre-registered on 2026-10-31, before any result was computed. The rules are the skill's own
defaults (trader-scanner orb, trader-exit, trader-budget/config.example.json, trader-risk);
nothing is tuned.

Signal, per stock and session (full sessions only, all bars on the grid):
  range    the first 15 minutes (5-minute bars 9:30, 9:35, 9:40); high and low of those bars
  trigger  the first completed bar to CLOSE above the range high, closing no later than 15:30
  plan     entry = trigger close, stop = range midpoint rounded down to the cent, target = 2R;
           refused when the risk is under 0.05% of the price (trader-exit MIN_RISK_PCT)
  fill     the open of the next bar (the skill's entry is the trigger close; a real order fills
           later). If that open is at or below the stop, no trade (trader-risk re-checks a fresh price)
  exit     bar by bar from the fill: open at/below stop -> that open; low touches stop -> stop;
           open at/above target -> that open; high touches target -> target; a bar touching both
           counts as the stop (as trader-timemachine). Flat at the 15:55 open (flat_by).
  costs    no commission (Alpaca), 3 bp a side for spread, slippage and SEC/FINRA fees; rerun at 6 bp.
Universe, causal: each day the 300 stocks with the highest median daily dollar volume over the 20
  sessions before it, among the 600 downloaded (today's most traded; see the bias note in the README).
Portfolio, the skill's config: $5,000, 10% cash buffer, at most 3 trades a day, $50 max loss a
  trade (measured from the higher of plan entry and fill), $150 daily risk, a position at most
  75% of the budget, no margin. Candidates in the order the scanner shows them (trigger time, then
  opening-range dollar volume). Fixed $5,000 base, no compounding.

Tests (4, each at 1x and 2x costs):
  A   5-minute bars, the last 60 days (Yahoo's limit)
  B   1-hour bars, 2024-01..2026-10: range = the first hour, trigger closes no later than 15:30,
      exit at the 16:00 close (the 1h grid has no 15:55)
  CA, CB  A and B for "stocks in play" only: range volume >= 2x its mean over the stock's 14
      previous full sessions
Bar:
  edge      in B, the day-clustered mean R per trade is > 0 with t >= 2 at 1x and > 0 at 2x;
            A must not contradict it (mean R > 0)
  the goal  the skill portfolio in B makes >= 5% a month with max drawdown <= 35%
Anything else is reported as it is.
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
OPEN = dt.time(9, 30)

CFG = {"budget": 5000.0, "cash_buffer_pct": 10, "max_position_pct": 75, "max_order_usd": 4000.0,
       "max_loss_per_trade_usd": 50.0, "max_daily_loss_usd": 150.0, "max_trades_per_day": 3}
UNIVERSE_N = 300
LIQ_DAYS = 20
RVOL_DAYS = 14
MIN_RISK = 0.0005


class Grid:
    def __init__(self, minutes, n_range, last_trigger_end, flat):
        self.minutes = minutes
        self.n_range = n_range
        self.last_trigger_end = last_trigger_end  # minutes after 9:30
        self.flat = flat  # ("open", k) or ("close", k)
        self.n_bars = 78 if minutes == 5 else 7


GRID5 = Grid(5, 3, 360, ("open", 77))
GRID60 = Grid(60, 1, 360, ("close", 6))


def sessions(path, grid):
    """{date: [(o, h, l, c, v)] * n_bars} for full sessions only."""
    with gzip.open(path, "rt") as fh:
        s = json.load(fh)
    by_day = defaultdict(dict)
    for i, ts in enumerate(s["t"]):
        o, h, l, c, v = (s[k][i] if s[k] else None for k in ("o", "h", "l", "c", "v"))
        if None in (o, h, l, c):
            continue
        et = dt.datetime.fromtimestamp(ts, NY)
        mins = (et.hour * 60 + et.minute) - (9 * 60 + 30)
        if et.second or mins < 0 or mins % grid.minutes:
            continue
        k = mins // grid.minutes
        if k >= grid.n_bars:
            continue
        by_day[et.date()][k] = (float(o), float(h), float(l), float(c), float(v or 0))
    return {d: [b[k] for k in range(grid.n_bars)] for d, b in by_day.items() if len(b) == grid.n_bars}


def trade(bars, grid, cost):
    """The skill's rule on one session -> dict or None."""
    rng = bars[:grid.n_range]
    oh, ol = max(b[1] for b in rng), min(b[2] for b in rng)
    for k in range(grid.n_range, grid.n_bars - 1):
        if (k + 1) * grid.minutes > grid.last_trigger_end:
            return None
        if bars[k][3] > oh:
            break
    else:
        return None
    entry = bars[k][3]
    stop = math.floor((oh + ol) / 2 * 100) / 100
    risk = entry - stop
    if risk < MIN_RISK * entry:
        return None
    target = round(entry + 2 * risk, 2)
    fill = bars[k + 1][0]
    if fill <= stop:
        return None
    kind, last = grid.flat
    exit_px, exit_k = None, None
    for j in range(k + 1, grid.n_bars):
        o, h, l, c, _ = bars[j]
        if kind == "open" and j == last:
            exit_px, exit_k = o, j
            break
        if j > k + 1 and o <= stop:
            exit_px, exit_k = o, j
        elif l <= stop:
            exit_px, exit_k = stop, j
        elif j > k + 1 and o >= target:
            exit_px, exit_k = o, j
        elif h >= target:
            exit_px, exit_k = target, j
        elif kind == "close" and j == last:
            exit_px, exit_k = c, j
        if exit_px is not None:
            break
    pnl_share = exit_px * (1 - cost) - fill * (1 + cost)
    range_dv = sum(b[3] * b[4] for b in rng)
    return {"k": k, "entry": entry, "stop": stop, "fill": fill, "exit": exit_px, "exit_k": exit_k,
            "r": pnl_share / risk, "pnl_share": pnl_share, "risk_share": max(entry, fill) - stop,
            "range_vol": sum(b[4] for b in rng), "range_dv": range_dv}


def daily_universe(hourly):
    """{date: set(symbols)}: top UNIVERSE_N by the median dollar volume of the LIQ_DAYS sessions before."""
    dv = defaultdict(dict)
    for sym, days in hourly.items():
        for d, bars in days.items():
            dv[d][sym] = sum(b[3] * b[4] for b in bars)
    dates = sorted(dv)
    hist = defaultdict(list)
    out = {}
    for d in dates:
        ranked = [(statistics.median(hist[s][-LIQ_DAYS:]), s) for s in hist
                  if len(hist[s]) >= LIQ_DAYS and hourly[s].get(d)]
        ranked.sort(reverse=True)
        if len(ranked) >= UNIVERSE_N // 2:
            out[d] = {s for _, s in ranked[:UNIVERSE_N]}
        for s, v in dv[d].items():
            hist[s].append(v)
    return out


def signals(series, grid, universe, cost, in_play):
    """Every eligible trade: [(date, sym, trade)]."""
    out = []
    for sym, days in series.items():
        prev_vol = []
        for d in sorted(days):
            bars = days[d]
            rv = sum(b[4] for b in bars[:grid.n_range])
            ok = sym in universe.get(d, ())
            if ok and in_play:
                ok = len(prev_vol) >= RVOL_DAYS and rv >= 2 * statistics.mean(prev_vol[-RVOL_DAYS:])
            if ok:
                t = trade(bars, grid, cost)
                if t:
                    out.append((d, sym, t))
            prev_vol.append(rv)
    return out


def portfolio(sigs, grid):
    """The skill's config on the signals -> {date: pnl_usd}, trades taken."""
    by_day = defaultdict(list)
    for d, sym, t in sigs:
        by_day[d].append((t["k"], -t["range_dv"], sym, t))
    cap_pos = min(CFG["budget"] * CFG["max_position_pct"] / 100, CFG["max_order_usd"])
    cash = CFG["budget"] * (1 - CFG["cash_buffer_pct"] / 100)
    pnl, taken = {}, []
    for d in sorted(by_day):
        day_pnl, risk_used, n, open_pos = 0.0, 0.0, 0, []
        for k, _, sym, t in sorted(by_day[d]):
            if n >= CFG["max_trades_per_day"]:
                break
            open_pos = [p for p in open_pos if p[0] > k]  # closed before this fill
            free = cash - sum(p[1] for p in open_pos)
            shares = math.floor(min(CFG["max_loss_per_trade_usd"] / t["risk_share"], cap_pos / t["fill"],
                                    max(free, 0) / t["fill"]))
            max_loss = shares * t["risk_share"]
            if shares < 1 or risk_used + max_loss > CFG["max_daily_loss_usd"] + 1e-9:
                continue
            n += 1
            risk_used += max_loss
            open_pos.append((t["exit_k"], shares * t["fill"]))
            day_pnl += shares * t["pnl_share"]
            taken.append((d, sym, shares, t))
        pnl[d] = day_pnl
    return pnl, taken


def edge(sigs):
    """Day-clustered mean R: the mean of each day's mean R, and its t."""
    per_day = defaultdict(list)
    for d, _, t in sigs:
        per_day[d].append(t["r"])
    means = [statistics.mean(v) for v in per_day.values()]
    rs = [t["r"] for _, _, t in sigs]
    if len(means) < 3:
        return {"trades": len(rs)}
    m, sd = statistics.mean(means), statistics.stdev(means)
    return {"trades": len(rs), "days": len(means), "mean_r": round(statistics.mean(rs), 4),
            "day_mean_r": round(m, 4), "t": round(m / sd * math.sqrt(len(means)), 2),
            "win_pct": round(100 * sum(r > 0 for r in rs) / len(rs), 1)}


def months(pnl):
    out = defaultdict(float)
    for d, v in pnl.items():
        out[d.strftime("%Y-%m")] += v
    return {k: round(v / CFG["budget"] * 100, 2) for k, v in sorted(out.items())}


def max_dd(pnl):
    eq, peak, dd = CFG["budget"], CFG["budget"], 0.0
    for d in sorted(pnl):
        eq += pnl[d]
        peak = max(peak, eq)
        dd = max(dd, (peak - eq) / peak)
    return round(dd * 100, 2)


def load(interval, grid, syms):
    out = {}
    for s in syms:
        p = os.path.join(DATA, f"{s}_{interval}.json.gz")
        if os.path.exists(p):
            days = sessions(p, grid)
            if days:
                out[s] = days
    return out


def main():
    syms = json.load(open(os.path.join(DATA, "symbols.json")))["symbols"]
    hourly = load("1h", GRID60, syms)
    five = load("5m", GRID5, syms)
    universe = daily_universe(hourly)
    print(f"symbols: 1h {len(hourly)}, 5m {len(five)}; universe days {len(universe)} "
          f"({min(universe)}..{max(universe)})", flush=True)
    results = {}
    for name, series, grid, in_play in (("A", five, GRID5, False), ("B", hourly, GRID60, False),
                                        ("CA", five, GRID5, True), ("CB", hourly, GRID60, True)):
        for mult in (1, 2):
            sigs = signals(series, grid, universe, 0.0003 * mult, in_play)
            e = edge(sigs)
            pnl, taken = portfolio(sigs, grid)
            m = months(pnl)
            mv = list(m.values())
            row = {**e, "taken": len(taken), "months": m,
                   "month_mean_pct": round(statistics.mean(mv), 2) if mv else None,
                   "month_median_pct": round(statistics.median(mv), 2) if mv else None,
                   "max_dd_pct": max_dd(pnl), "total_pct": round(sum(pnl.values()) / CFG["budget"] * 100, 2)}
            by_year = defaultdict(list)
            for d, _, t in sigs:
                by_year[d.year].append(t["r"])
            row["mean_r_by_year"] = {y: round(statistics.mean(v), 4) for y, v in sorted(by_year.items())}
            results[f"{name}|cost{mult}x"] = row
            print(f"{name:<3} cost {mult}x  trades {e.get('trades')}  meanR {e.get('mean_r')}  "
                  f"dayR {e.get('day_mean_r')} t {e.get('t')}  win {e.get('win_pct')}%  | skill book: "
                  f"{len(taken)} trades, month mean {row['month_mean_pct']}% med {row['month_median_pct']}% "
                  f"dd {row['max_dd_pct']}% total {row['total_pct']}%  R/yr {row['mean_r_by_year']}", flush=True)
    with open("results_orb.json", "w") as fh:
        json.dump(results, fh, indent=1, default=str)


if __name__ == "__main__":
    sys.exit(main())
