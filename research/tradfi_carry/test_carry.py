"""Does a stock perp pay different funding on Binance and on Hyperliquid's xyz, steadily enough to earn it hedged?
(round 39, research only)

Pre-registered on 2026-11-11 while download.py was fetching the history. Only row counts were printed. No funding rate
or price of these perps was read before this file and its selftest were committed, and the code was checked on
synthetic data only (selftest_carry.py). No orders are placed anywhere, and no keys are used.

Why: Binance sets the interest term of 231 of its 235 stock, fund and metal perps to 0 (ordinary perps carry 0.01% per
8 h). Hyperliquid's xyz dex funds hourly with a multiplier of 0.5 and the usual interest term. The same stock can
therefore cost a long different amounts on the two venues. A pair long on the cheap venue and short on the dear one
keeps the difference while the two prices move together. The stage 4 plan (research/PLAN.md, D2) put the ceiling near
2% a month with leverage, so this is a test of a steady base rather than of the whole target.

Data (download.py on the workstation, history to 2026-11-11 00:00 UTC)
  bn_funding  Binance fundingRate history: fundingTime, fundingRate (paid by longs)
  bn_klines   Binance 1h klines; the price at hour h is the close of the candle that ends at h
  hl_funding  Hyperliquid fundingHistory: time, fundingRate (hourly, paid by longs)
  hl_candles  Hyperliquid 1h candles, the same way. Only the latest 5,000 are served, so prices start about 2026-04-16.
Pairs: the 99 of tradfi_lag's pairs file of 2026-11-11, less the 2 of type PREMARKET. A pair delisted before then is
missing.

Grid: UTC hours. A funding event belongs to the hour its time falls in. A price is carried forward at most 6 hours, and
within a held day from the last one seen since the decision.
Decision each day at 01:00 UTC (d), when no Binance stock perp settles:
  G = (sum of xyz funding over (d - L, d] - sum of Binance funding over (d - L, d]) / (3 L days), i.e. per 8 h
  eligible: both venues had a funding event in the window's first day, and both prices exist at d
  hold the 5 eligible pairs with the largest |G| >= THETA (ties by name); G > 0: long Binance, short xyz; G < 0: the
  reverse
A pair held over (d, d + 24 h] earns, per unit of notional on each leg, with sgn = +1 when long Binance:
  sgn x [(xyz funding - Binance funding over (d, d + 24 h]) + (PB(d + 24 h) / PB(d) - 1) - (PH(d + 24 h) / PH(d) - 1)]
Costs: each leg opened or closed pays a taker fee: Binance 0.05%, xyz 0.09% (growth mode off: when growth mode was on in
the past is unknown). A flip closes and opens both legs. Keeping a pair in the same direction costs nothing, and the
daily return to equal notional is not charged. The 2x run doubles both fees.
Book: each pair holds 0.2 x s of equity on each leg, s in {1, 2, 3}. The equity is split evenly between the venues and
evened out each day. A liquidation is a venue whose margin falls under 5% of its notional at any hourly close of the day
(margin = half the equity plus the open price PnL of its legs). A liquidation fails the trial at that s.

Trials: 6 = THETA {0.005%, 0.015%, 0.03% per 8 h} x L {3, 14 days}.

Bar on development (decisions 2026-04-20 .. 2026-08-31)
  - mean daily book return > 0 with t >= 2.5, t over calendar-week sums
  - monthly figure = mean daily book return x 30 >= 5% at the smallest such s*, max drawdown <= 35% and no liquidation at s*
  - monthly figure at s* > 0 at 2x costs
  - no calendar week carries more than half of the net sum (s = 1, 1x costs)
Holdout: decisions 2026-09-01 .. 2026-11-09, one look, only for trials that pass, at the dev s*. A pass earns only a forward
paper series. Reported, not part of the bar: the funding part and the price part, entries, pairs used, the mean |G|
held, and the mean daily return with xyz's growth-mode fee (0.009%).

Known limits: funding is charged on the notional set at d, not on the hourly mark. Prices are last-trade closes, not
the marks a venue liquidates on. Margin transfers between venues are assumed free and daily. Funding and price history
are what each venue serves today.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_carry.py dev        results_carry_dev.json, /w/pnl_round39.parquet
  python test_carry.py holdout    results_carry_holdout.json (refuses a second time)
"""

from __future__ import annotations

import gzip
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("CARRY_DATA", "/w/data/tradfi_carry")
PNL = "/w/pnl_round39.parquet"
HOUR = 3_600_000
DAY = 24 * HOUR
FFILL_H = 6
DECIDE_H = 1
FEE_B, FEE_H, FEE_H_GROWTH = 0.0005, 0.0009, 0.00009
SLOT, OPEN_MAX, SIZES = 0.2, 5, (1, 2, 3)
MM = 0.05
BEST_WEEK = 0.5
THETAS = {"t005": 0.00005, "t015": 0.00015, "t030": 0.0003}
LOOKS = {"l3": 3, "l14": 14}
TRIALS = {f"{a}|{b}": (THETAS[a], LOOKS[b]) for a in THETAS for b in LOOKS}
PERIODS = {"dev": ("2026-04-20", "2026-09-01"), "holdout": ("2026-09-01", "2026-11-10")}
END = "2026-11-11"


def ms(x):
    return int(pd.Timestamp(x, tz="UTC").value // 1_000_000)


def load(name):
    path = os.path.join(DATA, name.replace(":", "_") + ".json.gz")
    if not os.path.exists(path):
        return []
    with gzip.open(path, "rt") as fh:
        return json.load(fh)


def funding_series(rows, key_t, grid0, n):
    """Funding rows -> array on the hourly grid (sum per hour, 0 where none), and a mask of hours with an event."""
    out, seen = np.zeros(n), np.zeros(n, dtype=bool)
    for r in rows:
        h = (int(r[key_t]) - grid0) // HOUR
        if 0 <= h < n:
            out[h] += float(r["fundingRate"])
            seen[h] = True
    return out, seen


def price_series(pairs_t_close, grid0, n):
    """[(open time ms, close)] of 1h candles -> array of the price at each grid hour (the close of the candle ending
    then), carried forward at most FFILL_H hours; NaN elsewhere."""
    out = np.full(n, np.nan)
    for t, c in pairs_t_close:
        h = (int(t) + HOUR - grid0) // HOUR
        if 0 <= h < n:
            out[h] = float(c)
    return pd.Series(out).ffill(limit=FFILL_H).to_numpy()


def load_pairs(pairs, grid0, n):
    """-> {coin: dict of grid arrays fb, sb, fh, sh, pb, ph}."""
    out = {}
    for coin, p in sorted(pairs.items()):
        if p.get("type") == "PREMARKET":
            continue
        sym = p["binance"]
        fb, sb = funding_series(load(f"bn_funding/{sym}"), "fundingTime", grid0, n)
        fh, sh = funding_series(load(f"hl_funding/{coin}"), "time", grid0, n)
        pb = price_series([(k[0], k[4]) for k in load(f"bn_klines/{sym}")], grid0, n)
        ph = price_series([(c["t"], c["c"]) for c in load(f"hl_candles/{coin}")], grid0, n)
        out[coin] = {"fb": fb, "sb": sb, "fh": fh, "sh": sh, "pb": pb, "ph": ph}
    return out


def decisions(period, grid0):
    lo, hi = PERIODS[period]
    days = pd.date_range(lo, pd.Timestamp(hi) - pd.Timedelta(days=1))
    return [int((ms(d) - grid0) // HOUR) + DECIDE_H for d in days]


def gap(x, h, L):
    """Per-8 h funding gap G at grid hour h over (h - 24 L, h], and whether the pair is eligible."""
    w = slice(h - 24 * L + 1, h + 1)
    first = slice(h - 24 * L + 1, h - 24 * L + 25)
    ok = (h - 24 * L + 1 >= 0 and x["sb"][first].any() and x["sh"][first].any()
          and np.isfinite(x["pb"][h]) and np.isfinite(x["ph"][h]))
    return (x["fh"][w].sum() - x["fb"][w].sum()) / (3 * L), bool(ok)


def pick(data, h, theta, L):
    """-> {coin: sign} of the pairs held from hour h: +1 long Binance / short xyz, -1 the reverse."""
    cand = []
    for coin in sorted(data):
        g, ok = gap(data[coin], h, L)
        if ok and abs(g) >= theta:
            cand.append((-abs(g), coin, 1 if g > 0 else -1, g))
    cand.sort()
    return {c: (s, g) for _, c, s, g in cand[:OPEN_MAX]}


def day_parts(x, h, sgn):
    """One pair over (h, h + 24]: (funding part, price part, hourly price PnL of the Binance leg, of the xyz leg) per
    unit of notional on each leg. A price missing in the day is carried forward from the last one seen since h."""
    e = h + 24
    fund = sgn * (x["fh"][h + 1:e + 1].sum() - x["fb"][h + 1:e + 1].sum())
    pb = pd.Series(x["pb"][h:e + 1]).ffill().to_numpy()
    ph = pd.Series(x["ph"][h:e + 1]).ffill().to_numpy()
    rb, rh = pb[1:] / pb[0] - 1, ph[1:] / ph[0] - 1
    return fund, sgn * (rb[-1] - rh[-1]), sgn * rb, -sgn * rh


def simulate(data, hours, theta, L, s, mult, fee_h=FEE_H):
    """-> daily frame: ret, fund, price, cost, entries, liquidated, held (number of pairs), g_abs (mean |G| held).
    Whatever is held after the last day is closed, and that cost lands on the last day."""
    w = SLOT * s
    leg = w * (FEE_B + fee_h) * mult
    rows, held = [], {}
    for h in hours:
        want = pick(data, h, theta, L)
        cost = leg * sum(1 for c, sg in held.items() if c not in want or want[c][0] != sg)
        entries = sum(1 for c, (sg, _) in want.items() if held.get(c) != sg)
        cost += leg * entries
        fund = price = 0.0
        path_b, path_h = np.zeros(24), np.zeros(24)
        for c, (sg, _) in want.items():
            f, p, lb, lh = day_parts(data[c], h, sg)
            fund, price = fund + w * f, price + w * p
            path_b, path_h = path_b + w * lb, path_h + w * lh
        notional = w * len(want)
        liq = bool(notional > 0 and min((0.5 + path_b).min(), (0.5 + path_h).min()) < MM * notional)
        held = {c: sg for c, (sg, _) in want.items()}
        g_abs = float(np.mean([abs(g) for _, g in want.values()])) if want else 0.0
        rows.append({"h": h, "ret": fund + price - cost, "fund": fund, "price": price, "cost": cost,
                     "entries": entries, "liquidated": liq, "held": len(want), "g_abs": g_abs})
    if rows and held:
        rows[-1]["cost"] += leg * len(held)
        rows[-1]["ret"] -= leg * len(held)
    return pd.DataFrame(rows)


def drawdown(r):
    eq = (1 + r).cumprod()
    return float((1 - eq / eq.cummax()).max())


def week_t(daily, idx):
    weeks = daily.groupby(idx.to_period("W-SUN")).sum()
    t = float(weeks.mean() / weeks.std() * math.sqrt(len(weeks))) if len(weeks) > 2 and weeks.std() > 0 else 0.0
    return t, weeks


def evaluate(data, period, grid0, trials):
    hours = decisions(period, grid0)
    idx = pd.to_datetime([grid0 + (h - DECIDE_H) * HOUR for h in hours], unit="ms")
    out, pnl = {}, {}
    for key, (theta, L) in trials.items():
        r = {}
        for mult, m in (("1x", 1.0), ("2x", 2.0)):
            r[mult] = {"sizes": {}}
            for s in SIZES:
                d = simulate(data, hours, theta, L, s, m)
                daily = pd.Series(d["ret"].to_numpy(), index=idx)
                r[mult]["sizes"][str(s)] = {"monthly_figure_pct": round(float(daily.mean() * 30 * 100), 3),
                                            "max_dd_pct": round(drawdown(daily) * 100, 2),
                                            "liquidations": int(d["liquidated"].sum())}
                if s == 1:
                    t, weeks = week_t(daily, idx)
                    total = float(weeks.sum())
                    r[mult].update({"mean_bp_day": round(float(daily.mean() * 1e4), 3), "t_weeks": round(t, 2),
                                    "fund_pct": round(float(d["fund"].sum() * 100), 3),
                                    "price_pct": round(float(d["price"].sum() * 100), 3),
                                    "cost_pct": round(float(d["cost"].sum() * 100), 3),
                                    "entries": int(d["entries"].sum()), "days_held": int((d["held"] > 0).sum()),
                                    "mean_pairs": round(float(d["held"].mean()), 2),
                                    "mean_g_abs_bp": round(float(d.loc[d["held"] > 0, "g_abs"].mean() * 1e4), 3)
                                    if (d["held"] > 0).any() else None,
                                    "best_week_share": round(float(weeks.max() / total), 3) if total > 0 else None})
                    if mult == "1x":
                        pnl[f"tradficarry|{key}"] = daily
        g = simulate(data, hours, theta, L, 1, 1.0, FEE_H_GROWTH)
        r["mean_bp_day_growth_fee"] = round(float(g["ret"].mean() * 1e4), 3)
        out[key] = r
    frame = pd.DataFrame(pnl)
    frame.index = frame.index.normalize()
    return out, frame


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_bp_day"] > 0 and a["t_weeks"] >= 2.5):
        return False, size
    if size is None:
        size = next((s for s in SIZES if a["sizes"][str(s)]["monthly_figure_pct"] >= 5), None)
    if size is None:
        return False, None
    x = a["sizes"][str(size)]
    share = a.get("best_week_share")
    ok = (x["monthly_figure_pct"] >= 5 and x["max_dd_pct"] <= 35 and x["liquidations"] == 0
          and b["sizes"][str(size)]["monthly_figure_pct"] > 0 and share is not None and share <= BEST_WEEK)
    return bool(ok), size


def main():
    mode = sys.argv[1]
    if mode not in PERIODS:
        raise SystemExit("usage: test_carry.py dev|holdout")
    path = f"{HERE}/results_carry_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_carry_dev.json"))
        ok = [k for k in TRIALS if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trials = {k: TRIALS[k] for k in ok}
    else:
        trials = TRIALS
    pairs = json.load(open(os.path.join(DATA, "pairs.json")))
    grid0 = ms(PERIODS["dev"][0]) - 15 * DAY
    n = (ms(END) - grid0) // HOUR + 1
    data = load_pairs(pairs, grid0, n)
    out, pnl = evaluate(data, mode, grid0, trials)
    if all(r["1x"]["entries"] == 0 for r in out.values()):
        raise SystemExit("no entries at all: a data bug until shown otherwise")
    for key in trials:
        out[key]["passes"], out[key]["size"] = passes(out[key], None if dev is None else dev[key]["size"])
    out["_data"] = {"pairs": len(data), "grid0": grid0, "hours": int(n),
                    "with_prices_both": int(sum(np.isfinite(x["pb"]).any() and np.isfinite(x["ph"]).any()
                                                for x in data.values()))}
    if mode == "dev":
        pnl.to_parquet(PNL)
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
