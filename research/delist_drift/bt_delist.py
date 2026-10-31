"""Short the perp after a Binance spot delisting announcement (round 15, research only).

Pre-registered on 2026-10-31, before any price around an announcement was looked at. Only the
announcement titles and the hourly archive's coverage were inspected. Nothing is tuned after the
first run. No orders are placed anywhere.

Idea: "Binance Will Delist X on <date>" forces Binance-only holders to sell X before that date. The
first drop takes seconds and is out of reach. The forced selling may keep pushing the price down for
days, in coins too illiquid for a fund. The trade shorts X's USDT-M perpetual after the drop.

Events (download_announcements.py: Binance CMS catalog 161, "Delisting")
  titles matching "Binance Will Delist <list> on <YYYY-MM-DD>" (case-insensitive); the list is split on
  ", " and " and "; each item must be a plain ticker [A-Z0-9]+. Titles about margin pairs, futures,
  options or earn products do not match and are left out.
  T       the announcement's releaseDate
  perp    <TICKER>USDT, else 1000<TICKER>USDT, in the Binance 1h archive (minus bt.EXCLUDE); an event
          without a perp bar with volume at the entry hour is skipped

Trade, per event
  entry   short at the open of the first whole hour starting at or after T + 15 minutes
  exit    the open of the bar entry + H hours, H in {24, 72}; if the perp stops trading earlier, the
          open of the bar one hour before its last bar with volume (its delisting is announced ahead)
  return  -(exit / entry - 1) + funding (a short receives positive rates, settlements s with
          entry < s <= exit) - 2 * (0.05% taker + bt.slippage tier of the 30-day median daily quote
          volume, known the day before); everything is rerun at 2x fees and slippage

Book
  5 slots of 20% of equity, no leverage. Events entering in the same hour are taken in order of
  their 30-day median volume, most liquid first; first come first served otherwise. A trade's return
  lands on its exit day; equity compounds daily. The book's months start at the first trade (the
  catalog's announcements start in 2022-03).

Trials: 2 (H = 24, 72)

Bar on development (announcements before 2025-07-01)
  - mean net return a trade > 0 with t >= 2.5, where t is over announcements (each announcement's
    accepted trades are averaged first, because one title often names several coins)
  - monthly mean >= 5% with max drawdown <= 35% at 1x
  - monthly mean > 0 at 2x

Holdout: announcements 2025-07-01..2026-09-30 (the hourly archive ends there), one look, only for
trials that pass, with the same bar. Daily and hourly research has seen the 2026-04..09 price
regime, not these events.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_delist.py events    print the parsed events with their entry hours (no prices)
  python bt_delist.py dev       results_dev.json, /w/pnl_round15.parquet
  python bt_delist.py holdout   results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import os
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
sys.path.insert(0, "/w/wick")
import bt  # noqa: E402  (EXCLUDE, stats)
import bt_wick  # noqa: E402  (universe slippage, funding events)

ROOT = "/w"
HERE = os.path.dirname(os.path.abspath(__file__))
ANNOUNCEMENTS = f"{ROOT}/data/delist/announcements.json"
HS = (24, 72)
TAKER = 0.0005
SLOTS = 5
HOUR = 3_600_000
DELAY = 15 * 60_000
PERIODS = {"dev": ("2020-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-10-01")}
TITLE = re.compile(r"^binance will delist (.+) on (\d{4}-\d{2}-\d{2})$", re.IGNORECASE)
TICKER = re.compile(r"^[A-Z0-9]+$")


def parse_title(title):
    """-> list of tickers, or [] when the title is not a plain spot delisting."""
    m = TITLE.match(title.strip())
    if not m:
        return []
    items = [x.strip() for x in re.split(r",\s*|\s+and\s+", m.group(1))]
    return items if items and all(TICKER.match(x) for x in items) else []


def entry_hour(release_ms):
    """Open time (ms) of the first whole hour starting at or after release + 15 minutes."""
    t = release_ms + DELAY
    return -(-t // HOUR) * HOUR


def events(articles, symbols):
    """articles: [{id, title, releaseDate}] -> DataFrame(ann, title, T, ticker, sym, entry_t)."""
    rows = []
    for a in articles:
        for tk in parse_title(a["title"]):
            sym = next((s for s in (f"{tk}USDT", f"1000{tk}USDT") if s in symbols and s not in bt.EXCLUDE), None)
            rows.append({"ann": a["id"], "title": a["title"], "T": int(a["releaseDate"]), "ticker": tk, "sym": sym,
                         "entry_t": entry_hour(int(a["releaseDate"]))})
    return pd.DataFrame(rows)


def trade(t, o, qv, entry_t, H):
    """t, o, qv: one perp's hourly bars (sorted). -> (entry_px, exit_t, exit_px) or None."""
    i = np.searchsorted(t, entry_t)
    if i >= len(t) or t[i] != entry_t or qv[i] <= 0:
        return None
    traded = np.flatnonzero(qv > 0)
    cut = t[traded[-1]] - HOUR
    exit_t = min(entry_t + H * HOUR, cut)
    if exit_t <= entry_t:
        return None
    j = np.searchsorted(t, exit_t)
    if j >= len(t) or t[j] != exit_t:
        return None
    return o[i], exit_t, o[j]


def net(entry_px, exit_px, funding_sum, slip, mult):
    return -(exit_px / entry_px - 1.0) + funding_sum - mult * 2 * (TAKER + slip)


def book(tr):
    """tr: DataFrame(entry_t, exit_t, liq, ret) -> (accepted, daily returns)."""
    tr = tr.sort_values(["entry_t", "liq"], ascending=[True, False])
    active, keep = [], []
    for idx, et, xt in zip(tr.index, tr["entry_t"].to_numpy(), tr["exit_t"].to_numpy()):
        active = [x for x in active if x > et]
        if len(active) < SLOTS:
            active.append(xt)
            keep.append(idx)
    acc = tr.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    return acc, (acc["ret"] / SLOTS).groupby(day).sum()


def load_hourly(symbols):
    h = pd.read_parquet(f"{ROOT}/data/klines_1h.parquet", columns=["symbol", "open_time", "open", "quote_volume"],
                        filters=[("symbol", "in", sorted(symbols))])
    return {s: (g["open_time"].to_numpy(np.int64), g["open"].to_numpy(float), g["quote_volume"].to_numpy(float))
            for s, g in h.sort_values("open_time").groupby("symbol")}


def all_symbols():
    import pyarrow.parquet as pq
    col = pq.read_table(f"{ROOT}/data/klines_1h.parquet", columns=["symbol"]).column("symbol")
    return set(col.unique().to_pylist())


def build(period):
    arts = json.load(open(ANNOUNCEMENTS))
    start, end = (int(pd.Timestamp(x).value // 1_000_000) for x in PERIODS[period])
    ev = events([a for a in arts if start <= int(a["releaseDate"]) < end], all_symbols())
    return ev


def run(period, hs=HS):
    ev = build(period)
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    bars = load_hourly(set(ev["sym"]))
    _, slip = bt_wick.universe()
    fund = bt_wick.funding_events()
    rows = []
    for _, e in ev.iterrows():
        t, o, qv = bars.get(e["sym"], (np.array([], np.int64), np.array([]), np.array([])))
        day = pd.Timestamp(e["entry_t"] // 86_400_000 * 86_400_000, unit="ms")
        s = float(slip[e["sym"]].get(day, 0.0025)) if e["sym"] in slip.columns else 0.0025
        s = 0.0025 if not np.isfinite(s) else s
        liq = 1.0 / s  # a lower slippage tier means a more liquid coin
        ft, fr = fund.get(e["sym"], (np.array([], np.int64), np.array([])))
        for H in hs:
            got = trade(t, o, qv, int(e["entry_t"]), H)
            if got is None:
                continue
            px_in, exit_t, px_out = got
            lo, hi = np.searchsorted(ft, e["entry_t"], "right"), np.searchsorted(ft, exit_t, "right")
            fsum = float(fr[lo:hi].sum())
            rows.append({"trial": f"delist|h{H}", "ann": e["ann"], "sym": e["sym"], "entry_t": int(e["entry_t"]),
                         "exit_t": int(exit_t), "entry": px_in, "exit": px_out, "funding": fsum, "slip": s, "liq": liq,
                         "ret1": net(px_in, px_out, fsum, s, 1.0), "ret2": net(px_in, px_out, fsum, s, 2.0)})
    return ev, pd.DataFrame(rows)


def evaluate(trades, period):
    start, end = PERIODS[period]
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        out[key] = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            acc, daily = book(g.assign(ret=g[col]))
            days = pd.date_range(max(pd.Timestamp(start), daily.index.min()), pd.Timestamp(end) - pd.Timedelta(days=1))
            daily = daily.reindex(days, fill_value=0.0)
            per_ann = acc.groupby("ann")[col].mean()
            t = per_ann.mean() / per_ann.std() * math.sqrt(len(per_ann)) if len(per_ann) > 2 else 0.0
            out[key][mult] = {"trades": int(len(acc)), "announcements": int(len(per_ann)),
                              "mean_pct": round(float(acc[col].mean() * 100), 2), "t_ann": round(float(t), 2),
                              "win_pct": round(float((acc[col] > 0).mean() * 100), 1),
                              "worst_pct": round(float(acc[col].min() * 100), 1),
                              "funding_mean_pct": round(float(acc["funding"].mean() * 100), 3),
                              "book": bt.stats(daily)}
            if mult == "1x":
                pnl[key] = daily
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r):
    a, b = r["1x"], r["2x"]
    return bool(a["mean_pct"] > 0 and a["t_ann"] >= 2.5 and a["book"]["monthly_mean_pct"] >= 5
                and a["book"]["max_dd_pct"] <= 35 and b["book"]["monthly_mean_pct"] > 0)


def show(out):
    for key, r in out.items():
        for mult in ("1x", "2x"):
            x, b = r[mult], r[mult]["book"]
            print(f"{key:<10} {mult} trades {x['trades']:>3} ann {x['announcements']:>2} mean {x['mean_pct']:6.2f}% "
                  f"t {x['t_ann']:5.2f} win {x['win_pct']}% worst {x['worst_pct']}% funding {x['funding_mean_pct']}% | "
                  f"month {b['monthly_mean_pct']}% median {b['monthly_median_pct']}% dd {b['max_dd_pct']}% "
                  f"pos {b['pos_months_pct']}% years {b['by_year_pct']}", flush=True)
        print(f"{'':<10} passes {r.get('passes')}", flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        for period in PERIODS:
            ev = build(period)
            n_ann = ev["ann"].nunique() if len(ev) else 0
            print(f"{period}: {n_ann} announcements, {len(ev)} tickers, {int(ev['sym'].notna().sum())} with a perp")
            sample = ev.sample(min(20, len(ev)), random_state=0).sort_values("T")
            for _, e in sample.iterrows():
                print(f"  {pd.Timestamp(e['T'], unit='ms'):%Y-%m-%d %H:%M} -> entry "
                      f"{pd.Timestamp(e['entry_t'], unit='ms'):%Y-%m-%d %H:%M}  {e['ticker']:<8} {e['sym']}  | {e['title']}")
        return
    if mode == "dev":
        ev, trades = run("dev")
        out, pnl = evaluate(trades, "dev")
        for key in out:
            out[key]["passes"] = passes(out[key])
        show(out)
        print("passing:", [k for k in out if out[k]["passes"]], flush=True)
        pnl.to_parquet(f"{ROOT}/pnl_round15.parquet")
        trades.to_parquet(f"{HERE}/trades_dev.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{HERE}/results_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        ev, trades = run("holdout", [int(k.split("|h")[1]) for k in ok])
        out, _ = evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"] = passes(out[key])
        show(out)
        trades.to_parquet(f"{HERE}/trades_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
