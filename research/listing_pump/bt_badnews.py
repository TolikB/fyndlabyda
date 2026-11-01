"""Short the perp a few seconds after bad exchange news, cover one minute after it (round 19, research only).

Pre-registered on 2026-11-01, before any price around these announcements was looked at. Only titles
and event counts were inspected. Nothing is tuned after the first run. No orders are placed anywhere.

Idea: round 16 showed that a listing post moves the Binance perp within seconds and a bot two seconds
late still caught part of it. Bad news (an Upbit investment warning, an Upbit or Binance delisting)
should work the same way downwards. Round 15 shorted Binance spot delistings only from the next whole
hour, and failed on a squeeze; the first minute was never tested.

Events
  upbit    Upbit "trade" notices whose title contains "유의 종목 지정 안내" (investment warning),
           "유의 촉구" (caution urged) or "거래지원 종료" (delisting), and not "해제" (lifted); a title
           naming only BTC or USDT markets and not KRW is skipped. Tickers: parenthesised groups of
           [A-Z0-9] tickers and "TICKER(한글)" items, as in bt_spotlist.parse_upbit. T = first_listed_at.
  binance  Binance CMS catalog 161 (/w/data/delist/announcements.json):
           - "Binance Will Delist <list> on <date>" (spot), tickers as in round 15;
           - "Binance Futures Will Delist ..." naming <SYM>USDT contracts, tickers = SYM, except titles
             that also "Update the Leverage & Margin Tiers" (they mix delisted and merely re-tiered
             contracts; excluded on 2026-11-01 before any price was seen);
           - "... Monitoring Tag to Include <list> on <date>".
           T = releaseDate.
  perp, dedupe  bt_spotlist.perp_for (perp at least 2 days old) and its 72-hour dedupe

Trades, per event (aggTrades in [T - 10 min, T + 10 min], diag_latency.window); trials S2 (L = 2 s), S5 (5 s)
  entry   short at the smaller of the first trade price at or after T + L and the volume-weighted price of
          the seller-initiated trades in [T + L, T + L + 1 s): we may queue behind faster sellers. No trade
          in [T + L, T + L + 10 s] skips the event.
  exit    the first trade at or after T + 60 s
  costs   taker 0.05% on both sides, bt.slippage tier twice at entry and once at exit, funding inside
          (entry, exit] received by the short when positive; everything rerun at 2x

Book and bar: exactly bt_spotlist's (s x equity an announcement split across tickers, at most 3 open,
s in {1, 2, 3}; mean > 0 with t >= 2.5 over announcements at 1x; development monthly figure = mean x s x
rate(2024-07..2025-06) >= 5% at s* with drawdown <= 35%; > 0 at 2x; the holdout reads the book's monthly
mean).

Data check before development (development events only, no returns printed): among events whose price
60 s after T is at least 4% below the last trade before T, the median time to reach a quarter of that
drop must be <= 3 s, and fewer than 25% of them may take longer than 30 s. Otherwise the post times are
suspect and the round stops unscored.

Holdout: announcements 2025-07-01..2026-10-28, one look, only for trials that pass.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_badnews.py events     parsed events (no prices)
  python bt_badnews.py timing     the data check above (downloads development windows)
  python bt_badnews.py dev        results_badnews_dev.json, /w/pnl_round19.parquet
  python bt_badnews.py holdout    results_badnews_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import re
import sys

import numpy as np
import pandas as pd

import bt_spotlist as sl
import diag_latency as dl

DELIST = f"{sl.ROOT}/data/delist/announcements.json"
TRIALS = {"S2": 2.0, "S5": 5.0}
EXIT_S = 60.0
QUEUE_MS = 1000
SKIP_MS = 10_000
UPBIT_BAD = ("유의 종목 지정 안내", "유의 촉구", "거래지원 종료")
SPOT_DELIST = re.compile(r"^binance will delist (.+) on (\d{4}-\d{2}-\d{2})$", re.IGNORECASE)
FUT_DELIST = re.compile(r"^binance futures will delist", re.IGNORECASE)
MONITORING = re.compile(r"monitoring tag to include (.+?) on (\d{4}-\d{2}-\d{2})", re.IGNORECASE)
CONTRACT = re.compile(r"\b([A-Z0-9]+)USDT\b")
TICKER = re.compile(r"^[A-Z0-9]+$")


def parse_upbit_bad(title):
    if "해제" in title or not any(w in title for w in UPBIT_BAD):
        return []
    if ("BTC 마켓" in title or "USDT 마켓" in title) and "KRW" not in title:
        return []
    out = [x.strip() for g in sl.GROUP.findall(title) for x in g.split(",")] + sl.REVERSED.findall(title)
    return [x for x in dict.fromkeys(out) if x not in sl.MARKETS]


def split_list(s):
    items = [x.strip() for x in re.split(r",\s*|\s+and\s+|\s*&\s*", s)]
    return items if items and all(TICKER.match(x) for x in items) else []


def parse_binance_bad(title):
    t = title.strip()
    m = SPOT_DELIST.match(t)
    if m:
        return split_list(m.group(1))
    if FUT_DELIST.match(t):
        return [] if "update the leverage" in t.lower() else list(dict.fromkeys(CONTRACT.findall(t)))
    m = MONITORING.search(t)
    if m:
        return split_list(m.group(1))
    return []


def announcements():
    rows = []
    for n in json.load(open(f"{sl.DATA}/upbit_notices.json", encoding="utf-8")):
        tks = parse_upbit_bad(n["title"])
        if tks:
            rows.append({"src": "upbit", "ann": f"u{n['id']}", "title": n["title"], "T": sl.kst_ms(n["first_listed_at"]),
                         "tickers": tks})
    for a in json.load(open(DELIST, encoding="utf-8")):
        tks = parse_binance_bad(a["title"])
        if tks:
            rows.append({"src": "binance", "ann": f"b{a['id']}", "title": a["title"], "T": int(a["releaseDate"]),
                         "tickers": tks})
    return sorted(rows, key=lambda r: r["T"])


def period_events(period):
    start, end = (int(pd.Timestamp(x).value // 1_000_000) for x in sl.PERIODS[period])
    ev = sl.events(announcements(), sl.first_days())
    return ev[(ev["T"] >= start) & (ev["T"] < end)].reset_index(drop=True)


def trade(w, T, L):
    """Short. w: aggTrades window (t, p, q, buy). -> (entry_t, entry_px, exit_t, exit_px) or None."""
    t, p, q, buy = (w[c].to_numpy() for c in ("t", "p", "q", "buy"))
    t = t.astype(np.int64)
    t0 = T + int(L * 1000)
    i = int(np.searchsorted(t, t0))
    if i >= len(t) or t[i] > t0 + SKIP_MS:
        return None
    sel = (t >= t0) & (t < t0 + QUEUE_MS) & ~buy.astype(bool)
    vwap = float((p[sel] * q[sel]).sum() / q[sel].sum()) if sel.any() else np.inf
    px_in = min(float(p[i]), vwap)
    j = int(np.searchsorted(t, T + int(EXIT_S * 1000)))
    if j >= len(t):
        return None
    return int(t[i]), px_in, int(t[j]), float(p[j])


def net(px_in, px_out, fsum, slip, mult):
    return -(px_out / px_in - 1.0) + fsum - mult * (2 * sl.TAKER + 3 * slip)


def run(period, trials=tuple(TRIALS)):
    ev = period_events(period)
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    slip, fund = sl.slip_table(), sl.funding_events()
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        w = dl.window(e["sym"], T)
        if len(w) == 0:
            continue
        day = pd.Timestamp(T // sl.DAY * sl.DAY, unit="ms")
        s = slip[e["sym"]].get(day, np.nan) if e["sym"] in slip.columns else np.nan
        s = 0.0025 if not np.isfinite(s) else float(s)
        ft, fr = fund.get(e["sym"], (np.array([], np.int64), np.array([])))
        for key in trials:
            got = trade(w, T, TRIALS[key])
            if got is None:
                continue
            et, px_in, xt, px_out = got
            fsum = float(fr[np.searchsorted(ft, et, "right"):np.searchsorted(ft, xt, "right")].sum())
            rows.append({"trial": key, "src": e["src"], "ann": e["ann"], "T": T, "sym": e["sym"], "entry_t": et,
                         "exit_t": xt, "entry": px_in, "exit": px_out, "stopped": False, "funding": fsum, "slip": s,
                         "ret1": net(px_in, px_out, fsum, s, 1.0), "ret2": net(px_in, px_out, fsum, s, 2.0)})
    return pd.DataFrame(rows)


def timing():
    ev = period_events("dev")
    ev = ev[ev["sym"].notna()]
    t25 = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        w = dl.window(e["sym"], T)
        if len(w) == 0:
            continue
        t, p = w["t"].to_numpy(np.int64), w["p"].to_numpy(float)
        before = np.flatnonzero(t < T)
        i60 = int(np.searchsorted(t, T + 60_000))
        if len(before) == 0 or i60 >= len(t):
            continue
        p0 = p[before[-1]]
        drop = p[i60] / p0 - 1
        if drop > -0.04:
            continue
        after = np.flatnonzero(t >= T - 5000)
        hit = after[p[after] <= p0 * (1 + 0.25 * drop)]
        t25.append((t[hit[0]] - T) / 1000 if len(hit) else np.inf)
    x = pd.Series(t25)
    ok = len(x) > 0 and x.median() <= 3 and (x > 30).mean() < 0.25
    print(f"events with a 60 s drop >= 4%: {len(x)}; seconds to a quarter of the drop, quartiles "
          f"{x.quantile([0.25, 0.5, 0.75]).round(1).tolist() if len(x) else []}; over 30 s: {(x > 30).mean() if len(x) else 0:.0%}"
          f" -> {'ok' if ok else 'SUSPECT, stop'}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        for period in sl.PERIODS:
            ev = period_events(period)
            ok = ev[ev["sym"].notna()]
            print(f"{period}: {ev['ann'].nunique()} announcements, {len(ev)} tickers, {len(ok)} with a perp "
                  f"({ok['ann'].nunique()} announcements); by source {ok.groupby('src')['ann'].nunique().to_dict()}; "
                  f"skipped {ev['reason'].value_counts().to_dict()}")
            for _, e in ok.sample(min(16, len(ok)), random_state=0).sort_values("T").iterrows():
                print(f"  {pd.Timestamp(e['T'], unit='ms'):%Y-%m-%d %H:%M:%S} {e['src']:<7} {e['ticker']:<9} "
                      f"{e['sym']:<16} | {e['title'][:90]}")
    elif mode == "timing":
        timing()
    elif mode == "dev":
        trades = run("dev")
        out, pnl = sl.evaluate(trades, "dev")
        for key in out:
            out[key]["passes"], out[key]["size"] = sl.passes(out[key])
        sl.show(out)
        pnl.columns = [f"badnews|{c}" for c in pnl.columns]
        pnl.to_parquet(f"{sl.ROOT}/pnl_round19.parquet")
        trades.to_parquet(f"{sl.HERE}/trades_badnews_dev.parquet")
        with open(f"{sl.HERE}/results_badnews_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{sl.HERE}/results_badnews_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{sl.HERE}/results_badnews_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", ok)
        out, _ = sl.evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"], out[key]["size"] = sl.passes(out[key], "holdout", dev[key]["size"])
        sl.show(out)
        trades.to_parquet(f"{sl.HERE}/trades_badnews_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
