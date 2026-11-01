"""Trade the Binance perp after a major spot venue announces it will list the coin (round 16, research only).

Pre-registered on 2026-11-01, before any price around an announcement was looked at. Only the
announcement titles and the archives' coverage were inspected. Nothing is tuned after the first run.
No orders are placed anywhere.

Idea: when Upbit opens a KRW market for a coin, or Binance spot lists a coin that already has a Binance
USDT-M perp, buyers who can only use that venue arrive hours later, when trading opens there. Fast money
buys the perp first. The first seconds are out of reach. The question is whether the move keeps going
for minutes after a 60-second delay (long trials), or gives back within two days (fade trial).

Events
  upbit    download_upbit.py: Upbit "trade" notices. A notice counts when its title names KRW and
           contains "디지털 자산 추가" or "신규 거래지원", and not "종료". Tickers:
           - when items carry their own markets, "NAME(TICKER)(KRW, BTC 마켓)", only the items whose
             markets name KRW;
           - otherwise every parenthesised group made only of [A-Z0-9] tickers and commas, plus
             "TICKER(한글)" items;
           - KRW, BTC and USDT themselves are never tickers.
           T = first_listed_at (KST, converted to UTC). Later edits of a notice keep their first time.
  binance  download_binance.py: Binance CMS catalog 48. Titles starting "Binance Will List" and not
           about leveraged tokens, trading pairs, futures or margin. Tickers: every "(TICKER)" group.
           T = releaseDate.
  perp     <TICKER>USDT, else 1000<TICKER>USDT (minus bt.EXCLUDE), whose first daily bar with volume
           opens at least 2 days before T (so the perp is not launched with the listing)
  dedupe   an event whose perp already had an accepted event in the previous 72 hours is dropped

Trades, per event (Binance USDT-M 1-minute bars, open prices)
  L15, L60  long at the open of the first minute starting at or after T + 60 s; exit at the open of
            the minute entry + 15 or + 60 minutes
  F48       short at the open of the first minute starting at or after T + 4 h; exit at the open of
            the minute entry + 48 h; buy-stop at 1.30 x entry: the first minute whose high reaches it
            exits at max(stop, that minute's open)
  bars      a trade needs an entry bar with volume; an exit bar missing from the archive is replaced by
            the next bar within 60 minutes, else the trade is skipped
  costs     taker 0.05% on both sides; slippage = bt.slippage tier of the perp's 30-day median daily
            quote volume known the day before (25 bp when unknown). Long trials pay the tier twice at
            entry, because they buy in the rush; F48 pays it once on each side. Funding: settlements s
            with entry < s <= exit, paid by longs and received by shorts when positive. Everything is
            rerun at 2x fees and slippage.

Book
  An announcement gets s x equity, split equally across its accepted tickers. At most 3 announcements
  are open at once; a later one is skipped. A notice's return lands on its exit day; bt.stats on the
  daily series gives drawdown. s in {1, 2, 3}.

Trials: 3 (L15, L60, F48)

Bar on development (announcements before 2025-07-01)
  - mean net return an announcement > 0 with t >= 2.5 over announcements, at 1x costs
  - monthly figure = mean net return an announcement x s x rate, where rate = announcements with a
    trade from 2024-07-01 to 2025-06-30, divided by 12 (the archives get denser over time, so the
    whole span understates today's rate). s* = the smallest s in {1, 2, 3} with a monthly figure
    >= 5% at 1x; the development book's max drawdown at s* must be <= 35%
  - monthly figure at s* > 0 at 2x costs

Data check before development (no prices printed): the minute where volume starts to rise must sit at
the announcement, not after our entry. Onset = the first minute in [T - 10 min, T + 30 min] with quote
volume >= 5x the median of [T - 70 min, T - 10 min). If more than 25% of the events with an onset have
it at or after their L15/L60 entry minute, the timestamps are suspect and the round stops unscored.

Holdout: announcements 2025-07-01..2026-10-28, one look, only for trials that pass, with the same bar,
s* from development, and the monthly figure read straight from the holdout book. Minute-level research
(round 11) never opened 2025-07..2026-10.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_spotlist.py events            parsed events and perps (no prices)
  python bt_spotlist.py download dev      1-minute daily archives around the development events
  python bt_spotlist.py timing            the data check above
  python bt_spotlist.py dev               results_dev.json, trades_dev.parquet, /w/pnl_round16.parquet
  python bt_spotlist.py holdout           results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import concurrent.futures as cf
import io
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
import bt  # noqa: E402  (EXCLUDE, slippage, stats)

ROOT = "/w"
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = f"{ROOT}/data/listing"
M1 = f"{DATA}/m1"
DAILY = f"{ROOT}/fwd/data"
FILES = "https://data.binance.vision/data/futures/um/daily/klines"
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
TAKER = 0.0005
DELAY_LONG = 60_000
DELAY_FADE = 4 * HOUR
FADE_H = 48 * HOUR
STOP = 1.30
OPEN_MAX = 3
SIZES = (1, 2, 3)
DEDUPE = 72 * HOUR
EXIT_GRACE = 60 * MIN
TRIALS = {"L15": ("long", DELAY_LONG, 15 * MIN), "L60": ("long", DELAY_LONG, 60 * MIN),
          "F48": ("fade", DELAY_FADE, FADE_H)}
PERIODS = {"dev": ("2020-01-01", "2025-07-01"), "holdout": ("2025-07-01", "2026-10-29")}
RATE_WINDOW = ("2024-07-01", "2025-07-01")

UPBIT_WORDS = ("디지털 자산 추가", "신규 거래지원")
ITEM = re.compile(r"\(([A-Z0-9]+)\)\(([^()]*)\)")
GROUP = re.compile(r"\(([A-Z0-9]+(?:\s*,\s*[A-Z0-9]+)*)\)")
REVERSED = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{2,})\([가-힣]")
MARKETS = {"KRW", "BTC", "USDT"}
BINANCE = re.compile(r"^binance will list\b", re.IGNORECASE)
BINANCE_OUT = re.compile(r"leveraged token|trading pair|futures|margin", re.IGNORECASE)
PAREN = re.compile(r"\(([A-Z0-9]+)\)")


def parse_upbit(title):
    """-> tickers of a KRW-market listing notice, or []."""
    if "KRW" not in title or "종료" in title or not any(w in title for w in UPBIT_WORDS):
        return []
    items = ITEM.findall(title)
    if items:
        return [tk for tk, markets in items if "KRW" in markets]
    out = [x.strip() for g in GROUP.findall(title) for x in g.split(",")] + REVERSED.findall(title)
    return [x for x in dict.fromkeys(out) if x not in MARKETS]


def parse_binance(title):
    """-> tickers of a Binance spot listing announcement, or []."""
    if not BINANCE.match(title.strip()) or BINANCE_OUT.search(title):
        return []
    return list(dict.fromkeys(PAREN.findall(title)))


def kst_ms(iso):
    return int(pd.Timestamp(iso).tz_convert("UTC").value // 1_000_000)


def minute_at_or_after(t):
    return -(-t // MIN) * MIN


def announcements():
    """-> [{src, ann, title, T, tickers}] from both sources, sorted by T."""
    rows = []
    for n in json.load(open(f"{DATA}/upbit_notices.json", encoding="utf-8")):
        tks = parse_upbit(n["title"])
        if tks:
            rows.append({"src": "upbit", "ann": f"u{n['id']}", "title": n["title"], "T": kst_ms(n["first_listed_at"]),
                         "tickers": tks})
    for a in json.load(open(f"{DATA}/binance_listings.json", encoding="utf-8")):
        tks = parse_binance(a["title"])
        if tks:
            rows.append({"src": "binance", "ann": f"b{a['id']}", "title": a["title"], "T": int(a["releaseDate"]),
                         "tickers": tks})
    return sorted(rows, key=lambda r: r["T"])


def first_days():
    k = pd.read_parquet(f"{DAILY}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[k["quote_volume"].astype(float) > 0]
    return k.groupby("symbol")["open_time"].min().astype("int64").to_dict()


def perp_for(tk, first, T):
    """-> (symbol, None) or (None, reason)."""
    for s in (f"{tk}USDT", f"1000{tk}USDT"):
        if s in first and s not in bt.EXCLUDE:
            return (s, None) if first[s] <= T - 2 * DAY else (None, "young")
    return None, "no perp"


def events(anns, first):
    """-> DataFrame(src, ann, title, T, ticker, sym, reason), one row per ticker, dedupe applied."""
    rows, last = [], {}
    for a in anns:
        for tk in a["tickers"]:
            sym, why = perp_for(tk, first, a["T"])
            if sym is not None and sym in last and a["T"] - last[sym] < DEDUPE:
                sym, why = None, "dedupe"
            if sym is not None:
                last[sym] = a["T"]
            rows.append({**{k: a[k] for k in ("src", "ann", "title", "T")}, "ticker": tk, "sym": sym, "reason": why})
    return pd.DataFrame(rows)


def period_events(period):
    start, end = (int(pd.Timestamp(x).value // 1_000_000) for x in PERIODS[period])
    ev = events(announcements(), first_days())
    return ev[(ev["T"] >= start) & (ev["T"] < end)].reset_index(drop=True)


# ---------------------------------------------------------------- 1-minute archives

def days_needed(T):
    lo, hi = (T - 2 * HOUR) // DAY, (T + DELAY_FADE + FADE_H + EXIT_GRACE + HOUR) // DAY
    return [pd.Timestamp(d * DAY, unit="ms").strftime("%Y-%m-%d") for d in range(lo, hi + 1)]


def fetch(url, tries=5):
    for attempt in range(tries):
        try:
            return urllib.request.urlopen(url, timeout=90).read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1 + 2 * attempt)
    raise RuntimeError(f"gave up on {url}")


def parse_zip(blob):
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        raw = archive.read(archive.namelist()[0])
    header = None if raw[:1].isdigit() else 0
    df = pd.read_csv(io.BytesIO(raw), header=header, usecols=range(8))
    t = df.iloc[:, 0].astype("int64")
    if len(t) and t.max() > 10 ** 14:  # microsecond timestamps
        t = t // 1000
    return pd.DataFrame({"t": t, "o": df.iloc[:, 1].astype(float), "h": df.iloc[:, 2].astype(float),
                         "l": df.iloc[:, 3].astype(float), "c": df.iloc[:, 4].astype(float),
                         "qv": df.iloc[:, 7].astype("float32")})


def one_day(job):
    sym, day = job
    path = f"{M1}/{sym}/{day}.parquet"
    if os.path.exists(path) or os.path.exists(path + ".missing"):
        return "kept"
    blob = fetch(f"{FILES}/{sym}/1m/{sym}-1m-{day}.zip")
    os.makedirs(f"{M1}/{sym}", exist_ok=True)
    if blob is None:
        open(path + ".missing", "w").close()
        return "missing"
    parse_zip(blob).to_parquet(path + ".tmp", index=False)
    os.replace(path + ".tmp", path)
    return "new"


def download(period):
    ev = period_events(period)
    ev = ev[ev["sym"].notna()]
    jobs = sorted({(s, d) for s, T in zip(ev["sym"], ev["T"]) for d in days_needed(int(T))})
    counts = {"kept": 0, "new": 0, "missing": 0}
    with cf.ThreadPoolExecutor(6) as pool:
        for res in pool.map(one_day, jobs):
            counts[res] += 1
    print(f"{period}: {len(jobs)} symbol-days {counts}", flush=True)


def bars(sym, T):
    frames = [pd.read_parquet(f"{M1}/{sym}/{d}.parquet") for d in days_needed(T)
              if os.path.exists(f"{M1}/{sym}/{d}.parquet")]
    if not frames:
        return None
    b = pd.concat(frames, ignore_index=True).drop_duplicates("t").sort_values("t")
    return {c: b[c].to_numpy(np.int64 if c == "t" else float) for c in ("t", "o", "h", "l", "c", "qv")}


# ---------------------------------------------------------------- trades

def bar_at(b, t, grace=0):
    """Index of the bar opening at t (or, with grace, the next bar within grace ms), else None."""
    i = int(np.searchsorted(b["t"], t))
    if i < len(b["t"]) and b["t"][i] - t <= grace:
        return i
    return None


def trade(b, T, kind, delay, hold):
    """-> (entry_t, entry_px, exit_t, exit_px, stopped) or None."""
    et = minute_at_or_after(T + delay)
    i = bar_at(b, et)
    if i is None or b["qv"][i] <= 0:
        return None
    j = bar_at(b, et + hold, EXIT_GRACE)
    if j is None:
        return None
    px = b["o"][i]
    if kind == "fade":
        stop = STOP * px
        hit = np.flatnonzero(b["h"][i:j] >= stop)
        if len(hit):
            k = i + int(hit[0])
            return int(et), px, int(b["t"][k]), max(stop, b["o"][k]), True
    return int(et), px, int(b["t"][j]), b["o"][j], False


def net(kind, px_in, px_out, fsum, slip, mult):
    if kind == "long":
        return (px_out / px_in - 1.0) - fsum - mult * (2 * TAKER + 3 * slip)
    return -(px_out / px_in - 1.0) + fsum - mult * (2 * TAKER + 2 * slip)


def slip_table():
    k = pd.read_parquet(f"{DAILY}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[k["quote_volume"].astype(float) > 0]
    k["date"] = pd.to_datetime(k["open_time"].astype("int64"), unit="ms").dt.normalize()
    qv = k.drop_duplicates(["symbol", "date"]).pivot(index="date", columns="symbol", values="quote_volume")
    med = qv.astype(float).asfreq("D").rolling(30, min_periods=20).median().shift(1)
    return bt.slippage(med).where(med.notna())


def funding_events():
    f = pd.read_parquet(f"{DAILY}/fundingRate_all.parquet").sort_values("calc_time")
    return {s: (g["calc_time"].astype("int64").to_numpy(), g["last_funding_rate"].astype(float).to_numpy())
            for s, g in f.groupby("symbol")}


def run(period, trials=tuple(TRIALS)):
    ev = period_events(period)
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    slip, fund = slip_table(), funding_events()
    rows = []
    for _, e in ev.iterrows():
        b = bars(e["sym"], int(e["T"]))
        if b is None:
            continue
        day = pd.Timestamp(int(e["T"]) // DAY * DAY, unit="ms")
        s = slip[e["sym"]].get(day, np.nan) if e["sym"] in slip.columns else np.nan
        s = 0.0025 if not np.isfinite(s) else float(s)
        ft, fr = fund.get(e["sym"], (np.array([], np.int64), np.array([])))
        for key in trials:
            kind, delay, hold = TRIALS[key]
            got = trade(b, int(e["T"]), kind, delay, hold)
            if got is None:
                continue
            et, px_in, xt, px_out, stopped = got
            fsum = float(fr[np.searchsorted(ft, et, "right"):np.searchsorted(ft, xt, "right")].sum())
            rows.append({"trial": key, "src": e["src"], "ann": e["ann"], "T": int(e["T"]), "sym": e["sym"],
                         "entry_t": et, "exit_t": xt, "entry": px_in, "exit": px_out, "stopped": stopped,
                         "funding": fsum, "slip": s, "ret1": net(kind, px_in, px_out, fsum, s, 1.0),
                         "ret2": net(kind, px_in, px_out, fsum, s, 2.0)})
    return ev, pd.DataFrame(rows)


# ---------------------------------------------------------------- book and verdict

def notices(tr, col):
    """-> DataFrame(ann, T, entry_t, exit_t, ret) with each announcement's tickers averaged."""
    g = tr.groupby("ann")
    return pd.DataFrame({"T": g["T"].first(), "entry_t": g["entry_t"].min(), "exit_t": g["exit_t"].max(),
                         "ret": g[col].mean()}).reset_index().sort_values("entry_t")


def book(nt, s):
    """-> (accepted notices, daily return series) with at most OPEN_MAX open announcements."""
    open_exits, keep = [], []
    for idx, et, xt in zip(nt.index, nt["entry_t"].to_numpy(), nt["exit_t"].to_numpy()):
        open_exits = [x for x in open_exits if x > et]
        if len(open_exits) < OPEN_MAX:
            open_exits.append(xt)
            keep.append(idx)
    acc = nt.loc[keep]
    day = pd.to_datetime(acc["exit_t"], unit="ms").dt.normalize()
    return acc, (s * acc["ret"]).groupby(day).sum()


def rate(acc):
    lo, hi = (int(pd.Timestamp(x).value // 1_000_000) for x in RATE_WINDOW)
    return float(((acc["T"] >= lo) & (acc["T"] < hi)).sum() / 12)


def daily_full(daily, period):
    start, end = PERIODS[period]
    days = pd.date_range(pd.Timestamp(start), pd.Timestamp(end) - pd.Timedelta(days=1))
    return daily.reindex(days, fill_value=0.0)


def t_stat(x):
    return float(x.mean() / x.std() * math.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0


def evaluate(trades, period, sizes=SIZES):
    out, pnl = {}, {}
    for key, g in trades.groupby("trial"):
        r = {}
        for mult, col in (("1x", "ret1"), ("2x", "ret2")):
            nt = notices(g, col)
            acc, _ = book(nt, 1)
            x = {"trades": int(g[g["ann"].isin(acc["ann"])].shape[0]), "announcements": int(len(acc)),
                 "mean_pct": round(float(acc["ret"].mean() * 100), 3), "t_ann": round(t_stat(acc["ret"]), 2),
                 "win_pct": round(float((acc["ret"] > 0).mean() * 100), 1),
                 "worst_pct": round(float(acc["ret"].min() * 100), 1),
                 "best_pct": round(float(acc["ret"].max() * 100), 1),
                 "funding_mean_pct": round(float(g["funding"].mean() * 100), 3),
                 "stopped": int(g["stopped"].sum()), "rate_per_month": round(rate(acc), 2),
                 "by_source": {src: {"n": int(len(h)), "mean_pct": round(float(h[col].mean() * 100), 3)}
                               for src, h in g.groupby("src")}, "sizes": {}}
            for s in sizes:
                _, daily = book(nt, s)
                st = bt.stats(daily_full(daily, period))
                x["sizes"][str(s)] = {"monthly_figure_pct": round(float(acc["ret"].mean() * s * rate(acc) * 100), 2),
                                      "book": st}
                if mult == "1x" and s == 1:
                    pnl[key] = daily_full(daily, period)
            r[mult] = x
        out[key] = r
    return out, pd.DataFrame(pnl).fillna(0.0)


def passes(r, period="dev", size=None):
    a, b = r["1x"], r["2x"]
    if not (a["mean_pct"] > 0 and a["t_ann"] >= 2.5):
        return False, None
    fig = (lambda x, s: x["sizes"][str(s)]["monthly_figure_pct"]) if period == "dev" else \
          (lambda x, s: x["sizes"][str(s)]["book"]["monthly_mean_pct"])
    if size is None:
        size = next((s for s in SIZES if fig(a, s) >= 5), None)
    if size is None:
        return False, None
    ok = a["sizes"][str(size)]["book"]["max_dd_pct"] <= 35 and fig(b, size) > 0 and fig(a, size) >= 5
    return bool(ok), size


def show(out):
    for key, r in out.items():
        for mult in ("1x", "2x"):
            x = r[mult]
            print(f"{key} {mult} ann {x['announcements']:>3} trades {x['trades']:>3} mean {x['mean_pct']:7.3f}% "
                  f"t {x['t_ann']:5.2f} win {x['win_pct']}% worst {x['worst_pct']}% best {x['best_pct']}% "
                  f"funding {x['funding_mean_pct']}% stopped {x['stopped']} rate {x['rate_per_month']}/mo "
                  f"src {x['by_source']}", flush=True)
            for s, v in x["sizes"].items():
                b = v["book"]
                print(f"      s={s} figure {v['monthly_figure_pct']}%/mo | book month {b['monthly_mean_pct']}% "
                      f"dd {b['max_dd_pct']}% years {b['by_year_pct']}", flush=True)
        print(f"   passes {r.get('passes')} s* {r.get('size')}", flush=True)


# ---------------------------------------------------------------- data check

def timing():
    ev = period_events("dev")
    ev = ev[ev["sym"].notna()]
    lags, late, none = [], 0, 0
    for _, e in ev.iterrows():
        T = int(e["T"])
        b = bars(e["sym"], T)
        if b is None:
            continue
        t, qv = b["t"], b["qv"]
        base = qv[(t >= T - 70 * MIN) & (t < T - 10 * MIN)]
        win = np.flatnonzero((t >= (T - 10 * MIN) // MIN * MIN) & (t <= T + 30 * MIN))
        if len(base) < 30 or np.median(base) <= 0:
            none += 1
            continue
        hit = [i for i in win if qv[i] >= 5 * np.median(base)]
        if not hit:
            none += 1
            continue
        onset = int(t[hit[0]])
        lags.append((onset - T // MIN * MIN) // MIN)
        late += onset >= minute_at_or_after(T + DELAY_LONG)
    lags = pd.Series(lags)
    print(f"events with an onset {len(lags)}, without {none}")
    print("onset minute minus announcement minute:", lags.clip(-11, 31).value_counts().sort_index().to_dict())
    share = late / max(len(lags), 1)
    print(f"onset at or after the entry minute: {late} ({share:.0%}) -> {'SUSPECT, stop' if share > 0.25 else 'ok'}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        for period in PERIODS:
            ev = period_events(period)
            ok = ev[ev["sym"].notna()]
            print(f"{period}: {ev['ann'].nunique()} announcements, {len(ev)} tickers, {len(ok)} with a perp "
                  f"({ok['ann'].nunique()} announcements); by source {ok.groupby('src')['ann'].nunique().to_dict()}; "
                  f"skipped {ev['reason'].value_counts().to_dict()}")
            for _, e in ok.sample(min(20, len(ok)), random_state=0).sort_values("T").iterrows():
                print(f"  {pd.Timestamp(e['T'], unit='ms'):%Y-%m-%d %H:%M:%S} {e['src']:<7} {e['ticker']:<9} "
                      f"{e['sym']:<16} | {e['title'][:90]}")
    elif mode == "download":
        download(sys.argv[2])
    elif mode == "timing":
        timing()
    elif mode == "dev":
        ev, trades = run("dev")
        out, pnl = evaluate(trades, "dev")
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key])
        show(out)
        print("passing:", [k for k in out if out[k]["passes"]], flush=True)
        pnl.to_parquet(f"{ROOT}/pnl_round16.parquet")
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
        download("holdout")
        ev, trades = run("holdout", ok)
        out, _ = evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key], "holdout", dev[key]["size"])
        show(out)
        trades.to_parquet(f"{HERE}/trades_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
