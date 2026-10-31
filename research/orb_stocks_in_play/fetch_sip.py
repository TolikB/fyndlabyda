"""Download Alpaca SIP data for bt_sip.py (research only). Resumable: each stage skips what exists.

  1 assets    active and delisted US equities on NYSE/NASDAQ/AMEX/ARCA/BATS, ETF-like names out
  2 daily     1-day bars raw and split-adjusted, 2015-11..2026-10, 100 symbols a request;
              -> daily/<batch>.json.gz, then candidates.json.gz {date: {sym: atr}} and calendar.json
  3 first     each session's 9:30 5-minute bar for every symbol that is a candidate on that day or
              in the 14 sessions after it (so each candidate has its own 14-day history)
              -> first/<date>.json.gz {sym: [o, h, l, c, v]}
  4 minute    1-minute bars 9:35-15:59 for each day's 20 stocks in play (bt_sip.select)
              -> minute/<date>.json.gz {sym: [[minute after 9:30, o, h, l, c, v], ...]}
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import alpaca_data
import bt_sip

NY = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
D = bt_sip.DATA
START, END = "2015-11-01T00:00:00Z", "2026-10-31T00:00:00Z"
TEST_START = dt.date(2016, 1, 4)
EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"}
SYM_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")
ETF_RE = re.compile(r"\bET[FNP]s?\b|\bFund\b|\bProShares\b|\biShares\b|\bSPDR\b|\bDirexion\b|\bVanEck\b|\bVanguard\b"
                    r"|\bInvesco\b|\bWisdomTree\b|\bGraniteShares\b|\bLeverage|\b\d(?:\.\d+)?[xX]\b|\bUltra(?:Pro|Short)\b"
                    r"|\bDaily (?:Bull|Bear|Inverse|Long|Short)\b|\bIndex\b|\bNotes?\b|\bWarrants?\b|\bRights?\b"
                    r"|\bUnits?\b|\bPreferred\b|\bDepositary Shares? Representing\b|\bBitcoin\b|\bEther(?:eum)?\b", re.I)
DAILY_BATCH = 100
FIRST_CHUNK = 400
WORKERS = 4


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def wjson(path, obj):
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt") if path.endswith(".gz") else open(tmp, "w") as fh:
        json.dump(obj, fh, separators=(",", ":"))
    os.replace(tmp, path)


def ny_date(t):
    return dt.datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(NY).date()


def ny_minute(t):
    x = dt.datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(NY)
    return x.hour * 60 + x.minute - (9 * 60 + 30)


def utc_at(day, hh, mm):
    return dt.datetime.combine(day, dt.time(hh, mm), tzinfo=NY).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def stage_assets(c):
    path = os.path.join(D, "assets.json")
    if os.path.exists(path):
        return json.load(open(path))
    keep, seen = [], set()
    for status in ("active", "inactive"):
        for a in c.assets(status):
            s, name = a.get("symbol", ""), a.get("name") or ""
            if a.get("exchange") in EXCHANGES and SYM_RE.match(s) and not ETF_RE.search(name) and s not in seen:
                seen.add(s)
                keep.append({"symbol": s, "name": name, "exchange": a.get("exchange"), "status": a.get("status"),
                             "shortable": a.get("shortable"), "easy_to_borrow": a.get("easy_to_borrow")})
    keep.sort(key=lambda a: a["symbol"])
    wjson(path, keep)
    log(f"assets: kept {len(keep)}")
    return keep


def run_pool(c, fn, jobs, label, every):
    """Run fn(*job) for each job on WORKERS threads (the client paces them); stop at the first error."""
    done = 0
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = [ex.submit(fn, *job) for job in jobs]
        try:
            for f in as_completed(futs):
                f.result()
                done += 1
                if done % every == 0 or done == len(futs):
                    log(f"{label}: {done} of {len(futs)} done, {c.calls} calls")
        except BaseException:
            ex.shutdown(cancel_futures=True)
            raise


def daily_batch(c, path, batch):
    raw = c.bars(batch, "1Day", START, END, "raw")
    adj = c.bars(batch, "1Day", START, END, "split")
    out = {"cands": {}, "days": {}, "rows": {}}
    for s, bars in raw.items():
        adj_close = {b["t"]: b["c"] for b in adj.get(s, [])}
        rows = []
        for b in bars:
            ac = adj_close.get(b["t"])
            if ac and b["c"]:
                rows.append((ny_date(b["t"]), b["o"], b["h"], b["l"], b["c"], b["v"], b["c"] / ac))
        out["days"][s] = [str(r[0]) for r in rows]
        cand = bt_sip.daily_candidates(rows)
        if cand:
            out["cands"][s] = {str(d): round(a, 4) for d, a in cand.items()}
            out["rows"][s] = [[str(r[0])] + list(r[1:]) for r in rows]
    wjson(path, out)


def stage_daily(c, symbols):
    os.makedirs(os.path.join(D, "daily"), exist_ok=True)
    batches = [symbols[i:i + DAILY_BATCH] for i in range(0, len(symbols), DAILY_BATCH)]
    jobs = [(c, p, b) for k, b in enumerate(batches)
            if not os.path.exists(p := os.path.join(D, "daily", f"{k:04d}.json.gz"))]
    run_pool(c, daily_batch, jobs, f"daily batches ({len(batches)} in all)", 10)
    cand_path, cal_path = os.path.join(D, "candidates.json.gz"), os.path.join(D, "calendar.json")
    if os.path.exists(cand_path) and os.path.exists(cal_path):
        return
    cands, count = defaultdict(dict), defaultdict(int)
    for k in range(len(batches)):
        with gzip.open(os.path.join(D, "daily", f"{k:04d}.json.gz"), "rt") as fh:
            doc = json.load(fh)
        for s, days in doc["days"].items():
            for d in days:
                count[d] += 1
        for s, m in doc["cands"].items():
            for d, a in m.items():
                cands[d][s] = a
    calendar = sorted(d for d, n in count.items() if n >= 500)
    wjson(cal_path, calendar)
    wjson(cand_path, {d: cands[d] for d in calendar if d in cands})
    log(f"calendar {len(calendar)} sessions {calendar[0]}..{calendar[-1]}; "
        f"candidates/day median {sorted(len(cands[d]) for d in calendar if d in cands)[len(calendar) // 2]}")


def first_day(c, path, d, need):
    got = {}
    for j in range(0, len(need), FIRST_CHUNK):
        bars = c.bars(need[j:j + FIRST_CHUNK], "5Min", utc_at(d, 9, 30), utc_at(d, 9, 34))
        for s, rows in bars.items():
            for b in rows:
                if ny_minute(b["t"]) == 0:
                    got[s] = [b["o"], b["h"], b["l"], b["c"], b["v"]]
    wjson(path, got)


def stage_first(c):
    cal = [dt.date.fromisoformat(x) for x in json.load(open(os.path.join(D, "calendar.json")))]
    with gzip.open(os.path.join(D, "candidates.json.gz"), "rt") as fh:
        cands = {dt.date.fromisoformat(d): m for d, m in json.load(fh).items()}
    os.makedirs(os.path.join(D, "first"), exist_ok=True)
    jobs = []
    for i, d in enumerate(cal):
        path = os.path.join(D, "first", f"{d}.json.gz")
        if not os.path.exists(path):
            need = sorted({s for dd in cal[i:i + bt_sip.LOOKBACK + 1] for s in cands.get(dd, {})})
            jobs.append((c, path, d, need))
    run_pool(c, first_day, jobs, f"first bars ({len(cal)} sessions in all)", 100)


def minute_day(c, path, d, picks):
    out = {}
    if picks:
        bars = c.bars([p[0] for p in picks], "1Min", utc_at(d, 9, 35), utc_at(d, 15, 59))
        for s, rows in bars.items():
            out[s] = [[ny_minute(b["t"]), b["o"], b["h"], b["l"], b["c"], b["v"]] for b in rows
                      if 5 <= ny_minute(b["t"]) <= 389]
    wjson(path, out)


def stage_minute(c):
    cal = [dt.date.fromisoformat(x) for x in json.load(open(os.path.join(D, "calendar.json")))]
    with gzip.open(os.path.join(D, "candidates.json.gz"), "rt") as fh:
        cands = {dt.date.fromisoformat(d): m for d, m in json.load(fh).items()}
    first = {}
    os.makedirs(os.path.join(D, "minute"), exist_ok=True)
    jobs = []
    for i, d in enumerate(cal):
        for dd in cal[max(0, i - bt_sip.LOOKBACK):i + 1]:
            if dd not in first:
                with gzip.open(os.path.join(D, "first", f"{dd}.json.gz"), "rt") as fh:
                    first[dd] = json.load(fh)
        for old in [x for x in first if x < cal[max(0, i - bt_sip.LOOKBACK)]]:
            del first[old]
        path = os.path.join(D, "minute", f"{d}.json.gz")
        if d >= TEST_START and not os.path.exists(path):
            jobs.append((c, path, d, bt_sip.select(i, cal, first, cands)))
    run_pool(c, minute_day, jobs, f"minute bars ({len(cal)} sessions in all)", 100)


def main():
    os.makedirs(D, exist_ok=True)
    c = alpaca_data.Client()
    assets = stage_assets(c)
    stage_daily(c, [a["symbol"] for a in assets])
    stage_first(c)
    stage_minute(c)
    log(f"done, {c.calls} calls")


if __name__ == "__main__":
    sys.exit(main())
