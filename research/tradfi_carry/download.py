"""Download funding and hourly prices of the stock perps listed on both Binance and Hyperliquid's xyz (research only).

Round 39 asks whether the same stock, fund or metal pays different funding on the two venues often and steadily enough
for a hedged pair to earn it. This script only fetches public history; it prints counts, never rates or prices, so that
the rules of round 39 can be fixed before any value is read.

Input: a pairs file of the tradfi_lag collector (pairs/<date>.json: {xyz coin: {"binance": symbol, ...}}).
Endpoints (public, no keys), paced well below the per-IP limits:
  Binance  GET /fapi/v1/fundingRate (1,000 rows a call, its own limit of 500 calls per 5 min), GET /fapi/v1/klines 1h
           (1,500 rows a call, weight 10): one call a second
  HL       POST /info fundingHistory (500 rows a call, weight 20 + 1 per 20 rows) and candleSnapshot 1h (5,000 rows a
           call, weight 20 + 1 per 60 rows; only the latest 5,000 candles are served, about 208 days): one call every
           3 s, so under 700 weight a minute of 1,200. Run it from a host whose IP no collector shares.
A 429 or 418 stops the script. Output in OUT_DIR, one gzipped JSON per table and name:
  bn_funding/<SYMBOL>.json.gz   [{fundingTime, fundingRate, markPrice}, ...]
  bn_klines/<SYMBOL>.json.gz    [[open time, open, high, low, close, volume, close time, quote volume, ...], ...]
  hl_funding/<coin>.json.gz     [{coin, fundingRate, premium, time}, ...]
  hl_candles/<coin>.json.gz     [{t, T, s, i, o, c, h, l, v, n}, ...]
  manifest.json                 rows and first/last time per file, the pairs, the end time

  python download.py PAIRS_JSON OUT_DIR [END_ISO]
"""

from __future__ import annotations

import calendar
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request

BN = "https://fapi.binance.com"
HL = "https://api.hyperliquid.xyz/info"
START = 1_735_689_600_000          # 2025-01-01 UTC, before any of these perps existed
HOUR = 3_600_000
HEADERS = {"User-Agent": "funding-research/1.0 (research script)", "Content-Type": "application/json"}
PACE = {"bn": 1.0, "hl": 3.0}
_last = {"bn": 0.0, "hl": 0.0}


class Refused(Exception):
    pass


def call(venue, url, body=None):
    wait = _last[venue] + PACE[venue] - time.time()
    if wait > 0:
        time.sleep(wait)
    _last[venue] = time.time()
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=HEADERS)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (403, 418, 429):
                raise Refused(f"{venue} answered {e.code}: stopping") from e
            if attempt == 2:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(10 * (attempt + 1))
    raise RuntimeError("unreachable")


def bn_funding(sym, end):
    out, t = [], START
    while t < end:
        rows = call("bn", f"{BN}/fapi/v1/fundingRate?symbol={sym}&startTime={t}&endTime={end}&limit=1000")
        if not rows:
            break
        out += [{k: r[k] for k in ("fundingTime", "fundingRate", "markPrice") if k in r} for r in rows]
        t = rows[-1]["fundingTime"] + 1
        if len(rows) < 1000:
            break
    return out


def bn_klines(sym, end):
    out, t = [], START
    while t < end:
        rows = call("bn", f"{BN}/fapi/v1/klines?symbol={sym}&interval=1h&startTime={t}&endTime={end - 1}&limit=1500")
        if not rows:
            break
        out += rows
        t = rows[-1][0] + HOUR
        if len(rows) < 1500:
            break
    return out


def hl_funding(coin, end):
    out, t = [], START
    while t < end:
        rows = call("hl", HL, {"type": "fundingHistory", "coin": coin, "startTime": t, "endTime": end})
        if not rows:
            break
        out += rows
        t = rows[-1]["time"] + 1
        if len(rows) < 500:
            break
    return out


def hl_candles(coin, end):
    out, t = [], START
    while t < end:
        rows = call("hl", HL, {"type": "candleSnapshot", "req": {"coin": coin, "interval": "1h", "startTime": t,
                                                                  "endTime": end}})
        if not rows:
            break
        out += rows
        t = rows[-1]["t"] + HOUR
        if len(rows) < 5000:
            break
    return out


def save(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path + ".tmp", "wt") as fh:
        json.dump(rows, fh)
    os.replace(path + ".tmp", path)


def span(rows, key):
    return [rows[0][key], rows[-1][key]] if rows else None


def main():
    pairs = json.load(open(sys.argv[1]))
    out_dir = sys.argv[2]
    end = (calendar.timegm(time.strptime(sys.argv[3], "%Y-%m-%dT%H:%M")) * 1000 if len(sys.argv) > 3
           else int(time.time() // 3600 * 3600 * 1000))
    manifest = {"end": end, "pairs": {c: p["binance"] for c, p in pairs.items()}, "files": {}}
    jobs = []
    for coin, p in sorted(pairs.items()):
        sym = p["binance"]
        jobs += [(f"bn_funding/{sym}", bn_funding, sym, "fundingTime"), (f"bn_klines/{sym}", bn_klines, sym, 0),
                 (f"hl_funding/{coin}", hl_funding, coin, "time"), (f"hl_candles/{coin}", hl_candles, coin, "t")]
    done = 0
    for name, fn, arg, key in jobs:
        path = os.path.join(out_dir, name.replace(":", "_") + ".json.gz")
        if os.path.exists(path):
            done += 1
            continue
        rows = fn(arg, end)
        save(path, rows)
        manifest["files"][name] = {"rows": len(rows), "span": span(rows, key)}
        done += 1
        print(f"{done}/{len(jobs)} {name} rows {len(rows)}", flush=True)
    old = os.path.join(out_dir, "manifest.json")
    if os.path.exists(old):
        prev = json.load(open(old))
        prev["files"].update(manifest["files"])
        manifest["files"] = prev["files"]
    with open(old, "w") as fh:
        json.dump(manifest, fh, indent=1)


if __name__ == "__main__":
    main()
