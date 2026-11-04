"""Descriptive look at Upbit's market_event caution flags (development data only, no trial).

`upbit-watch` logs every flag change on the official market list. This asks whether a flag turning
on for a KRW market says anything about the coin's Binance perp in the next hour. An event is the
first time a flag turns on for a market in 6 h. Entry is the open of the first whole minute after
the event, so this is a coarse minute-level view. The sign comes from the perp's move over the
5 minutes before the event: + means continuation, - means reversal.
Run on the VM: python diag_flags.py [end_ms], from the listing_pump directory.
"""

import json
import sys
import time
import urllib.parse
import urllib.request

import numpy as np

WATCH = "watch/markets.jsonl"
FAPI = "https://fapi.binance.com"
H, M = 3_600_000, 60_000
FRESH = 6 * H
HORIZONS = (1, 5, 10, 15, 20, 30, 45, 60)
PRE = 5


def fapi(path, **params):
    url = f"{FAPI}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def events(end_ms=None):
    state, last, out = {}, {}, []
    for line in open(WATCH):
        d = json.loads(line)
        if d.get("baseline") or "flags" not in d or (end_ms and d["t"] >= end_ms):
            continue
        for market, warn, caution in d["flags"]:
            cur = set(caution) | ({"WARNING"} if warn else set())
            prev = state.get(market)
            state[market] = cur
            if prev is None or not market.startswith("KRW-"):
                continue
            for flag in cur - prev:
                key = (market, flag)
                if key not in last or d["t"] - last[key] >= FRESH:
                    out.append((d["t"], market.split("-", 1)[1], flag))
                last[key] = d["t"]
    return out


def perp_map():
    info = fapi("/fapi/v1/exchangeInfo")
    live = {s["symbol"] for s in info["symbols"] if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
            and s["contractType"] == "PERPETUAL"}
    return lambda coin: next((s for s in (f"{coin}USDT", f"1000{coin}USDT") if s in live), None)


def path(sym, t):
    """Returns (pre-move, {horizon: return from the entry minute's open}) or None."""
    start = t // M * M - PRE * M
    rows = fapi("/fapi/v1/klines", symbol=sym, interval="1m", startTime=start, limit=PRE + 1 + max(HORIZONS) + 1)
    o = {int(r[0]): float(r[1]) for r in rows}
    entry = t // M * M + M
    if entry not in o or start not in o:
        return None
    rets = {h: (o[entry + h * M] / o[entry] - 1 if entry + h * M in o else np.nan) for h in HORIZONS}
    return o[entry] / o[start] - 1, rets


def stats(x):
    x = x[np.isfinite(x)]
    return f"mean {x.mean():+.2%} med {np.median(x):+.2%} win {(x > 0).mean():.0%} t {x.mean() / (x.std(ddof=1) / np.sqrt(len(x))):+.2f} n {len(x)}"


def main():
    end_ms = int(sys.argv[1]) if len(sys.argv) > 1 else None
    perp, rows, btc = perp_map(), [], {}
    for t, coin, flag in events(end_ms):
        sym = perp(coin)
        got = path(sym, t) if sym else None
        time.sleep(0.15)
        if got:
            key = t // M
            if key not in btc:
                btc[key] = path("BTCUSDT", t)
                time.sleep(0.15)
            rows.append((flag, coin, t, got[0], got[1], btc[key][1]))
    print("events with a perp:", len(rows))
    for flag in sorted({r[0] for r in rows}):
        sub = sorted((r for r in rows if r[0] == flag), key=lambda r: r[2])
        pre = np.array([r[3] for r in sub])
        print(f"{flag}: n={len(sub)}, coins={len({r[1] for r in sub})}, pre-5m median {np.median(pre):+.2%}")
        for h in HORIZONS:
            raw = np.array([r[4][h] for r in sub])
            exc = raw - np.array([r[5][h] for r in sub])
            # one mean per 30-minute bucket, so clustered events count once
            b = np.array([r[2] // (30 * M) for r in sub])
            bucket = np.array([np.nanmean(exc[b == k]) for k in np.unique(b)])
            print(f"  +{h:>2}m raw {stats(raw)} | excess over BTC {stats(exc)} | by 30m bucket {stats(bucket)}")
        if flag == "TRADING_VOLUME_SOARING":
            days = {}
            for r in sub:
                d = time.strftime("%m-%d", time.gmtime(r[2] / 1000))
                days.setdefault(d, []).append(r[4][15] - r[5][15])
            for d, v in sorted(days.items()):
                print("  ", d, stats(np.array(v)))
            for r in sub:
                print("  ", time.strftime("%m-%d %H:%M:%S", time.gmtime(r[2] / 1000)), r[1],
                      f"pre {r[3]:+.2%} +5 {r[4][5]:+.2%} +15 {r[4][15]:+.2%} +60 {r[4][60]:+.2%} btc15 {r[5][15]:+.2%}")


if __name__ == "__main__":
    main()
