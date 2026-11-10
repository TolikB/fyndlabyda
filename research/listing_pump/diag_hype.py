"""Descriptive: HYPEUSDT perp around Upbit's KRW-HYPE notice, 2026-11-10 04:50:00 UTC (no trial)."""
import json
import time
import urllib.request

T = 1794286200000
URL = "https://fapi.binance.com/fapi/v1/aggTrades?symbol=HYPEUSDT&startTime={a}&endTime={z}&limit=1000"


def trades(a, z):
    out = []
    while a < z:
        rows = json.loads(urllib.request.urlopen(URL.format(a=a, z=min(z, a + 3_600_000)), timeout=30).read())
        if not rows:
            break
        out += rows
        if len(rows) < 1000:
            break
        a = rows[-1]["T"] + 1
        time.sleep(0.3)
    return out


rows = trades(T - 120_000, T + 600_000)
print(len(rows), "trades")
before = [r for r in rows if r["T"] < T]
p0 = float(before[-1]["p"])
print("last before T", p0)


def first_at(ms):
    for r in rows:
        if r["T"] >= T + ms:
            return float(r["p"]), r["T"] - T
    return None, None


for ms in (0, 250, 500, 1000, 1500, 2000, 3000, 5000, 10000, 30000, 60000, 62000, 120000, 300000, 600000 - 1):
    p, at = first_at(ms)
    if p:
        print(f"T+{ms/1000:>6.2f}s first trade at +{at/1000:.3f}s price {p:.4f} move {100*(p/p0-1):+.2f}%")
w = [float(r["p"]) for r in rows if T <= r["T"] < T + 60_000]
print(f"max in 60 s {100*(max(w)/p0-1):+.2f}%  min {100*(min(w)/p0-1):+.2f}%")
pre = [float(r["p"]) for r in rows if T - 120_000 <= r["T"] < T - 60_000]
print(f"move T-120..T-60 s {100*(pre[-1]/pre[0]-1):+.2f}%")
p2, _ = first_at(2000)
p62, _ = first_at(62000)
print(f"S2 rule gross (T+2 s -> T+62 s) {100*(p62/p2-1):+.2f}%")
