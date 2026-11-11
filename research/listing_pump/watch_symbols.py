"""Log when a new symbol first shows in Binance's or Coinbase's public market lists (research only, no keys, no orders).

Step C4 of research/PLAN.md. The 2 s listing entry of round 16b needs a listing to be seen within about a second of its
post. Upbit's market list was 43 and 50 minutes late, and Bithumb's notice feed sits behind an hourly cache. This logger
asks one question of three more official, documented, public endpoints: does a new symbol appear in them before the
listing is announced, after it, or only when trading opens? It records first-seen times and status changes only; the
announcement times are matched afterwards with download_binance.py.

Endpoints and pace (Binance limits are per IP: 6,000 weight a minute on spot, 2,400 on futures)
  spot_break  api.binance.com/api/v3/exchangeInfo?symbolStatus=BREAK   every 10 s, weight 20 (120 a minute)
  spot_all    api.binance.com/api/v3/exchangeInfo                      every 300 s, weight 20 (catches any other status)
  futures     fapi.binance.com/fapi/v1/exchangeInfo                    every 5 s, weight 1 (12 a minute)
  coinbase    api.exchange.coinbase.com/products                       every 10 s (public limit 10 a second)
A 429 or 418, a 403, or a Cloudflare page pauses that endpoint for an hour (longer if Retry-After says so) and is
logged. It never changes headers or addresses to get around a refusal. The largest x-mbx-used-weight-1m seen is
logged hourly, to show the host stays far below Binance's limits.

Output (append-only) in $WATCH (default ./watch_symbols)
  symbols.jsonl    {"t": ms, "src": name, "baseline": true, "n": count, "statuses": {status: count}} on every start;
                   {"t": ms, "src": name, "added": [{symbol, status, ...}], "removed": [...],
                    "changed": [[symbol, old, new], ...], "rtt_ms": n}
  heartbeat.jsonl  {"t": ms, "requests": {...}, "errors": {...}, "rtt_ms_p50": n, "rtt_ms_p95": n, "weight_max": n}
  blocked.jsonl    {"t": ms, "src": name, "status": code, "pause_s": n}

  python watch_symbols.py
"""

from __future__ import annotations

import gzip
import http.client
import json
import os
import ssl
import time

WATCH = os.environ.get("WATCH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "watch_symbols"))
HEADERS = {"Accept": "application/json", "Accept-Encoding": "gzip",
           "User-Agent": "funding-research/1.0 (research script)"}
PAUSE_BLOCKED = 3600.0
SOURCES = {
    "spot_break": ("api.binance.com", "/api/v3/exchangeInfo?symbolStatus=BREAK&showPermissionSets=false", 10.0),
    "spot_all": ("api.binance.com", "/api/v3/exchangeInfo?showPermissionSets=false", 300.0),
    "futures": ("fapi.binance.com", "/fapi/v1/exchangeInfo", 5.0),
    "coinbase": ("api.exchange.coinbase.com", "/products", 10.0),
}


def now_ms():
    return int(time.time() * 1000)


def append(name, row):
    with open(os.path.join(WATCH, name), "a") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse(src, body):
    """Answer body -> {symbol: state} where state is a short dict of what can change."""
    data = json.loads(body)
    if src == "coinbase":
        return {p["id"]: {"status": p.get("status"), "trading_disabled": p.get("trading_disabled"),
                          "auction_mode": p.get("auction_mode"), "cancel_only": p.get("cancel_only"),
                          "post_only": p.get("post_only"), "limit_only": p.get("limit_only")} for p in data}
    out = {}
    for s in data["symbols"]:
        state = {"status": s.get("status"), "base": s.get("baseAsset"), "quote": s.get("quoteAsset")}
        if src == "futures":
            state.update({"type": s.get("contractType"), "onboard": s.get("onboardDate"),
                          "underlying": s.get("underlyingType")})
        out[s["symbol"]] = state
    return out


def diff(old, new):
    """Two {symbol: state} maps -> (added [{symbol, ...state}], removed [symbol], changed [[symbol, old, new]])."""
    added = [{"symbol": k, **new[k]} for k in sorted(new.keys() - old.keys())]
    removed = sorted(old.keys() - new.keys())
    changed = [[k, old[k], new[k]] for k in sorted(new.keys() & old.keys()) if old[k] != new[k]]
    return added, removed, changed


def counts(state):
    out = {}
    for v in state.values():
        out[str(v.get("status"))] = out.get(str(v.get("status")), 0) + 1
    return out


class Endpoint:
    def __init__(self, name, host, path, every):
        self.name, self.host, self.path, self.every = name, host, path, every
        self.conn, self.due, self.state, self.fails = None, 0.0, None, 0

    def get(self):
        """-> (status, body, headers with lower-case keys, rtt ms)."""
        if self.conn is None:
            self.conn = http.client.HTTPSConnection(self.host, timeout=15, context=ssl.create_default_context())
        t0 = time.monotonic()
        try:
            self.conn.request("GET", self.path, headers=HEADERS)
            r = self.conn.getresponse()
            body = r.read()
            if (r.getheader("Content-Encoding") or "").lower() == "gzip":
                body = gzip.decompress(body)
            return r.status, body, {k.lower(): v for k, v in r.getheaders()}, (time.monotonic() - t0) * 1000
        except Exception:  # noqa: BLE001
            self.conn.close()
            self.conn = None
            raise

    def next_wait(self, status, body, headers):
        """Seconds to the next request after an answer (status None: a network error)."""
        if status == 200:
            self.fails = 0
            return self.every
        if status in (403, 418, 429) or (status is not None and b"cloudflare" in body.lower()[:2000]):
            retry = headers.get("retry-after", "")
            pause = max(PAUSE_BLOCKED, float(retry) if retry.isdigit() else 0.0)
            append("blocked.jsonl", {"t": now_ms(), "src": self.name, "status": status, "pause_s": pause})
            return pause
        self.fails += 1
        return min(self.every * 2 ** self.fails, 300.0)


def main():
    os.makedirs(WATCH, exist_ok=True)
    eps = [Endpoint(k, *v) for k, v in SOURCES.items()]
    stats = {"requests": {}, "errors": {}, "rtt": [], "weight_max": 0}
    beat = time.time()
    while True:
        for ep in eps:
            if time.time() < ep.due:
                continue
            start = time.time()
            try:
                status, body, headers, rtt = ep.get()
            except Exception as exc:  # noqa: BLE001
                status, body, headers, rtt = None, b"", {}, None
                key = f"{ep.name}:{type(exc).__name__}"
                stats["errors"][key] = stats["errors"].get(key, 0) + 1
            stats["requests"][ep.name] = stats["requests"].get(ep.name, 0) + 1
            if status not in (None, 200):
                stats["errors"][f"{ep.name}:{status}"] = stats["errors"].get(f"{ep.name}:{status}", 0) + 1
            w = headers.get("x-mbx-used-weight-1m", "")
            if w.isdigit():
                stats["weight_max"] = max(stats["weight_max"], int(w))
            if status == 200:
                t = now_ms()
                stats["rtt"].append(rtt)
                try:
                    state = parse(ep.name, body)
                except (ValueError, KeyError, TypeError):
                    stats["errors"][f"{ep.name}:parse"] = stats["errors"].get(f"{ep.name}:parse", 0) + 1
                    state = None
                if state is not None and ep.state is None:
                    append("symbols.jsonl", {"t": t, "src": ep.name, "baseline": True, "n": len(state),
                                             "statuses": counts(state), "rtt_ms": round(rtt)})
                elif state is not None:
                    added, removed, changed = diff(ep.state, state)
                    if added or removed or changed:
                        append("symbols.jsonl", {"t": t, "src": ep.name, "added": added, "removed": removed,
                                                 "changed": changed, "rtt_ms": round(rtt)})
                if state is not None:
                    ep.state = state
            ep.due = start + ep.next_wait(status, body, headers)
        if time.time() - beat >= 3600:
            r = sorted(stats["rtt"]) or [0]
            append("heartbeat.jsonl", {"t": now_ms(), "requests": stats["requests"], "errors": stats["errors"],
                                       "rtt_ms_p50": round(r[len(r) // 2]), "rtt_ms_p95": round(r[int(len(r) * 0.95)]),
                                       "weight_max": stats["weight_max"]})
            stats, beat = {"requests": {}, "errors": {}, "rtt": [], "weight_max": 0}, time.time()
        time.sleep(max(0.05, min(ep.due for ep in eps) - time.time()))


if __name__ == "__main__":
    main()
