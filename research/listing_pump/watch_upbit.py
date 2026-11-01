"""Log when markets and their warning flags change in Upbit's official market list (research only, public data, no orders).

Round 16b needs a bot that sees a KRW listing within about a second of the notice. Upbit's notice
page is not an API and refuses fast polling; its official Open API is meant for programs. This logger
asks one question: does a new KRW market show up in GET /v1/market/all at the notice time, or only
when trading opens hours later? It compares the time it first sees each new market with the notice
time (looked up later) and with the Binance perp's jump (aggTrades archive). It also logs every change of
a market's market_event flags (warning, caution.*), a possible fast signal for round 19.

Polite by construction:
  - one request a second, about a tenth of the published quotation limit, on one kept-alive connection,
    gzip-encoded (about 15 KB a request with details);
  - it reads the Remaining-Req header and waits when the per-second budget is low;
  - 429 doubles the wait (up to 60 s); 403 or a Cloudflare error page pauses polling for an hour.
    It never changes headers or addresses to get around a refusal.

Output (append-only) in $WATCH (default ./watch):
  markets.jsonl   {"t": ms, "added": [...], "removed": [...], "flags": [[market, warning, [caution...]], ...],
                  "rtt_ms": n}, one line per change; a baseline line on every start lists the flagged markets
  heartbeat.jsonl {"t": ms, "requests": n, "errors": {...}, "rtt_ms_p50": n, "rtt_ms_p95": n} hourly

  python watch_upbit.py
"""

from __future__ import annotations

import gzip
import http.client
import json
import os
import ssl
import time

HOST = "api.upbit.com"
PATH = "/v1/market/all?is_details=true"
WATCH = os.environ.get("WATCH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "watch"))
INTERVAL = 1.0
PAUSE_BLOCKED = 3600.0


def now_ms():
    return int(time.time() * 1000)


def append(name, row):
    with open(os.path.join(WATCH, name), "a") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def flags(m):
    """market row -> (warning, sorted caution keys that are true)."""
    ev = m.get("market_event") or {}
    return bool(ev.get("warning")), sorted(k for k, v in (ev.get("caution") or {}).items() if v)


def remaining_sec(header):
    """'group=market; min=599; sec=9' -> 9, or None."""
    for part in (header or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == "sec" and v.isdigit():
            return int(v)
    return None


class Client:
    def __init__(self):
        self.conn = None

    def get(self):
        """-> (status, body bytes, headers dict, rtt ms)."""
        if self.conn is None:
            self.conn = http.client.HTTPSConnection(HOST, timeout=10, context=ssl.create_default_context())
        t0 = time.monotonic()
        try:
            self.conn.request("GET", PATH, headers={"Accept": "application/json", "Accept-Encoding": "gzip"})
            r = self.conn.getresponse()
            body = r.read()
            if (r.getheader("Content-Encoding") or "").lower() == "gzip":
                body = gzip.decompress(body)
            return r.status, body, dict(r.getheaders()), (time.monotonic() - t0) * 1000
        except Exception:  # noqa: BLE001
            self.conn.close()
            self.conn = None
            raise


def main():
    os.makedirs(WATCH, exist_ok=True)
    client, known, wait = Client(), None, INTERVAL
    stats = {"requests": 0, "errors": {}, "rtt": []}
    beat = time.time()
    while True:
        start = time.time()
        try:
            status, body, headers, rtt = client.get()
        except Exception as exc:  # noqa: BLE001
            status, body, headers, rtt = None, b"", {}, None
            stats["errors"][type(exc).__name__] = stats["errors"].get(type(exc).__name__, 0) + 1
        stats["requests"] += 1
        if status == 200:
            wait = INTERVAL
            stats["rtt"].append(rtt)
            state = {m["market"]: flags(m) for m in json.loads(body)}
            if known is None:
                append("markets.jsonl", {"t": now_ms(), "baseline": True, "markets": sorted(state),
                                         "flags": [[m, *f] for m, f in sorted(state.items()) if f != (False, [])],
                                         "rtt_ms": round(rtt)})
            elif state != known:
                append("markets.jsonl", {"t": now_ms(), "added": sorted(state.keys() - known.keys()),
                                         "removed": sorted(known.keys() - state.keys()),
                                         "flags": [[m, *f] for m, f in sorted(state.items())
                                                   if m in known and known[m] != f],
                                         "rtt_ms": round(rtt)})
            known = state
            sec = remaining_sec(headers.get("Remaining-Req") or headers.get("remaining-req"))
            if sec is not None and sec <= 1:
                wait = max(wait, 1.0)
        elif status == 429:
            stats["errors"]["429"] = stats["errors"].get("429", 0) + 1
            wait = min(max(wait * 2, 2.0), 60.0)
        elif status is not None:
            stats["errors"][str(status)] = stats["errors"].get(str(status), 0) + 1
            if status == 403 or b"cloudflare" in body.lower()[:2000]:
                append("markets.jsonl", {"t": now_ms(), "blocked": status, "pause_s": PAUSE_BLOCKED})
                wait = PAUSE_BLOCKED
            else:
                wait = min(max(wait * 2, 2.0), 60.0)
        else:
            wait = min(max(wait * 2, 2.0), 60.0)
        if time.time() - beat >= 3600:
            r = sorted(stats["rtt"]) or [0]
            append("heartbeat.jsonl", {"t": now_ms(), "requests": stats["requests"], "errors": stats["errors"],
                                       "rtt_ms_p50": round(r[len(r) // 2]), "rtt_ms_p95": round(r[int(len(r) * 0.95)])})
            stats, beat = {"requests": 0, "errors": {}, "rtt": []}, time.time()
        time.sleep(max(0.0, wait - (time.time() - start)))


if __name__ == "__main__":
    main()
