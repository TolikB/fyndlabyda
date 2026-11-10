"""Log when Bithumb's official API first shows a notice or a market (research only, public data, no orders).

Upbit has no notice feed a program may read, and its market list shows a new KRW market 43 to 50 minutes after the
notice. Bithumb's Open API has a notice endpoint: GET https://api.bithumb.com/v1/notices redirects to
feed-api.bithumb.com and returns the five latest notices with published_at to the second. On 2026-11-10 the answer came
from Cloudflare's cache with max-age=14400 and age 3306 s, and it ignored page and count. So before any test of prices
this logger asks: how long after published_at does a notice show up here, and is the cache refreshed when a notice is
published or only when it runs out? It also logs new markets and market_warning changes in GET /v1/market/all, the same
question round 16b asked of Upbit.

Polite by construction:
  - one request every 2 s to each endpoint; the notice endpoint's own headers allow one a second;
  - 429 doubles the wait (up to 60 s); 403 or a Cloudflare error page pauses that endpoint for an hour.
    It never changes headers or addresses to get around a refusal.

Output (append-only) in $WATCH (default ./watch_bithumb):
  notices.jsonl   {"t": ms, "new": [{url, title, categories, published_at, modified_at, lag_s}], "age": s,
                  "cache": "HIT", "rtt_ms": n}; lag_s = t - published_at (Seoul time); a "baseline" line on every
                  start; {"t": ms, "refresh": true, "age": s, "prev_age": s} when the cached copy was replaced
  markets.jsonl   {"t": ms, "added": [...], "removed": [...], "flags": [[market, warning], ...], "rtt_ms": n}
  heartbeat.jsonl {"t": ms, "requests": {...}, "errors": {...}, "rtt_ms_p50": n, "rtt_ms_p95": n} hourly

  python watch_bithumb.py
"""

from __future__ import annotations

import calendar
import gzip
import http.client
import json
import os
import ssl
import time

NOTICES = ("feed-api.bithumb.com", "/v1/notices")
MARKETS = ("api.bithumb.com", "/v1/market/all?isDetails=true")
WATCH = os.environ.get("WATCH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "watch_bithumb"))
INTERVAL = 2.0
PAUSE_BLOCKED = 3600.0
SEOUL_S = 9 * 3600
HEADERS = {"Accept": "application/json", "Accept-Encoding": "gzip",
           "User-Agent": "funding-research/1.0 (research script)"}


def now_ms():
    return int(time.time() * 1000)


def append(name, row):
    with open(os.path.join(WATCH, name), "a") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def seoul_ms(text):
    """'2026-11-10 20:00:00' (Seoul wall clock) -> epoch ms, or None."""
    try:
        return (calendar.timegm(time.strptime(text, "%Y-%m-%d %H:%M:%S")) - SEOUL_S) * 1000
    except (TypeError, ValueError):
        return None


def new_notices(rows, known, t):
    """-> (notices not seen before or modified since, the new state {url: modified_at})."""
    state, out = {}, []
    for n in rows:
        url, mod = n.get("pc_url"), n.get("modified_at")
        state[url] = mod
        if url in known and known[url] == mod:
            continue
        pub = seoul_ms(n.get("published_at"))
        out.append({"url": url, "title": n.get("title"), "categories": n.get("categories"),
                    "published_at": n.get("published_at"), "modified_at": mod,
                    "lag_s": None if pub is None else round((t - pub) / 1000, 1), "modified": url in known})
    return out, state


class Endpoint:
    def __init__(self, host, path):
        self.host, self.path = host, path
        self.conn, self.wait, self.due = None, INTERVAL, 0.0

    def get(self):
        """-> (status, body bytes, headers dict with lower-case keys, rtt ms)."""
        if self.conn is None:
            self.conn = http.client.HTTPSConnection(self.host, timeout=10, context=ssl.create_default_context())
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

    def settle(self, status, body, log):
        """Set the next wait from the answer; -> True when the body is usable."""
        if status == 200:
            self.wait = INTERVAL
            return True
        if status is not None and (status == 403 or b"cloudflare" in body.lower()[:2000]):
            append(log, {"t": now_ms(), "blocked": status, "pause_s": PAUSE_BLOCKED})
            self.wait = PAUSE_BLOCKED
        else:
            self.wait = min(max(self.wait * 2, 4.0), 60.0)
        return False


def main():
    os.makedirs(WATCH, exist_ok=True)
    notices, markets = Endpoint(*NOTICES), Endpoint(*MARKETS)
    known_n, known_m, prev_age = None, None, None
    stats = {"requests": {}, "errors": {}, "rtt": []}
    beat = time.time()
    while True:
        for ep, name in ((notices, "notices"), (markets, "markets")):
            if time.time() < ep.due:
                continue
            start = time.time()
            try:
                status, body, headers, rtt = ep.get()
            except Exception as exc:  # noqa: BLE001
                status, body, headers, rtt = None, b"", {}, None
                key = f"{name}:{type(exc).__name__}"
                stats["errors"][key] = stats["errors"].get(key, 0) + 1
            stats["requests"][name] = stats["requests"].get(name, 0) + 1
            if status not in (None, 200):
                stats["errors"][f"{name}:{status}"] = stats["errors"].get(f"{name}:{status}", 0) + 1
            if ep.settle(status, body, f"{name}.jsonl"):
                t = now_ms()
                stats["rtt"].append(rtt)
                try:
                    rows = json.loads(body)
                    if not isinstance(rows, list):
                        raise ValueError("not a list")
                except ValueError:
                    stats["errors"][f"{name}:json"] = stats["errors"].get(f"{name}:json", 0) + 1
                    rows = None
                if rows is not None and name == "notices":
                    age = int(headers["age"]) if headers.get("age", "").isdigit() else None
                    fresh, state = new_notices(rows, known_n or {}, t)
                    if known_n is None:
                        append("notices.jsonl", {"t": t, "baseline": True, "new": fresh, "age": age,
                                                 "cache": headers.get("cf-cache-status"),
                                                 "cache_control": headers.get("cache-control"), "rtt_ms": round(rtt)})
                    elif fresh:
                        append("notices.jsonl", {"t": t, "new": fresh, "age": age,
                                                 "cache": headers.get("cf-cache-status"), "rtt_ms": round(rtt)})
                    if prev_age is not None and (age is None or age < prev_age):
                        append("notices.jsonl", {"t": t, "refresh": True, "age": age, "prev_age": prev_age,
                                                 "cache": headers.get("cf-cache-status")})
                    known_n, prev_age = state, age
                elif rows is not None:
                    state = {m["market"]: m.get("market_warning") for m in rows}
                    if known_m is None:
                        append("markets.jsonl", {"t": t, "baseline": True, "markets": sorted(state),
                                                 "flags": [[m, w] for m, w in sorted(state.items()) if w != "NONE"],
                                                 "rtt_ms": round(rtt)})
                    elif state != known_m:
                        append("markets.jsonl", {"t": t, "added": sorted(state.keys() - known_m.keys()),
                                                 "removed": sorted(known_m.keys() - state.keys()),
                                                 "flags": [[m, w] for m, w in sorted(state.items())
                                                           if m in known_m and known_m[m] != w],
                                                 "rtt_ms": round(rtt)})
                    known_m = state
            ep.due = start + ep.wait
        if time.time() - beat >= 3600:
            r = sorted(stats["rtt"]) or [0]
            append("heartbeat.jsonl", {"t": now_ms(), "requests": stats["requests"], "errors": stats["errors"],
                                       "rtt_ms_p50": round(r[len(r) // 2]), "rtt_ms_p95": round(r[int(len(r) * 0.95)])})
            stats, beat = {"requests": {}, "errors": {}, "rtt": []}, time.time()
        time.sleep(max(0.05, min(notices.due, markets.due) - time.time()))


if __name__ == "__main__":
    main()
