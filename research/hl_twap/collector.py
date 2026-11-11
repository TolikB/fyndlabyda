"""Log every TWAP order on Hyperliquid as it starts and ends, with prices around it (research only, no keys, no orders).

Collector of round 42 (see README.md). A TWAP order on Hyperliquid is public from its first block: the coin, the side,
the whole size and the duration. So the rest of its flow is known in advance. This process records, market-wide,
when each TWAP is first seen, when it ends, and what the market did meanwhile. It reads nothing but public endpoints
and sends no order.

Sources and pace
  scan  GET api.hypurrscan.io/twap/*            every 10 s, weight 1 (the explorer allows 1,000 a minute per IP)
  hl    POST api.hyperliquid.xyz/info
          allMids, per dex with a stored asset   every 10 s, weight 2 each
          metaAndAssetCtxs, one dex a tick       each dex every 300 s, weight 20
          spotMetaAndAssetCtxs                   every 300 s, weight 20
          perpDexs                               every 3,600 s, weight 20
        About 70-180 of Hyperliquid's 1,200 weight a minute per IP.
A 403, 418 or 429 pauses that host (an hour for the explorer, ten minutes for Hyperliquid) and is logged. Nothing is
ever changed to get around a refusal.

Asset ids (Hyperliquid's own): a main perp is its index in the universe, a builder perp is
100000 + 10000 x dex index + index, a spot pair is 10000 + its index.

Output in $OUT (default ./data), append-only, one file a UTC day for the first three
  twaps-YYYYMMDD.jsonl  {"t": ms seen, "ev": "base"|"start", "key", "time": ms the order was sent, "user", "a", "b"
                         (true = buy), "s" (size in coins), "r" (reduce only), "m" (minutes), "rnd" (randomised),
                         "block", "error", "ended", "px", "ntl", "back": seen before} on the first list and on
                         every new order;
                        {"t", "ev": "change", "key", "ended", "error"} when its status changes;
                        {"t", "ev": "gone", "key"} when it leaves the list (finished or cancelled).
  mids-YYYYMMDD.jsonl   {"t": ms, "dex", "rtt": ms, "m": {coin: mid}} for coins that had a TWAP of at least
                        MIN_NTL USD in the last KEEP_S seconds. Smaller orders get no prices.
  ctx-YYYYMMDD.jsonl    {"t": ms, "dex", "rows": [[coin, mark, mid, impact bid, impact ask, day volume USD, open
                        interest, funding, oracle], ...]}; for spot {"t", "dex": "@spot", "rows": [[coin, mark, mid,
                        day volume USD], ...]}
  assets.jsonl          {"t": ms, "dex", "map": {asset id: coin}} on the first read and on every change
  heartbeat.jsonl       hourly counts, errors, round-trip times and how late new orders were seen
  blocked.jsonl         {"t": ms, "host", "status", "pause_s"}

  python collector.py
"""

from __future__ import annotations

import glob
import gzip
import http.client
import json
import os
import ssl
import time

OUT = os.environ.get("OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
HEADERS = {"Accept": "application/json", "Accept-Encoding": "gzip", "Content-Type": "application/json",
           "User-Agent": "funding-research/1.0 (research script)"}
TWAP_EVERY = 10.0
MIDS_EVERY = 10.0
CTX_EVERY = 300.0
DEXS_EVERY = 3600.0
KEEP_S = 24 * 3600.0
MIN_NTL = 20000.0
PAUSE = {"scan": 3600.0, "hl": 600.0}
RETRY = 60.0
CTX_GAP = 1.0                 # seconds between two dexes of one price round
READY_WAIT = 120.0            # start listing orders after this long even if the first price round is not done
EVER_S = 7 * 24 * 3600.0      # how long a seen order is remembered
SPOT = "@spot"                # internal name of the spot book; no perp dex can be called this
SHORT_RUN = 3
BLOCKED = (403, 418, 429)


def day(t_ms):
    return time.strftime("%Y%m%d", time.gmtime(t_ms / 1000))


def key_of(row):
    """A TWAP's identity: its transaction hash, or sender, time and asset when the explorer gives none."""
    h = row.get("hash")
    if h and set(h) - set("0x"):
        return h
    return f"{row['user']}:{row['time']}:{row['action']['twap']['a']}"


def norm(row):
    """An explorer row -> the flat record that is stored. Raises KeyError or TypeError on a malformed row."""
    tw = row["action"]["twap"]
    return {"key": key_of(row), "time": int(row["time"]), "user": row["user"], "a": int(tw["a"]), "b": bool(tw["b"]),
            "s": str(tw["s"]), "r": bool(tw["r"]), "m": int(tw["m"]), "rnd": bool(tw["t"]),
            "block": row.get("block"), "error": row.get("error"), "ended": row.get("ended")}


def perp_assets(dex_index, meta):
    """One perp dex's meta -> {asset id: coin}. dex_index 0 is the main dex."""
    base = 0 if dex_index == 0 else 100000 + 10000 * dex_index
    return {base + j: u["name"] for j, u in enumerate(meta["universe"])}


def spot_assets(meta):
    return {10000 + u["index"]: u["name"] for u in meta["universe"]}


def diff(old, new):
    """Two {key: record} maps -> (started [record], changed [record], gone [key]), each in key order."""
    started = [new[k] for k in sorted(new.keys() - old.keys())]
    changed = [new[k] for k in sorted(new.keys() & old.keys())
               if (old[k]["ended"], old[k]["error"]) != (new[k]["ended"], new[k]["error"])]
    return started, changed, sorted(old.keys() - new.keys())


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class Collector:
    """All state and decisions; `fetch(host, payload)` does the network and `clock()` gives the time in seconds."""

    def __init__(self, out, fetch, clock=time.time):
        self.out, self.fetch, self.clock = out, fetch, clock
        self.due = {"twap": 0.0, "mids": 0.0, "ctx": 0.0, "dexs": 0.0}
        self.pause = {"scan": 0.0, "hl": 0.0}
        self.rows = None          # {key: record} of the last accepted list; None until the first one
        self.ever = {}            # key -> ms last seen, to flag an order that comes back
        self.dexs = [""]          # perp dex names by index; "" is the main dex
        self.queue = []           # dexes (and "spot") still to refresh in this ctx round
        self.assets = {}          # asset id -> (dex name, coin)
        self.maps = {}            # dex name -> {asset id: coin}, as last written
        self.px = {}              # coin -> last known price
        self.interest = {}        # asset id -> time until which its mids are stored
        self.short = 0
        self.rtt = 0.0
        self.round_start = 0.0
        self.ready = False        # the first price round is done, so a new order's size in USD is known
        self.born = clock()
        self.beat = clock()
        self.stats = self.fresh()
        os.makedirs(out, exist_ok=True)
        self.restore()

    @staticmethod
    def fresh():
        return {"requests": {}, "errors": {}, "rtt": {"scan": [], "hl": []}, "started": 0, "lag": [], "short": 0}

    def append(self, stem, row, daily=True):
        name = f"{stem}-{day(row['t'])}.jsonl" if daily else f"{stem}.jsonl"
        with open(os.path.join(self.out, name), "a") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def restore(self):
        """After a restart: remember which orders were seen and keep storing prices for the recent large ones."""
        now = self.clock()
        for path in sorted(glob.glob(os.path.join(self.out, "twaps-*.jsonl")))[-2:]:
            with open(path) as fh:
                for line in fh:
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    if d.get("ev") in ("base", "start"):
                        self.ever[d["key"]] = d["t"]
                        if d.get("ntl") is None or d["ntl"] >= MIN_NTL:
                            if d["t"] / 1000 + KEEP_S > now:
                                self.interest[d["a"]] = max(self.interest.get(d["a"], 0.0), d["t"] / 1000 + KEEP_S)

    def call(self, host, name, payload=None):
        """One request -> the parsed answer, or None on any failure (counted, and a refusal pauses the host)."""
        st = self.stats
        st["requests"][name] = st["requests"].get(name, 0) + 1
        try:
            status, obj, rtt = self.fetch(host, payload)
        except Exception as exc:  # noqa: BLE001
            k = f"{name}:{type(exc).__name__}"
            st["errors"][k] = st["errors"].get(k, 0) + 1
            return None
        if status == 200 and obj is not None:
            st["rtt"][host].append(rtt)
            self.rtt = rtt
            return obj
        k = f"{name}:{status if status != 200 else 'parse'}"
        st["errors"][k] = st["errors"].get(k, 0) + 1
        if status in BLOCKED:
            self.pause[host] = self.clock() + PAUSE[host]
            self.append("blocked", {"t": int(self.clock() * 1000), "host": host, "status": status,
                                    "pause_s": PAUSE[host]}, daily=False)
        return None

    def set_assets(self, dex, mapping):
        for a in [a for a, (d, _) in self.assets.items() if d == dex and a not in mapping]:
            del self.assets[a]
        for a, coin in mapping.items():
            self.assets[a] = (dex, coin)
        if self.maps.get(dex) != mapping:
            self.maps[dex] = mapping
            self.append("assets", {"t": int(self.clock() * 1000), "dex": dex,
                                   "map": {str(a): c for a, c in sorted(mapping.items())}}, daily=False)

    def do_dexs(self, now):
        obj = self.call("hl", "perpDexs", {"type": "perpDexs"})
        if isinstance(obj, list) and obj:
            self.dexs = ["" if x is None else str(x.get("name", "")) for x in obj]
            self.due["dexs"] = now + DEXS_EVERY
        else:
            self.due["dexs"] = now + RETRY

    def do_ctx(self, now):
        """Refresh one dex (or spot) a call; a round over all of them starts every CTX_EVERY seconds."""
        if not self.queue:
            self.queue = [""] + [d for d in self.dexs[1:] if d] + [SPOT]
            self.round_start = now
        dex = self.queue.pop(0)
        t = int(now * 1000)
        try:
            if dex == SPOT:
                obj = self.call("hl", "spotCtx", {"type": "spotMetaAndAssetCtxs"})
                if obj is not None:
                    meta, ctxs = obj
                    listed = spot_assets(meta)
                    self.set_assets(SPOT, listed)
                    ctxs = [c for c in ctxs if c.get("coin") in set(listed.values())]
                    rows = [[c.get("coin"), c.get("markPx"), c.get("midPx"), c.get("dayNtlVlm")] for c in ctxs]
                    for c in ctxs:
                        p = num(c.get("midPx")) or num(c.get("markPx"))
                        if p:
                            self.px[c.get("coin")] = p
                    self.append("ctx", {"t": t, "dex": SPOT, "rows": rows})
            else:
                body = {"type": "metaAndAssetCtxs", "dex": dex} if dex else {"type": "metaAndAssetCtxs"}
                obj = self.call("hl", "ctx", body)
                if obj is not None:
                    meta, ctxs = obj
                    self.set_assets(dex, perp_assets(self.dexs.index(dex), meta))
                    rows = []
                    for u, c in zip(meta["universe"], ctxs):
                        imp = c.get("impactPxs") or [None, None]
                        rows.append([u["name"], c.get("markPx"), c.get("midPx"), imp[0], imp[1], c.get("dayNtlVlm"),
                                     c.get("openInterest"), c.get("funding"), c.get("oraclePx")])
                        p = num(c.get("midPx")) or num(c.get("markPx"))
                        if p:
                            self.px[u["name"]] = p
                    self.append("ctx", {"t": t, "dex": dex, "rows": rows})
        except (KeyError, TypeError, ValueError, IndexError):
            self.stats["errors"]["ctx:shape"] = self.stats["errors"].get("ctx:shape", 0) + 1
        if self.queue:
            self.due["ctx"] = now + CTX_GAP
        else:
            self.ready = True
            self.due["ctx"] = self.round_start + CTX_EVERY

    def price(self, rec):
        hit = self.assets.get(rec["a"])
        return self.px.get(hit[1]) if hit else None

    def do_twap(self, now):
        self.due["twap"] = now + TWAP_EVERY
        obj = self.call("scan", "twap")
        if obj is None:
            return
        try:
            new = {}
            for row in obj:
                rec = norm(row)
                new[rec["key"]] = rec
        except (KeyError, TypeError, ValueError):
            self.stats["errors"]["twap:shape"] = self.stats["errors"].get("twap:shape", 0) + 1
            return
        t = int(self.clock() * 1000)
        if self.rows is not None and len(self.rows) >= 20 and len(new) * 2 < len(self.rows):
            self.short += 1
            self.stats["short"] += 1
            if self.short < SHORT_RUN:
                return                      # a truncated answer would log half the orders as gone
        self.short = 0
        if self.rows is None:
            started, changed, gone, ev = list(new.values()), [], [], "base"
        else:
            started, changed, gone = diff(self.rows, new)
            ev = "start"
        for rec in started:
            p = self.price(rec)
            ntl = None if p is None or num(rec["s"]) is None else round(num(rec["s"]) * p, 2)
            self.append("twaps", {"t": t, "ev": ev, **rec, "px": p, "ntl": ntl, "back": rec["key"] in self.ever})
            if ev == "start":
                self.stats["started"] += 1
                self.stats["lag"].append((t - rec["time"]) / 1000)
        for rec in changed:
            self.append("twaps", {"t": t, "ev": "change", "key": rec["key"], "ended": rec["ended"],
                                  "error": rec["error"]})
        for k in gone:
            self.append("twaps", {"t": t, "ev": "gone", "key": k})
        for rec in new.values():
            if rec["ended"] is None and rec["error"] is None:
                p = self.price(rec)
                size = num(rec["s"])
                if p is None or size is None or size * p >= MIN_NTL:
                    self.interest[rec["a"]] = now + KEEP_S
        for k in new:
            self.ever[k] = t
        self.rows = new

    def do_mids(self, now):
        self.due["mids"] = now + MIDS_EVERY
        for a in [a for a, until in self.interest.items() if until < now]:
            del self.interest[a]
        want = {}
        for a in self.interest:
            dex, coin = self.assets.get(a, (None, None))
            if coin is not None:
                want.setdefault("" if dex == SPOT else dex, set()).add(coin)
        for dex in sorted(want):
            if self.clock() < self.pause["hl"]:
                return
            obj = self.call("hl", "mids", {"type": "allMids", "dex": dex} if dex else {"type": "allMids"})
            if not isinstance(obj, dict):
                continue
            m = {c: obj[c] for c in sorted(want[dex]) if c in obj}
            for c, v in m.items():
                p = num(v)
                if p:
                    self.px[c] = p
            self.append("mids", {"t": int(self.clock() * 1000), "dex": dex, "rtt": round(self.rtt), "m": m})

    def heartbeat(self, now):
        st = self.stats

        def q(xs, f):
            xs = sorted(xs)
            return round(xs[int(len(xs) * f)], 1) if xs else None
        active = 0 if self.rows is None else sum(1 for r in self.rows.values() if r["ended"] is None)
        self.append("heartbeat", {"t": int(now * 1000), "requests": st["requests"], "errors": st["errors"],
                                  "rtt_ms_p50": {h: q(v, 0.5) for h, v in st["rtt"].items()},
                                  "rtt_ms_p95": {h: q(v, 0.95) for h, v in st["rtt"].items()},
                                  "listed": 0 if self.rows is None else len(self.rows), "active": active,
                                  "started": st["started"], "lag_s_p50": q(st["lag"], 0.5),
                                  "lag_s_p95": q(st["lag"], 0.95), "stored_assets": len(self.interest),
                                  "short": st["short"]}, daily=False)
        self.stats, self.beat = self.fresh(), now
        old = now * 1000 - EVER_S * 1000
        self.ever = {k: v for k, v in self.ever.items() if v >= old}

    def tick(self):
        """Do what is due; -> seconds to sleep before the next call."""
        now = self.clock()
        if now >= self.pause["hl"]:
            if now >= self.due["dexs"]:
                self.do_dexs(now)
            if now >= self.due["ctx"]:
                self.do_ctx(now)
        else:
            self.due["dexs"] = max(self.due["dexs"], self.pause["hl"])
            self.due["ctx"] = max(self.due["ctx"], self.pause["hl"])
        if now >= self.due["twap"] and not (self.ready or now - self.born >= READY_WAIT):
            self.due["twap"] = now + 1.0
        elif now >= self.due["twap"]:
            if now >= self.pause["scan"]:
                self.do_twap(now)
            else:
                self.due["twap"] = self.pause["scan"]
        if now >= self.due["mids"]:
            if now >= self.pause["hl"]:
                self.do_mids(now)
            else:
                self.due["mids"] = self.pause["hl"]
        if now - self.beat >= 3600:
            self.heartbeat(now)
        return max(0.05, min(self.due.values()) - self.clock())


class Net:
    """Keep-alive HTTPS to the two hosts. fetch(host, payload) -> (status, parsed JSON or None, round trip in ms)."""

    HOSTS = {"scan": "api.hypurrscan.io", "hl": "api.hyperliquid.xyz"}

    def __init__(self):
        self.conns = {}

    def fetch(self, host, payload):
        conn = self.conns.get(host)
        if conn is None:
            conn = self.conns[host] = http.client.HTTPSConnection(self.HOSTS[host], timeout=15,
                                                                 context=ssl.create_default_context())
        t0 = time.monotonic()
        try:
            if host == "scan":
                conn.request("GET", "/twap/*", headers=HEADERS)
            else:
                conn.request("POST", "/info", body=json.dumps(payload).encode(), headers=HEADERS)
            r = conn.getresponse()
            body = r.read()
        except Exception:
            conn.close()
            self.conns.pop(host, None)
            raise
        rtt = (time.monotonic() - t0) * 1000
        if (r.getheader("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(body)
        try:
            return r.status, json.loads(body), rtt
        except ValueError:
            return r.status, None, rtt


def main():
    c = Collector(OUT, Net().fetch)
    while True:
        time.sleep(c.tick())


if __name__ == "__main__":
    main()
