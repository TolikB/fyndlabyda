"""Keep the public history of two crypto news feeds before it scrolls away (research only, no keys, no orders).

Step D7 of research/PLAN.md. A bot cannot win the first second after a headline, but it may still be early for the
minutes after an ambiguous one. Testing that needs headlines with the time a subscriber got them, the coin and the
kind of event. Tree of Alpha's public history has all three but reaches back only about six days, so it has to be
saved as it goes. BWEnews' public RSS adds one-second stamps for exchange notices.

Sources and pace
  tree  GET news.treeofalpha.com/api/news?limit=3000   every 12 hours. The service documents this endpoint as its
        partial history and asks that it not be polled for updates; twice a day is a history pull, not a live feed.
  bwe   GET rss-public.bwe-ws.com/                      every 15 minutes (a public RSS; it holds the last 10 items).
A 403, 418 or 429, or a Cloudflare page, pauses that source for a day and is logged. Nothing is changed to get
around a refusal.

Output in $OUT (default ./data), append-only, one file a source and month of the pull (UTC)
  tree-YYYYMM.jsonl   {"t": ms pulled, "id": the item's _id, "item": the item without its icon and image fields}
  bwe-YYYYMM.jsonl    {"t": ms pulled, "id": guid or link or title, "title", "pub": pubDate text, "link"}
  heartbeat.jsonl     daily: {"t": ms, "requests": {...}, "errors": {...}, "new": {...}, "known": {...}}
  blocked.jsonl       {"t": ms, "src": name, "status": code, "pause_s": n}
An item is written once, the first time it is seen; a restart reads the files back and writes nothing twice.

  python archive.py
"""

from __future__ import annotations

import glob
import gzip
import html
import http.client
import json
import os
import re
import ssl
import time

OUT = os.environ.get("OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
HEADERS = {"Accept": "*/*", "Accept-Encoding": "gzip", "User-Agent": "funding-research/1.0 (research script)"}
SOURCES = {"tree": ("news.treeofalpha.com", "/api/news?limit=3000", 12 * 3600.0),
           "bwe": ("rss-public.bwe-ws.com", "/", 900.0)}
PAUSE_BLOCKED = 24 * 3600.0
RETRY_MAX = 3600.0
BEAT_S = 24 * 3600.0
BLOCKED = (403, 418, 429)
DROP = ("icon", "image")
ITEM = re.compile(r"<item>(.*?)</item>", re.S)


def month(t_ms):
    return time.strftime("%Y%m", time.gmtime(t_ms / 1000))


def tag(block, name):
    m = re.search(rf"<{name}(?:\s[^>]*)?>(.*?)</{name}>", block, re.S)
    if not m:
        return None
    text = m.group(1).strip()
    if text.startswith("<![CDATA[") and text.endswith("]]>"):
        text = text[9:-3]
    return html.unescape(text).strip()


def parse_tree(body):
    """The history answer -> [(id, item without bulky fields)], oldest first. Raises ValueError when it is not a list."""
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError("not a list")
    out = []
    for x in data:
        if isinstance(x, dict) and x.get("_id") is not None:
            out.append((str(x["_id"]), {k: v for k, v in x.items() if k not in DROP}))
    out.sort(key=lambda r: (r[1].get("time") or 0, r[0]))
    return out


def parse_bwe(body):
    """The RSS answer -> [(id, {"title", "pub", "link"})] in feed order. Raises ValueError when no channel is found."""
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else body
    if "<channel>" not in text:
        raise ValueError("no channel")
    out = []
    for block in ITEM.findall(text):
        title, pub, link, guid = tag(block, "title"), tag(block, "pubDate"), tag(block, "link"), tag(block, "guid")
        key = guid or link or (f"{pub}|{title}" if title else None)
        if key:
            out.append((key, {"title": title, "pub": pub, "link": link}))
    return out


PARSE = {"tree": parse_tree, "bwe": parse_bwe}


class Source:
    def __init__(self, name, host, path, every):
        self.name, self.host, self.path, self.every = name, host, path, every
        self.due, self.fails, self.known = 0.0, 0, set()

    def wait_after(self, status, body, headers):
        """Seconds to the next request after an answer (status None: a network error) and whether it was a refusal."""
        if status == 200:
            self.fails = 0
            return self.every, False
        if status in BLOCKED or (status is not None and b"cloudflare" in body.lower()[:2000]):
            retry = headers.get("retry-after", "")
            return max(PAUSE_BLOCKED, float(retry) if retry.isdigit() else 0.0), True
        self.fails += 1
        return min(60.0 * 2 ** self.fails, RETRY_MAX, self.every), False


class Archive:
    """State and decisions; `fetch(host, path)` -> (status, body bytes, headers with lower-case keys)."""

    def __init__(self, out, fetch, clock=time.time):
        self.out, self.fetch, self.clock = out, fetch, clock
        self.sources = [Source(k, *v) for k, v in SOURCES.items()]
        self.stats = self.fresh()
        self.beat = clock()
        os.makedirs(out, exist_ok=True)
        for s in self.sources:
            for path in sorted(glob.glob(os.path.join(out, f"{s.name}-*.jsonl")))[-2:]:
                with open(path, encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            s.known.add(json.loads(line)["id"])
                        except (ValueError, KeyError):
                            continue

    @staticmethod
    def fresh():
        return {"requests": {}, "errors": {}, "new": {}}

    def append(self, name, row):
        with open(os.path.join(self.out, name), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def bump(self, kind, key):
        self.stats[kind][key] = self.stats[kind].get(key, 0) + 1

    def pull(self, s):
        start = self.clock()
        self.bump("requests", s.name)
        try:
            status, body, headers = self.fetch(s.host, s.path)
        except Exception as exc:  # noqa: BLE001
            status, body, headers = None, b"", {}
            self.bump("errors", f"{s.name}:{type(exc).__name__}")
        t = int(self.clock() * 1000)
        if status not in (None, 200):
            self.bump("errors", f"{s.name}:{status}")
        if status == 200:
            try:
                rows = PARSE[s.name](body)
            except (ValueError, TypeError, KeyError):
                rows, status = [], 0                       # an unreadable answer is retried like an error
                self.bump("errors", f"{s.name}:parse")
            for key, item in rows:
                if key not in s.known:
                    s.known.add(key)
                    row = {"t": t, "id": key, "item": item} if s.name == "tree" else {"t": t, "id": key, **item}
                    self.append(f"{s.name}-{month(t)}.jsonl", row)
                    self.bump("new", s.name)
        wait, refused = s.wait_after(status if status != 0 else 500, body, headers)
        if refused:
            self.append("blocked.jsonl", {"t": t, "src": s.name, "status": status, "pause_s": wait})
        s.due = start + wait

    def tick(self):
        """Do what is due; -> seconds to sleep before the next call."""
        for s in self.sources:
            if self.clock() >= s.due:
                self.pull(s)
        now = self.clock()
        if now - self.beat >= BEAT_S:
            self.append("heartbeat.jsonl", {"t": int(now * 1000), **self.stats,
                                            "known": {s.name: len(s.known) for s in self.sources}})
            self.stats, self.beat = self.fresh(), now
        return max(1.0, min(min(s.due for s in self.sources), self.beat + BEAT_S) - self.clock())


def fetch(host, path):
    conn = http.client.HTTPSConnection(host, timeout=60, context=ssl.create_default_context())
    try:
        conn.request("GET", path, headers=HEADERS)
        r = conn.getresponse()
        body = r.read()
        if (r.getheader("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(body)
        return r.status, body, {k.lower(): v for k, v in r.getheaders()}
    finally:
        conn.close()


def main():
    a = Archive(OUT, fetch)
    while True:
        time.sleep(a.tick())


if __name__ == "__main__":
    main()
