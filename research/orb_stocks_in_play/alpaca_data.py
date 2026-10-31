"""A small read-only Alpaca market-data client (stdlib only).

Keys come from ~/.alpaca/keys.env (APCA_API_KEY_ID, APCA_API_SECRET_KEY) and are only ever
sent as request headers to Alpaca. Nothing here prints them, and no trading endpoint is used
except the read-only asset list.
"""

from __future__ import annotations

import gzip
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

KEYS = os.path.expanduser("~/.alpaca/keys.env")
DATA_URL = "https://data.alpaca.markets"
TRADE_URL = "https://paper-api.alpaca.markets"


def load_keys(path=KEYS):
    vals = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip("\"'")
    kid, secret = vals.get("APCA_API_KEY_ID"), vals.get("APCA_API_SECRET_KEY")
    if not kid or not secret:
        raise SystemExit(f"{path}: set both APCA_API_KEY_ID and APCA_API_SECRET_KEY")
    return {"APCA-API-KEY-ID": kid, "APCA-API-SECRET-KEY": secret}


class Client:
    """200 historical calls a minute on the free plan; stay a little under it, across threads too."""

    def __init__(self, min_interval=0.32):
        self.headers = load_keys()
        self.min_interval = min_interval
        self.last = 0.0
        self.calls = 0
        self.lock = threading.Lock()

    def get(self, base, path, params):
        url = base + path + "?" + urllib.parse.urlencode(params)
        for attempt in range(8):
            with self.lock:
                wait = self.min_interval - (time.monotonic() - self.last)
                if wait > 0:
                    time.sleep(wait)
                self.last = time.monotonic()
                self.calls += 1
            req = urllib.request.Request(url, headers={**self.headers, "Accept": "application/json",
                                                       "Accept-Encoding": "gzip"})
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    body = r.read()
                    if r.headers.get("Content-Encoding") == "gzip":
                        body = gzip.decompress(body)
                    return json.loads(body)
            except urllib.error.HTTPError as e:
                if e.code == 429 or e.code >= 500:
                    time.sleep(min(60, 3 * 2 ** attempt))
                    continue
                detail = e.read()[:200].decode("utf-8", "replace")
                raise RuntimeError(f"Alpaca HTTP {e.code} on {path}: {detail}") from None
            except (urllib.error.URLError, OSError, TimeoutError):
                time.sleep(min(60, 3 * 2 ** attempt))
        raise RuntimeError(f"Alpaca: gave up on {path} after retries")

    def bars(self, symbols, timeframe, start, end, adjustment="raw"):
        """{symbol: [bar, ...]} over every page; bars are Alpaca's dicts (t, o, h, l, c, v, ...)."""
        params = {"symbols": ",".join(symbols), "timeframe": timeframe, "start": start, "end": end,
                  "limit": 10000, "adjustment": adjustment, "feed": "sip", "sort": "asc"}
        out = defaultdict(list)
        while True:
            doc = self.get(DATA_URL, "/v2/stocks/bars", params)
            for sym, rows in (doc.get("bars") or {}).items():
                out[sym].extend(rows)
            token = doc.get("next_page_token")
            if not token:
                return dict(out)
            params["page_token"] = token

    def assets(self, status):
        return self.get(TRADE_URL, "/v2/assets", {"status": status, "asset_class": "us_equity"})
