"""Download Hyperliquid vault histories for round 35 (research only, public data, no keys).

The list comes from stats-data.hyperliquid.xyz, and one `vaultDetails` call per candidate fetches its portfolio, one call every 3 s, so
that the other collectors on this host keep their share of the API. A 429 pauses 60 s. Resumable.

Output: /w/data/hl_vaults/summaries.json.gz, candidates.json, details/<address>.json.gz. `followers` is dropped from each reply: it names
other people's accounts and the test does not use it.

  python download_vaults.py
"""

from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.request

OUT = "/w/data/hl_vaults"
LIST = "https://stats-data.hyperliquid.xyz/Mainnet/vaults"
API = "https://api.hyperliquid.xyz/info"
PAUSE = 3.0
MIN_PNL = 1_000.0
MIN_TVL = 10_000.0


def fetch_list():
    req = urllib.request.Request(LIST, headers={"User-Agent": "research", "Accept-Encoding": "gzip"})
    r = urllib.request.urlopen(req, timeout=120)
    raw = r.read()
    return json.loads(gzip.decompress(raw) if r.headers.get("Content-Encoding") == "gzip" else raw)


def is_candidate(x):
    """x: one row of the list. Parent and child vaults are the protocol's own."""
    s = x["summary"]
    if (s.get("relationship") or {}).get("type") != "normal":
        return False
    if float(s.get("tvl") or 0) >= MIN_TVL:
        return True
    return any(abs(float(p)) >= MIN_PNL for _, series in x.get("pnls") or [] for p in series)


def details(addr, tries=6):
    body = json.dumps({"type": "vaultDetails", "vaultAddress": addr}).encode()
    for attempt in range(tries):
        try:
            req = urllib.request.Request(API, data=body, headers={"Content-Type": "application/json"})
            return json.loads(urllib.request.urlopen(req, timeout=60).read())
        except urllib.error.HTTPError as exc:
            time.sleep(60 if exc.code == 429 else 5 + 5 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(5 + 5 * attempt)
    return None


def main():
    os.makedirs(f"{OUT}/details", exist_ok=True)
    rows = fetch_list()
    with gzip.open(f"{OUT}/summaries.json.gz", "wt") as fh:
        json.dump([x["summary"] for x in rows], fh)
    want = [x["summary"]["vaultAddress"] for x in rows if is_candidate(x)]
    with open(f"{OUT}/candidates.json", "w") as fh:
        json.dump(want, fh)
    print(f"{len(rows)} vaults, {len(want)} candidates", flush=True)
    for n, addr in enumerate(want):
        path = f"{OUT}/details/{addr}.json.gz"
        if os.path.exists(path):
            continue
        d = details(addr)
        time.sleep(PAUSE)
        if d is None:
            continue
        d.pop("followers", None)
        d.pop("followerState", None)
        with gzip.open(path + ".tmp", "wt") as fh:
            json.dump(d, fh)
        os.replace(path + ".tmp", path)
        if n % 100 == 0:
            print(n, flush=True)
    print("done", flush=True)


if __name__ == "__main__":
    main()
