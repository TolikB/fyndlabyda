"""Download Hyperliquid portfolio histories of the leaderboard's $1M+ accounts (research only).

Round 13 asks whether the best Hyperliquid traders keep beating the rest (test_persist.py). This
script only fetches the data:
  leaderboard.json.gz      the leaderboard snapshot the sample is drawn from (kept for the record)
  portfolio/<address>.json.gz  the `portfolio` info reply: pnlHistory and accountValueHistory per window
Accounts: accountValue >= $1M in the snapshot. The ten largest are kept in the download too; the
test drops them, as pre-registered.

Pacing: one call every 4 s (15 a minute, weight 300 of the 1,200 per IP), so the hl-liqmap collector
(about 400) and the paper stacks keep their share. A 429 pauses 60 s. Resumable.
"""

from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.request

OUT = "/w/data/hl_portfolio"
INFO = "https://api.hyperliquid.xyz/info"
BOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
MIN_VALUE = 1e6
PAUSE = 4.0


def portfolio(addr, tries=6):
    body = json.dumps({"type": "portfolio", "user": addr}).encode()
    for attempt in range(tries):
        try:
            req = urllib.request.Request(INFO, data=body, headers={"Content-Type": "application/json"})
            return urllib.request.urlopen(req, timeout=60).read()
        except urllib.error.HTTPError as exc:
            time.sleep(60 if exc.code == 429 else 5 + 5 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(5 + 5 * attempt)
    raise RuntimeError(f"gave up on {addr}")


def main():
    os.makedirs(f"{OUT}/portfolio", exist_ok=True)
    board_path = f"{OUT}/leaderboard.json.gz"
    if not os.path.exists(board_path):
        raw = urllib.request.urlopen(BOARD, timeout=120).read()
        with gzip.open(board_path + ".tmp", "wb") as fh:
            fh.write(raw)
        os.replace(board_path + ".tmp", board_path)
    rows = json.load(gzip.open(board_path))["leaderboardRows"]
    addrs = [r["ethAddress"] for r in sorted(rows, key=lambda r: -float(r["accountValue"]))
             if float(r["accountValue"]) >= MIN_VALUE]
    print(f"{len(addrs)} accounts >= ${MIN_VALUE:,.0f}", flush=True)
    done = 0
    for n, addr in enumerate(addrs):
        path = f"{OUT}/portfolio/{addr}.json.gz"
        if os.path.exists(path):
            continue
        raw = portfolio(addr)
        with gzip.open(path + ".tmp", "wb") as fh:
            fh.write(raw)
        os.replace(path + ".tmp", path)
        done += 1
        if done % 100 == 0:
            print(f"  {n + 1}/{len(addrs)}", flush=True)
        time.sleep(PAUSE)
    print("done", flush=True)


if __name__ == "__main__":
    main()
