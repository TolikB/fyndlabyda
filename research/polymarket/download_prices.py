"""Download hourly price histories of resolved Polymarket markets from the public CLOB API (research only).

One request per market, about 8 a second, with a pause on any 429. Only markets bt_bonds.universe keeps are fetched.

Output: /w/data/polymarket/px/<chunk>.parquet, 1,000 markets a chunk, columns m (market id), t (s), p (price of the
first outcome, float32). Resumable by chunk. Markets with no history are listed in px/empty_<chunk>.json.

  python download_prices.py
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import os
import threading
import time
import urllib.error
import urllib.request

import pandas as pd

import bt_bonds as b

URL = "https://clob.polymarket.com/prices-history?market={token}&interval=max&fidelity=60"
HEADERS = {"User-Agent": "research"}
OUT = f"{b.DATA}/px"
WORKERS = 4
PACE = 0.45                 # seconds between one worker's calls
pause_until = [0.0]
lock = threading.Lock()


def fetch(token, tries=6):
    for attempt in range(tries):
        wait = pause_until[0] - time.time()
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(URL.format(token=token), headers=HEADERS)
            return json.loads(urllib.request.urlopen(req, timeout=60).read()).get("history") or []
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                with lock:
                    pause_until[0] = time.time() + 30
            elif exc.code in (400, 404):
                return []
            else:
                time.sleep(3 + 5 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(3 + 5 * attempt)
    return None


def one(row):
    m, token = row
    time.sleep(PACE)
    return m, fetch(token)


def main():
    os.makedirs(OUT, exist_ok=True)
    u = b.universe(pd.read_parquet(f"{b.DATA}/markets.parquet")).sort_values("id")
    rows = list(zip(u["id"], u["token_yes"]))
    print(f"{len(rows)} markets", flush=True)
    for c in range(0, len(rows), 1000):
        path = f"{OUT}/{c // 1000:04d}.parquet"
        if os.path.exists(path):
            continue
        frames, empty = [], []
        with cf.ThreadPoolExecutor(WORKERS) as pool:
            for m, hist in pool.map(one, rows[c:c + 1000]):
                if hist:
                    frames.append(pd.DataFrame({"m": m, "t": [x["t"] for x in hist], "p": [x["p"] for x in hist]}))
                else:
                    empty.append({"m": int(m), "failed": hist is None})
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame({"m": [], "t": [], "p": []})
        df["p"] = df["p"].astype("float32")
        df.to_parquet(path + ".tmp", index=False, compression="zstd")
        os.replace(path + ".tmp", path)
        with open(f"{OUT}/empty_{c // 1000:04d}.json", "w") as fh:
            json.dump(empty, fh)
        print(f"{c + 1000}/{len(rows)} empty {len(empty)}", flush=True)


if __name__ == "__main__":
    main()
