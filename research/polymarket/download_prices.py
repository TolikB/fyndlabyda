"""Download hourly price histories of resolved Polymarket markets from the public CLOB API (research only).

One request per market, about 8 a second, with a pause on any 429. Only markets of bt_bonds.universe that can give a
trade in a test period are fetched.

Each request names its own time range (windows()). The first download used interval=max, which returns only the last
31 days at hourly fidelity, so development had no prices. A range of up to 15 days works on every quarter since
2024; 21 days is refused.

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

URL = "https://clob.polymarket.com/prices-history?market={token}&startTs={a}&endTs={z}&fidelity=60"
HEADERS = {"User-Agent": "research"}
OUT = f"{b.DATA}/px"
WORKERS = 4
PACE = 0.45                 # seconds between one worker's calls
LEAD = max(b.KS.values()) * b.DAY + 3 * b.HOUR          # before the scheduled end: covers the oldest price an entry may use
TAIL = 7 * b.DAY                                        # after it, for the daily marks; later marks keep the last price
pause_until = [0.0]
lock = threading.Lock()


def windows(u):
    """u: markets with times in ms. -> rows (id, token, from, to) for the ones that can enter inside a test period."""
    s, e, c = u["start"] // 1000, u["end"] // 1000, u["closed"] // 1000
    oldest = e - max(b.KS.values()) * b.DAY
    keep = (c > oldest) & (e - min(b.KS.values()) * b.DAY >= b.sec(b.PERIODS["dev"][0]))         & (oldest < b.sec(b.PERIODS["holdout"][1]))
    a = pd.concat([s, e - LEAD], axis=1).max(axis=1)
    z = pd.concat([c + 60, e + TAIL], axis=1).min(axis=1)
    return list(zip(u["id"][keep], u["token_yes"][keep], a[keep], z[keep]))


def fetch(token, a, z, tries=6):
    for attempt in range(tries):
        wait = pause_until[0] - time.time()
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(URL.format(token=token, a=int(a), z=int(z)), headers=HEADERS)
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
    time.sleep(PACE)
    return row[0], fetch(*row[1:])


def main():
    os.makedirs(OUT, exist_ok=True)
    u = b.universe(pd.read_parquet(f"{b.DATA}/markets.parquet")).sort_values("id")
    rows = windows(u)
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
