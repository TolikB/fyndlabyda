"""Download Polymarket's resolved markets from its public Gamma API (research only, no keys, no orders).

Pages through closed markets with at least MIN_VOLUME of volume by scheduled end date, one calendar month at a time.
Offsets past about 2,000 are not served, so a window that reaches that cap is split in two. Prices are not fetched here.

Output: /w/data/polymarket/markets.parquet, one row per market: id, event, slug, question, start, end (scheduled),
closed (actual, all ms), token_yes, token_no, outcome_yes (final price of the first outcome), volume, neg_risk,
tick, fees (feesEnabled), yes_label, no_label.

  python download_markets.py
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

OUT = "/w/data/polymarket"
GAMMA = "https://gamma-api.polymarket.com/markets"
HEADERS = {"User-Agent": "research"}
PAGE = 500
PAUSE = 0.25
MIN_VOLUME = 50_000
OFFSET_CAP = 2000


def get(params, tries=6):
    url = f"{GAMMA}?{urllib.parse.urlencode(params)}"
    for attempt in range(tries):
        try:
            return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=60).read())
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 404, 422):
                return None
            time.sleep(5 + 10 * attempt)
        except Exception:  # noqa: BLE001
            time.sleep(5 + 10 * attempt)
    raise RuntimeError(f"gave up on {url}")


def stamp(x):
    if not x:
        return None
    t = pd.to_datetime(x, utc=True, errors="coerce", format="mixed")
    return None if pd.isna(t) else int(t.value // 1_000_000)


def row(m):
    """-> one flat record, or None when the market is not a resolved two-token market."""
    try:
        tokens = json.loads(m.get("clobTokenIds") or "[]")
        outcomes = json.loads(m.get("outcomes") or "[]")
        prices = json.loads(m.get("outcomePrices") or "[]")
    except ValueError:
        return None
    if len(tokens) != 2 or len(outcomes) != 2 or len(prices) != 2:
        return None
    start, end, closed = stamp(m.get("startDate")), stamp(m.get("endDate")), stamp(m.get("closedTime"))
    if None in (start, end, closed):
        return None
    y = float(prices[0])
    if abs(y + float(prices[1]) - 1) > 0.02:
        return None
    events = m.get("events") or [{}]
    return {"id": int(m["id"]), "event": str(events[0].get("id", m["id"])), "slug": m.get("slug") or "",
            "question": m.get("question") or "", "start": start, "end": end, "closed": closed, "token_yes": tokens[0],
            "token_no": tokens[1], "outcome_yes": y, "volume": float(m.get("volumeNum") or 0.0),
            "neg_risk": bool(m.get("negRisk")), "tick": float(m.get("orderPriceMinTickSize") or 0.0),
            "fees": bool(m.get("feesEnabled")), "yes_label": outcomes[0], "no_label": outcomes[1]}


def window_rows(lo, hi):
    """All liquid closed markets ending in [lo, hi); a window past the offset cap is split in two."""
    rows, offset = {}, 0
    while True:
        data = get({"closed": "true", "limit": PAGE, "offset": offset, "end_date_min": f"{lo:%Y-%m-%dT%H:%M:%SZ}",
                    "end_date_max": f"{hi:%Y-%m-%dT%H:%M:%SZ}", "volume_num_min": MIN_VOLUME, "order": "id",
                    "ascending": "true"})
        if not data:
            return rows, offset
        for m in data:
            r = row(m)
            if r:
                rows[r["id"]] = r
        offset += len(data)
        time.sleep(PAUSE)
        if offset >= OFFSET_CAP:
            mid = lo + (hi - lo) / 2
            left, a = window_rows(lo, mid)
            right, b = window_rows(mid, hi)
            return {**left, **right}, a + b


def main():
    os.makedirs(OUT, exist_ok=True)
    rows = {}
    for a in pd.date_range("2020-10-01", "2026-11-01", freq="MS"):
        b = a + pd.offsets.MonthBegin(1)
        got, seen = window_rows(a, b)
        rows.update(got)
        print(f"{a:%Y-%m} seen {seen} kept {len(got)} total {len(rows)}", flush=True)
    pd.DataFrame(rows.values()).to_parquet(f"{OUT}/markets.parquet", index=False, compression="zstd")


if __name__ == "__main__":
    main()
