"""Download Binance's "New Cryptocurrency Listing" announcements (CMS catalog 48) with release times (research only).

Output: /w/data/listing/binance_listings.json, a list of {id, code, title, releaseDate (ms)}.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

OUT = "/w/data/listing"
LIST = ("https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
        "?type=1&catalogId=48&pageSize=50&pageNo={page}")


def get(page, tries=5):
    for attempt in range(tries):
        try:
            return json.loads(urllib.request.urlopen(LIST.format(page=page), timeout=60).read())["data"]
        except Exception:  # noqa: BLE001
            time.sleep(3 + 5 * attempt)
    raise RuntimeError(f"gave up on page {page}")


def main():
    os.makedirs(OUT, exist_ok=True)
    arts, page, total = {}, 1, None
    while total is None or len(arts) < total:
        cat = get(page)["catalogs"][0]
        total = cat["total"]
        if not cat["articles"]:
            break
        for a in cat["articles"]:
            arts[a["id"]] = {k: a[k] for k in ("id", "code", "title", "releaseDate")}
        page += 1
        time.sleep(1.0)
    rows = sorted(arts.values(), key=lambda a: a["releaseDate"])
    with open(f"{OUT}/binance_listings.json", "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=0)
    print(f"{len(rows)} announcements of {total}")


if __name__ == "__main__":
    main()
