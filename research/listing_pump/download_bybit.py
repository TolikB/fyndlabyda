"""Download Bybit's "New Listings" announcements from its official v5 API (research only).

Output: /w/data/listing/bybit_new.json, a list of {title, tags, url, dateTimestamp, publishTime} (ms).
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

OUT = "/w/data/listing"
LIST = "https://api.bybit.com/v5/announcements/index?locale=en-US&type=new_crypto&limit=20&page={page}"


def main():
    os.makedirs(OUT, exist_ok=True)
    rows, page = {}, 1
    while True:
        r = json.loads(urllib.request.urlopen(LIST.format(page=page), timeout=30).read())["result"]
        if not r["list"]:
            break
        for a in r["list"]:
            rows[a["url"]] = {k: a.get(k) for k in ("title", "tags", "url", "dateTimestamp", "publishTime")}
        page += 1
        time.sleep(1.0)
    # a few old announcements have no publishTime; their dateTimestamp stands in
    for a in rows.values():
        a["publishTime"] = a["publishTime"] or a["dateTimestamp"]
    out = sorted(rows.values(), key=lambda a: a["publishTime"])
    with open(f"{OUT}/bybit_new.json", "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=0)
    print(f"{len(out)} announcements, pages {page - 1}")


if __name__ == "__main__":
    main()
