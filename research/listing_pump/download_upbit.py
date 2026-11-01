"""Download Upbit's "trade" notices with their post times (research only, standard library only).

Upbit's API answers Cloudflare error 1015 to the research VM, so this runs on the workstation and the
file is copied to the VM afterwards.

Output: <out>/upbit_notices.json, a list of {id, title, listed_at, first_listed_at} (KST ISO strings).

  python download_upbit.py <out_dir>
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

LIST = "https://api-manager.upbit.com/api/v1/announcements?os=web&page={page}&per_page=20&category=trade"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def get(page, tries=8):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(LIST.format(page=page), headers=HEADERS)
            return json.loads(urllib.request.urlopen(req, timeout=60).read())["data"]
        except Exception:  # noqa: BLE001
            time.sleep(5 + 10 * attempt)
    raise RuntimeError(f"gave up on page {page}")


def main():
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    rows, page, pages = {}, 1, None
    while pages is None or page <= pages:
        data = get(page)
        pages = data["total_pages"]
        for n in data["notices"]:
            rows[n["id"]] = {k: n[k] for k in ("id", "title", "listed_at", "first_listed_at")}
        page += 1
        time.sleep(2.5)
    rows = sorted(rows.values(), key=lambda r: r["id"])
    with open(f"{out}/upbit_notices.json", "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=0)
    print(f"{len(rows)} notices, {rows[0]['first_listed_at']} .. {rows[-1]['first_listed_at']}")


if __name__ == "__main__":
    main()
