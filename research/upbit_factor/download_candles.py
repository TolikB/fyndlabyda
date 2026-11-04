"""Daily candles of every Upbit KRW market from the official Open API (round 22, research only).

GET api.upbit.com/v1/candles/days at two requests a second, against a published limit of about ten
a second. A 429 backs off; a block ends the run. It never changes headers or addresses to get around
a refusal.

Markets: the live KRW list plus every ticker in an Upbit delisting notice, so that delisted coins
stay in the sample where the API still serves them. A market it does not serve is listed in
missing.json. Output: /w/data/upbit/days/KRW-<coin>.json, rows of [UTC date, close, KRW turnover].
Rerunning skips markets already on disk.
"""

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

OUT = "/w/data/upbit/days"
NOTICES = "/w/data/listing/upbit_notices.json"
API = "https://api.upbit.com/v1"
PACE = 0.5
PAGE = 200


def get(path, **params):
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            if e.code in (400, 404):
                return None
            raise SystemExit(f"stopped: HTTP {e.code} on {path}")
        except (TimeoutError, urllib.error.URLError):
            time.sleep(5 * (attempt + 1))
    raise SystemExit(f"stopped: {path} kept failing")


def markets():
    live = {m["market"] for m in get("/market/all") if m["market"].startswith("KRW-")}
    gone = set()
    for n in json.load(open(NOTICES, encoding="utf-8")):
        if "거래지원 종료" in n["title"] and "해제" not in n["title"]:
            gone |= {f"KRW-{t}" for t in re.findall(r"[A-Z][A-Z0-9]{1,9}", n["title"])}
    return sorted(live), sorted(gone - live)


def history(market):
    rows, to = [], None
    while True:
        params = {"market": market, "count": PAGE}
        if to:
            params["to"] = to
        page = get("/candles/days", **params)
        time.sleep(PACE)
        if not page:
            return rows
        rows += [[c["candle_date_time_utc"][:10], c["trade_price"], c["candle_acc_trade_price"]] for c in page]
        if len(page) < PAGE:
            return rows
        to = page[-1]["candle_date_time_utc"] + "Z"


def main():
    os.makedirs(OUT, exist_ok=True)
    live, gone = markets()
    print(f"{len(live)} live KRW markets, {len(gone)} tickers from delisting notices", flush=True)
    missing = []
    for i, market in enumerate(live + gone):
        path = f"{OUT}/{market}.json"
        if os.path.exists(path):
            continue
        rows = history(market)
        if rows:
            with open(path + ".tmp", "w") as fh:
                json.dump(sorted(rows), fh)
            os.replace(path + ".tmp", path)
        else:
            missing.append(market)
        if i % 25 == 0:
            print(i, market, len(rows), flush=True)
    json.dump(missing, open(f"{OUT}/missing.json", "w"))
    print(f"done, {len(missing)} markets not served", flush=True)


if __name__ == "__main__":
    main()
