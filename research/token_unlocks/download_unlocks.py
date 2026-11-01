"""Download DefiLlama's token unlock schedules (free datasets bucket) and its protocol list (research only).

Output in /w/data/unlocks/:
  protocols.json        id, name, symbol, gecko_id of every DefiLlama protocol
  list.json             the protocols that have an emissions file
  emissions/<p>.json    per protocol: name, pId, gecko_id, defillamaIds, metadata.events (cliff and linear
                        unlock events with timestamps and token counts), and the documented daily
                        cumulative unlocked series per allocation
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request

OUT = "/w/data/unlocks"
BUCKET = "https://defillama-datasets.llama.fi"


def get(url, tries=5):
    for attempt in range(tries):
        try:
            return json.loads(urllib.request.urlopen(url, timeout=90).read())
        except Exception:  # noqa: BLE001
            time.sleep(3 + 5 * attempt)
    return None


def main():
    os.makedirs(f"{OUT}/emissions", exist_ok=True)
    protocols = get("https://api.llama.fi/protocols")
    with open(f"{OUT}/protocols.json", "w") as fh:
        json.dump([{k: p.get(k) for k in ("id", "name", "symbol", "gecko_id", "parentProtocol")} for p in protocols], fh)
    names = get(f"{BUCKET}/emissionsProtocolsList")
    with open(f"{OUT}/list.json", "w") as fh:
        json.dump(names, fh)
    got = 0
    for name in names:
        path = f"{OUT}/emissions/{name}.json"
        if os.path.exists(path):
            got += 1
            continue
        d = get(f"{BUCKET}/emissions/{urllib.parse.quote(name)}")
        if not d:
            continue
        keep = {k: d.get(k) for k in ("name", "pId", "gecko_id", "defillamaIds", "categories")}
        keep["events"] = (d.get("metadata") or {}).get("events") or []
        keep["token"] = (d.get("metadata") or {}).get("token")
        keep["series"] = [{"label": s["label"], "data": [[x["timestamp"], x["unlocked"]] for x in s["data"]]}
                          for s in (d.get("documentedData") or {}).get("data", [])]
        with open(path + ".tmp", "w") as fh:
            json.dump(keep, fh)
        os.replace(path + ".tmp", path)
        got += 1
        time.sleep(0.3)
    print(f"{len(protocols)} protocols, {got}/{len(names)} emissions files")


if __name__ == "__main__":
    main()
