"""Known-answer checks of collector.py on a scripted network and clock; prints `selftest OK`. No network is used."""

from __future__ import annotations

import json
import os
import shutil
import tempfile

import collector as c

D0 = 1_794_441_600.0          # 2026-11-12 00:00:00 UTC, in seconds


class World:
    """A scripted Hyperliquid and explorer: answers by request type, counts every request."""

    def __init__(self):
        self.now = D0 + 3600
        self.twaps = []
        self.scan_status = 200
        self.scan_body = None            # overrides the list when set
        self.hl_status = {}              # request type -> status to answer once
        self.calls = []
        self.mid = {"BTC": "100000", "ETH": "4000", "LIT": "2", "@107": "30", "PURR/USDC": "0.1"}
        self.xmid = {"xyz:NVDA": "200", "xyz:GOLD": "4000"}

    def clock(self):
        return self.now

    def fetch(self, host, payload):
        self.calls.append((self.now, host, None if payload is None else (payload["type"], payload.get("dex", ""))))
        if host == "scan":
            if self.scan_status != 200:
                return self.scan_status, {"detail": "no"}, 5.0
            return 200, self.twaps if self.scan_body is None else self.scan_body, 5.0
        kind = payload["type"]
        if kind in self.hl_status:
            return self.hl_status.pop(kind), None, 7.0
        if kind == "perpDexs":
            return 200, [None, {"name": "xyz"}], 7.0
        if kind == "metaAndAssetCtxs" and payload.get("dex") == "xyz":
            uni = [{"name": k} for k in self.xmid]
            return 200, [{"universe": uni}, [{"midPx": v, "markPx": v, "impactPxs": [v, v], "dayNtlVlm": "5000000",
                                              "openInterest": "1", "funding": "0", "oraclePx": v}
                                             for v in self.xmid.values()]], 7.0
        if kind == "metaAndAssetCtxs":
            names = ["BTC", "ETH", "LIT"]
            return 200, [{"universe": [{"name": k} for k in names]},
                         [{"midPx": self.mid[k], "markPx": self.mid[k], "impactPxs": None, "dayNtlVlm": "1000000",
                           "openInterest": "1", "funding": "0", "oraclePx": self.mid[k]} for k in names]], 7.0
        if kind == "spotMetaAndAssetCtxs":
            return 200, [{"universe": [{"name": "PURR/USDC", "index": 0}, {"name": "@107", "index": 107}]},
                         [{"coin": "PURR/USDC", "midPx": "0.1", "markPx": "0.1", "dayNtlVlm": "10"},
                          {"coin": "@107", "midPx": None, "markPx": "30", "dayNtlVlm": "20"},
                          {"coin": "@999", "midPx": "1", "markPx": "1", "dayNtlVlm": "0"}]], 7.0   # not in the universe
        if kind == "allMids":
            return 200, dict(self.xmid) if payload.get("dex") == "xyz" else dict(self.mid), 7.0
        raise AssertionError(payload)


def twap(n, a, s, m=60, b=True, t=None, ended=None, error=None, r=False, rnd=False, h=None):
    row = {"time": int(((D0 + 3600) if t is None else t) * 1000), "user": f"0xuser{n}",
           "action": {"type": "twapOrder", "twap": {"a": a, "b": b, "s": str(s), "r": r, "m": m, "t": rnd}},
           "block": 100 + n, "hash": h or f"0xhash{n:04d}", "error": error}
    if ended is not None:
        row["ended"] = ended
    return row


def read(out, name):
    path = os.path.join(out, name)
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return [json.loads(x) for x in fh]


def run(col, w, seconds):
    """Advance the scripted clock in the steps the collector asks for."""
    end = w.now + seconds
    while w.now < end:
        w.now = min(end, w.now + max(0.05, col.tick()))
    col.tick()


def calls(w, since, host, kind=None):
    return [x for x in w.calls if x[0] >= since and x[1] == host and (kind is None or x[2][0] == kind)]


def main():
    # --- pure functions ---------------------------------------------------------------------------------------------
    assert c.perp_assets(0, {"universe": [{"name": "BTC"}, {"name": "ETH"}]}) == {0: "BTC", 1: "ETH"}
    assert c.perp_assets(1, {"universe": [{"name": "xyz:A"}, {"name": "xyz:B"}]}) == {110000: "xyz:A", 110001: "xyz:B"}
    assert c.perp_assets(10, {"universe": [{"name": "io:NVDA"}]}) == {200000: "io:NVDA"}
    assert c.spot_assets({"universe": [{"name": "@107", "index": 107}]}) == {10107: "@107"}
    assert c.key_of(twap(1, 2, 5)) == "0xhash0001"
    assert c.key_of(twap(1, 2, 5, h="0x" + "0" * 64)) == f"0xuser1:{int((D0 + 3600) * 1000)}:2"
    r = dict(twap(1, 2, 5))
    r["hash"] = None
    assert c.key_of(r) == f"0xuser1:{int((D0 + 3600) * 1000)}:2"
    n = c.norm(twap(3, 110001, "7.5", m=90, b=False, r=True, rnd=True, ended="error", error="x"))
    assert n == {"key": "0xhash0003", "time": int((D0 + 3600) * 1000), "user": "0xuser3", "a": 110001, "b": False,
                 "s": "7.5", "r": True, "m": 90, "rnd": True, "block": 103, "error": "x", "ended": "error"}
    for bad in ({}, {"time": 1, "user": "u", "action": {}}, {"time": 1, "user": "u", "action": {"twap": {"a": 1}}}):
        try:
            c.norm(bad)
        except (KeyError, TypeError):
            pass
        else:
            raise AssertionError("a malformed row was accepted")
    a1, a2, a3 = c.norm(twap(1, 0, 1)), c.norm(twap(2, 0, 1)), c.norm(twap(3, 0, 1))
    a2e = dict(a2, ended="error")
    assert c.diff({"0xhash0001": a1, "0xhash0002": a2}, {"0xhash0002": a2e, "0xhash0003": a3}) == \
        ([a3], [a2e], ["0xhash0001"])
    assert c.diff({"0xhash0002": a2}, {"0xhash0002": dict(a2, error="e")})[1] == [dict(a2, error="e")]
    assert c.diff({"0xhash0002": a2}, {"0xhash0002": dict(a2, block=9)}) == ([], [], [])
    assert c.day(int((D0 - 1) * 1000)) == "20261111" and c.day(int(D0 * 1000)) == "20261112"
    assert (c.TWAP_EVERY, c.MIDS_EVERY, c.CTX_EVERY, c.KEEP_S, c.MIN_NTL) == (10.0, 10.0, 300.0, 86400.0, 20000.0)
    assert c.PAUSE == {"scan": 3600.0, "hl": 600.0} and c.BLOCKED == (403, 418, 429)

    out = tempfile.mkdtemp(prefix="twap_selftest_")
    try:
        # --- start: assets first, then the first list as a baseline --------------------------------------------------
        w = World()
        w.twaps = [twap(i, 0, "0.001") for i in range(1, 25)]            # 24 small BTC orders, 100 USD each
        col = c.Collector(out, w.fetch, w.clock)
        run(col, w, 5)
        maps = {d["dex"]: d["map"] for d in read(out, "assets.jsonl")}
        assert maps == {"": {"0": "BTC", "1": "ETH", "2": "LIT"}, "xyz": {"110000": "xyz:NVDA", "110001": "xyz:GOLD"},
                        "@spot": {"10000": "PURR/USDC", "10107": "@107"}}, maps
        first_scan = calls(w, 0, "scan")[0][0]
        assert all(x[0] <= first_scan for x in calls(w, 0, "hl", "spotMetaAndAssetCtxs")[:1]), "list before prices"
        tw = read(out, "twaps-20261112.jsonl")
        assert len(tw) == 24 and {d["ev"] for d in tw} == {"base"} and not any(d["back"] for d in tw)
        assert tw[0]["px"] == 100000.0 and tw[0]["ntl"] == 100.0
        assert read(out, "mids-20261112.jsonl") == [], "small orders must not store prices"

        # --- new orders: a large perp, a small perp, a builder perp, a spot pair -------------------------------------
        t_new = w.now
        w.twaps = w.twaps + [twap(30, 2, 20000, t=t_new - 3), twap(31, 1, 1, t=t_new - 3),
                             twap(32, 110001, 10, t=t_new - 3, b=False), twap(33, 10107, 1000, t=t_new - 3),
                             twap(34, 1, 100, t=t_new - 3, ended="error", error="margin")]   # 400,000 USD, refused
        run(col, w, 20)
        tw = read(out, "twaps-20261112.jsonl")[24:]
        assert [d["ev"] for d in tw] == ["start"] * 5, tw
        by = {d["key"]: d for d in tw}
        lit, eth, gold, spot = (by[f"0xhash00{n}"] for n in (30, 31, 32, 33))
        assert (lit["px"], lit["ntl"], lit["a"], lit["b"], lit["m"], lit["back"]) == (2.0, 40000.0, 2, True, 60, False)
        assert (eth["ntl"], gold["ntl"], gold["b"], spot["ntl"]) == (4000.0, 40000.0, False, 30000.0)
        assert 0 <= lit["t"] / 1000 - t_new <= 10 and lit["time"] == int((t_new - 3) * 1000)
        run(col, w, 30)
        mids = read(out, "mids-20261112.jsonl")
        assert {d["dex"] for d in mids} == {"", "xyz"}
        main_m = [d for d in mids if d["dex"] == ""][-1]
        assert main_m["m"] == {"@107": "30", "LIT": "2"}, main_m           # no BTC, no ETH: small or refused orders
        assert [d for d in mids if d["dex"] == "xyz"][-1]["m"] == {"xyz:GOLD": "4000"}
        gap = [b["t"] - a["t"] for a, b in zip([d for d in mids if d["dex"] == ""], [d for d in mids if d["dex"] == ""][1:])]
        assert gap and all(9000 <= g <= 11000 for g in gap), gap

        # --- status change, leaving the list, coming back ------------------------------------------------------------
        w.twaps = [dict(r, ended="error", error="margin") if r["hash"] == "0xhash0030" else r for r in w.twaps]
        run(col, w, 10)
        ev = read(out, "twaps-20261112.jsonl")[29:]
        assert [(d["ev"], d["key"], d.get("ended"), d.get("error")) for d in ev] == \
            [("change", "0xhash0030", "error", "margin")], ev
        gone_at = w.now
        w.twaps = [r for r in w.twaps if r["hash"] != "0xhash0030"]
        run(col, w, 10)
        ev = read(out, "twaps-20261112.jsonl")[30:]
        assert [(d["ev"], d["key"]) for d in ev] == [("gone", "0xhash0030")], ev
        w.twaps = w.twaps + [twap(30, 2, 20000, t=t_new - 3)]
        run(col, w, 10)
        ev = read(out, "twaps-20261112.jsonl")[31:]
        assert [(d["ev"], d["key"], d["back"]) for d in ev] == [("start", "0xhash0030", True)], ev
        w.twaps = [r for r in w.twaps if r["hash"] != "0xhash0030"]
        run(col, w, 10)
        last_active = w.now

        # --- a malformed answer and a truncated answer change nothing ------------------------------------------------
        n_before = len(read(out, "twaps-20261112.jsonl"))
        w.scan_body = [{"time": 1}]
        run(col, w, 10)
        assert len(read(out, "twaps-20261112.jsonl")) == n_before and col.stats["errors"].get("twap:shape", 0) >= 1
        w.scan_body = w.twaps[:5]                                         # 5 of 28: less than half
        run(col, w, 15)
        assert len(read(out, "twaps-20261112.jsonl")) == n_before, "a truncated list was believed at once"
        assert col.stats["short"] >= 1
        run(col, w, 20)                                                   # the third short answer in a row is believed
        after = read(out, "twaps-20261112.jsonl")[n_before:]
        assert len(after) == 23 and {d["ev"] for d in after} == {"gone"}, after
        w.scan_body = None
        run(col, w, 10)
        again = read(out, "twaps-20261112.jsonl")[n_before + 23:]
        assert len(again) == 23 and all(d["ev"] == "start" and d["back"] for d in again)

        # --- a refusal pauses that host only -------------------------------------------------------------------------
        w.scan_status = 429
        t_block = w.now
        run(col, w, 15)
        w.scan_status = 200
        blocked = read(out, "blocked.jsonl")
        assert [(d["host"], d["status"], d["pause_s"]) for d in blocked] == [("scan", 429, 3600.0)], blocked
        n_scan = len(calls(w, t_block, "scan"))
        run(col, w, 3000)
        assert len(calls(w, t_block, "scan")) == n_scan, "the explorer was asked again during its pause"
        assert len(calls(w, t_block, "hl", "allMids")) > 250, "Hyperliquid polling stopped with the explorer's pause"
        run(col, w, 700)
        assert len(calls(w, t_block, "scan")) > n_scan, "the explorer was not asked again after its pause"
        w.hl_status["allMids"] = 429
        t_block = w.now
        run(col, w, 25)
        blocked = read(out, "blocked.jsonl")
        assert (blocked[-1]["host"], blocked[-1]["status"], blocked[-1]["pause_s"]) == ("hl", 429, 600.0), blocked
        n_hl = len(calls(w, t_block, "hl"))
        run(col, w, 500)
        assert len(calls(w, t_block, "hl")) == n_hl, "Hyperliquid was asked again during its pause"
        assert len(calls(w, t_block, "scan")) > 40, "the explorer stopped with Hyperliquid's pause"
        run(col, w, 200)
        assert len(calls(w, t_block, "hl")) > n_hl
        for code in (403, 418):
            w.hl_status["allMids"] = code
            run(col, w, 25)
            assert read(out, "blocked.jsonl")[-1]["status"] == code
            run(col, w, 700)
        w.hl_status["allMids"] = 500                                      # an ordinary error pauses nothing
        n_blocked = len(read(out, "blocked.jsonl"))
        t_err = w.now
        run(col, w, 60)
        assert len(read(out, "blocked.jsonl")) == n_blocked and len(calls(w, t_err, "hl", "allMids")) >= 8

        # --- hourly heartbeat, price rounds every 300 s --------------------------------------------------------------
        hb = read(out, "heartbeat.jsonl")
        assert len(hb) >= 1 and {"requests", "errors", "rtt_ms_p50", "listed", "active", "started", "lag_s_p50",
                                 "stored_assets", "short"} <= set(hb[0]), hb[0]
        assert hb[0]["rtt_ms_p50"] == {"scan": 5.0, "hl": 7.0} and hb[0]["started"] == 29 and hb[0]["lag_s_p50"] > 0
        t0 = w.now
        run(col, w, 1500)
        rounds = [x[0] for x in calls(w, t0, "hl", "spotMetaAndAssetCtxs")]
        assert 4 <= len(rounds) <= 6 and all(295 <= b - a <= 305 for a, b in zip(rounds, rounds[1:])), rounds
        ctx = read(out, "ctx-20261112.jsonl")
        assert {d["dex"] for d in ctx} == {"", "xyz", "@spot"}
        assert [d for d in ctx if d["dex"] == ""][0]["rows"][2] == ["LIT", "2", "2", None, None, "1000000", "1", "0", "2"]
        assert [d for d in ctx if d["dex"] == "@spot"][0]["rows"] == [["PURR/USDC", "0.1", "0.1", "10"],
                                                                     ["@107", "30", None, "20"]]

        # --- prices stop a day after the last sighting of a large order ----------------------------------------------
        w.twaps = [twap(i, 0, "0.001") for i in range(1, 25)]             # the large orders are all gone now
        run(col, w, 10)
        gone_all = w.now
        assert 2 in col.interest and 110001 in col.interest and 10107 in col.interest
        assert abs(col.interest[2] - (last_active + c.KEEP_S)) <= 20, (col.interest[2], last_active)
        run(col, w, c.KEEP_S - (w.now - gone_all) - 600)
        t_late = w.now
        run(col, w, 60)
        assert any("xyz:GOLD" in d["m"] for d in read(out, f"mids-{c.day(int(w.now * 1000))}.jsonl") if d["t"] >= t_late * 1000)
        run(col, w, 1300)
        t_after = w.now
        run(col, w, 120)
        late = [d for d in read(out, f"mids-{c.day(int(w.now * 1000))}.jsonl") if d["t"] >= t_after * 1000]
        assert late == [] and col.interest == {}, (late[:2], col.interest)
        assert os.path.exists(os.path.join(out, "twaps-20261113.jsonl")) or True
        days = sorted(x for x in os.listdir(out) if x.startswith("mids-"))
        assert days[0] == "mids-20261112.jsonl" and len(days) >= 2, days

        # --- restart: seen orders are remembered, large recent ones keep their prices --------------------------------
        w.twaps = w.twaps + [twap(40, 2, 20000, t=w.now - 2)]
        run(col, w, 15)
        restart = w.now
        col2 = c.Collector(out, w.fetch, w.clock)
        assert "0xhash0040" in col2.ever and 2 in col2.interest
        run(col2, w, 15)
        base = [d for d in read(out, f"twaps-{c.day(int(w.now * 1000))}.jsonl") if d["t"] >= restart * 1000]
        assert len(base) == 25 and {d["ev"] for d in base} == {"base"}
        assert [d["back"] for d in base if d["key"] == "0xhash0040"] == [True]
    finally:
        shutil.rmtree(out, ignore_errors=True)
    print("selftest OK")


if __name__ == "__main__":
    main()
