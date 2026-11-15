"""Known-answer checks of archive.py on a scripted network and clock; prints `selftest OK`. No network is used."""

from __future__ import annotations

import json
import os
import shutil
import tempfile

import archive as a

T0 = 1_794_441_600.0          # 2026-11-12 00:00:00 UTC


def rss(items):
    body = "".join(f"<item><title>{t}</title><pubDate>{p}</pubDate><link>{u}</link>{g}</item>" for t, p, u, g in items)
    return ("<?xml version='1.0'?><rss><channel><title>feed</title>" + body + "</channel></rss>").encode()


class World:
    def __init__(self):
        self.now = T0
        self.tree = [{"_id": "2", "time": 2000, "title": "b", "icon": "x", "image": "y", "source": "Upbit", "kind": "listing"},
                     {"_id": "1", "time": 1000, "title": "a", "source": "Blogs"}, {"time": 5, "title": "no id"}, "junk"]
        self.bwe = [("UPBIT LISTING: A &amp; B", "Thu, 05 Nov 2026 06:03:05 +0000", "https://x/1", "<guid>g1</guid>"),
                    ("<![CDATA[Second <b>]]>", "Thu, 05 Nov 2026 07:00:00 +0000", "https://x/2", "")]
        self.status = {}              # source host -> status to answer (default 200)
        self.raw = {}                 # source host -> body override
        self.headers = {}
        self.calls = []

    def clock(self):
        return self.now

    def fetch(self, host, path):
        self.calls.append((self.now, host, path))
        status = self.status.get(host, 200)
        if host in self.raw:
            return status, self.raw[host], self.headers.get(host, {})
        if status != 200:
            return status, b"no", self.headers.get(host, {})
        body = json.dumps(self.tree).encode() if host.startswith("news.") else rss(self.bwe)
        return 200, body, {}


def read(out, name):
    path = os.path.join(out, name)
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(x) for x in fh]


def run(arch, w, seconds):
    end = w.now + seconds
    while w.now < end:
        w.now = min(end, w.now + arch.tick())
    arch.tick()


def n(w, since, host):
    return sum(1 for t, h, _ in w.calls if t >= since and h == host)


TREE, BWE = "news.treeofalpha.com", "rss-public.bwe-ws.com"


def main():
    # --- parsers ---------------------------------------------------------------------------------------------------
    w = World()
    rows = a.parse_tree(json.dumps(w.tree))
    assert rows == [("1", {"_id": "1", "time": 1000, "title": "a", "source": "Blogs"}),
                    ("2", {"_id": "2", "time": 2000, "title": "b", "source": "Upbit", "kind": "listing"})], rows
    for bad in ("{}", '"x"', "not json"):
        try:
            a.parse_tree(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("a non-list history was accepted")
    rows = a.parse_bwe(rss(w.bwe))
    assert rows == [("g1", {"title": "UPBIT LISTING: A & B", "pub": "Thu, 05 Nov 2026 06:03:05 +0000", "link": "https://x/1"}),
                    ("https://x/2", {"title": "Second <b>", "pub": "Thu, 05 Nov 2026 07:00:00 +0000", "link": "https://x/2"})], rows
    assert a.parse_bwe(b"<rss><channel><item><title>only</title><pubDate>P</pubDate></item><item></item></channel></rss>") == \
        [("P|only", {"title": "only", "pub": "P", "link": None})]
    try:
        a.parse_bwe(b"<html>cloud</html>")
    except ValueError:
        pass
    else:
        raise AssertionError("a page without a channel was accepted")
    assert a.month(int((T0 - 1) * 1000)) == "202611" and a.month(int((T0 + 20 * 86400) * 1000)) == "202612"
    assert a.SOURCES == {"tree": (TREE, "/api/news?limit=3000", 43200.0), "bwe": (BWE, "/", 900.0)}
    assert (a.PAUSE_BLOCKED, a.RETRY_MAX, a.BLOCKED, a.DROP) == (86400.0, 3600.0, (403, 418, 429), ("icon", "image"))

    out = tempfile.mkdtemp(prefix="news_selftest_")
    try:
        # --- first pull, then only what is new --------------------------------------------------------------------
        arch = a.Archive(out, w.fetch, w.clock)
        arch.tick()
        tree, bwe = read(out, "tree-202611.jsonl"), read(out, "bwe-202611.jsonl")
        assert [r["id"] for r in tree] == ["1", "2"] and tree[1]["item"]["kind"] == "listing" and "icon" not in tree[1]["item"]
        assert tree[0]["t"] == int(T0 * 1000)
        assert [(r["id"], r["title"], r["pub"], r["link"]) for r in bwe] == \
            [("g1", "UPBIT LISTING: A & B", "Thu, 05 Nov 2026 06:03:05 +0000", "https://x/1"),
             ("https://x/2", "Second <b>", "Thu, 05 Nov 2026 07:00:00 +0000", "https://x/2")]
        w.tree.append({"_id": "3", "time": 3000, "title": "c"})
        w.bwe.append(("Third", "Thu, 05 Nov 2026 08:00:00 +0000", "https://x/3", "<guid>g3</guid>"))
        run(arch, w, 86400)
        assert [r["id"] for r in read(out, "tree-202611.jsonl")] == ["1", "2", "3"]
        assert [r["id"] for r in read(out, "bwe-202611.jsonl")] == ["g1", "https://x/2", "g3"]
        assert n(w, 0, TREE) == 3 and n(w, 0, BWE) == 97, (n(w, 0, TREE), n(w, 0, BWE))     # 12 h and 15 min apart
        hb = read(out, "heartbeat.jsonl")
        assert len(hb) == 1 and hb[0]["requests"] == {"tree": 3, "bwe": 97} and hb[0]["new"] == {"tree": 3, "bwe": 3}
        assert hb[0]["known"] == {"tree": 3, "bwe": 3} and hb[0]["errors"] == {}

        # --- a refusal pauses that source for a day, the other goes on -----------------------------------------------
        w.status[BWE] = 429
        t_block = w.now
        run(arch, w, 1000)
        w.status.pop(BWE)
        blocked = read(out, "blocked.jsonl")
        assert [(b["src"], b["status"], b["pause_s"]) for b in blocked] == [("bwe", 429, 86400.0)], blocked
        k = n(w, t_block, BWE)
        run(arch, w, 80000)
        assert n(w, t_block, BWE) == k, "a refused source was asked again within a day"
        assert n(w, t_block, TREE) >= 1, "the other source stopped too"
        run(arch, w, 7000)
        assert n(w, t_block, BWE) > k, "the source was not asked again after its pause"
        w.status[TREE], w.headers[TREE] = 403, {"retry-after": "200000"}
        run(arch, w, 43200)
        w.status.pop(TREE)
        assert read(out, "blocked.jsonl")[-1]["src"] == "tree" and read(out, "blocked.jsonl")[-1]["pause_s"] == 200000.0
        w.status[BWE], w.raw[BWE] = 503, b"<html>Attention Required! | Cloudflare</html>"
        run(arch, w, 1000)
        w.status.pop(BWE), w.raw.pop(BWE)
        last = read(out, "blocked.jsonl")[-1]
        assert (last["src"], last["status"], last["pause_s"]) == ("bwe", 503, 86400.0), last
        for code in (418,):
            run(arch, w, 90000)
            w.status[BWE] = code
            run(arch, w, 1000)
            w.status.pop(BWE)
            assert read(out, "blocked.jsonl")[-1]["status"] == code

        # --- an ordinary error or an unreadable answer is retried sooner and pauses nothing -------------------------
        run(arch, w, 200000)
        n_blocked = len(read(out, "blocked.jsonl"))
        w.status[BWE] = 500
        t_err = w.now
        run(arch, w, 1800)
        w.status.pop(BWE)
        gaps = [t for t, h, _ in w.calls if t >= t_err and h == BWE]
        assert len(gaps) >= 3 and gaps[1] - gaps[0] == 120.0 and gaps[2] - gaps[1] == 240.0, gaps[:4]
        assert len(read(out, "blocked.jsonl")) == n_blocked
        run(arch, w, 2000)
        before = len(read(out, f"bwe-{a.month(int(w.now * 1000))}.jsonl"))
        w.raw[BWE] = b"<html>maintenance</html>"
        t_bad = w.now
        run(arch, w, 1000)
        w.raw.pop(BWE)
        assert arch.stats["errors"].get("bwe:parse", 0) >= 1 and len(read(out, "blocked.jsonl")) == n_blocked
        gaps = [t for t, h, _ in w.calls if t >= t_bad and h == BWE]
        assert len(gaps) >= 2 and gaps[1] - gaps[0] == 120.0, gaps[:3]      # retried soon, and the count restarted
        assert len(read(out, f"bwe-{a.month(int(w.now * 1000))}.jsonl")) == before

        # --- the file follows the month of the pull; a restart writes nothing twice ---------------------------------
        w.now = T0 + 25 * 86400
        w.tree.append({"_id": "4", "time": 4000, "title": "d"})
        run(arch, w, 50000)
        assert [r["id"] for r in read(out, "tree-202612.jsonl")] == ["4"], read(out, "tree-202612.jsonl")
        arch2 = a.Archive(out, w.fetch, w.clock)
        assert arch2.sources[0].known == {"1", "2", "3", "4"} and "g3" in arch2.sources[1].known
        arch2.tick()
        assert [r["id"] for r in read(out, "tree-202612.jsonl")] == ["4"]
        assert [r["id"] for r in read(out, "tree-202611.jsonl")] == ["1", "2", "3"]
    finally:
        shutil.rmtree(out, ignore_errors=True)
    print("selftest OK")


if __name__ == "__main__":
    main()
