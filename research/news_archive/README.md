# An archive of timed, labelled crypto headlines (step D7, research only)

## Why

A bot cannot win the first second after a headline (see "Public news feeds" in `../listing_pump/README.md`). It may
still be early for the minutes after an ambiguous one: a hack, a buyback, a treasury purchase, a caution notice.
Testing that needs each headline with the time a subscriber received it, the coin and the kind of event.

Tree of Alpha's public history carries all three, but it reaches back only about six days. So it is saved as it
goes. No round is registered on it yet and nothing in it has been read against prices.

## Files

| File | What it is |
|---|---|
| `archive.py` | Pulls the two feeds and appends what is new |
| `selftest_archive.py` | Known-answer checks on a scripted network and clock; prints `selftest OK` |

## Collector

Container `news-archive` on the research VM since 2026-11-12 11:33 UTC. Public endpoints only, no keys, no orders.

| Source | Endpoint | Pace |
|---|---|---|
| Tree of Alpha | `news.treeofalpha.com/api/news?limit=3000`, which its documentation gives as the partial history and asks not to poll for updates | every 12 hours |
| BWEnews | `rss-public.bwe-ws.com`, a public RSS of the last 10 items with one-second stamps | every 15 minutes |

A 403, 418 or 429, or a Cloudflare page, pauses that source for a day and is logged to `blocked.jsonl`. Nothing is
changed to get around a refusal.

**What is stored** (in `data/`, one file a source and month):

- `tree-YYYYMM.jsonl`: each item once, with its `time` (when subscribers got it, in ms), `source`, `kind`
  (`listing`, `delisting`, `hack`, `buyback`, `treasury`, `outage` and others), title, link and the coins the feed
  matched. Its icon and image links are dropped.
- `bwe-YYYYMM.jsonl`: title, publish stamp and link of each RSS item.

The first pull held items from 2026-11-06; 243 older ones (from 11-05 13:30 UTC) were added from a pull made on the
workstation the evening before, marked with a `seed` field. On 2026-11-15 the archive held 3,617 Tree items and 18
RSS items.

**Checks.** The selftest covers both parsers, writing each item once, the two paces, a refusal pausing one source
only, an error or unreadable answer being retried sooner, the month in the file name, and a restart. Each of 25
one-line mutations of the archive makes it fail.

## Run

```bash
cd /opt/funding_arbitrage_research/news_archive
docker run --rm -v $PWD:/w/news_archive -w /w/news_archive -e OUT=/tmp/x funding-research:py312 python selftest_archive.py
docker run -d --name news-archive --restart unless-stopped --memory 256m --log-opt max-size=5m --log-opt max-file=2 \
  -v $PWD:/w/news_archive -w /w/news_archive funding-research:py312 python -u archive.py
```
