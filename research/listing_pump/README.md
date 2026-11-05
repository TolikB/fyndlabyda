# Trade the perp after a spot listing announcement (round 16, research only)

## Question

When Upbit opens a KRW market for a coin, or Binance spot lists a coin that already has a Binance USDT-M
perp, the venue's own buyers can only trade hours later. Fast money buys the perp first. A bot that
sees the notice a minute late cannot take the first jump. Does the move keep going for minutes after
that (long), or give back within two days (fade)?

## Rules

The rules are in the `bt_spotlist.py` docstring. They were committed in 921c6b7 on 2026-11-01,
before any price around an announcement was looked at.

- **Events:**
  - Upbit notices that open a KRW market;
  - Binance "Binance Will List …" announcements;
  - in both cases, only coins whose Binance perp has traded for at least 2 days.
- **L15 and L60:** long at the first minute starting 60 s after the post, out after 15 or 60 minutes.
- **F48:** short from post + 4 h for 48 h, with a buy-stop at 1.30 × entry.
- **Costs:**
  - fee and slippage tier, with longs paying the tier twice at entry;
  - funding;
  - everything rerun at 2× costs.
- **Bar on development:**
  - mean net return per announcement > 0, with t ≥ 2.5;
  - 5% a month at the smallest size s ∈ {1, 2, 3} that reaches it, with drawdown ≤ 35% at that size;
  - > 0 at 2× costs.
- **Trials:** 3.

`selftest_spotlist.py` checks the title parsing on 23 real titles, the entry minute, missing bars,
the stop, costs, the book and the verdict.

## Data

- **Upbit:** `download_upbit.py` fetched all 806 "trade" notices. It runs on the workstation, because
  Upbit answers the research VM with Cloudflare error 1015. The archive has few notices before
  2021-10.
- **Binance:** `download_binance.py` fetched all 2,292 announcements of catalog 48.
- **Development** (before 2025-07):
  - 257 announcements parsed;
  - 65 have a perp that is old enough, 55 from Upbit and 10 from Binance;
  - 158 tickers were skipped because their perp was launched with the listing.
- **Holdout** (2025-07..2026-10): 103 announcements with a perp. It stays closed.
- **Prices:** 1-minute daily archives of Binance USDT-M around each event.

**Timestamp check** (pre-registered, prices not printed). Volume starts to rise in the announcement
minute in 52 of 72 events, and earlier in the other 20, which look like noise. In no event does it
start at or after our entry minute, so the post times are right.

## Result (development): FAIL, the holdout stays closed

| Trial | Announcements | Mean 1× | t | Win | Funding | Figure s=1 | Max DD s=1 |
|---|---|---|---|---|---|---|---|
| L15 | 63 | −0.94% | −1.12 | 41% | 0.00% | −2.8%/mo | 56.5% |
| L60 | 63 | −1.45% | −1.33 | 37% | −0.08% | −4.4%/mo | 70.8% |
| F48 | 63 | +2.15% | 1.09 | 70% | −1.48% | +6.5%/mo | 62.5% |

- **Late longs lose.** After a 60 s delay, both long trials lose their costs and more.
- **The fade is not significant.**
  - t is 1.09, against the 2.5 bar.
  - The book loses in 2024 (−36%) and in 2025 H1 (−23%).
  - Shorts pay 1.5% in funding over 48 hours, so the fade is crowded.
  - 7 of 72 trades hit the stop.

## Where the move is (`diag_spotlist.py` → `diag_spotlist.txt`, descriptive)

Price against the open of the announcement minute, over the 73 development events:

| Minutes after | −10 | −1 | +1 | +2 | +5 | +15 | +60 | +240 | +1440 | +2880 |
|---|---|---|---|---|---|---|---|---|---|---|
| Median | −0.1% | −0.0% | **+14.7%** | +14.8% | +14.2% | +16.7% | +15.0% | +14.8% | +7.5% | +8.1% |
| Mean | −0.2% | −0.0% | +21.9% | +24.4% | +22.7% | +23.0% | +22.2% | +22.2% | +14.5% | +13.9% |

**The jump is complete by the next minute.**
- 99% of events are up one minute later.
- The price then stays flat for four hours, and gives back about half within a day.
- There is no drift before the post, so there is no sign of a leak at minute scale.

## Second by second (`diag_latency.py` → `diag_latency.txt`, descriptive)

This uses the Binance aggTrades archive: every trade in the 10 minutes on either side of each
development post. It measures the price of the first trade at or after T + L, against the last trade
before T.

| L (s) | 0 | 0.5 | 1 | 2 | 3 | 5 | 10 | 30 | 60 | 120 |
|---|---|---|---|---|---|---|---|---|---|---|
| Median | 0.0% | 0.0% | +2.0% | +8.0% | +11.7% | +14.3% | +14.6% | +14.1% | +16.3% | +14.9% |
| Share of the +60 s move | 0% | 0% | 13% | 46% | 65% | 80% | 84% | 97% | 100% | 100% |

**The jump starts about 0.7 s after the post time and takes a few seconds.** This covers the 68 events
whose +60 s move is at least 4%.

| Share of the +60 s move reached | Quartiles (s after T) |
|---|---|
| 25% | 0.7 / 1.1 / 2.2 |
| 50% | 1.0 / 1.9 / 4.3 |
| 80% | 1.1 / 3.1 / 14.8 |

**Left to catch by +60 s** (median, before costs):

| Delay | Left |
|---|---|
| 1 s | 13.9% |
| 2 s | 8.2% |
| 3 s | 5.4% |
| 5 s | 2.8% |
| 10 s | 2.0% |

So the minute-level trials were 50 seconds too slow, not hopeless. A bot that sees the notice within
2–3 seconds might keep 5–8% before costs. This becomes round 16b (`bt_latency.py`): it is
pre-registered on these development numbers and tested once on the unspent holdout events.

**Caveats:**
- Upbit's post time has one-second resolution, so the true post may come up to a second after T.
- A market order in the burst queues behind faster bots.
- Only a forward run with a real detector can measure our actual delay.

## Verdict

**FAIL for the three registered trials.** Each announcement moves the price, by +15% in the median
case, but the move is over in the first seconds. A minute later, what remains is a crowded fade with
no significant edge. The speed version is round 16b.

## Round 16b: buy 2 or 5 seconds after the post (`bt_latency.py`, 4c53204)

**Rules.** The rules were pre-registered after the development numbers above were seen, so the
development run is not independent. The holdout is the test.

- **Events:** the same as round 16.
- **Entry:** at T + L seconds, L ∈ {2, 5}. The price is the larger of the first trade at or after T + L
  and the VWAP of buyer-initiated trades in the next second. This floor models queueing behind faster
  buyers.
- **Exit:** the first trade at or after T + 60 s.
- **Costs:** as in round 16.
- **Bar:** the same as round 16. On the holdout, the monthly figure is read straight from the book.

**Development** (`latency_dev.log`):

| Trial | Mean | t | Win | Max DD |
|---|---|---|---|---|
| S2 | +10.6% | 5.34 | 73% | 23% |
| S5 | +5.4% | 3.35 | 54% | 34% |

Both pass, as they had to.

**Holdout, one look** (`latency_holdout.log`, 2025-07-01..2026-10-28, 99 announcements):

| Trial | Mean 1× | t | Win | Mean 2× | Max DD at s=1 | Verdict |
|---|---|---|---|---|---|---|
| S2 | +8.73% | 4.06 | 55% | +7.94% | 32.3% | **PASS** |
| S5 | +4.83% | 2.52 | 47% | +4.05% | 67.5% | FAIL (drawdown) |

This is the first holdout pass in 577 trials. Its terms matter more than the headline
(`diag_holdout.py` → `diag_holdout.txt`).

**A few events carry the mean.**
- The median S2 trade is +1.4%. The 10%-winsorised mean is +6.5%, and dropping the 5 best trades
  leaves +5.3%.
- The best trades are CYBER +105%, IN +85%, OMNI +79%, API3 +63% and HYPER +60%.
- The simple monthly sum at s=1 averages +54%, but its median month is +18% and its worst is −26%.

**It is shrinking fast.**

| Quarter | S2 mean | S2 median |
|---|---|---|
| 2025 Q3 | +15.9% | −0.5% |
| 2025 Q4 | +17.3% | +8.6% |
| 2026 Q1 | +2.1% | −0.4% |
| 2026 Q2 | +2.6% | +2.2% |
| 2026 Q3 | +2.3% | −0.3% |
| 2026 Q4 (8 trades) | +13.1% | +8.7% |

**The race got faster.**
- In the holdout, half the +60 s jump is done 1.1 s after T, against 1.9 s on development.
- The median move left after 2 s fell from 11.2% (2025) to 4.3% (2026).
- After 5 s it fell from 2.4% to −0.6%.

**Latency budget** (`diag_budget.py` → `diag_budget.txt`). This is descriptive, computed on the
spent holdout with the S2 entry floor, exit and costs.

| Entry delay | 2026 mean a trade | 2026 median | ≈ a month at s=1 (6 trades) |
|---|---|---|---|
| 0.5 s | +10.3% | +6.0% | 61% |
| 1 s | +5.0% | +3.7% | 29% |
| 2 s | +3.7% | +0.9% | 21% |
| 3 s | +2.1% | −0.6% | 12% |
| 5 s | +0.3% | −2.1% | 2% |

A bot that lands within about one second earns most trades. At 2–3 s the median trade is already
flat or losing, and the mean rests on a few large jumps.

**The bar does not test the main risk.** The backtest assumes we see the notice and our order lands
2 s after T. Nothing here shows that a bot of ours can do that.

- **Upbit has no official notice feed.** Its web API answers the research VM with Cloudflare error
  1015 and throttles the workstation at about one request a second. Fast polling would mean working
  around a bot check, which this project will not do.
- **Legitimate routes exist.** For Binance listings (17 of the 101 holdout trades, mean +8.2%,
  median +2.4%), Binance's official announcement WebSocket. For Upbit, a paid low-latency news feed.
- **Next step.** Only a forward paper run with a real detector can measure our delay: detection time
  against T, and fills from the live trade stream at that time. Without it, this pass is not a
  strategy. Whether to use either route is the user's decision.

## Forward step 0: can a legitimate feed see a listing in time? (`watch_upbit.py`, since 2026-11-01)

Upbit's official Open API is meant for programs. Container `upbit-watch` on the research VM polls
`GET /v1/market/all` once a second, about a tenth of the published quotation limit, and logs when
each new market first appears.

- **Output:** `watch/markets.jsonl`, plus an hourly `watch/heartbeat.jsonl` with request counts,
  errors and round-trip times.
- **On refusal:** a 429 slows it down, and a block pauses it for an hour. It never changes headers
  or addresses.
- **The question:** does a new KRW market appear at the notice time T, or only when trading opens
  hours later? After a few listings, each first-seen time is compared with the notice's
  first_listed_at and with the Binance perp's jump in the aggTrades archive.
  - If it appears at T, the remaining gap is round-trip time, and a server near Seoul could close
    it.
  - If it appears only later, this route is useless for 16b.

**First event, CARDS (2026-11-05): 43 minutes late.** The notice for KRW, BTC and USDT markets was
posted at 06:03:03 UTC, and the three markets first appeared at 06:46:27 UTC, 2,604 s after it. The
budget is about 1 s. They vanished 0.9 s later and came back 2.5 s after that, so the list rolls out
unevenly across servers.

The plan's rule needs 3 events, so there is no formal verdict yet (`analyze_watch.py`: "no verdict
yet"). CARDS has no perp on Binance or Hyperliquid, so the sniper took no trade and there is no jump to
compare against. Upbit answered 429 to a request for the notice body, which was not repeated, so
the trading open time is unknown.

## Round 19: short the perp seconds after bad exchange news (`bt_badnews.py`, 5c3aad4)

**Events.** These are Upbit notices of an investment warning, a caution or a delisting, and Binance
spot delistings, futures delistings and monitoring tags.
- Development: 57 announcements with a perp.
- Holdout: 79 announcements.

**Rules.**
- Short at T + 2 s (S2) or T + 5 s (S5), with the mirror of the 16b queueing floor.
- Cover at T + 60 s.
- The book and bar are those of 16b.
- Titles that mix futures delistings with margin-tier updates were excluded before any price was
  seen.

**Timestamp check: ok.** A quarter of a ≥ 4% drop is reached after 1.8 / 2.8 / 3.3 s (quartiles).
This is slower than a listing jump.

**Development** (`badnews_dev.log`):
- **S2:** +3.22% an announcement, t 3.40. It passes at s* = 2, with a figure of 9.7% a month and a
  drawdown of 12.9%; at 2x costs the figure is +7.5%.
- **S5:** fails, t 1.86.
- Most of the S2 edge came from Binance events (+7.8% over 29 trades). Upbit events made +0.2%.

**Holdout, one look: FAIL** (`badnews_holdout.log`).
- **S2:** 70 announcements, −0.60% each, t −0.45, max drawdown 89%.
- **Futures delistings squeeze.** The four worst trades are Binance futures delistings in 2025 Q3:
  LEVER −52%, ALPHA −48%, NEIROETH −32% and HIFI −24% within a minute. When Binance closes the perp
  itself, shorts rush to cover. This is the opposite of a spot delisting.
- **Recent quarters.** By quarter the mean was −15.8% and −3.2% (2025 Q3–Q4), then +2.9%, +6.8%,
  +1.4% and +0.7% (2026).

A version without futures delistings was suggested by the holdout itself. It may only be judged on
forward data.

## Bybit listings: no move to trade (`diag_bybit.py` → `diag_bybit.txt`, descriptive, no trial)

Bybit's official v5 announcement API is a legitimate fast feed. `download_bybit.py` fetched its
1,826 "New Listings" posts (2022–2026).

**The Binance perp hardly moves.** On development posts whose coin already had an old Binance perp
(42 posts), the move 60 s after either timestamp has a median of +0.02% to +0.12%. Almost none
exceeds +2%. So Bybit adds no event worth a trial.

**OKX is not used.** Its announcement API answers the EU-based VM with the EEA site's feed, and this
project does not work around regional restrictions.

## Round 21: buy 2 s after an Upbit BTC/USDT-market listing (`bt_upbit_btc.py`, 718e384)

**Why it looked worth a trial.** Round 16 used only KRW notices. `diag_upbit_btc.py` showed that
BTC/USDT-only listings also move the Binance perp, and more slowly. Over 41 development notices the
median move was +2.2% at T+2 s, +8.9% at 10 s and +5.2% at 60 s. Those numbers set the design, so
the holdout is the test.

**Rules.**
- Enter at T+2 s with the 16b queueing floor.
- Exit at T+10 s (E2X10) or T+60 s (E2X60), with a seller-side floor.
- The book and bar are those of 16b.

| | Announcements | Mean 1× | t | Win | Result |
|---|---|---|---|---|---|
| Development E2X10 | 26 | +3.24% | 2.84 | 62% | passes at s*=2: figure 7.0%/mo, DD 13.4% |
| Development E2X60 | 26 | +2.22% | 1.63 | 54% | fails |
| **Holdout E2X10, one look** | **14** | **−1.97%** | **−1.55** | **21%** | **FAIL** (DD 26% at s=1) |

The holdout says the same as 16b's 2026 quarters. By 2025 H2 the race is fast enough that a 2 s
entry buys the top. Whatever edge is left belongs to sub-second bots.

## Forward step 1: the paper sniper (`fwd_sniper.py`, container `fwd-sniper`, since 2026-11-01)

The rules are in the docstring and were fixed before any forward event.

- **Trigger:** every market `upbit-watch` sees added to Upbit's official list, whether KRW, BTC or
  USDT. One coin counts once within 72 h.
- **Perp:** the coin's Binance perp, at least 2 days old.
- **Fill:** 3 minutes later it fetches REST aggTrades and fills the 16b rule on this host's clock:
  - entry at t_seen + one-way delay, with the queueing floor;
  - exit at t_seen + 60 s;
  - 16b costs.
- **Price-side latency check:** it also records `pre_move_pct`, the part of the jump that happened
  in the 10 s before we saw the market.
- **Ledger:** `fwd_sniper/ledger.jsonl`, plus `report.json`.
- **Verdict:** on or after 2026-12-27, equity ≥ 1 and drawdown ≤ 35% at s = 1.

**Dry run.** It was replayed on the KRW-NIL notice of 2026-10-27 07:30:00 UTC, part of the spent 16b
holdout. REST and the archive give the same S2 fill: entry 0.07163, exit 0.07169.

## Upbit caution flags: daily batches, no signal (`diag_flags.py` → `diag_flags.txt`, descriptive, no trial)

`upbit-watch` logs every `market_event` flag change. The question was whether a flag turning on for a
KRW market predicts the coin's Binance perp. Data: 67 h from 2026-11-01, 84 events with a perp, entry
at the next minute's open.

`TRADING_VOLUME_SOARING` looked good at first: +0.40% at +15 min, t 3.58, n 52. It is not a signal.
Every one of those events turned on within 40 s of 01:00 UTC (10:00 KST) on three days, so Upbit
computes the flag in a daily batch. The figure is those three days' market move. Over BTC and counted
per 30-minute bucket, it is +0.09%, t 0.41, n 4. `DEPOSIT_AMOUNT_SOARING` has the same shape (3
buckets), `PRICE_FLUCTUATIONS` never fired on a KRW market, and `GLOBAL_PRICE_DIFFERENCES` has only 5
events.

Closed. Clustering events in time before quoting a t-stat is what caught this.

## Registry

- `/w/pnl_round16.parquet` adds 3 trials.
- `/w/pnl_round16b.parquet` adds 2 trials.
- That makes 577 in all. Rounds 17 and 18 bring it to 581, `/w/pnl_round19.parquet` (2 trials) to 583,
  and `/w/pnl_round21.parquet` (2 trials) to 585. The diagnostics add none. Round 22 (`../upbit_factor`) brings it to 593, and round 26 (`../zerofee`) to 629.

## Files

- **Download:** `download_upbit.py`, `download_binance.py`, `download_bybit.py`.
- **Test:** `bt_spotlist.py`, `selftest_spotlist.py`, `bt_latency.py`, `selftest_latency.py`, `bt_badnews.py`,
  `selftest_badnews.py`.
- **Results:** `results_dev.json`, `dev.log`, `results_latency_dev.json`, `latency_dev.log`,
  `results_latency_holdout.json`, `latency_holdout.log`, `badnews_*.log`, `results_badnews_*.json`.
- **Diagnostics:** `diag_spotlist.py`, `diag_spotlist.txt`, `diag_latency.py`, `diag_latency.txt`,
  `diag_holdout.py`, `diag_holdout.txt`, `diag_budget.py`, `diag_budget.txt`, `diag_bybit.py`, `diag_bybit.txt`.
- **Forward:** `watch_upbit.py`, `analyze_watch.py`, `fwd_sniper.py`, `selftest_sniper.py`.
- **Data on the VM:** `/w/data/listing/`, `listing_pump/watch/`.
