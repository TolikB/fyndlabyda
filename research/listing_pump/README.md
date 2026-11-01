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

## Registry

`/w/pnl_round16.parquet` adds 3 trials: 575 in all. The diagnostics add none.

## Files

- **Download:** `download_upbit.py`, `download_binance.py`.
- **Test:** `bt_spotlist.py`, `selftest_spotlist.py`.
- **Results:** `results_dev.json`, `dev.log`.
- **Diagnostics:** `diag_spotlist.py`, `diag_spotlist.txt`, `diag_latency.py`, `diag_latency.txt`.
- **Data on the VM:** `/w/data/listing/`.
