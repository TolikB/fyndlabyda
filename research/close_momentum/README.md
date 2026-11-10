# Does the day's move carry on into the last half hour? (round 36, research only)

No orders are placed anywhere, and no keys are used.

A 3x fund must trade in the direction of the day's move before the close, and option hedgers do the same. Round 36 asked
whether that shows as a carry-on in SOXL and TQQQ from 15:30 to 16:00 New York time, and whether Binance's perps on
those two funds could trade it. Rules are in the docstring of `bt_close.py`, committed before any price of the funds was
read (54dc2dc).

| File | Purpose |
|---|---|
| `download_bars.py`, `download.log` | 60-minute bars of the two funds from Yahoo's public chart API, development period only |
| `bt_close.py`, `selftest_close.py` | The test and its known-answer checks |
| `diag_close.py`, `diag.log` | What the mirror image did (descriptive, written after the result) |
| `results_close_dev.json`, `dev.log` | Development results |
| `results_close_diag.json` | The breakdown |

## Result, 2026-11-10: all 3 trials fail development

600 full sessions, 2023-12-13 to 2026-05-14. Every trial loses, so the holdout stays closed: the perps' own bars from
2026-05-15 on were never downloaded.

| Trial | Legs | bp a leg | Won | Month, s = 1 | t | DD | Month at 2x costs |
|---|---:|---:|---:|---:|---:|---:|---:|
| follow, 3% | 574 | −34.2 | 36% | −3.44% | −4.77 | 98% | −4.84% |
| follow, 6% | 254 | −37.2 | 39% | −1.65% | −2.93 | 48% | −2.28% |
| follow, 9% | 114 | −33.7 | 44% | −0.67% | −1.51 | 24% | −0.95% |

## The loss is bigger than the costs, and the mirror is no edge either

A round trip costs 14 bp, and the legs lose 34 to 37 bp. Before costs the half hour went against the day's move:

| Threshold | bp a leg before costs | t | 2024 | 2025 | 2026 | After an up day | After a down day |
|---|---:|---:|---:|---:|---:|---:|---:|
| 3% | −20.2 | −2.85 | −4.5 | −30.4 | −36.8 | −12.0 | −31.2 |
| 6% | −23.2 | −1.84 | +12.0 | −45.6 | −47.2 | −9.7 | −39.3 |
| 9% | −19.7 | −0.89 | +32.0 | −41.5 | −50.3 | +14.6 | −50.6 |

So the published carry-on is absent here in 2024 and reversed since 2025, most of all after a down day. Fading it does
not pay:

- **Costs take it.** The fade at 3% makes +0.63% a month at s = 1 with t 0.88, and −0.78% at 2x costs. At 6% and 9% it
  makes +0.41% and +0.11%.
- **Five days carry it.** Without its best five days the 3% fade makes −0.09% a month.
- **It was picked from this sample.** The sign was chosen after the result, so even these figures flatter it.

The fade's three series are in `/w/pnl_round36b.parquet`, so the registry counts them: 658 recorded + 24 unrecorded = 682
trials.

## Data notes

- Yahoo's chart API serves 730 days of 60-minute bars. The bars start at 09:30, and the last one, 15:30, is half an hour
  long. Whether its close is the auction print was not checked: the round failed without it.
- Six bars a fund came back empty and were dropped. Six half days end with a 13:00 bar. TQQQ split on 2025-11-20, and
  that session is skipped.
- The perps' taker fee of 5 bp was read in secondary sources only.
- SOXL gives 369 of the 574 legs at 3%, because it moves more than TQQQ.
