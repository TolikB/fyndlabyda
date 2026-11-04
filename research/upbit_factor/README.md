# Round 22: Korean retail demand as a cross-sectional factor

The rules, trials and bar are in the docstring of `bt_upbit_factor.py`. It was committed before any
Upbit price was downloaded.

| File | Purpose |
|---|---|
| `download_candles.py` | Upbit's official day candles at 2 requests a second; run once on the VM |
| `bt_upbit_factor.py` | Signals, book, `coverage`, `dev` and `holdout` modes |
| `selftest_upbit_factor.py` | Known answers on synthetic frames; prints `selftest OK` |

Run it in `funding-research:py312` with `/opt/funding_arbitrage_research` mounted at `/w` and this
folder at `/w/upbit_factor`.

## Results

**All 8 trials fail development (2022-01..2025-06), so the holdout stays closed.** The registry is
now at 593 trials.

| Trial | Month at 1x | t | Max DD | Gross / costs over 2022–25 |
|---|---|---|---|---|
| `prem` H1 fade | −16.1% | −12.5 | 100% | +2.2 / 13.5 |
| `prem` H1 follow | −19.2% | −15.5 | 100% | −2.4 / 13.5 |
| `prem` H7 fade | −2.9% | −1.5 | 86% | +0.5 / 2.1 |
| `prem` H7 follow | −2.4% | −1.1 | 83% | −0.6 / 2.1 |
| `share` H1 fade | −7.3% | −4.4 | 97% | +2.5 / 9.7 |
| `share` H1 follow | −18.2% | −13.2 | 100% | −2.6 / 9.7 |
| `share` H7 fade | **+0.9%** | 0.4 | 68% | +1.1 / 1.9 |
| `share` H7 follow | −5.5% | −2.6 | 94% | −1.2 / 1.9 |

**What it says.**
- Fading hot Korean coins does have a small pre-cost sign: gross is positive on every fade and
  negative on every follow. It is far too small for the costs.
- The best case, `share` H7 fade, swings by year (+77%, +3%, −53%, +19%), so even the gross is not stable.

**Limit of the data.** Upbit's API serves no delisted markets: all 68 tickers from its delisting
notices came back empty. The sample is therefore coins that survived on Upbit to 2026-11. That bias
cannot explain results this far below the bar.
