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

Not run yet.
