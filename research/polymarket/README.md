# Polymarket favourites and underdogs (rounds 27 and 29, research only)

No orders are placed anywhere, and no keys are used.

**Access.** Polymarket's [own list](https://docs.polymarket.com/polymarket-learn/FAQ/geoblocking) names only some regions of
Ukraine, but the network of the workstation behind this project sends every Polymarket host to a third-party address, which
looks like a provider's block. These files therefore say nothing about whether the account holder may trade there, and
nothing here suggests working around a block.

| File | Purpose |
|---|---|
| `download_markets.py` | Market metadata from Gamma, in windows because its offsets stop at about 2,000 |
| `download_prices.py` | Hourly prices from the CLOB, one range a market |
| `bt_bonds.py`, `selftest_bonds.py` | Round 27: buy favourites priced 0.80..0.985 before the scheduled end |
| `diag_bonds.py` | Round 27's gap by volume floor and kind of market (descriptive, no trial) |
| `results_bonds_dev.json`, `dev.log` | Round 27's development results |

## Round 27, 2026-11-05: all 9 trials fail development, with the wrong sign

The rules are in the docstring of `bt_bonds.py`, committed before any price (19af3d6). 105,505 markets have history. Every
trial loses, between −0.9% and −4.0% a month at f = 5%, and the holdout was not opened.

Favourites win less often than their price says, on development entries, 2024–2025:

| Favourite's price | 1 day before | 3 days before | 7 days before |
|---|---:|---:|---:|
| 0.60–0.70 | −3.0 pp | −2.4 pp | −2.6 pp |
| 0.70–0.80 | −3.2 pp | −3.8 pp | −1.1 pp |
| 0.80–0.90 | −3.1 pp | −3.1 pp | −1.3 pp |
| 0.90–0.95 | −1.1 pp | −1.7 pp | −0.5 pp |
| 0.95–0.985 | −0.2 pp | −1.0 pp | −0.4 pp |
| 0.985 and over | +0.1 pp | 0.0 pp | +0.1 pp |

Cells are the win rate less the price. So the studies that find favourites underpriced do not hold here.

## Is the gap a product of the volume floor?

The universe keeps markets by final volume, which is unknown at entry, and a surprise draws trading. If that made the gap,
it would shrink as the floor rises. It does not: favourites priced 0.60–0.985 one day before show −2.3 pp at $50k, −3.0 pp at
$200k, −4.2 pp at $1M and −1.9 pp at $5M (208 markets). Politics is the exception, near zero.

## Notes

- `prices-history` with `interval=max` returns only the last 31 days at hourly fidelity, empty on older markets. Ask with
  `startTs` and `endTs`: 15 days works on every quarter since 2024, 21 days is a 400. The first run had no development prices
  and gave 0 signals; its table was deleted unread, because all its prices lay in the holdout.
- Hourly prices are not quotes, and the 1x cost assumes a half spread of 0.5 cent.
