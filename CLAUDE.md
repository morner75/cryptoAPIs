# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```r
# Run all tests
devtools::test()

# Run a single test file
testthat::test_file("tests/testthat/test-upbit.R")

# Regenerate documentation and NAMESPACE from Roxygen comments
devtools::document()

# Build and check the package
devtools::check()

# Install the package locally
devtools::install()
```

Tests that call live APIs are guarded with `skip_if_offline()` and will be skipped in offline environments.

## Architecture

The package provides unified OHLCV candlestick and market data wrappers for 8 cryptocurrency exchanges: Upbit, Bithumb, GOPAX, Coinone, Korbit (Korean), and Binance, Coinbase, OKX (international).

Each exchange module (`R/{exchange}.R`) exposes the same three-function surface:

| Function | Purpose |
|---|---|
| `{exchange}_trading_pairs(market, ...)` | List available markets; pass a symbol to look up a specific pair |
| `fetch_{exchange}(market, count)` | Fetch the most recent N 1-minute candles |
| `fetch_{exchange}_range(market, from, to, unit)` | Paginate over a date range, auto-sleeping between requests |

**Standardized market format:** All exchanges use `"ASSET-QUOTE"` (e.g., `"BTC-KRW"`, `"BTC-USDT"`). Internal translation to exchange-native formats (e.g., `"BTCUSDT"` for Binance, `"KRW-BTC"` for Upbit) happens inside each module.

**Standardized OHLCV output columns:** `time_kst` (POSIXct, Asia/Seoul), `opening_price`, `high_price`, `low_price`, `trade_price` (close), `volume`.

**`R/utils.R`** contains `merge_ohlc_datasets(...)` for combining multiple saved RDS files.

**Upbit extras:** `get_upbit_orderbook()` for real-time orderbook data; `get_upbit_alarm()` for risk alarms via headless Chromium (requires `chromote` + `rvest`).

**Bithumb extras:** `get_bithumb_alarm()` for risk alarms via direct API.

## Key Implementation Details

- Range functions paginate in chunks of 200–1024 rows with `Sys.sleep(0.1–0.15)` between requests to respect rate limits.
- Error paths return `NULL` with Korean-language messages (the intended user base is Korean traders).
- `_pick_symbol()` (internal utility per exchange) handles the dual-mode behavior of trading pair functions (return all vs. look up one).
- OKX supports `inst_type` parameter: `"SPOT"`, `"FUTURES"`, `"SWAP"`, `"OPTION"`.
- Coinone `trading_pairs` accepts a `quote` parameter (default `"KRW"`).
- Documentation and NAMESPACE are managed by Roxygen2 — edit `@` tags in source files, then run `devtools::document()`.
