
<!-- README.md is generated from README.Rmd. Please edit that file -->

# cryptoAPIs

> Tidy R wrappers for cryptocurrency exchange REST APIs — OHLCV candles,
> trade ticks, and orderbooks from 8 exchanges in one unified interface.

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![R-CMD-check](https://github.com/morner75/cryptoAPIs/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/morner75/cryptoAPIs/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

cryptoAPIs provides a consistent set of functions for pulling market
data from eight cryptocurrency exchanges. Each exchange module exposes
the same function surface — trading pair lookup, recent candles,
paginated date-range candles, current prices, trade ticks, and
orderbooks — so switching exchanges requires changing only the function
prefix.

## Supported exchanges

| Exchange | Region | Quote currencies | Prices | Trades | Orderbook |
|----|----|----|----|----|----|
| [Upbit](https://upbit.com) | 🇰🇷 Korea | KRW, BTC, USDT | ✔ | ✔ | ✔ |
| [Bithumb](https://bithumb.com) | 🇰🇷 Korea | KRW, BTC | ✔ | ✔ | ✔ |
| [Coinone](https://coinone.co.kr) | 🇰🇷 Korea | KRW | ✔ | ✔ | ✔ |
| [Korbit](https://korbit.co.kr) | 🇰🇷 Korea | KRW | ✔ | ✔ | ✔ |
| [GOPAX](https://gopax.co.kr) | 🇰🇷 Korea | KRW | ✔ | ✔ | ✔ |
| [Binance](https://binance.com) | 🌐 Global | USDT, BTC, ETH, … | ✔ | ✔ | — |
| [Coinbase](https://coinbase.com) | 🌐 Global | USD, USDC, BTC, … | ✔ | ✔ | — |
| [OKX](https://okx.com) | 🌐 Global | USDT, BTC, ETH, … | ✔ | ✔ | — |

## Features

- **Unified market format.** All exchanges use `"ASSET-QUOTE"`
  (e.g. `"BTC-KRW"`, `"BTC-USDT"`). Internal translation to
  exchange-native symbols happens inside each module.
- **Standardized OHLCV columns.** Every candle function returns the same
  six columns: `time_kst`, `opening_price`, `high_price`, `low_price`,
  `trade_price`, `volume`.
- **Paginated range queries.** `fetch_*_range()` paginates automatically
  over any date interval, respecting rate limits with small sleeps
  between requests.
- **Current price snapshots.** `get_*_prices()` returns a
  one-row-per-market ticker with `trade_price`, OHLC, 24-hour volumes,
  and change rate.
- **Trade tick data.** `get_*_trades()` returns individual executions
  with `time_kst`, `trade_price`, `volume`, `ask_bid`, and
  `sequential_id`.
- **Session-level caching.** Trading pair lists are fetched once per R
  session and cached in memory, avoiding redundant API calls.
- **Tidy output.** All functions return a `data.frame` (or `tibble`)
  ready for downstream analysis with dplyr, ggplot2, etc.
- **Graceful error handling.** Every function returns `NULL` on failure
  and prints a descriptive message; no uncaught exceptions propagate to
  the caller.

## Installation

``` r
# install.packages("remotes")
remotes::install_github("morner75/cryptoAPIs")
```

## Usage

### Look up trading pairs

``` r
library(cryptoAPIs)

# All KRW pairs on Upbit
upbit_trading_pairs()

# Resolve to the exchange-native symbol
upbit_trading_pairs("BTC-KRW")   #> "KRW-BTC"
binance_trading_pairs("BTC-USDT") #> "BTCUSDT"
```

### Fetch the most recent candles

Each `fetch_*()` function returns the most recent *N* 1-minute candles,
sorted by time.

``` r
# Last 60 one-minute candles from Upbit
fetch_upbit("BTC-KRW", count = 60)

# Last 100 candles from Binance
fetch_binance("BTC-USDT", count = 100)

# Last 200 candles from OKX
fetch_okx("BTC-USDT", count = 200)
```

       time_kst            opening_price high_price low_price trade_price    volume
    1  2024-03-01 09:00:00      89245000   89312000  89198000    89280000  3.241587
    2  2024-03-01 09:01:00      89280000   89350000  89255000    89310000  2.876543
    ...

### Fetch candles over a date range

`fetch_*_range()` paginates through the exchange’s history endpoint and
returns all bars between `from` and `to`, with duplicates removed.

``` r
from <- as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul")
to   <- as.POSIXct("2024-01-01 18:00:00", tz = "Asia/Seoul")

# One-minute candles
fetch_upbit_range("BTC-KRW", from = from, to = to, unit = "min")

# Hourly candles from Binance
fetch_binance_range("ETH-USDT", from = from, to = to, unit = "hour")

# Daily candles from Coinbase
fetch_coinbase_range("BTC-USD", from = from, to = to, unit = "day")
```

The `unit` argument accepts `"min"`, `"hour"`, or `"day"` for all
exchanges.

### Fetch trade ticks

`get_*_trades()` retrieves individual executions within a time window.
Exchanges that support cursor-based pagination (Upbit, Bithumb, Binance,
Coinbase, GOPAX, OKX) retrieve all trades in the range; exchanges with
no pagination (Coinone, Korbit) are limited to the most recent trades
available from the public endpoint.

``` r
from <- as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul")
to   <- as.POSIXct("2024-01-01 09:05:00", tz = "Asia/Seoul")

get_upbit_trades("BTC-KRW", from = from, to = to)
get_binance_trades("BTC-USDT", from = from, to = to)
get_okx_trades("BTC-USDT", from = from, to = to)
```

       time_kst            trade_price    volume ask_bid sequential_id
    1  2024-01-01 09:00:00    59823000  0.004521     ASK    1234567890
    2  2024-01-01 09:00:01    59825000  0.012300     BID    1234567891
    ...

`ask_bid` follows the taker-side convention: `"BID"` = buyer-initiated,
`"ASK"` = seller-initiated.

### Get current prices

`get_*_prices()` returns the latest ticker snapshot — current price,
24-hour OHLC, volumes, and change rate — for one or more markets in a
single call.

``` r
# Single market
get_upbit_prices("BTC-KRW")
get_binance_prices("BTC-USDT")
get_okx_prices("BTC-USDT")

# Multiple markets in one call
get_upbit_prices(c("BTC-KRW", "ETH-KRW", "XRP-KRW"))
get_binance_prices(c("BTC-USDT", "ETH-USDT"))
```

       market            time_kst trade_price opening_price high_price  low_price prev_closing_price change signed_change_rate acc_trade_volume_24h acc_trade_price_24h
    1 BTC-KRW 2024-03-01 15:00:00   105933000     105399000  106000000  105304000          105314000   RISE            0.00588             1088.376        114493560880

All eight exchanges share the same eleven output columns. Exchanges that
do not provide a particular statistic return `NA` for that column
(GOPAX: `prev_closing_price`, `change`, `signed_change_rate`; Coinbase:
`prev_closing_price`, `acc_trade_price_24h`).

### Get orderbook data

``` r
# Top 10 price levels from Upbit
get_upbit_orderbook("BTC-KRW", count = 10)

# Top 30 levels from Korbit (default)
get_korbit_orderbook("ETH-KRW")
```

       market    timestamp           ask_price bid_price ask_size bid_size total_ask_size total_bid_size level
    1  BTC-KRW  2024-01-01 09:00:00  90100000  90050000    0.312    0.451          8.231          9.102     0
    2  BTC-KRW  2024-01-01 09:00:00  90150000  90000000    0.200    0.600          8.231          9.102     0
    ...

### Merge saved datasets

When saving multiple range-query results to disk,
`merge_ohlc_datasets()` combines them and removes duplicate rows:

``` r
# Each .rds file should be a list: list(exchange = list(market = data.frame(...)))
merged <- merge_ohlc_datasets("data_jan.rds", "data_feb.rds")
merged$upbit$`BTC-KRW`
merged$binance$`BTC-USDT`
```

### Risk alarm (Upbit / Bithumb)

`get_upbit_alarm()` scrapes the Upbit exchange pages to retrieve the
current alarm tier (`"주의"` / `"경고"` / `"위험"`) for flagged markets.
Requires `chromote` and `rvest`.

``` r
# All KRW markets currently under caution
get_upbit_alarm(quote = "KRW", verbose = TRUE)
```

`get_bithumb_alarm()` fetches the same alarm information from the
Bithumb public API directly — no headless browser required.

``` r
get_bithumb_alarm(quote = "KRW", verbose = TRUE)
```

## Output columns

### Candles (`fetch_*`, `fetch_*_range`)

| Column          | Type                 | Description                  |
|-----------------|----------------------|------------------------------|
| `time_kst`      | POSIXct (Asia/Seoul) | Candle open timestamp in KST |
| `opening_price` | numeric              | Open price                   |
| `high_price`    | numeric              | High price                   |
| `low_price`     | numeric              | Low price                    |
| `trade_price`   | numeric              | Close price                  |
| `volume`        | numeric              | Traded volume in base asset  |

### Current prices (`get_*_prices`)

| Column | Type | Description |
|----|----|----|
| `market` | character | Market in `"ASSET-QUOTE"` format |
| `time_kst` | POSIXct (Asia/Seoul) | Ticker snapshot timestamp in KST |
| `trade_price` | numeric | Most recent trade price |
| `opening_price` | numeric | 24-hour open price |
| `high_price` | numeric | 24-hour high price |
| `low_price` | numeric | 24-hour low price |
| `prev_closing_price` | numeric | Previous close price (`NA` for GOPAX, Coinbase) |
| `change` | character | `"RISE"` / `"FALL"` / `"EVEN"` vs. previous close (`NA` for GOPAX) |
| `signed_change_rate` | numeric | Signed rate of change (`NA` for GOPAX) |
| `acc_trade_volume_24h` | numeric | 24-hour cumulative volume in base asset |
| `acc_trade_price_24h` | numeric | 24-hour cumulative volume in quote asset (`NA` for Coinbase) |

### Trade ticks (`get_*_trades`)

| Column | Type | Description |
|----|----|----|
| `time_kst` | POSIXct (Asia/Seoul) | Execution timestamp in KST |
| `trade_price` | numeric | Executed price |
| `volume` | numeric | Executed volume in base asset |
| `ask_bid` | character | `"BID"` buyer-initiated · `"ASK"` seller-initiated |
| `sequential_id` | numeric | Exchange-assigned trade ID (deduplication key) |

## Dependencies

| Package                 | Role                                  |
|-------------------------|---------------------------------------|
| `httr` / `httr2`        | HTTP requests                         |
| `jsonlite`              | JSON parsing                          |
| `dplyr`                 | Data wrangling and pipe               |
| `stringr`               | Symbol parsing                        |
| `purrr`                 | Iteration (`get_upbit_alarm`)         |
| `tibble`                | Tidy data frames                      |
| `chromote` *(Suggests)* | Headless Chrome for `get_upbit_alarm` |
| `rvest` *(Suggests)*    | HTML parsing for `get_upbit_alarm`    |

## License

MIT © 2024
