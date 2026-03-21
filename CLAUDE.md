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

## Trade Tick Functions (`get_*_trades`)

모든 8개 거래소에서 제공. 시간 범위 내 체결 틱 데이터를 조회합니다.

```r
get_{exchange}_trades(market, from, to)
# market: "ASSET-QUOTE" 형식 (e.g., "BTC-KRW")
# from, to: POSIXct (tz = "Asia/Seoul")
```

**출력 컬럼:** `time_kst`, `trade_price`, `volume`, `ask_bid` (`"ASK"` = 매도자 체결 / `"BID"` = 매수자 체결), `sequential_id`

**페이지네이션 방식 (거래소별 상이):**

| 거래소 | 커서 방식 | 1회 최대 | 비고 |
|---|---|---|---|
| Upbit | `cursor` (sequential_id) | 500 | `to` 파라미터로 시작점 지정 |
| Bithumb | `cursor` (sequential_id) | 500 | `to` 파라미터로 시작점 지정 |
| Binance | `fromId` (aggTrade ID) | 1000 | `startTime`/`endTime` ms 사용 |
| GOPAX | `pastmax` (trade ID) | 100 | `before` Unix 초로 시작점 지정 |
| OKX | `after` (tradeId) | 100 | `history-trades` 엔드포인트 |
| Coinbase | `after` (trade_id) | 100 | ISO 8601 타임스탬프 파싱; `after=N`이 구버전 trades (ID < N) 반환 |
| Korbit | 없음 (단일 요청) | 500 | 범위 필터만 적용 |
| Coinone | 없음 (단일 요청) | 200 | 범위 필터만 적용 |

## Orderbook Functions (`get_*_orderbook`)

한국 거래소 5개(Upbit, Bithumb, GOPAX, Korbit, Coinone)에서 제공. Binance, Coinbase, OKX는 미구현.

```r
get_{exchange}_orderbook(market, count = 30)
# Upbit, Bithumb, GOPAX는 level 파라미터 추가 지원 (기본값 0)
```

**출력 컬럼:** `market`, `timestamp` (POSIXct, KST), `ask_price`, `bid_price`, `ask_size`, `bid_size`, `total_ask_size`, `total_bid_size`, `level`

**거래소별 특이사항:**

| 거래소 | `level` 지원 | 비고 |
|---|---|---|
| Upbit | O | KRW 마켓만 의미 있음 |
| Bithumb | O | KRW 마켓만 의미 있음 |
| GOPAX | O (출력 컬럼만) | API에서 집계 미지원, 항상 0 |
| Korbit | X | `level` 컬럼은 항상 `0` |
| Coinone | X | `level` 컬럼은 항상 `0` |

## Key Implementation Details

- Range functions paginate in chunks of 200–1024 rows with `Sys.sleep(0.1–0.15)` between requests to respect rate limits.
- Error paths return `NULL` with Korean-language messages (the intended user base is Korean traders).
- `_pick_symbol()` (internal utility per exchange) handles the dual-mode behavior of trading pair functions (return all vs. look up one).
- OKX supports `inst_type` parameter: `"SPOT"`, `"FUTURES"`, `"SWAP"`, `"OPTION"`.
- Coinone `trading_pairs` accepts a `quote` parameter (default `"KRW"`).
- Documentation and NAMESPACE are managed by Roxygen2 — edit `@` tags in source files, then run `devtools::document()`.
