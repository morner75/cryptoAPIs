.okx_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from OKX
#'
#' @description
#' Fetches all live instruments of the specified type from the OKX exchange API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-USDT"`) is supplied,
#'   returns the matching exchange-native `instId`.
#' @param inst_type Character. Instrument type to query. One of `"SPOT"`,
#'   `"FUTURES"`, `"SWAP"`, `"OPTION"`. Defaults to `"SPOT"`.
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native `instId` string (or `NULL` if not
#'   found).
#'
#' @examples
#' \dontrun{
#' okx_trading_pairs()
#' okx_trading_pairs(market = "BTC-USDT")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter transmute
#' @export

okx_trading_pairs <- function(market = NULL, inst_type = "SPOT") {
  if (!exists(inst_type, envir = .okx_pairs_cache)) {
    tryCatch({
      res <- GET("https://www.okx.com/api/v5/public/instruments",
                 query  = list(instType = inst_type),
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("OKX \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"))
      if (parsed$code != "0") {
        message("OKX \uc624\ub958: ", parsed$msg); return(NULL)
      }
      df <- parsed$data %>%
        filter(state == "live") %>%
        transmute(
          exchange = "okx",
          asset    = baseCcy,
          quote    = quoteCcy,
          symbol   = instId,
          market   = paste(asset, quote, sep = "-")
        )
      assign(inst_type, df, envir = .okx_pairs_cache)
    }, error = function(e) { message("OKX \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists(inst_type, envir = .okx_pairs_cache)) return(NULL)
  .pick_symbol(get(inst_type, envir = .okx_pairs_cache), market, "OKX")
}


#' Fetch recent 1-minute OHLCV candles from OKX
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the OKX exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USDT"`).
#' @param count Integer. Number of candles to retrieve (default `100`, max `300`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_okx("BTC-USDT", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows arrange
#' @export
fetch_okx <- function(market, count = 100) {
  sym <- okx_trading_pairs(market)
  if (is.null(sym)) { message("OKX: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0("https://www.okx.com/api/v5/market/candles",
                "?instId=", sym, "&bar=1m&limit=", min(count, 300))
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("OKX HTTP \uc624\ub958: ", status_code(res), " / ", market); return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
    if (is.null(parsed$data) || length(parsed$data) == 0) return(NULL)
    bind_rows(lapply(parsed$data, function(r) data.frame(
      time_kst      = as.POSIXct(as.numeric(r[[1]]) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(r[[2]]),
      high_price    = as.numeric(r[[3]]),
      low_price     = as.numeric(r[[4]]),
      trade_price   = as.numeric(r[[5]]),
      volume        = as.numeric(r[[6]]),
      stringsAsFactors = FALSE
    ))) %>% arrange(time_kst)
  }, error = function(e) { message("OKX \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from OKX
#'
#' @description
#' Paginates through the OKX history-candles endpoint to retrieve all bars
#' between `from` and `to`.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USDT"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#' @param unit Character. Candle unit: `"min"` (`1m`), `"hour"` (`1H`), or
#'   `"day"` (`1D`). Defaults to `"min"`.
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`
#'   with duplicates removed. Returns `NULL` on error or when no data is
#'   available.
#'
#' @examples
#' \dontrun{
#' fetch_okx_range(
#'   "BTC-USDT",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
fetch_okx_range <- function(market, from, to, unit = "min") {
  sym <- okx_trading_pairs(market)
  if (is.null(sym)) { message("OKX: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  bar_str <- switch(unit, "min" = "1m", "hour" = "1H", "day" = "1D",
                    stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  from_ms  <- as.numeric(from) * 1000
  to_ms    <- as.numeric(to)   * 1000
  after_ms <- round(to_ms) + 1
  all_rows <- list()
  tryCatch({
    repeat {
      url <- paste0("https://www.okx.com/api/v5/market/history-candles",
                    "?instId=", sym, "&bar=", bar_str,
                    "&limit=100&after=", round(after_ms))
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(30))
      if (status_code(res) != 200) break

      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
      if (is.null(parsed$data) || length(parsed$data) == 0) break

      df <- bind_rows(lapply(parsed$data, function(r) data.frame(
        time_kst      = as.POSIXct(as.numeric(r[[1]]) / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
        opening_price = as.numeric(r[[2]]),
        high_price    = as.numeric(r[[3]]),
        low_price     = as.numeric(r[[4]]),
        trade_price   = as.numeric(r[[5]]),
        volume        = as.numeric(r[[6]]),
        stringsAsFactors = FALSE
      )))
      all_rows <- c(all_rows, list(df))

      oldest_ms <- min(as.numeric(df$time_kst)) * 1000
      if (oldest_ms <= from_ms || nrow(df) < 100) break
      after_ms <- oldest_ms
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("OKX \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from OKX
#'
#' @description
#' Returns the latest ticker snapshot for one or more markets from the OKX
#' public quotation API.
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-USDT"`, `c("BTC-USDT", "ETH-USDT")`).
#' @param inst_type Character. Instrument type: `"SPOT"`, `"SWAP"`, `"FUTURES"`,
#'   or `"OPTION"`. Defaults to `"SPOT"`.
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct ticker timestamp in KST}
#'     \item{trade_price}{Most recent trade price}
#'     \item{opening_price}{Rolling 24-hour open price}
#'     \item{high_price}{Rolling 24-hour high price}
#'     \item{low_price}{Rolling 24-hour low price}
#'     \item{prev_closing_price}{Start-of-day price at UTC+8 midnight (`sodUtc8`)}
#'     \item{change}{Direction vs. `sodUtc8`: `"RISE"`, `"FALL"`, or `"EVEN"`}
#'     \item{signed_change_rate}{Signed rate of change vs. `sodUtc8`}
#'     \item{acc_trade_volume_24h}{24-hour volume in base currency}
#'     \item{acc_trade_price_24h}{24-hour volume in quote currency}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_okx_prices("BTC-USDT")
#' get_okx_prices(c("BTC-USDT", "ETH-USDT"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows
#' @export
get_okx_prices <- function(market, inst_type = "SPOT") {
  syms <- vapply(market, function(m) {
    s <- okx_trading_pairs(m, inst_type = inst_type)
    if (is.null(s)) { message("OKX: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  valid_idx <- !is.na(syms)
  if (!any(valid_idx)) return(NULL)
  valid_syms    <- syms[valid_idx]
  valid_markets <- market[valid_idx]

  rows <- lapply(seq_along(valid_syms), function(i) {
    sym <- valid_syms[i]
    mkt <- valid_markets[i]
    url <- paste0("https://www.okx.com/api/v5/market/ticker?instId=", sym)
    tryCatch({
      res <- GET(url,
                 add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(10))
      if (status_code(res) != 200) {
        message("OKX HTTP \uc624\ub958: ", status_code(res), " / ", mkt); return(NULL)
      }
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(parsed$code) || parsed$code != "0" || length(parsed$data) == 0) {
        message("OKX API \uc624\ub958: ", parsed$msg, " / ", mkt); return(NULL)
      }
      d <- parsed$data[1, ]

      last <- as.numeric(d$last)
      sod  <- as.numeric(d$sodUtc8)
      diff <- last - sod
      chg  <- ifelse(diff > 0, "RISE", ifelse(diff < 0, "FALL", "EVEN"))
      rate <- if (!is.na(sod) && sod != 0) diff / sod else NA_real_

      data.frame(
        market               = mkt,
        time_kst             = as.POSIXct(as.numeric(d$ts) / 1000,
                                          origin = "1970-01-01", tz = "Asia/Seoul"),
        trade_price          = last,
        opening_price        = as.numeric(d$open24h),
        high_price           = as.numeric(d$high24h),
        low_price            = as.numeric(d$low24h),
        prev_closing_price   = sod,
        change               = chg,
        signed_change_rate   = rate,
        acc_trade_volume_24h = as.numeric(d$vol24h),
        acc_trade_price_24h  = as.numeric(d$volCcy24h),
        stringsAsFactors = FALSE
      )
    }, error = function(e) { message("OKX \uc624\ub958 (", mkt, "): ", e$message); NULL })
  })

  result <- bind_rows(rows)
  if (nrow(result) == 0) return(NULL)
  result
}


#' Fetch trade tick data from OKX over a date range
#'
#' @description
#' Paginates through the OKX historical trades endpoint
#' (`/api/v5/market/history-trades`) to retrieve all trades between `from`
#' and `to`. Uses `after` (tradeId) cursor-based pagination.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USDT"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#'
#' @return A [data.frame] with columns `time_kst`, `trade_price`, `volume`,
#'   `ask_bid` (`"ASK"` = seller-initiated / `"BID"` = buyer-initiated),
#'   `sequential_id`, sorted by `time_kst`. Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_okx_trades(
#'   "BTC-USDT",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 00:05:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
get_okx_trades <- function(market, from, to) {
  sym <- okx_trading_pairs(market)
  if (is.null(sym)) { message("OKX: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  all_rows <- list()
  after    <- NULL
  tryCatch({
    repeat {
      url <- paste0("https://www.okx.com/api/v5/market/history-trades",
                    "?instId=", sym, "&limit=100",
                    if (!is.null(after)) paste0("&after=", after) else "")
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(15))
      if (status_code(res) != 200) break
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(parsed$code) || parsed$code != "0") break
      raw <- parsed$data
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break
      df <- data.frame(
        time_kst      = as.POSIXct(as.numeric(raw$ts) / 1000,
                                   origin = "1970-01-01", tz = "Asia/Seoul"),
        trade_price   = as.numeric(raw$px),
        volume        = as.numeric(raw$sz),
        ask_bid       = ifelse(raw$side == "buy", "BID", "ASK"),
        sequential_id = as.numeric(raw$tradeId),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))
      oldest <- min(df$time_kst)
      if (oldest <= from || nrow(df) < 100) break
      after <- min(df$sequential_id)
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("OKX \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}
