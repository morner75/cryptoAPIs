.binance_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from Binance
#'
#' @description
#' Fetches all actively trading spot pairs from the Binance exchange API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-USDT"`) is supplied,
#'   returns the matching exchange-native symbol (e.g., `"BTCUSDT"`).
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error.
#'
#' @examples
#' \dontrun{
#' binance_trading_pairs()
#' binance_trading_pairs(market = "BTC-USDT")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter transmute
#' @export
binance_trading_pairs <- function(market = NULL) {
  if (!exists("pairs", envir = .binance_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.binance.com/api/v3/exchangeInfo",
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\ubc14\uc774\ub0b8\uc2a4 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        `[[`("symbols") %>%
        filter(status == "TRADING") %>%
        transmute(
          exchange = "binance",
          asset    = baseAsset,
          quote    = quoteAsset,
          symbol   = symbol,
          market   = paste(asset, quote, sep = "-")
        )
      assign("pairs", df, envir = .binance_pairs_cache)
    }, error = function(e) { message("\ubc14\uc774\ub0b8\uc2a4 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .binance_pairs_cache)) return(NULL)
  .pick_symbol(.binance_pairs_cache$pairs, market, "\ubc14\uc774\ub0b8\uc2a4")
}


#' Fetch recent 1-minute OHLCV candles from Binance
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Binance exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USDT"`).
#' @param count Integer. Number of candles to retrieve (default `200`, max `1000`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_binance("BTC-USDT", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
fetch_binance <- function(market, count = 200) {
  sym <- binance_trading_pairs(market)
  if (is.null(sym)) { message("\ubc14\uc774\ub0b8\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.binance.com/api/v3/klines",
    "?symbol=", sym, "&interval=1m&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ubc14\uc774\ub0b8\uc2a4 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
    if (is.null(raw) || length(raw) == 0) return(NULL)
    m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
    data.frame(
      time_kst      = as.POSIXct(as.numeric(m[, 1]) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(m[, 2]),
      high_price    = as.numeric(m[, 3]),
      low_price     = as.numeric(m[, 4]),
      trade_price   = as.numeric(m[, 5]),
      volume        = as.numeric(m[, 6]),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("\ubc14\uc774\ub0b8\uc2a4 \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Binance
#'
#' @description
#' Paginates through the Binance klines endpoint to retrieve all bars between
#' `from` and `to`.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USDT"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#' @param unit Character. Candle unit: `"min"`, `"hour"`, or `"day"`.
#'   Defaults to `"min"`.
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`
#'   with duplicates removed. Returns `NULL` on error or when no data is
#'   available.
#'
#' @examples
#' \dontrun{
#' fetch_binance_range(
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
fetch_binance_range <- function(market, from, to, unit = "min") {
  sym <- binance_trading_pairs(market)
  if (is.null(sym)) { message("\ubc14\uc774\ub0b8\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  interval_str <- switch(unit, "min" = "1m", "hour" = "1h", "day" = "1d",
                          stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  start_ms <- as.numeric(from) * 1000
  end_ms   <- as.numeric(to)   * 1000
  all_rows <- list()
  tryCatch({
    repeat {
      url <- paste0("https://api.binance.com/api/v3/klines",
                    "?symbol=", sym, "&interval=", interval_str,
                    "&startTime=", round(start_ms), "&endTime=", round(end_ms),
                    "&limit=1000")
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(30))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
      if (is.null(raw) || length(raw) == 0) break

      m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
      df <- data.frame(
        time_kst      = as.POSIXct(as.numeric(m[, 1]) / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
        opening_price = as.numeric(m[, 2]),
        high_price    = as.numeric(m[, 3]),
        low_price     = as.numeric(m[, 4]),
        trade_price   = as.numeric(m[, 5]),
        volume        = as.numeric(m[, 6]),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))

      last_ms <- as.numeric(m[nrow(m), 1])
      if (nrow(m) < 1000 || last_ms >= end_ms) break
      start_ms <- last_ms + 1
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ubc14\uc774\ub0b8\uc2a4 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from Binance
#'
#' @description
#' Returns the latest 24-hour rolling ticker snapshot for one or more markets
#' from the Binance public quotation API.
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-USDT"`, `c("BTC-USDT", "ETH-USDT")`).
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct close time of the 24h window in KST}
#'     \item{trade_price}{Most recent trade price}
#'     \item{opening_price}{24-hour open price}
#'     \item{high_price}{24-hour high price}
#'     \item{low_price}{24-hour low price}
#'     \item{prev_closing_price}{Previous close price}
#'     \item{change}{Direction vs. previous close: `"RISE"`, `"FALL"`, or `"EVEN"`}
#'     \item{signed_change_rate}{Signed rate of change from previous close}
#'     \item{acc_trade_volume_24h}{24-hour cumulative base asset volume}
#'     \item{acc_trade_price_24h}{24-hour cumulative quote asset volume}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_binance_prices("BTC-USDT")
#' get_binance_prices(c("BTC-USDT", "ETH-USDT"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom dplyr %>%
#' @export
get_binance_prices <- function(market) {
  syms <- vapply(market, function(m) {
    s <- binance_trading_pairs(m)
    if (is.null(s)) { message("\ubc14\uc774\ub0b8\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  valid_idx <- !is.na(syms)
  if (!any(valid_idx)) return(NULL)
  valid_syms    <- syms[valid_idx]
  valid_markets <- market[valid_idx]

  url <- paste0(
    "https://api.binance.com/api/v3/ticker/24hr",
    "?symbols=", URLencode(toJSON(valid_syms, auto_unbox = FALSE), reserved = FALSE)
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ubc14\uc774\ub0b8\uc2a4 HTTP \uc624\ub958: ", status_code(res)); return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(raw) || nrow(raw) == 0) return(NULL)

    chg_val <- as.numeric(raw$priceChange)
    chg <- ifelse(chg_val > 0, "RISE", ifelse(chg_val < 0, "FALL", "EVEN"))

    result <- data.frame(
      market               = valid_markets[match(raw$symbol, valid_syms)],
      time_kst             = as.POSIXct(as.numeric(raw$closeTime) / 1000,
                                        origin = "1970-01-01", tz = "Asia/Seoul"),
      trade_price          = as.numeric(raw$lastPrice),
      opening_price        = as.numeric(raw$openPrice),
      high_price           = as.numeric(raw$highPrice),
      low_price            = as.numeric(raw$lowPrice),
      prev_closing_price   = as.numeric(raw$prevClosePrice),
      change               = chg,
      signed_change_rate   = as.numeric(raw$priceChangePercent) / 100,
      acc_trade_volume_24h = as.numeric(raw$volume),
      acc_trade_price_24h  = as.numeric(raw$quoteVolume),
      stringsAsFactors = FALSE
    )
    result[match(valid_markets, result$market), ]
  }, error = function(e) { message("\ubc14\uc774\ub0b8\uc2a4 \uc624\ub958: ", e$message); NULL })
}


#' Fetch trade tick data from Binance over a date range
#'
#' @description
#' Paginates through the Binance aggregate trades endpoint (`/api/v3/aggTrades`)
#' to retrieve all trades between `from` and `to`. Uses `fromId`-based
#' pagination after the first page.
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
#' get_binance_trades(
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
get_binance_trades <- function(market, from, to) {
  sym <- binance_trading_pairs(market)
  if (is.null(sym)) { message("\ubc14\uc774\ub0b8\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  from_ms  <- round(as.numeric(from) * 1000)
  to_ms    <- round(as.numeric(to)   * 1000)
  all_rows <- list()
  from_id  <- NULL
  tryCatch({
    repeat {
      if (is.null(from_id)) {
        url <- paste0("https://api.binance.com/api/v3/aggTrades",
                      "?symbol=", sym,
                      "&startTime=", from_ms, "&endTime=", to_ms, "&limit=1000")
      } else {
        url <- paste0("https://api.binance.com/api/v3/aggTrades",
                      "?symbol=", sym,
                      "&fromId=", from_id, "&endTime=", to_ms, "&limit=1000")
      }
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(15))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break
      df <- data.frame(
        time_kst      = as.POSIXct(as.numeric(raw$T) / 1000,
                                   origin = "1970-01-01", tz = "Asia/Seoul"),
        trade_price   = as.numeric(raw$p),
        volume        = as.numeric(raw$q),
        ask_bid       = ifelse(raw$m, "ASK", "BID"),
        sequential_id = as.numeric(raw$a),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))
      if (nrow(df) < 1000) break
      from_id <- max(df$sequential_id) + 1
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ubc14\uc774\ub0b8\uc2a4 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}
