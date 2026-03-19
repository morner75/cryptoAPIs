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
  tryCatch({
    res <- GET("https://api.binance.com/api/v3/exchangeInfo",
               accept("application/json"))
    if (status_code(res) != 200) {
      message("바이낸스 오류: HTTP ", status_code(res)); return(NULL)
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
    .pick_symbol(df, market, "바이낸스")
  }, error = function(e) { message("바이낸스 오류: ", e$message); NULL })
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
  if (is.null(sym)) { message("바이낸스: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.binance.com/api/v3/klines",
    "?symbol=", sym, "&interval=1m&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("바이낸스 HTTP 오류: ", status_code(res), " / ", market)
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
  }, error = function(e) { message("바이낸스 오류 (", market, "): ", e$message); NULL })
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
#' @return A [data.frame] sorted by `time_kst` with duplicates removed, or
#'   `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_binance_range(
#'   "BTC-USDT",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "UTC"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "UTC")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
fetch_binance_range <- function(market, from, to, unit = "min") {
  sym <- binance_trading_pairs(market)
  if (is.null(sym)) { message("바이낸스: symbol 조회 실패 (", market, ")"); return(NULL) }
  interval_str <- switch(unit, "min" = "1m", "hour" = "1h", "day" = "1d",
                          stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
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
  }, error = function(e) { message("바이낸스 범위 오류: ", e$message); NULL })
}
