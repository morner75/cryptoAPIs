#' Get trading pairs from Coinbase
#'
#' @description
#' Fetches all online trading pairs from the Coinbase Exchange (Advanced Trade)
#' API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-USD"`) is supplied,
#'   returns the matching exchange-native symbol.
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error.
#'
#' @examples
#' \dontrun{
#' coinbase_trading_pairs()
#' coinbase_trading_pairs(market = "BTC-USD")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter transmute
#' @export
coinbase_trading_pairs <- function(market = NULL) {
  tryCatch({
    res <- GET("https://api.exchange.coinbase.com/products",
               accept("application/json"))
    if (status_code(res) != 200) {
      message("코인베이스 오류: HTTP ", status_code(res)); return(NULL)
    }
    df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
      filter(status == "online") %>%
      transmute(
        exchange = "coinbase",
        asset    = base_currency,
        quote    = quote_currency,
        symbol   = id,
        market   = paste(asset, quote, sep = "-")
      )
    .pick_symbol(df, market, "코인베이스")
  }, error = function(e) { message("코인베이스 오류: ", e$message); NULL })
}


#' Fetch recent 1-minute OHLCV candles from Coinbase
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Coinbase Exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USD"`).
#' @param count Integer. Number of candles to retrieve (default `200`, max `300`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_coinbase("BTC-USD", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
fetch_coinbase <- function(market, count = 200) {
  sym <- coinbase_trading_pairs(market)
  if (is.null(sym)) { message("코인베이스: symbol 조회 실패 (", market, ")"); return(NULL) }
  end_sec   <- as.integer(Sys.time())
  start_sec <- end_sec - count * 60
  url <- paste0(
    "https://api.exchange.coinbase.com/products/", sym, "/candles",
    "?granularity=60&start=", start_sec, "&end=", end_sec
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("코인베이스 HTTP 오류: ", status_code(res), " / ", market)
      return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
    if (is.null(raw) || length(raw) == 0) return(NULL)
    m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
    data.frame(
      time_kst      = as.POSIXct(as.numeric(m[, 1]),
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(m[, 4]),
      high_price    = as.numeric(m[, 3]),
      low_price     = as.numeric(m[, 2]),
      trade_price   = as.numeric(m[, 5]),
      volume        = as.numeric(m[, 6]),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("코인베이스 오류 (", market, "): ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Coinbase
#'
#' @description
#' Iterates over time chunks (max 300 bars each) to retrieve all bars between
#' `from` and `to` from the Coinbase Exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USD"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#' @param unit Character. Candle unit: `"min"` (60 s), `"hour"` (3600 s), or
#'   `"day"` (86400 s). Defaults to `"min"`.
#'
#' @return A [data.frame] sorted by `time_kst` with duplicates removed, or
#'   `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_coinbase_range(
#'   "BTC-USD",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "UTC"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "UTC")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
fetch_coinbase_range <- function(market, from, to, unit = "min") {
  sym <- coinbase_trading_pairs(market)
  if (is.null(sym)) { message("코인베이스: symbol 조회 실패 (", market, ")"); return(NULL) }
  granularity <- switch(unit, "min" = 60L, "hour" = 3600L, "day" = 86400L,
                         stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
  chunk     <- 300L * granularity
  start_sec <- as.integer(from)
  end_sec   <- as.integer(to)
  all_rows  <- list()
  tryCatch({
    cur_start <- start_sec
    repeat {
      cur_end <- min(cur_start + chunk, end_sec)
      url <- paste0("https://api.exchange.coinbase.com/products/", sym, "/candles",
                    "?granularity=", granularity,
                    "&start=", cur_start, "&end=", cur_end)
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(30))
      if (status_code(res) != 200) { cur_start <- cur_end + 1; if (cur_start > end_sec) break; next }

      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
      if (!is.null(raw) && length(raw) > 0) {
        m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
        all_rows <- c(all_rows, list(data.frame(
          time_kst      = as.POSIXct(as.numeric(m[, 1]), origin = "1970-01-01", tz = "Asia/Seoul"),
          opening_price = as.numeric(m[, 4]),
          high_price    = as.numeric(m[, 3]),
          low_price     = as.numeric(m[, 2]),
          trade_price   = as.numeric(m[, 5]),
          volume        = as.numeric(m[, 6]),
          stringsAsFactors = FALSE
        )))
      }
      cur_start <- cur_end + 1
      if (cur_start > end_sec) break
      Sys.sleep(0.15)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("코인베이스 범위 오류: ", e$message); NULL })
}
