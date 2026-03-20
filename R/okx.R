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
        message("OKX 오류: HTTP ", status_code(res)); return(NULL)
      }
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"))
      if (parsed$code != "0") {
        message("OKX 오류: ", parsed$msg); return(NULL)
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
    }, error = function(e) { message("OKX 오류: ", e$message); return(NULL) })
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
  if (is.null(sym)) { message("OKX: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0("https://www.okx.com/api/v5/market/candles",
                "?instId=", sym, "&bar=1m&limit=", min(count, 300))
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("OKX HTTP 오류: ", status_code(res), " / ", market); return(NULL)
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
  }, error = function(e) { message("OKX 오류 (", market, "): ", e$message); NULL })
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
  if (is.null(sym)) { message("OKX: symbol 조회 실패 (", market, ")"); return(NULL) }
  bar_str <- switch(unit, "min" = "1m", "hour" = "1H", "day" = "1D",
                    stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
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
  }, error = function(e) { message("OKX 범위 오류: ", e$message); NULL })
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
  if (is.null(sym)) { message("OKX: symbol 조회 실패 (", market, ")"); return(NULL) }
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
  }, error = function(e) { message("OKX 체결 오류: ", e$message); NULL })
}
