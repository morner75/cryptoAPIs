.coinone_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from Coinone
#'
#' @description
#' Fetches all available trading pairs from the Coinone exchange API for a
#' given quote currency.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-KRW"`) is supplied,
#'   returns the matching exchange-native symbol.
#' @param quote Character. Quote currency to query (default `"KRW"`).
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native symbol string (or `NULL` if not
#'   found).
#'
#' @examples
#' \dontrun{
#' coinone_trading_pairs()
#' coinone_trading_pairs(market = "BTC-KRW")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute
#' @importFrom stringr str_c
#' @export
coinone_trading_pairs <- function(market = NULL, quote = "KRW") {
  if (!exists(quote, envir = .coinone_pairs_cache)) {
    tryCatch({
      res <- GET(str_c("https://api.coinone.co.kr/public/v2/markets/", quote),
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("코인원 오류: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        `[[`("markets") %>%
        transmute(
          exchange = "coinone",
          asset    = target_currency,
          quote    = quote_currency,
          symbol   = target_currency,
          market   = paste(asset, quote, sep = "-")
        )
      assign(quote, df, envir = .coinone_pairs_cache)
    }, error = function(e) { message("코인원 오류: ", e$message); return(NULL) })
  }
  if (!exists(quote, envir = .coinone_pairs_cache)) return(NULL)
  .pick_symbol(get(quote, envir = .coinone_pairs_cache), market, "코인원")
}


#' Fetch recent 1-minute OHLCV candles from Coinone
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Coinone exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of candles to retrieve (default `200`, max `500`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_coinone("BTC-KRW", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @importFrom stringr str_extract
#' @export
fetch_coinone <- function(market, count = 200) {
  quote <- str_extract(market, "[^-]+$")
  sym   <- coinone_trading_pairs(market, quote = quote)
  if (is.null(sym)) { message("코인원: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.coinone.co.kr/public/v2/chart/", quote, "/", sym,
    "?interval=1m&size=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("코인원 HTTP 오류: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$result) || parsed$result != "success") {
      message("코인원 API 오류: ", parsed$error_code, " / ", market)
      return(NULL)
    }
    ch <- parsed$chart
    if (is.null(ch) || nrow(ch) == 0) return(NULL)
    data.frame(
      time_kst      = as.POSIXct(as.numeric(ch$timestamp) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(ch$open),
      high_price    = as.numeric(ch$high),
      low_price     = as.numeric(ch$low),
      trade_price   = as.numeric(ch$close),
      volume        = as.numeric(ch$target_volume),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("코인원 오류 (", market, "): ", e$message); NULL })
}


#' Fetch recent trade tick data from Coinone
#'
#' @description
#' Returns the most recent trades for a given market from the Coinone public
#' API (up to 200 trades). Coinone's public endpoint does not support
#' cursor-based pagination, so results are limited to the latest trades that
#' fall within `from`–`to`.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#'
#' @return A [data.frame] with columns `time_kst`, `trade_price`, `volume`,
#'   `ask_bid` (`"ASK"` = seller-initiated / `"BID"` = buyer-initiated),
#'   `sequential_id`, sorted by `time_kst`. Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_coinone_trades(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 09:05:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter distinct arrange
#' @importFrom stringr str_extract
#' @export
get_coinone_trades <- function(market, from, to) {
  quote_cur <- str_extract(market, "[^-]+$")
  sym <- coinone_trading_pairs(market, quote = quote_cur)
  if (is.null(sym)) { message("코인원: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0("https://api.coinone.co.kr/public/v2/trades/", quote_cur, "/", sym, "/?size=200")
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(15))
    if (status_code(res) != 200) return(NULL)
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$result) || parsed$result != "success") {
      message("코인원 API 오류: ", parsed$error_code, " / ", market)
      return(NULL)
    }
    tx <- parsed$transactions
    if (is.null(tx) || nrow(tx) == 0) return(NULL)
    df <- data.frame(
      time_kst      = as.POSIXct(as.numeric(tx$timestamp) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      trade_price   = as.numeric(tx$price),
      volume        = as.numeric(tx$qty),
      ask_bid       = ifelse(tx$is_seller_maker, "BID", "ASK"),
      sequential_id = as.numeric(tx$id),
      stringsAsFactors = FALSE
    )
    df %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("코인원 체결 오류: ", e$message); NULL })
}


#' Get real-time orderbook (호가) data from Coinone
#'
#' @description
#' Retrieves the current orderbook for a given market from the Coinone public
#' API. Returns one row per price level (ask/bid paired), sorted by ask price
#' ascending.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of price levels to retrieve (default `30`).
#'
#' @return A [data.frame] with columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`, e.g. `"BTC-KRW"`)}
#'     \item{timestamp}{POSIXct timestamp in KST}
#'     \item{ask_price}{Ask price at this level}
#'     \item{bid_price}{Bid price at this level}
#'     \item{ask_size}{Ask volume at this level}
#'     \item{bid_size}{Bid volume at this level}
#'     \item{total_ask_size}{Total ask volume across all levels}
#'     \item{total_bid_size}{Total bid volume across all levels}
#'     \item{level}{Price aggregation tier (always `0`)}
#'   }
#'   Returns `NULL` on error or when the market is not found.
#'
#' @examples
#' \dontrun{
#' get_coinone_orderbook("BTC-KRW")
#' get_coinone_orderbook("ETH-KRW", count = 5)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @importFrom stringr str_extract
#' @export
get_coinone_orderbook <- function(market, count = 30) {
  quote_cur <- str_extract(market, "[^-]+$")
  sym <- coinone_trading_pairs(market, quote = quote_cur)
  if (is.null(sym)) { message("코인원: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.coinone.co.kr/public/v2/orderbook/", quote_cur, "/", sym
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) return(NULL)
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$result) || parsed$result != "success") {
      message("코인원 API 오류: ", parsed$error_code, " / ", market)
      return(NULL)
    }
    asks <- parsed$asks
    bids <- parsed$bids
    n    <- min(nrow(asks), nrow(bids), count)
    asks <- asks[seq_len(n), ]
    bids <- bids[seq_len(n), ]
    data.frame(
      market         = market,
      timestamp      = as.POSIXct(as.numeric(parsed$timestamp) / 1000,
                                  origin = "1970-01-01", tz = "Asia/Seoul"),
      ask_price      = as.numeric(asks$price),
      bid_price      = as.numeric(bids$price),
      ask_size       = as.numeric(asks$qty),
      bid_size       = as.numeric(bids$qty),
      total_ask_size = sum(as.numeric(parsed$asks$qty)),
      total_bid_size = sum(as.numeric(parsed$bids$qty)),
      level          = 0L,
      stringsAsFactors = FALSE
    ) %>% arrange(ask_price)
  }, error = function(e) { message("코인원 오류: ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Coinone
#'
#' @description
#' Paginates through the Coinone chart endpoint to retrieve all bars between
#' `from` and `to`.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
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
#' fetch_coinone_range(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @importFrom stringr str_extract
#' @export
fetch_coinone_range <- function(market, from, to, unit = "min") {
  quote <- str_extract(market, "[^-]+$")
  sym   <- coinone_trading_pairs(market, quote = quote)
  if (is.null(sym)) { message("코인원: symbol 조회 실패 (", market, ")"); return(NULL) }
  interval_str <- switch(unit, "min" = "1m", "hour" = "1h", "day" = "1d",
                          stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
  cur_ts   <- round(as.numeric(to) * 1000)
  from_ms  <- round(as.numeric(from) * 1000)
  all_rows <- list()
  tryCatch({
    repeat {
      url <- paste0("https://api.coinone.co.kr/public/v2/chart/", quote, "/", sym,
                    "?interval=", interval_str, "&size=500&timestamp=", cur_ts)
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(15))
      if (status_code(res) != 200) break
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(parsed$result) || parsed$result != "success") break
      ch <- parsed$chart
      if (is.null(ch) || nrow(ch) == 0) break

      df <- data.frame(
        time_kst      = as.POSIXct(as.numeric(ch$timestamp) / 1000,
                                   origin = "1970-01-01", tz = "Asia/Seoul"),
        opening_price = as.numeric(ch$open),
        high_price    = as.numeric(ch$high),
        low_price     = as.numeric(ch$low),
        trade_price   = as.numeric(ch$close),
        volume        = as.numeric(ch$target_volume),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))

      oldest_ms <- min(as.numeric(ch$timestamp))
      if (oldest_ms <= from_ms || nrow(df) < 500) break
      cur_ts <- oldest_ms - 1
      Sys.sleep(0.15)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("코인원 범위 오류: ", e$message); NULL })
}
