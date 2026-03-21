.korbit_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from Korbit
#'
#' @description
#' Fetches all available (launched) trading pairs from the Korbit exchange API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-KRW"`) is supplied,
#'   returns the matching exchange-native symbol.
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native symbol string (e.g., `"btc_krw"`) or
#'   `NULL` if not found.
#'
#' @examples
#' \dontrun{
#' korbit_trading_pairs()
#' korbit_trading_pairs(market = "BTC-KRW")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter transmute
#' @importFrom stringr str_extract str_to_upper
#' @export
korbit_trading_pairs <- function(market = NULL) {
  if (!exists("pairs", envir = .korbit_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.korbit.co.kr/v2/currencyPairs",
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\ucf54\ube57 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        `[[`("data") %>%
        filter(status == "launched") %>%
        transmute(
          exchange = "korbit",
          asset    = str_extract(symbol, "^\\w+(?=\\_)") %>% str_to_upper(),
          quote    = str_extract(symbol, "(?<=\\_)\\w+$") %>% str_to_upper(),
          symbol   = symbol,
          market   = paste(asset, quote, sep = "-")
        )
      assign("pairs", df, envir = .korbit_pairs_cache)
    }, error = function(e) { message("\ucf54\ube57 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .korbit_pairs_cache)) return(NULL)
  .pick_symbol(.korbit_pairs_cache$pairs, market, "\ucf54\ube57")
}


#' Fetch recent 1-minute OHLCV candles from Korbit
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Korbit exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of candles to retrieve (default `200`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_korbit("BTC-KRW", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
fetch_korbit <- function(market, count = 200) {
  sym <- korbit_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\ube57: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.korbit.co.kr/v2/candles",
    "?symbol=", sym, "&interval=1&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ucf54\ube57 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$success) || !isTRUE(parsed$success)) {
      message("\ucf54\ube57 API \uc624\ub958 / ", market)
      return(NULL)
    }
    d <- parsed$data
    if (is.null(d) || nrow(d) == 0) return(NULL)
    data.frame(
      time_kst      = as.POSIXct(as.numeric(d$timestamp) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(d$open),
      high_price    = as.numeric(d$high),
      low_price     = as.numeric(d$low),
      trade_price   = as.numeric(d$close),
      volume        = as.numeric(d$volume),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("\ucf54\ube57 \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch recent trade tick data from Korbit
#'
#' @description
#' Returns the most recent trades for a given market from the Korbit public
#' API (up to 500 trades). Korbit's public endpoint does not support
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
#' get_korbit_trades(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 09:05:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter distinct arrange
#' @export
get_korbit_trades <- function(market, from, to) {
  sym <- korbit_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\ube57: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0("https://api.korbit.co.kr/v2/trades?symbol=", sym, "&limit=500")
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(15))
    if (status_code(res) != 200) {
      message("\ucf54\ube57 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$success) || !isTRUE(parsed$success)) {
      message("\ucf54\ube57 API \uc624\ub958 / ", market)
      return(NULL)
    }
    d <- parsed$data
    if (is.null(d) || nrow(d) == 0) return(NULL)
    df <- data.frame(
      time_kst      = as.POSIXct(as.numeric(d$timestamp) / 1000,
                                 origin = "1970-01-01", tz = "Asia/Seoul"),
      trade_price   = as.numeric(d$price),
      volume        = as.numeric(d$qty),
      ask_bid       = ifelse(d$isBuyerTaker, "BID", "ASK"),
      sequential_id = as.numeric(d$tradeId),
      stringsAsFactors = FALSE
    )
    df %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ucf54\ube57 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from Korbit
#'
#' @description
#' Returns the latest ticker snapshot for one or more markets from the Korbit
#' public quotation API.
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-KRW"`, `c("BTC-KRW", "ETH-KRW")`).
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct timestamp of last trade in KST}
#'     \item{trade_price}{Most recent trade price (current price)}
#'     \item{opening_price}{24-hour open price}
#'     \item{high_price}{24-hour high price}
#'     \item{low_price}{24-hour low price}
#'     \item{prev_closing_price}{Previous close price}
#'     \item{change}{Direction vs. previous close: `"RISE"`, `"FALL"`, or `"EVEN"`}
#'     \item{signed_change_rate}{Signed rate of change from previous close}
#'     \item{acc_trade_volume_24h}{24-hour cumulative trade volume}
#'     \item{acc_trade_price_24h}{24-hour cumulative trade value in quote currency}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_korbit_prices("BTC-KRW")
#' get_korbit_prices(c("BTC-KRW", "ETH-KRW"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @export
get_korbit_prices <- function(market) {
  syms <- vapply(market, function(m) {
    s <- korbit_trading_pairs(m)
    if (is.null(s)) { message("\ucf54\ube57: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  valid <- !is.na(syms)
  if (!any(valid)) return(NULL)

  url <- paste0(
    "https://api.korbit.co.kr/v2/tickers",
    "?symbol=", paste(syms[valid], collapse = ",")
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ucf54\ube57 HTTP \uc624\ub958: ", status_code(res)); return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$success) || !isTRUE(parsed$success)) {
      message("\ucf54\ube57 API \uc624\ub958"); return(NULL)
    }
    d <- parsed$data
    if (is.null(d) || nrow(d) == 0) return(NULL)

    chg_val <- as.numeric(d$priceChange)
    chg <- ifelse(chg_val > 0, "RISE", ifelse(chg_val < 0, "FALL", "EVEN"))

    result <- data.frame(
      market               = toupper(gsub("_", "-", sub("^(.+)_(.+)$", "\\1-\\2", d$symbol))),
      time_kst             = as.POSIXct(as.numeric(d$lastTradedAt) / 1000,
                                        origin = "1970-01-01", tz = "Asia/Seoul"),
      trade_price          = as.numeric(d$close),
      opening_price        = as.numeric(d$open),
      high_price           = as.numeric(d$high),
      low_price            = as.numeric(d$low),
      prev_closing_price   = as.numeric(d$prevClose),
      change               = chg,
      signed_change_rate   = as.numeric(d$priceChangePercent) / 100,
      acc_trade_volume_24h = as.numeric(d$volume),
      acc_trade_price_24h  = as.numeric(d$quoteVolume),
      stringsAsFactors = FALSE
    )
    result[match(market[valid], result$market), ]
  }, error = function(e) { message("\ucf54\ube57 \uc624\ub958: ", e$message); NULL })
}


#' Get real-time orderbook (호가) data from Korbit
#'
#' @description
#' Retrieves the current orderbook for a given market from the Korbit public
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
#' get_korbit_orderbook("BTC-KRW")
#' get_korbit_orderbook("ETH-KRW", count = 5)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
get_korbit_orderbook <- function(market, count = 30) {
  sym <- korbit_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\ube57: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.korbit.co.kr/v2/orderbook",
    "?symbol=", sym, "&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ucf54\ube57 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$success) || !isTRUE(parsed$success)) {
      message("\ucf54\ube57 API \uc624\ub958 / ", market)
      return(NULL)
    }
    d    <- parsed$data
    asks <- d$asks
    bids <- d$bids
    n    <- min(nrow(asks), nrow(bids), count)
    asks <- asks[seq_len(n), ]
    bids <- bids[seq_len(n), ]
    data.frame(
      market         = market,
      timestamp      = as.POSIXct(as.numeric(d$timestamp) / 1000,
                                  origin = "1970-01-01", tz = "Asia/Seoul"),
      ask_price      = as.numeric(asks$price),
      bid_price      = as.numeric(bids$price),
      ask_size       = as.numeric(asks$qty),
      bid_size       = as.numeric(bids$qty),
      total_ask_size = sum(as.numeric(d$asks$qty)),
      total_bid_size = sum(as.numeric(d$bids$qty)),
      level          = 0L,
      stringsAsFactors = FALSE
    ) %>% arrange(ask_price)
  }, error = function(e) { message("\ucf54\ube57 \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Korbit
#'
#' @description
#' Paginates through the Korbit candles endpoint to retrieve all bars between
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
#' fetch_korbit_range(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
fetch_korbit_range <- function(market, from, to, unit = "min") {
  sym <- korbit_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\ube57: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  interval_str <- switch(unit, "min" = "1", "hour" = "60", "day" = "1D",
                          stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  step_ms    <- switch(unit, "min" = 60000, "hour" = 3600000, "day" = 86400000)
  cur_before <- round(as.numeric(to) * 1000) + step_ms
  from_ms    <- round(as.numeric(from) * 1000)
  all_rows   <- list()
  tryCatch({
    repeat {
      url <- paste0("https://api.korbit.co.kr/v2/candles",
                    "?symbol=", sym,
                    "&interval=", interval_str,
                    "&limit=200",
                    "&before=", cur_before)
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(15))
      if (status_code(res) != 200) break
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(parsed$success) || !isTRUE(parsed$success)) break
      d <- parsed$data
      if (is.null(d) || nrow(d) == 0) break

      df <- data.frame(
        time_kst      = as.POSIXct(as.numeric(d$timestamp) / 1000,
                                   origin = "1970-01-01", tz = "Asia/Seoul"),
        opening_price = as.numeric(d$open),
        high_price    = as.numeric(d$high),
        low_price     = as.numeric(d$low),
        trade_price   = as.numeric(d$close),
        volume        = as.numeric(d$volume),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))

      oldest_ms <- min(as.numeric(d$timestamp))
      if (oldest_ms <= from_ms || nrow(d) < 200) break
      cur_before <- oldest_ms
      Sys.sleep(0.15)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ucf54\ube57 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}
