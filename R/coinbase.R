.coinbase_pairs_cache <- new.env(parent = emptyenv())

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
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native symbol string (or `NULL` if not
#'   found).
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
  if (!exists("pairs", envir = .coinbase_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.exchange.coinbase.com/products",
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\ucf54\uc778\ubca0\uc774\uc2a4 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
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
      assign("pairs", df, envir = .coinbase_pairs_cache)
    }, error = function(e) { message("\ucf54\uc778\ubca0\uc774\uc2a4 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .coinbase_pairs_cache)) return(NULL)
  .pick_symbol(.coinbase_pairs_cache$pairs, market, "\ucf54\uc778\ubca0\uc774\uc2a4")
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
  if (is.null(sym)) { message("\ucf54\uc778\ubca0\uc774\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
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
      message("\ucf54\uc778\ubca0\uc774\uc2a4 HTTP \uc624\ub958: ", status_code(res), " / ", market)
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
  }, error = function(e) { message("\ucf54\uc778\ubca0\uc774\uc2a4 \uc624\ub958 (", market, "): ", e$message); NULL })
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
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`
#'   with duplicates removed. Returns `NULL` on error or when no data is
#'   available.
#'
#' @examples
#' \dontrun{
#' fetch_coinbase_range(
#'   "BTC-USD",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
fetch_coinbase_range <- function(market, from, to, unit = "min") {
  sym <- coinbase_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\uc778\ubca0\uc774\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  granularity <- switch(unit, "min" = 60L, "hour" = 3600L, "day" = 86400L,
                         stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
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
  }, error = function(e) { message("\ucf54\uc778\ubca0\uc774\uc2a4 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from Coinbase
#'
#' @description
#' Returns the latest ticker snapshot for one or more markets from the Coinbase
#' Exchange public API. Combines the per-product `/ticker` endpoint (current
#' price, timestamp) with the `/stats` endpoint (24h OHLC, volume).
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-USD"`, `c("BTC-USD", "ETH-USD")`).
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct timestamp of the latest trade in KST}
#'     \item{trade_price}{Most recent trade price}
#'     \item{opening_price}{24-hour open price}
#'     \item{high_price}{24-hour high price}
#'     \item{low_price}{24-hour low price}
#'     \item{prev_closing_price}{`NA` — not provided by Coinbase Exchange API}
#'     \item{change}{Direction vs. 24h open: `"RISE"`, `"FALL"`, or `"EVEN"`}
#'     \item{signed_change_rate}{Signed rate of change vs. 24h open}
#'     \item{acc_trade_volume_24h}{24-hour cumulative trade volume}
#'     \item{acc_trade_price_24h}{`NA` — not provided by Coinbase Exchange API}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_coinbase_prices("BTC-USD")
#' get_coinbase_prices(c("BTC-USD", "ETH-USD"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows
#' @export
get_coinbase_prices <- function(market) {
  syms <- vapply(market, function(m) {
    s <- coinbase_trading_pairs(m)
    if (is.null(s)) { message("\ucf54\uc778\ubca0\uc774\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  valid_idx <- !is.na(syms)
  if (!any(valid_idx)) return(NULL)
  valid_syms    <- syms[valid_idx]
  valid_markets <- market[valid_idx]

  rows <- lapply(seq_along(valid_syms), function(i) {
    sym <- valid_syms[i]
    mkt <- valid_markets[i]
    base_url <- "https://api.exchange.coinbase.com/products/"
    hdrs <- add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json")
    tryCatch({
      r_tk <- GET(paste0(base_url, sym, "/ticker"), hdrs, timeout(10))
      r_st <- GET(paste0(base_url, sym, "/stats"),  hdrs, timeout(10))
      if (status_code(r_tk) != 200 || status_code(r_st) != 200) {
        message("\ucf54\uc778\ubca0\uc774\uc2a4 HTTP \uc624\ub958 / ", mkt); return(NULL)
      }
      tk <- fromJSON(content(r_tk, as = "text", encoding = "UTF-8"), flatten = TRUE)
      st <- fromJSON(content(r_st, as = "text", encoding = "UTF-8"), flatten = TRUE)

      price <- as.numeric(tk$price)
      open  <- as.numeric(st$open)
      diff  <- price - open
      chg   <- ifelse(diff > 0, "RISE", ifelse(diff < 0, "FALL", "EVEN"))
      rate  <- if (!is.na(open) && open != 0) diff / open else NA_real_

      data.frame(
        market               = mkt,
        time_kst             = as.POSIXct(tk$time, format = "%Y-%m-%dT%H:%M:%OSZ",
                                          tz = "UTC") |>
                                 (\(x) { attr(x, "tzone") <- "Asia/Seoul"; x })(),
        trade_price          = price,
        opening_price        = open,
        high_price           = as.numeric(st$high),
        low_price            = as.numeric(st$low),
        prev_closing_price   = NA_real_,
        change               = chg,
        signed_change_rate   = rate,
        acc_trade_volume_24h = as.numeric(st$volume),
        acc_trade_price_24h  = NA_real_,
        stringsAsFactors = FALSE
      )
    }, error = function(e) { message("\ucf54\uc778\ubca0\uc774\uc2a4 \uc624\ub958 (", mkt, "): ", e$message); NULL })
  })

  result <- bind_rows(rows)
  if (nrow(result) == 0) return(NULL)
  result
}


#' Fetch trade tick data from Coinbase over a date range
#'
#' @description
#' Paginates through the Coinbase Exchange trades endpoint
#' (`/products/{id}/trades`) to retrieve all trades between `from` and `to`.
#' Uses `before` (trade ID) cursor-based pagination.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-USD"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#'
#' @return A [data.frame] with columns `time_kst`, `trade_price`, `volume`,
#'   `ask_bid` (`"ASK"` = seller-initiated / `"BID"` = buyer-initiated),
#'   `sequential_id`, sorted by `time_kst`. Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_coinbase_trades(
#'   "BTC-USD",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 00:05:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
get_coinbase_trades <- function(market, from, to) {
  sym <- coinbase_trading_pairs(market)
  if (is.null(sym)) { message("\ucf54\uc778\ubca0\uc774\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  all_rows <- list()
  cursor   <- NULL
  tryCatch({
    repeat {
      # 'after=N' returns trades older than trade_id N (lower IDs = older trades)
      # 'before=N' returns trades newer than trade_id N — do NOT use for backward pagination
      url <- paste0("https://api.exchange.coinbase.com/products/", sym, "/trades?limit=100",
                    if (!is.null(cursor)) paste0("&after=", cursor) else "")
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(15))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break
      df <- data.frame(
        time_kst      = as.POSIXct(raw$time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
        trade_price   = as.numeric(raw$price),
        volume        = as.numeric(raw$size),
        ask_bid       = ifelse(raw$side == "buy", "BID", "ASK"),
        sequential_id = as.numeric(raw$trade_id),
        stringsAsFactors = FALSE
      )
      attr(df$time_kst, "tzone") <- "Asia/Seoul"
      all_rows <- c(all_rows, list(df))
      oldest <- min(df$time_kst)
      if (oldest <= from || nrow(df) < 100) break
      cursor <- min(df$sequential_id)
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ucf54\uc778\ubca0\uc774\uc2a4 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}
