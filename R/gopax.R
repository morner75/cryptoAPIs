.gopax_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from GOPAX
#'
#' @description
#' Fetches all available trading pairs from the GOPAX exchange API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-KRW"`) is supplied,
#'   returns the matching exchange-native symbol.
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native symbol string (or `NULL` if not
#'   found).
#'
#' @examples
#' \dontrun{
#' gopax_trading_pairs()
#' gopax_trading_pairs(market = "BTC-KRW")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute
#' @importFrom stringr str_c
#' @export
gopax_trading_pairs <- function(market = NULL) {
  if (!exists("pairs", envir = .gopax_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.gopax.co.kr/trading-pairs",
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\uace0\ud30d\uc2a4 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        transmute(
          exchange = "gopax",
          asset    = baseAsset,
          quote    = quoteAsset,
          symbol   = str_c(baseAsset, quoteAsset, sep = "-"),
          market   = paste(asset, quote, sep = "-")
        )
      assign("pairs", df, envir = .gopax_pairs_cache)
    }, error = function(e) { message("\uace0\ud30d\uc2a4 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .gopax_pairs_cache)) return(NULL)
  .pick_symbol(.gopax_pairs_cache$pairs, market, "\uace0\ud30d\uc2a4")
}


#' Fetch recent 1-minute OHLCV candles from GOPAX
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the GOPAX exchange.
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
#' fetch_gopax("BTC-KRW", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
fetch_gopax <- function(market, count = 200) {
  sym <- gopax_trading_pairs(market)
  if (is.null(sym)) { message("\uace0\ud30d\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  end_ms   <- round(as.numeric(Sys.time()) * 1000)
  start_ms <- end_ms - max(count * 60 * 60 * 1000, 24 * 60 * 60 * 1000)
  url <- paste0(
    "https://api.gopax.co.kr/trading-pairs/", sym,
    "/candles?start=", start_ms, "&end=", end_ms,
    "&interval=1&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\uace0\ud30d\uc2a4 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
    if (is.null(raw) || length(raw) == 0) return(NULL)
    m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
    if (ncol(m) < 6) { message("\uace0\ud30d\uc2a4 \uceec\ub7fc \ubd80\uc871: ", ncol(m), "\uac1c"); return(NULL) }
    data.frame(
      time_kst      = as.POSIXct(m[, 1] / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(m[, 4]),
      high_price    = as.numeric(m[, 3]),
      low_price     = as.numeric(m[, 2]),
      trade_price   = as.numeric(m[, 5]),
      volume        = as.numeric(m[, 6]),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("\uace0\ud30d\uc2a4 \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch trade tick data from GOPAX over a date range
#'
#' @description
#' Paginates through the GOPAX trades endpoint to retrieve all individual
#' trades between `from` and `to`. Uses cursor-based pagination via trade `id`
#' (`pastmax` parameter).
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
#' get_gopax_trades(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 09:05:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
get_gopax_trades <- function(market, from, to) {
  sym <- gopax_trading_pairs(market)
  if (is.null(sym)) { message("\uace0\ud30d\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  all_rows <- list()
  pastmax  <- NULL
  before_s <- round(as.numeric(to))
  tryCatch({
    repeat {
      url <- paste0("https://api.gopax.co.kr/trading-pairs/", sym, "/trades",
                    "?limit=100",
                    if (!is.null(pastmax)) paste0("&pastmax=", pastmax)
                    else paste0("&before=", before_s))
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(15))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break
      df <- data.frame(
        time_kst      = as.POSIXct(raw$time, format = "%Y-%m-%dT%H:%M:%OSZ",
                                   tz = "UTC"),
        trade_price   = as.numeric(raw$price),
        volume        = as.numeric(raw$amount),
        ask_bid       = ifelse(raw$side == "buy", "BID", "ASK"),
        sequential_id = as.numeric(raw$id),
        stringsAsFactors = FALSE
      )
      attr(df$time_kst, "tzone") <- "Asia/Seoul"
      all_rows <- c(all_rows, list(df))
      oldest <- min(df$time_kst)
      if (oldest <= from || nrow(df) < 100) break
      pastmax <- min(df$sequential_id)
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\uace0\ud30d\uc2a4 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from GOPAX
#'
#' @description
#' Returns the latest ticker snapshot for one or more markets from the GOPAX
#' public quotation API. Combines the per-pair `/ticker` endpoint (current
#' price, volumes, timestamp) with the bulk `/trading-pairs/stats` endpoint
#' (24h OHLC).
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-KRW"`, `c("BTC-KRW", "ETH-KRW")`).
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct timestamp of last ticker update in KST}
#'     \item{trade_price}{Current price}
#'     \item{opening_price}{24-hour open price}
#'     \item{high_price}{24-hour high price}
#'     \item{low_price}{24-hour low price}
#'     \item{prev_closing_price}{`NA` — not provided by GOPAX API}
#'     \item{change}{`NA` — not provided by GOPAX API}
#'     \item{signed_change_rate}{`NA` — not provided by GOPAX API}
#'     \item{acc_trade_volume_24h}{24-hour cumulative trade volume}
#'     \item{acc_trade_price_24h}{24-hour cumulative trade value in quote currency}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_gopax_prices("BTC-KRW")
#' get_gopax_prices(c("BTC-KRW", "ETH-KRW"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows
#' @export
get_gopax_prices <- function(market) {
  syms <- vapply(market, function(m) {
    s <- gopax_trading_pairs(m)
    if (is.null(s)) { message("\uace0\ud30d\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  valid_idx <- !is.na(syms)
  if (!any(valid_idx)) return(NULL)
  valid_syms    <- syms[valid_idx]
  valid_markets <- market[valid_idx]

  # Bulk OHLC stats (one call for all pairs)
  stats_map <- tryCatch({
    res <- GET("https://api.gopax.co.kr/trading-pairs/stats",
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) list()
    else {
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || !is.data.frame(raw)) list()
      else {
        stats <- raw[raw$name %in% valid_syms, ]
        setNames(
          lapply(seq_len(nrow(stats)), function(i) stats[i, ]),
          stats$name
        )
      }
    }
  }, error = function(e) { message("\uace0\ud30d\uc2a4 stats \uc624\ub958: ", e$message); list() })

  # Per-market ticker calls
  rows <- lapply(seq_along(valid_syms), function(i) {
    sym <- valid_syms[i]
    mkt <- valid_markets[i]
    url <- paste0("https://api.gopax.co.kr/trading-pairs/", sym, "/ticker")
    tryCatch({
      res <- GET(url,
                 add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
                 timeout(10))
      if (status_code(res) != 200) {
        message("\uace0\ud30d\uc2a4 HTTP \uc624\ub958: ", status_code(res), " / ", mkt)
        return(NULL)
      }
      tk <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      st <- stats_map[[sym]]
      data.frame(
        market               = mkt,
        time_kst             = as.POSIXct(tk$time, format = "%Y-%m-%dT%H:%M:%OSZ",
                                          tz = "UTC") |>
                                 (\(x) { attr(x, "tzone") <- "Asia/Seoul"; x })(),
        trade_price          = as.numeric(tk$price),
        opening_price        = if (!is.null(st)) as.numeric(st$open)  else NA_real_,
        high_price           = if (!is.null(st)) as.numeric(st$high)  else NA_real_,
        low_price            = if (!is.null(st)) as.numeric(st$low)   else NA_real_,
        prev_closing_price   = NA_real_,
        change               = NA_character_,
        signed_change_rate   = NA_real_,
        acc_trade_volume_24h = as.numeric(tk$volume),
        acc_trade_price_24h  = as.numeric(tk$quoteVolume),
        stringsAsFactors = FALSE
      )
    }, error = function(e) { message("\uace0\ud30d\uc2a4 \uc624\ub958 (", mkt, "): ", e$message); NULL })
  })

  result <- bind_rows(rows)
  if (nrow(result) == 0) return(NULL)
  result
}


#' Get real-time orderbook (호가) data from GOPAX
#'
#' @description
#' Retrieves the current orderbook for a given market from the GOPAX public
#' API. Returns one row per price level (ask/bid paired), sorted by ask price
#' ascending.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of price levels to retrieve (default `30`).
#' @param level Integer. Stored in the `level` output column (default `0`).
#'   GOPAX does not support price-level aggregation via the public API.
#'
#' @return A [data.frame] with columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`, e.g. `"BTC-KRW"`)}
#'     \item{timestamp}{POSIXct request time in KST (GOPAX does not return a timestamp)}
#'     \item{ask_price}{Ask price at this level}
#'     \item{bid_price}{Bid price at this level}
#'     \item{ask_size}{Ask volume at this level}
#'     \item{bid_size}{Bid volume at this level}
#'     \item{total_ask_size}{Total ask volume across all levels}
#'     \item{total_bid_size}{Total bid volume across all levels}
#'     \item{level}{Applied price aggregation tier}
#'   }
#'   Returns `NULL` on error or when the market is not found.
#'
#' @examples
#' \dontrun{
#' get_gopax_orderbook("BTC-KRW")
#' get_gopax_orderbook("ETH-KRW", count = 5)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
get_gopax_orderbook <- function(market, count = 30, level = 0) {
  sym <- gopax_trading_pairs(market)
  if (is.null(sym)) { message("\uace0\ud30d\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0("https://api.gopax.co.kr/trading-pairs/", sym, "/book")
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\uace0\ud30d\uc2a4 HTTP \uc624\ub958: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
    asks <- parsed$ask
    bids <- parsed$bid
    if (is.null(asks) || is.null(bids)) return(NULL)
    n    <- min(nrow(asks), nrow(bids), count)
    asks <- asks[seq_len(n), , drop = FALSE]
    bids <- bids[seq_len(n), , drop = FALSE]
    data.frame(
      market         = market,
      timestamp      = as.POSIXct(Sys.time(), tz = "Asia/Seoul"),
      ask_price      = as.numeric(asks[, 1]),
      bid_price      = as.numeric(bids[, 1]),
      ask_size       = as.numeric(asks[, 2]),
      bid_size       = as.numeric(bids[, 2]),
      total_ask_size = sum(as.numeric(parsed$ask[, 2])),
      total_bid_size = sum(as.numeric(parsed$bid[, 2])),
      level          = as.integer(level),
      stringsAsFactors = FALSE
    ) %>% arrange(ask_price)
  }, error = function(e) { message("\uace0\ud30d\uc2a4 \uc624\ub958 (", market, "): ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from GOPAX
#'
#' @description
#' Iterates over time chunks to retrieve all bars between `from` and `to`
#' from the GOPAX exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#' @param unit Character. Candle unit: `"min"` (1-minute), `"hour"` (30-minute
#'   approximation), or `"day"`. Defaults to `"min"`.
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`
#'   with duplicates removed. Returns `NULL` on error or when no data is
#'   available.
#'
#' @examples
#' \dontrun{
#' fetch_gopax_range(
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
fetch_gopax_range <- function(market, from, to, unit = "min") {
  sym <- gopax_trading_pairs(market)
  if (is.null(sym)) { message("\uace0\ud30d\uc2a4: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  interval_min <- switch(unit, "min" = 1L, "hour" = 30L, "day" = 1440L,
                          stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  chunk_ms <- 1024 * as.numeric(interval_min) * 60 * 1000
  all_rows <- list()
  tryCatch({
    cur_start <- round(as.numeric(from) * 1000)
    end_ms    <- round(as.numeric(to)   * 1000)
    repeat {
      cur_end <- min(cur_start + chunk_ms, end_ms)
      url <- paste0("https://api.gopax.co.kr/trading-pairs/", sym,
                    "/candles?start=", sprintf("%.0f", cur_start),
                    "&end=", sprintf("%.0f", cur_end),
                    "&interval=", interval_min, "&limit=1024")
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"), timeout(15))
      if (status_code(res) == 200) {
        raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
        if (!is.null(raw) && length(raw) > 0) {
          m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
          if (ncol(m) >= 6)
            all_rows <- c(all_rows, list(data.frame(
              time_kst      = as.POSIXct(m[, 1] / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
              opening_price = as.numeric(m[, 4]),
              high_price    = as.numeric(m[, 3]),
              low_price     = as.numeric(m[, 2]),
              trade_price   = as.numeric(m[, 5]),
              volume        = as.numeric(m[, 6]),
              stringsAsFactors = FALSE
            )))
        }
      }
      cur_start <- cur_end + 1
      if (cur_start > end_ms) break
      Sys.sleep(0.1)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\uace0\ud30d\uc2a4 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}
