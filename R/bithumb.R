.bithumb_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from Bithumb
#'
#' @description
#' Fetches all available trading pairs from the Bithumb exchange API.
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
#' bithumb_trading_pairs()
#' bithumb_trading_pairs(market = "BTC-KRW")
#' }
#'
#' @importFrom httr GET content status_code accept content_type
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute
#' @importFrom stringr str_extract
#' @export
bithumb_trading_pairs <- function(market = NULL) {
  if (!exists("pairs", envir = .bithumb_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.bithumb.com/v1/market/all",
                 query   = list(isDetails = "false"),
                 content_type("application/octet-stream"),
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\ube57\uc378 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        transmute(
          exchange = "bithumb",
          asset    = str_extract(market, "(?<=\\-)\\w+$"),
          quote    = str_extract(market, "^\\w+(?=\\-)"),
          symbol   = market,
          market   = paste(asset, quote, sep = "-")
        )
      assign("pairs", df, envir = .bithumb_pairs_cache)
    }, error = function(e) { message("\ube57\uc378 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .bithumb_pairs_cache)) return(NULL)
  .pick_symbol(.bithumb_pairs_cache$pairs, market, "\ube57\uc378")
}


#' Fetch recent 1-minute OHLCV candles from Bithumb
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Bithumb exchange.
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
#' fetch_bithumb("BTC-KRW", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute arrange
#' @importFrom lubridate as_datetime
#' @export
fetch_bithumb <- function(market, count = 200) {
  sym <- bithumb_trading_pairs(market)
  if (is.null(sym)) { message("\ube57\uc378: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.bithumb.com/v1/candles/minutes/1?market=", sym,
    "&count=", count
  )
  tryCatch({
    res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0"), timeout(10))
    if (status_code(res) != 200) return(NULL)
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (!is.data.frame(parsed)) {
      message("\ube57\uc378 \uc751\ub2f5 \ud30c\uc2f1 \uc624\ub958 (", market, "): \ub370\uc774\ud130\ud504\ub808\uc784\uc774 \uc544\ub2d8")
      return(NULL)
    }
    parsed %>%
      transmute(
        time_kst      = as_datetime(candle_date_time_kst, tz = "Asia/Seoul"),
        opening_price,
        high_price, low_price, trade_price,
        volume = candle_acc_trade_volume
      ) %>% arrange(time_kst)
  }, error = function(e) { message("\ube57\uc378 \uc624\ub958: ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Bithumb
#'
#' @description
#' Paginates through the Bithumb candles endpoint to retrieve all bars between
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
#' fetch_bithumb_range(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 01:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute bind_rows filter distinct arrange
#' @importFrom lubridate as_datetime
#' @export
fetch_bithumb_range <- function(market, from, to, unit = "min") {
  sym  <- bithumb_trading_pairs(market)
  if (is.null(sym)) { message("\ube57\uc378: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  path <- switch(unit, "min" = "minutes/1", "hour" = "minutes/60", "day" = "days",
                 stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  cur_to   <- to
  all_rows <- list()
  tryCatch({
    repeat {
      to_str <- format(cur_to, "%Y-%m-%dT%H:%M:%S", tz = "Asia/Seoul")
      url <- paste0("https://api.bithumb.com/v1/candles/", path,
                    "?market=", sym, "&count=200&to=", URLencode(to_str))
      res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0"), timeout(15))
      if (status_code(res) != 200) break
      parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(parsed) || !is.data.frame(parsed) || nrow(parsed) == 0) break

      df <- parsed %>%
        transmute(
          time_kst      = as_datetime(candle_date_time_kst, tz = "Asia/Seoul"),
          opening_price = as.numeric(opening_price),
          high_price    = as.numeric(high_price),
          low_price     = as.numeric(low_price),
          trade_price   = as.numeric(trade_price),
          volume        = as.numeric(candle_acc_trade_volume)
        )
      all_rows <- c(all_rows, list(df))

      oldest <- min(df$time_kst)
      if (oldest <= from || nrow(df) < 200) break
      cur_to <- oldest - 1
      Sys.sleep(0.15)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(time_kst, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ube57\uc378 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}


#' Fetch trade tick data from Bithumb over a date range
#'
#' @description
#' Paginates through the Bithumb trade ticks endpoint to retrieve all individual
#' trades between `from` and `to`. Uses cursor-based pagination via
#' `sequential_id`.
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
#' get_bithumb_trades(
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
get_bithumb_trades <- function(market, from, to) {
  sym <- bithumb_trading_pairs(market)
  if (is.null(sym)) { message("\ube57\uc378: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  all_rows <- list()
  cursor   <- NULL
  tryCatch({
    repeat {
      if (is.null(cursor)) {
        to_str <- URLencode(format(to, "%H:%M:%S", tz = "Asia/Seoul"))
        url <- paste0("https://api.bithumb.com/v1/trades/ticks",
                      "?market=", sym, "&count=500&to=", to_str)
      } else {
        url <- paste0("https://api.bithumb.com/v1/trades/ticks",
                      "?market=", sym, "&count=500&cursor=", cursor)
      }
      res <- GET(url, add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"),
                 timeout(15))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break
      df <- data.frame(
        time_kst      = as.POSIXct(raw$timestamp / 1000,
                                   origin = "1970-01-01", tz = "Asia/Seoul"),
        trade_price   = as.numeric(raw$trade_price),
        volume        = as.numeric(raw$trade_volume),
        ask_bid       = raw$ask_bid,
        sequential_id = as.numeric(raw$sequential_id),
        stringsAsFactors = FALSE
      )
      all_rows <- c(all_rows, list(df))
      oldest <- min(df$time_kst)
      if (oldest <= from || nrow(df) < 500) break
      cursor <- min(df$sequential_id)
      Sys.sleep(0.15)
    }
    if (length(all_rows) == 0) return(NULL)
    bind_rows(all_rows) %>%
      filter(time_kst >= from, time_kst <= to) %>%
      distinct(sequential_id, .keep_all = TRUE) %>%
      arrange(time_kst)
  }, error = function(e) { message("\ube57\uc378 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}


#' Get current price (ticker) from Bithumb
#'
#' @description
#' Returns the latest ticker snapshot for one or more markets from the Bithumb
#' public quotation API.
#'
#' @param market Character vector. One or more markets in `"ASSET-QUOTE"` format
#'   (e.g., `"BTC-KRW"`, `c("BTC-KRW", "ETH-KRW")`).
#'
#' @return A [data.frame] with one row per market and columns:
#'   \describe{
#'     \item{market}{Standardised market (`"ASSET-QUOTE"`)}
#'     \item{time_kst}{POSIXct snapshot timestamp in KST}
#'     \item{trade_price}{Most recent trade price (current price)}
#'     \item{opening_price}{Day open price}
#'     \item{high_price}{Day high price}
#'     \item{low_price}{Day low price}
#'     \item{prev_closing_price}{Previous day close price}
#'     \item{change}{Direction vs. previous close: `"RISE"`, `"FALL"`, or `"EVEN"`}
#'     \item{signed_change_rate}{Signed rate of change from previous close}
#'     \item{acc_trade_volume_24h}{Rolling 24-hour cumulative trade volume}
#'     \item{acc_trade_price_24h}{Rolling 24-hour cumulative trade value}
#'   }
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_bithumb_prices("BTC-KRW")
#' get_bithumb_prices(c("BTC-KRW", "ETH-KRW"))
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @export
get_bithumb_prices <- function(market) {
  syms <- vapply(market, function(m) {
    s <- bithumb_trading_pairs(m)
    if (is.null(s)) { message("\ube57\uc378: symbol \uc870\ud68c \uc2e4\ud328 (", m, ")"); NA_character_ }
    else s
  }, character(1))
  syms <- syms[!is.na(syms)]
  if (length(syms) == 0) return(NULL)

  url <- paste0(
    "https://api.bithumb.com/v1/ticker",
    "?markets=", paste(syms, collapse = ",")
  )
  tryCatch({
    res <- GET(url,
               add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"),
               timeout(10))
    if (status_code(res) != 200) {
      message("\ube57\uc378 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    data.frame(
      market               = paste0(
        sub("^[^-]+-", "", raw$market), "-", sub("-.*$", "", raw$market)
      ),
      time_kst             = as.POSIXct(raw$timestamp / 1000,
                                        origin = "1970-01-01", tz = "Asia/Seoul"),
      trade_price          = as.numeric(raw$trade_price),
      opening_price        = as.numeric(raw$opening_price),
      high_price           = as.numeric(raw$high_price),
      low_price            = as.numeric(raw$low_price),
      prev_closing_price   = as.numeric(raw$prev_closing_price),
      change               = raw$change,
      signed_change_rate   = as.numeric(raw$signed_change_rate),
      acc_trade_volume_24h = as.numeric(raw$acc_trade_volume_24h),
      acc_trade_price_24h  = as.numeric(raw$acc_trade_price_24h),
      stringsAsFactors = FALSE
    )
  }, error = function(e) { message("\ube57\uc378 \uc624\ub958: ", e$message); NULL })
}


#' Get real-time orderbook (호가) data from Bithumb
#'
#' @description
#' Retrieves the current orderbook for a given market from the Bithumb public
#' quotation API. Returns one row per price level, sorted by ask price ascending.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of price levels to retrieve (default `30`, max `30`).
#' @param level Integer. Price aggregation unit; only meaningful for KRW markets
#'   (default `0`, no aggregation).
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
#'     \item{level}{Applied price aggregation tier}
#'   }
#'   Returns `NULL` on error or when the market is not found.
#'
#' @examples
#' \dontrun{
#' get_bithumb_orderbook("BTC-KRW")
#' get_bithumb_orderbook("BTC-KRW", count = 5)
#' get_bithumb_orderbook("ETH-KRW", level = 1000)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
get_bithumb_orderbook <- function(market, count = 30, level = 0) {
  sym <- bithumb_trading_pairs(market)
  if (is.null(sym)) { message("\ube57\uc378: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.bithumb.com/v1/orderbook",
    "?markets=", sym, "&count=", count, "&level=", level
  )
  tryCatch({
    res <- GET(url,
               add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"),
               timeout(10))
    if (status_code(res) != 200) return(NULL)
    raw   <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    ob    <- raw[1, ]
    units <- ob$orderbook_units[[1]]
    n     <- min(nrow(units), count)
    units <- units[seq_len(n), ]
    data.frame(
      market         = market,
      timestamp      = as.POSIXct(ob$timestamp / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
      ask_price      = as.numeric(units$ask_price),
      bid_price      = as.numeric(units$bid_price),
      ask_size       = as.numeric(units$ask_size),
      bid_size       = as.numeric(units$bid_size),
      total_ask_size = as.numeric(ob$total_ask_size),
      total_bid_size = as.numeric(ob$total_bid_size),
      level          = as.integer(level),
      stringsAsFactors = FALSE
    ) %>% arrange(ask_price)
  }, error = function(e) { message("\ube57\uc378 \uc624\ub958: ", e$message); NULL })
}


#' Retrieve Bithumb market risk alarm data
#'
#' @description
#' Queries the Bithumb virtual asset warning API and returns markets with
#' active warnings, consolidating multiple warning types per market into a
#' single row showing the highest alarm tier.
#'
#' @param quote Character. Optional quote-currency filter (`"KRW"`).
#'   If `NULL` (default), all markets are returned.
#' @param verbose Logical. Print progress messages (default `FALSE`).
#'
#' @return A [tibble::tibble()] with columns `market` (`"ASSET-QUOTE"`),
#'   `symbol` (Bithumb native), `alarm` (`"주의"` / `"경고"` / `"위험"`),
#'   and `reason` (`" / "`-delimited when multiple warning types apply).
#'
#' @examples
#' \dontrun{
#' get_bithumb_alarm(quote = "KRW", verbose = TRUE)
#' }
#'
#' @importFrom httr2 request req_user_agent req_retry req_perform resp_body_string
#' @importFrom jsonlite fromJSON
#' @importFrom tibble as_tibble tibble
#' @importFrom dplyr %>% mutate filter group_by summarise select arrange desc
#' @importFrom stringr str_starts str_extract
#' @export
get_bithumb_alarm <- function(quote = NULL, verbose = FALSE) {

  step_map   <- c(CAUTION = "\uc8fc\uc758", WARNING = "\uacbd\uace0", DANGER = "\uc704\ud5d8")
  step_order <- c("\uc8fc\uc758" = 1L,      "\uacbd\uace0"  = 2L,    "\uc704\ud5d8" = 3L)
  type_map   <- c(
    PRICE_SUDDEN_FLUCTUATION          = "\uac00\uaca9 \uae09\ub4f1\ub77d",
    TRADING_VOLUME_SUDDEN_FLUCTUATION = "\uac70\ub798\ub7c9 \uae09\uc99d",
    DEPOSIT_AMOUNT_SUDDEN_FLUCTUATION = "\uc785\uae08\ub7c9 \uae09\uc99d",
    SPECIFIC_ACCOUNT_HIGH_TRANSACTION = "\uc18c\uc218\uacc4\uc815 \uc9d1\uc911",
    PRICE_DIFFERENCE_HIGH             = "\uae00\ub85c\ubc8c \uc2dc\uc138 \ub300\ube44 \uc774\uaca9"
  )

  if (verbose) message("\ube57\uc378 \uacbd\ubcf4\uc81c API \uc870\ud68c \uc911...")

  raw <- tryCatch({
    resp <- request("https://api.bithumb.com/v1/market/virtual_asset_warning") |>
      req_user_agent("R/bithumb-risk-checker") |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_perform()
    fromJSON(resp_body_string(resp), simplifyDataFrame = TRUE)
  }, error = function(e) {
    message("\ube57\uc378 \uacbd\ubcf4\uc81c API \uc624\ub958: ", e$message)
    NULL
  })

  if (is.null(raw) || length(raw) == 0 || nrow(as.data.frame(raw)) == 0) {
    if (verbose) message("\uacbd\ubcf4 \uc885\ubaa9\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.")
    return(tibble(market = character(), symbol = character(),
                  alarm = character(), reason = character()))
  }

  df <- as_tibble(raw) |>
    mutate(
      alarm  = step_map[warning_step],
      reason = dplyr::coalesce(type_map[warning_type], warning_type)
    )

  if (!is.null(quote)) {
    df <- filter(df, str_starts(market, paste0(quote, "-")))
  }

  if (nrow(df) == 0) {
    if (verbose) message("\ud574\ub2f9 \ub9c8\ucf13\uc5d0 \uacbd\ubcf4 \uc885\ubaa9\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.")
    return(tibble(market = character(), symbol = character(),
                  alarm = character(), reason = character()))
  }

  df |>
    mutate(alarm_ord = step_order[alarm]) |>
    group_by(market) |>
    summarise(
      alarm  = alarm[which.max(alarm_ord)],
      reason = paste(unique(reason), collapse = " / "),
      .groups = "drop"
    ) |>
    mutate(
      symbol = market,
      market = paste0(
        str_extract(symbol, "(?<=-)[^-]+$"), "-",
        str_extract(symbol, "^[^-]+")
      )
    ) |>
    select(market, symbol, alarm, reason) |>
    arrange(desc(match(alarm, c("\uc704\ud5d8", "\uacbd\uace0", "\uc8fc\uc758"))), market)
}
