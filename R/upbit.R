.upbit_pairs_cache <- new.env(parent = emptyenv())

#' Get trading pairs from Upbit
#'
#' @description
#' Fetches all available trading pairs from the Upbit exchange API.
#'
#' @param market Character. Optional filter. If `NULL` (default), returns all
#'   pairs as a tibble. If a market string (e.g., `"BTC-KRW"`) is supplied,
#'   returns the matching exchange-native symbol.
#'
#' @return A [tibble::tibble()] with columns `exchange`, `asset`, `quote`,
#'   `symbol`, `market`, or `NULL` on error. When `market` is non-`NULL`,
#'   returns the matching exchange-native symbol string (e.g., `"KRW-BTC"`) or
#'   `NULL` if not found.
#'
#' @examples
#' \dontrun{
#' upbit_trading_pairs()
#' upbit_trading_pairs(market = "BTC-KRW")
#' }
#'
#' @importFrom httr GET content status_code accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% transmute
#' @importFrom stringr str_extract
#' @export
upbit_trading_pairs <- function(market = NULL) {
  if (!exists("pairs", envir = .upbit_pairs_cache)) {
    tryCatch({
      res <- GET("https://api.upbit.com/v1/market/all",
                 query   = list(is_details = "false"),
                 accept("application/json"))
      if (status_code(res) != 200) {
        message("\uc5c5\ube44\ud2b8 \uc624\ub958: HTTP ", status_code(res)); return(NULL)
      }
      df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
        transmute(
          exchange = "upbit",
          asset    = str_extract(market, "(?<=\\-)\\w+$"),
          quote    = str_extract(market, "^\\w+(?=\\-)"),
          symbol   = market,
          market   = paste(asset, quote, sep = "-")
        )
      assign("pairs", df, envir = .upbit_pairs_cache)
    }, error = function(e) { message("\uc5c5\ube44\ud2b8 \uc624\ub958: ", e$message); return(NULL) })
  }
  if (!exists("pairs", envir = .upbit_pairs_cache)) return(NULL)
  .pick_symbol(.upbit_pairs_cache$pairs, market, "\uc5c5\ube44\ud2b8")
}


#' Fetch recent 1-minute OHLCV candles from Upbit
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the Upbit exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Number of candles to retrieve (default `60`, max `200`).
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' fetch_upbit("BTC-KRW", count = 10)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
fetch_upbit <- function(market, count = 60) {
  sym <- upbit_trading_pairs(market)
  if (is.null(sym)) { message("\uc5c5\ube44\ud2b8: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.upbit.com/v1/candles/minutes/1",
    "?market=", sym, "&count=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"),
               timeout(10))
    if (status_code(res) != 200) return(NULL)
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    data.frame(
      time_kst      = as.POSIXct(raw$candle_date_time_kst,
                                 format = "%Y-%m-%dT%H:%M:%S", tz = "Asia/Seoul"),
      opening_price = as.numeric(raw$opening_price),
      high_price    = as.numeric(raw$high_price),
      low_price     = as.numeric(raw$low_price),
      trade_price   = as.numeric(raw$trade_price),
      volume        = as.numeric(raw$candle_acc_trade_volume),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("\uc5c5\ube44\ud2b8 \uc624\ub958: ", e$message); NULL })
}


#' Fetch a date-range of OHLCV candles from Upbit
#'
#' @description
#' Paginates through the Upbit candles endpoint to retrieve all 1-minute (or
#' hourly / daily) bars between `from` and `to`.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#' @param unit Character. Candle unit: `"min"` (1-minute), `"hour"` (60-minute),
#'   or `"day"`. Defaults to `"min"`.
#'
#' @return A [data.frame] with columns `time_kst`, `opening_price`,
#'   `high_price`, `low_price`, `trade_price`, `volume`, sorted by `time_kst`
#'   with duplicates removed. Returns `NULL` on error or when no data is
#'   available.
#'
#' @examples
#' \dontrun{
#' fetch_upbit_range(
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
fetch_upbit_range <- function(market, from, to, unit = "min") {
  sym  <- upbit_trading_pairs(market)
  if (is.null(sym)) { message("\uc5c5\ube44\ud2b8: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  path <- switch(unit, "min" = "minutes/1", "hour" = "minutes/60", "day" = "days",
                 stop("unit\uc740 'min', 'hour', 'day' \uc911 \ud558\ub098\uc5ec\uc57c \ud569\ub2c8\ub2e4."))
  cur_to   <- to
  all_rows <- list()
  tryCatch({
    repeat {
      to_str <- format(cur_to, "%Y-%m-%dT%H:%M:%S", tz = "UTC")
      url <- paste0("https://api.upbit.com/v1/candles/", path,
                    "?market=", sym, "&count=200&to=", URLencode(to_str))
      res <- GET(url, add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"), timeout(15))
      if (status_code(res) != 200) break
      raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      if (is.null(raw) || length(raw) == 0 || !is.data.frame(raw)) break

      df <- data.frame(
        time_kst      = as.POSIXct(raw$candle_date_time_kst,
                                   format = "%Y-%m-%dT%H:%M:%S", tz = "Asia/Seoul"),
        opening_price = as.numeric(raw$opening_price),
        high_price    = as.numeric(raw$high_price),
        low_price     = as.numeric(raw$low_price),
        trade_price   = as.numeric(raw$trade_price),
        volume        = as.numeric(raw$candle_acc_trade_volume),
        stringsAsFactors = FALSE
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
  }, error = function(e) { message("\uc5c5\ube44\ud2b8 \ubc94\uc704 \uc624\ub958: ", e$message); NULL })
}


#' Fetch trade tick data from Upbit over a date range
#'
#' @description
#' Paginates through the Upbit trade ticks endpoint to retrieve all individual
#' trades between `from` and `to`. The first page uses `to` as the upper bound;
#' subsequent pages use the `sequential_id` cursor returned by each response.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param from POSIXct or Date. Start of the range (inclusive).
#' @param to POSIXct or Date. End of the range (inclusive).
#'
#' @return A [data.frame] with columns:
#'   \describe{
#'     \item{time_kst}{POSIXct trade timestamp in KST}
#'     \item{trade_price}{Executed price}
#'     \item{volume}{Executed volume (`trade_volume`)}
#'     \item{ask_bid}{Trade direction: `"ASK"` (sell) or `"BID"` (buy)}
#'     \item{sequential_id}{Unique trade identifier (used for deduplication)}
#'   }
#'   Sorted by `time_kst` ascending, with duplicates removed by `sequential_id`.
#'   Returns `NULL` on error.
#'
#' @examples
#' \dontrun{
#' get_upbit_trades(
#'   "BTC-KRW",
#'   from = as.POSIXct("2024-01-01 09:00:00", tz = "Asia/Seoul"),
#'   to   = as.POSIXct("2024-01-01 10:00:00", tz = "Asia/Seoul")
#' )
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% bind_rows filter distinct arrange
#' @export
get_upbit_trades <- function(market, from, to) {
  sym <- upbit_trading_pairs(market)
  if (is.null(sym)) { message("\uc5c5\ube44\ud2b8: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  all_rows <- list()
  cursor   <- NULL
  tryCatch({
    repeat {
      if (is.null(cursor)) {
        to_str <- format(to, "%H:%M:%S", tz = "UTC")
        url <- paste0("https://api.upbit.com/v1/trades/ticks",
                      "?market=", sym, "&count=500&to=", to_str)
      } else {
        url <- paste0("https://api.upbit.com/v1/trades/ticks",
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
  }, error = function(e) { message("\uc5c5\ube44\ud2b8 \uccb4\uacb0 \uc624\ub958: ", e$message); NULL })
}


#' Get real-time orderbook (호가) data from Upbit
#'
#' @description
#' Retrieves the current orderbook for a given market from the Upbit public
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
#' get_upbit_orderbook("BTC-KRW")
#' get_upbit_orderbook("BTC-KRW", count = 5)
#' get_upbit_orderbook("ETH-KRW", level = 1000)
#' }
#'
#' @importFrom httr GET content status_code add_headers timeout accept
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% arrange
#' @export
get_upbit_orderbook <- function(market, count = 30, level = 0) {
  sym <- upbit_trading_pairs(market)
  if (is.null(sym)) { message("\uc5c5\ube44\ud2b8: symbol \uc870\ud68c \uc2e4\ud328 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.upbit.com/v1/orderbook",
    "?markets=", sym, "&count=", count, "&level=", level
  )
  tryCatch({
    res <- GET(url,
               add_headers(accept = "application/json", `User-Agent` = "Mozilla/5.0"),
               timeout(10))
    if (status_code(res) != 200) return(NULL)
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
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
      level          = as.integer(ob$level),
      stringsAsFactors = FALSE
    ) %>% arrange(ask_price)
  }, error = function(e) { message("\uc5c5\ube44\ud2b8 \uc624\ub958: ", e$message); NULL })
}


#' Retrieve Upbit market risk alarm data
#'
#' @description
#' Queries the Upbit public API for markets with active caution flags
#' (price fluctuation, volume surge, deposit surge, global price gap,
#' small-account concentration), then scrapes each market's exchange page
#' via a headless Chromium session to extract the alarm tier
#' (`"주의"`, `"경고"`, or `"위험"`) and reason text.
#'
#' @param quote Character. Optional quote-currency filter (`"KRW"`, `"BTC"`,
#'   `"USDT"`). If `NULL` (default) all markets are checked.
#' @param wait_sec Numeric. Seconds to wait after each page navigation for
#'   JavaScript to render (default `2`).
#' @param verbose Logical. Print progress messages (default `FALSE`).
#'
#' @return A [tibble::tibble()] with columns `market` (`"ASSET-QUOTE"`),
#'   `symbol` (Upbit native, e.g. `"KRW-BTC"`), `alarm`, and `reason`.
#'   Returns an empty tibble when no alarms are active, or `NULL` on error.
#'
#' @note Requires the `chromote` and `rvest` packages (listed under `Suggests`).
#'   A working Chrome/Chromium installation is needed at runtime.
#'
#' @examples
#' \dontrun{
#' get_upbit_alarm(quote = "KRW", verbose = TRUE)
#' }
#'
#' @importFrom httr2 request req_user_agent req_retry req_perform resp_body_string
#' @importFrom jsonlite fromJSON
#' @importFrom tibble tibble
#' @importFrom dplyr %>% bind_cols mutate filter select arrange
#' @importFrom stringr str_starts str_detect str_match str_trim str_extract str_squish
#' @importFrom purrr map_dfr
#' @export
get_upbit_alarm <- function(
    quote    = NULL,
    wait_sec = 2,
    verbose  = FALSE
) {
  if (verbose) message("[1/2] \uc5c5\ube44\ud2b8 \uacf5\uc2dd API \uc870\ud68c \uc911...")

  raw <- tryCatch({
    resp <- request("https://api.upbit.com/v1/market/all?is_details=true") |>
      req_user_agent("R/upbit-risk-checker") |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_perform()
    fromJSON(resp_body_string(resp), simplifyDataFrame = TRUE)
  }, error = function(e) {
    message("\uc5c5\ube44\ud2b8 API \uc624\ub958: ", e$message)
    NULL
  })
  if (is.null(raw)) return(NULL)

  api_df <- tibble(
    market  = raw$market,
    warning = raw$market_event$warning
  ) |>
    bind_cols(raw$market_event$caution) |>
    mutate(
      has_any_caution =
        PRICE_FLUCTUATIONS | TRADING_VOLUME_SOARING |
        DEPOSIT_AMOUNT_SOARING | GLOBAL_PRICE_DIFFERENCES |
        CONCENTRATION_OF_SMALL_ACCOUNTS
    ) |>
    filter(!warning, has_any_caution)

  if (!is.null(quote)) {
    api_df <- filter(api_df, str_starts(market, paste0(quote, "-")))
  }

  if (nrow(api_df) == 0) {
    if (verbose) message("\uc8fc\uc758/\uacbd\uace0/\uc704\ud5d8 \uc885\ubaa9\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.")
    return(tibble(market = character(), symbol = character(),
                  alarm = character(), reason = character()))
  }

  if (verbose) message("[2/2] \uc5c5\ube44\ud2b8 \uc6f9\ud398\uc774\uc9c0 \ud06c\ub864\ub9c1 \uc911... (\uc885\ubaa9 \uc218: ", nrow(api_df), ")")

  b <- tryCatch(chromote::ChromoteSession$new(),
                error = function(e) { message("Chrome \uc2e4\ud589 \uc624\ub958: ", e$message); NULL })
  if (is.null(b)) {
    message("\ud06c\ub86c \uc2e4\ud589 \ubd88\uac00 \u2014 \ud06c\ub864\ub9c1 \ubd88\uac00")
    return(NULL)
  }
  on.exit(try(b$close(), silent = TRUE), add = TRUE)

  tier_re <- "(\uc704\ud5d8|\uacbd\uace0|\uc8fc\uc758)(\uac70\ub798\ub7c9|\uc785\uae08\ub7c9|\uac00\uaca9|\uc18c\uc218|\uae00\ub85c\ubc8c)"

  detect_alarm <- function(market_code) {
    url <- paste0("https://upbit.com/exchange?code=CRIX.UPBIT.", market_code)
    tryCatch({
      b$Page$navigate(url)
      Sys.sleep(wait_sec)
      html      <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
      page_text <- str_squish(rvest::html_text2(rvest::read_html(html)))

      if (!str_detect(page_text, tier_re)) {
        if (verbose) message("  ", market_code, " \u2192 \ubbf8\uac10\uc9c0")
        return(tibble(market = market_code, alarm = NA_character_, reason = NA_character_))
      }

      m      <- str_match(page_text, paste0(tier_re, "(.{0,60})"))
      alarm  <- m[1, 2]
      reason <- str_trim(paste0(m[1, 3], m[1, 4]))
      trimmed <- str_extract(reason, paste0("^.*?(?=", tier_re, ")")) |> str_trim()
      if (!is.na(trimmed) && nchar(trimmed) > 0) reason <- trimmed
      natural <- str_extract(reason, "^.+?(?:\uc774\uc0c1|\ub192\uc74c|\ub0ae\uc74c|\uae09\ub77d|\uc9d1\uc911)") |> str_trim()
      if (!is.na(natural) && nchar(natural) > 0) reason <- natural

      if (verbose) message("  ", market_code, " \u2192 ", alarm, ": ", reason)
      tibble(market = market_code, alarm = alarm, reason = reason)
    }, error = function(e) {
      message("  \ud06c\ub864\ub9c1 \uc624\ub958 (", market_code, "): ", e$message)
      tibble(market = market_code, alarm = NA_character_, reason = NA_character_)
    })
  }

  map_dfr(api_df$market, detect_alarm) |>
    filter(!is.na(alarm)) |>
    mutate(
      symbol = market,
      market = paste0(
        str_extract(symbol, "(?<=-)[^-]+$"), "-",
        str_extract(symbol, "^[^-]+")
      )
    ) |>
    select(market, symbol, alarm, reason)
}
