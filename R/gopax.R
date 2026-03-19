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
#'   `symbol`, `market`, or `NULL` on error.
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
  tryCatch({
    res <- GET("https://api.gopax.co.kr/trading-pairs",
               accept("application/json"))
    if (status_code(res) != 200) {
      message("고팍스 오류: HTTP ", status_code(res)); return(NULL)
    }
    df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
      transmute(
        exchange = "gopax",
        asset    = baseAsset,
        quote    = quoteAsset,
        symbol   = str_c(baseAsset, quoteAsset, sep = "-"),
        market   = paste(asset, quote, sep = "-")
      )
    .pick_symbol(df, market, "고팍스")
  }, error = function(e) { message("고팍스 오류: ", e$message); NULL })
}


#' Fetch recent 1-minute OHLCV candles from GOPAX
#'
#' @description
#' Returns the most recent 1-minute candlestick data for a given market from
#' the GOPAX exchange.
#'
#' @param market Character. Market in `"ASSET-QUOTE"` format (e.g., `"BTC-KRW"`).
#' @param count Integer. Approximate number of candles (default `200`).
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
  if (is.null(sym)) { message("고팍스: symbol 조회 실패 (", market, ")"); return(NULL) }
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
      message("고팍스 HTTP 오류: ", status_code(res), " / ", market)
      return(NULL)
    }
    raw <- fromJSON(content(res, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
    if (is.null(raw) || length(raw) == 0) return(NULL)
    m <- if (is.matrix(raw)) raw else do.call(rbind, lapply(raw, as.numeric))
    if (ncol(m) < 6) { message("고팍스 컬럼 부족: ", ncol(m), "개"); return(NULL) }
    data.frame(
      time_kst      = as.POSIXct(m[, 1] / 1000, origin = "1970-01-01", tz = "Asia/Seoul"),
      opening_price = as.numeric(m[, 4]),
      high_price    = as.numeric(m[, 3]),
      low_price     = as.numeric(m[, 2]),
      trade_price   = as.numeric(m[, 5]),
      volume        = as.numeric(m[, 6]),
      stringsAsFactors = FALSE
    ) %>% arrange(time_kst)
  }, error = function(e) { message("고팍스 오류 (", market, "): ", e$message); NULL })
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
#' @return A [data.frame] sorted by `time_kst` with duplicates removed, or
#'   `NULL` on error.
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
  if (is.null(sym)) { message("고팍스: symbol 조회 실패 (", market, ")"); return(NULL) }
  interval_min <- switch(unit, "min" = 1L, "hour" = 30L, "day" = 1440L,
                          stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
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
  }, error = function(e) { message("고팍스 범위 오류: ", e$message); NULL })
}
