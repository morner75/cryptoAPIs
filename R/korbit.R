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
#'   `symbol`, `market`, or `NULL` on error.
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
  tryCatch({
    res <- GET("https://api.korbit.co.kr/v2/currencyPairs",
               accept("application/json"))
    if (status_code(res) != 200) {
      message("코빗 오류: HTTP ", status_code(res)); return(NULL)
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
    .pick_symbol(df, market, "코빗")
  }, error = function(e) { message("코빗 오류: ", e$message); NULL })
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
  if (is.null(sym)) { message("코빗: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.korbit.co.kr/v2/candles",
    "?symbol=", sym, "&interval=1&limit=", count
  )
  tryCatch({
    res <- GET(url,
               add_headers(`User-Agent` = "Mozilla/5.0", `Accept` = "application/json"),
               timeout(10))
    if (status_code(res) != 200) {
      message("코빗 HTTP 오류: ", status_code(res), " / ", market)
      return(NULL)
    }
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (is.null(parsed$success) || !isTRUE(parsed$success)) {
      message("코빗 API 오류 / ", market)
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
  }, error = function(e) { message("코빗 오류 (", market, "): ", e$message); NULL })
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
#' @return A [data.frame] sorted by `time_kst` with duplicates removed, or
#'   `NULL` on error.
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
  if (is.null(sym)) { message("코빗: symbol 조회 실패 (", market, ")"); return(NULL) }
  interval_str <- switch(unit, "min" = "1", "hour" = "60", "day" = "1D",
                          stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
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
  }, error = function(e) { message("코빗 범위 오류: ", e$message); NULL })
}
