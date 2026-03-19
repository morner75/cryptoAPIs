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
#'   `symbol`, `market`, or `NULL` on error.
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
  tryCatch({
    res <- GET("https://api.bithumb.com/v1/market/all",
               query   = list(isDetails = "false"),
               content_type("application/octet-stream"),
               accept("application/json"))
    if (status_code(res) != 200) {
      message("빗썸 오류: HTTP ", status_code(res)); return(NULL)
    }
    df <- fromJSON(content(res, as = "text", encoding = "UTF-8")) %>%
      transmute(
        exchange = "bithumb",
        asset    = str_extract(market, "(?<=\\-)\\w+$"),
        quote    = str_extract(market, "^\\w+(?=\\-)"),
        symbol   = market,
        market   = paste(asset, quote, sep = "-")
      )
    .pick_symbol(df, market, "빗썸")
  }, error = function(e) { message("빗썸 오류: ", e$message); NULL })
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
  if (is.null(sym)) { message("빗썸: symbol 조회 실패 (", market, ")"); return(NULL) }
  url <- paste0(
    "https://api.bithumb.com/v1/candles/minutes/1?market=", sym,
    "&count=", count
  )
  tryCatch({
    res <- GET(url, add_headers(`User-Agent` = "Mozilla/5.0"), timeout(10))
    if (status_code(res) != 200) return(NULL)
    parsed <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
    if (!is.data.frame(parsed)) {
      message("빗썸 응답 파싱 오류 (", market, "): 데이터프레임이 아님")
      return(NULL)
    }
    parsed %>%
      transmute(
        time_kst      = as_datetime(candle_date_time_kst, tz = "Asia/Seoul"),
        opening_price, trade_price,
        high_price, low_price,
        volume = candle_acc_trade_volume
      ) %>% arrange(time_kst)
  }, error = function(e) { message("빗썸 오류: ", e$message); NULL })
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
#' @return A [data.frame] sorted by `time_kst` with duplicates removed, or
#'   `NULL` on error.
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
  if (is.null(sym)) { message("빗썸: symbol 조회 실패 (", market, ")"); return(NULL) }
  path <- switch(unit, "min" = "minutes/1", "hour" = "minutes/60", "day" = "days",
                 stop("unit은 'min', 'hour', 'day' 중 하나여야 합니다."))
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
  }, error = function(e) { message("빗썸 범위 오류: ", e$message); NULL })
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
#'   and `reason` (pipe-delimited when multiple types apply).
#'
#' @examples
#' \dontrun{
#' get_bithumb_alarm(quote = "KRW", verbose = TRUE)
#' }
#'
#' @importFrom httr2 request req_user_agent req_retry req_perform resp_body_string
#' @importFrom jsonlite fromJSON
#' @importFrom tibble as_tibble tibble
#' @importFrom dplyr %>% mutate filter group_by summarise select arrange
#' @importFrom stringr str_starts str_extract
#' @export
get_bithumb_alarm <- function(quote = NULL, verbose = FALSE) {

  step_map   <- c(CAUTION = "주의", WARNING = "경고", DANGER = "위험")
  step_order <- c("주의" = 1L,      "경고"  = 2L,    "위험" = 3L)
  type_map   <- c(
    PRICE_SUDDEN_FLUCTUATION          = "가격 급등락",
    TRADING_VOLUME_SUDDEN_FLUCTUATION = "거래량 급증",
    DEPOSIT_AMOUNT_SUDDEN_FLUCTUATION = "입금량 급증",
    SPECIFIC_ACCOUNT_HIGH_TRANSACTION = "소수계정 집중",
    PRICE_DIFFERENCE_HIGH             = "글로벌 시세 대비 이격"
  )

  if (verbose) message("빗썸 경보제 API 조회 중...")

  raw <- tryCatch({
    resp <- request("https://api.bithumb.com/v1/market/virtual_asset_warning") |>
      req_user_agent("R/bithumb-risk-checker") |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_perform()
    fromJSON(resp_body_string(resp), simplifyDataFrame = TRUE)
  }, error = function(e) {
    message("빗썸 경보제 API 오류: ", e$message)
    NULL
  })

  if (is.null(raw) || length(raw) == 0 || nrow(as.data.frame(raw)) == 0) {
    if (verbose) message("경보 종목이 없습니다.")
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
    if (verbose) message("해당 마켓에 경보 종목이 없습니다.")
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
    arrange(desc(match(alarm, c("위험", "경고", "주의"))), market)
}
