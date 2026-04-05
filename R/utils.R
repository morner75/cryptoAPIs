#' @importFrom dplyr %>% bind_rows distinct arrange group_by summarise rename first last
NULL

# Internal helper: return df when market is NULL, otherwise lookup symbol ------

.pick_symbol <- function(df, market, exch_name) {
  if (is.null(market)) return(df)
  matched <- df$symbol[df$market == market]
  if (length(matched) == 0) {
    message(exch_name, ": market '", market, "' \uc5c6\uc74c")
    return(NULL)
  }
  matched[[1]]
}


#' Merge multiple OHLCV datasets
#'
#' @description
#' Reads several saved OHLCV list objects (`.rds` files produced by the
#' `fetch_*_range()` family) and combines them per exchange and per market,
#' removing duplicate rows by `time_kst`.
#'
#' @param ... Character vector(s) of file paths to `.rds` files. Each file
#'   must contain a named list of the form
#'   `list(exchange = list(market = data.frame(...)))`.
#'
#' @return A nested list with the same structure:
#'   `list(exchange -> list(market -> tibble))`. Rows are sorted by
#'   `time_kst` with duplicates removed.
#'
#' @examples
#' \dontrun{
#' merged <- merge_ohlc_datasets("data_2024_01.rds", "data_2024_02.rds")
#' merged$binance$`BTC-USDT`
#' }
#'
#' @importFrom dplyr bind_rows distinct arrange
#' @export
merge_ohlc_datasets <- function(...) {
  files    <- c(...)
  datasets <- lapply(files, readRDS)

  exchanges <- unique(unlist(lapply(datasets, names)))

  result <- lapply(setNames(exchanges, exchanges), function(exch) {
    exch_lists <- lapply(datasets, function(d) d[[exch]])
    exch_lists <- Filter(Negate(is.null), exch_lists)

    if (length(exch_lists) == 0) return(list())

    markets <- unique(unlist(lapply(exch_lists, names)))

    lapply(setNames(markets, markets), function(mkt) {
      dfs <- lapply(exch_lists, function(el) el[[mkt]])
      dfs <- Filter(Negate(is.null), dfs)
      if (length(dfs) == 0) return(NULL)
      bind_rows(dfs) %>%
        distinct(time_kst, .keep_all = TRUE) %>%
        arrange(time_kst)
    })
  })

  result
}

#' Merge multiple trade tick datasets
#'
#' @description
#' Reads several saved trade tick list objects (`.rds` files produced by the
#' `get_*_trades()` family) and combines them per exchange and per market,
#' removing fully duplicate rows (all columns identical).
#'
#' Unlike [merge_ohlc_datasets()] which deduplicates by `time_kst` alone,
#' this function uses full-row deduplication because multiple trades can share
#' the same timestamp.
#'
#' @param ... Character vector(s) of file paths to `.rds` files. Each file
#'   must contain a named list of the form
#'   `list(exchange = list(market = data.frame(...)))`.
#'
#' @return A nested list with the same structure:
#'   `list(exchange -> list(market -> data.frame))`. Fully duplicate rows are
#'   removed and rows are sorted by `time_kst`.
#'
#' @examples
#' \dontrun{
#' merged <- merge_trades_datasets("trades_2024_01.rds", "trades_2024_02.rds")
#' merged$binance$`BTC-USDT`
#' }
#'
#' @importFrom dplyr bind_rows distinct arrange
#' @export
merge_trades_datasets <- function(...) {
  files    <- c(...)
  datasets <- lapply(files, readRDS)

  exchanges <- unique(unlist(lapply(datasets, names)))

  result <- lapply(setNames(exchanges, exchanges), function(exch) {
    exch_lists <- lapply(datasets, function(d) d[[exch]])
    exch_lists <- Filter(Negate(is.null), exch_lists)

    if (length(exch_lists) == 0) return(list())

    markets <- unique(unlist(lapply(exch_lists, names)))

    lapply(setNames(markets, markets), function(mkt) {
      dfs <- lapply(exch_lists, function(el) el[[mkt]])
      dfs <- Filter(Negate(is.null), dfs)
      if (length(dfs) == 0) return(NULL)
      bind_rows(dfs) %>%
        distinct() %>%
        arrange(time_kst)
    })
  })

  result
}


#' Convert OHLCV to a coarser time unit
#'
#' @description
#' Aggregates a 1-minute (or other interval) OHLCV data.frame into a coarser
#' time unit. The input interval is detected automatically from the minimum
#' time gap in the data; requesting a unit shorter than that interval raises
#' an error.
#'
#' @param .data A data.frame with columns `time_kst` (POSIXct, Asia/Seoul),
#'   `opening_price`, `high_price`, `low_price`, `trade_price`, `volume`
#'   (as returned by `fetch_*()` / `fetch_*_range()`).
#' @param unit Target time unit. One of `"5 min"`, `"10 min"`, `"15 min"`,
#'   `"30 min"`, `"1 hour"`, `"4 hour"`, `"1 day"`.
#'
#' @return A data.frame with the same six columns aggregated to `unit`.
#'   Each row is one completed candle: open = first trade price, high = max,
#'   low = min, close (`trade_price`) = last trade price, volume = sum.
#'
#' @examples
#' \dontrun{
#' ohlcv_1m <- readRDS("tmp/ohlcv_1m.rds")
#' convert_ohlcv(ohlcv_1m, unit = "5 min")
#' convert_ohlcv(ohlcv_1m, unit = "1 hour")
#' convert_ohlcv(ohlcv_1m, unit = "1 day")
#' }
#'
#' @importFrom dplyr arrange group_by summarise rename first last
#' @export
convert_ohlcv <- function(.data,
                          unit = c("5 min", "10 min", "15 min",
                                   "30 min", "1 hour", "4 hour", "1 day")) {
  unit <- match.arg(unit)

  unit_mins <- switch(unit,
    "5 min"  =    5,
    "10 min" =   10,
    "15 min" =   15,
    "30 min" =   30,
    "1 hour" =   60,
    "4 hour" =  240,
    "1 day"  = 1440
  )

  # Detect input interval from the minimum non-zero time gap
  times     <- sort(.data$time_kst)
  if (length(times) < 2)
    stop("\ub370\uc774\ud130\uac00 \ub108\ubb34 \uc801\uc2b5\ub2c8\ub2e4 (\ucd5c\uc18c 2\ud589 \ud544\uc694).")
  diffs_min <- as.numeric(diff(times), units = "mins")
  input_mins <- min(diffs_min[diffs_min > 0])

  if (unit_mins < input_mins)
    stop("\uc785\ub825 ohlcv \ubcf4\ub2e4 \uc8fc\uae30\uac00 \uc9e7\uc740 ohlcv\ub294 \uc0dd\uc131\ud560 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.")

  # Floor each timestamp to the target unit boundary
  unit_secs <- unit_mins * 60L
  floored   <- as.POSIXct(
    floor(as.numeric(.data$time_kst) / unit_secs) * unit_secs,
    origin = "1970-01-01", tz = "Asia/Seoul"
  )

  .data$time_group <- floored

  .data %>%
    arrange(time_kst) %>%
    group_by(time_group) %>%
    summarise(
      opening_price = dplyr::first(opening_price),
      high_price    = max(high_price),
      low_price     = min(low_price),
      trade_price   = dplyr::last(trade_price),
      volume        = sum(volume),
      .groups = "drop"
    ) %>%
    dplyr::rename(time_kst = time_group) %>%
    arrange(time_kst)
}
