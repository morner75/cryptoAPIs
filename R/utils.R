#' @importFrom dplyr %>% bind_rows distinct arrange
NULL

# Internal helper: return df when market is NULL, otherwise lookup symbol ------

.pick_symbol <- function(df, market, exch_name) {
  if (is.null(market)) return(df)
  matched <- df$symbol[df$market == market]
  if (length(matched) == 0) {
    message(exch_name, ": market '", market, "' 없음")
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
