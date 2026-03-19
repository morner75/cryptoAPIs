test_that("merge_ohlc_datasets combines and deduplicates correctly", {
  # Build two minimal OHLCV data frames with one overlapping row
  t1 <- as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul")
  t2 <- as.POSIXct("2024-01-01 00:01:00", tz = "Asia/Seoul")
  t3 <- as.POSIXct("2024-01-01 00:02:00", tz = "Asia/Seoul")

  make_df <- function(times) {
    data.frame(
      time_kst      = times,
      opening_price = 1, high_price = 2, low_price = 0.5,
      trade_price   = 1.5, volume = 10,
      stringsAsFactors = FALSE
    )
  }

  ds1 <- list(binance = list(`BTC-USDT` = make_df(c(t1, t2))))
  ds2 <- list(binance = list(`BTC-USDT` = make_df(c(t2, t3))))  # t2 duplicated

  f1 <- tempfile(fileext = ".rds"); saveRDS(ds1, f1)
  f2 <- tempfile(fileext = ".rds"); saveRDS(ds2, f2)
  on.exit({ unlink(f1); unlink(f2) })

  merged <- merge_ohlc_datasets(f1, f2)

  expect_type(merged, "list")
  expect_true("binance" %in% names(merged))
  btc <- merged$binance$`BTC-USDT`
  expect_s3_class(btc, "data.frame")
  expect_equal(nrow(btc), 3L)   # t1, t2, t3 — no duplicate
  expect_equal(btc$time_kst, sort(c(t1, t2, t3)))
})

test_that("merge_ohlc_datasets handles empty inputs gracefully", {
  ds_empty <- list(upbit = list())
  f <- tempfile(fileext = ".rds"); saveRDS(ds_empty, f)
  on.exit(unlink(f))

  merged <- merge_ohlc_datasets(f)
  expect_type(merged, "list")
  expect_equal(merged$upbit, list())
})
