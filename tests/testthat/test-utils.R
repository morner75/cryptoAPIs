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


# merge_trades_datasets() -------------------------------------------------

make_trades_df <- function(times, ids) {
  data.frame(
    time_kst      = times,
    trade_price   = 100,
    volume        = 0.01,
    ask_bid       = "BID",
    sequential_id = ids,
    stringsAsFactors = FALSE
  )
}

test_that("merge_trades_datasets combines and deduplicates correctly", {
  t1 <- as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul")
  t2 <- as.POSIXct("2024-01-01 00:00:01", tz = "Asia/Seoul")
  t3 <- as.POSIXct("2024-01-01 00:00:02", tz = "Asia/Seoul")

  ds1 <- list(binance = list(`BTC-USDT` = make_trades_df(c(t1, t2), c(1, 2))))
  ds2 <- list(binance = list(`BTC-USDT` = make_trades_df(c(t2, t3), c(2, 3))))  # row id=2 duplicated

  f1 <- tempfile(fileext = ".rds"); saveRDS(ds1, f1)
  f2 <- tempfile(fileext = ".rds"); saveRDS(ds2, f2)
  on.exit({ unlink(f1); unlink(f2) })

  merged <- merge_trades_datasets(f1, f2)

  btc <- merged$binance$`BTC-USDT`
  expect_s3_class(btc, "data.frame")
  expect_equal(nrow(btc), 3L)           # id=1,2,3 — duplicate row removed
  expect_equal(btc$sequential_id, c(1, 2, 3))
  expect_equal(btc$time_kst, sort(c(t1, t2, t3)))
})

test_that("merge_trades_datasets preserves same-timestamp distinct rows", {
  # Two different trades at the exact same millisecond must both be kept
  t <- as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul")

  ds1 <- list(upbit = list(`BTC-KRW` = make_trades_df(c(t, t), c(10, 11))))
  ds2 <- list(upbit = list(`BTC-KRW` = make_trades_df(c(t, t), c(11, 12))))  # id=11 duplicated

  f1 <- tempfile(fileext = ".rds"); saveRDS(ds1, f1)
  f2 <- tempfile(fileext = ".rds"); saveRDS(ds2, f2)
  on.exit({ unlink(f1); unlink(f2) })

  merged <- merge_trades_datasets(f1, f2)

  btc <- merged$upbit$`BTC-KRW`
  expect_equal(nrow(btc), 3L)           # ids 10, 11, 12 — same-time rows kept
  expect_equal(sort(btc$sequential_id), c(10, 11, 12))
})

test_that("merge_trades_datasets handles multiple exchanges", {
  t1 <- as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Seoul")

  ds1 <- list(
    binance = list(`BTC-USDT` = make_trades_df(t1, 1)),
    upbit   = list(`BTC-KRW`  = make_trades_df(t1, 100))
  )
  ds2 <- list(
    binance = list(`BTC-USDT` = make_trades_df(t1, 2))
  )

  f1 <- tempfile(fileext = ".rds"); saveRDS(ds1, f1)
  f2 <- tempfile(fileext = ".rds"); saveRDS(ds2, f2)
  on.exit({ unlink(f1); unlink(f2) })

  merged <- merge_trades_datasets(f1, f2)

  expect_true("binance" %in% names(merged))
  expect_true("upbit"   %in% names(merged))
  expect_equal(nrow(merged$binance$`BTC-USDT`), 2L)
  expect_equal(nrow(merged$upbit$`BTC-KRW`),    1L)
})

test_that("merge_trades_datasets handles empty inputs gracefully", {
  ds_empty <- list(bithumb = list())
  f <- tempfile(fileext = ".rds"); saveRDS(ds_empty, f)
  on.exit(unlink(f))

  merged <- merge_trades_datasets(f)
  expect_type(merged, "list")
  expect_equal(merged$bithumb, list())
})
