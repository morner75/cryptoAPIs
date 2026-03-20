test_that("bithumb_trading_pairs returns expected structure", {
  skip_if_offline()
  result <- bithumb_trading_pairs()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("exchange", "asset", "quote", "symbol", "market"))
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$exchange), "bithumb")
})

test_that("bithumb_trading_pairs returns symbol for specific market", {
  skip_if_offline()
  sym <- bithumb_trading_pairs(market = "BTC-KRW")
  expect_type(sym, "character")
  expect_equal(sym, "KRW-BTC")
})

test_that("fetch_bithumb returns OHLCV data frame", {
  skip_if_offline()
  result <- fetch_bithumb("BTC-KRW", count = 5)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
  expect_true(nrow(result) > 0)
})

test_that("fetch_bithumb_range returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 3600
  result <- fetch_bithumb_range("BTC-KRW", from = from, to = to, unit = "min")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
})

test_that("get_bithumb_alarm returns expected structure", {
  skip_if_offline()
  result <- get_bithumb_alarm(quote = "KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, c("market", "symbol", "alarm", "reason"))
})
