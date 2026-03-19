test_that("korbit_trading_pairs returns expected structure", {
  skip_if_offline()
  result <- korbit_trading_pairs()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("exchange", "asset", "quote", "symbol", "market"))
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$exchange), "korbit")
})

test_that("korbit_trading_pairs returns symbol for specific market", {
  skip_if_offline()
  sym <- korbit_trading_pairs(market = "BTC-KRW")
  expect_type(sym, "character")
  expect_true(grepl("btc", sym, ignore.case = TRUE))
})

test_that("fetch_korbit returns OHLCV data frame", {
  skip_if_offline()
  result <- fetch_korbit("BTC-KRW", count = 10)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
  expect_true(nrow(result) > 0)
})

test_that("fetch_korbit_range returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 3600
  result <- fetch_korbit_range("BTC-KRW", from = from, to = to, unit = "min")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
})
