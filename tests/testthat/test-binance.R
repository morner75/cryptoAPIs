test_that("binance_trading_pairs returns expected structure", {
  skip_if_offline()
  result <- binance_trading_pairs()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("exchange", "asset", "quote", "symbol", "market"))
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$exchange), "binance")
})

test_that("binance_trading_pairs returns symbol for specific market", {
  skip_if_offline()
  sym <- binance_trading_pairs(market = "BTC-USDT")
  expect_type(sym, "character")
  expect_equal(sym, "BTCUSDT")
})

test_that("fetch_binance returns OHLCV data frame", {
  skip_if_offline()
  result <- fetch_binance("BTC-USDT", count = 5)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
  expect_equal(nrow(result), 5)
})

test_that("fetch_binance_range returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time())
  from <- to - 3600
  result <- fetch_binance_range("BTC-USDT", from = from, to = to, unit = "min")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(all(result$time_kst >= from))
  expect_true(all(result$time_kst <= to))
})
