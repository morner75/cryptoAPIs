test_that("okx_trading_pairs returns expected structure", {
  skip_if_offline()
  result <- okx_trading_pairs()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("exchange", "asset", "quote", "symbol", "market"))
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$exchange), "okx")
})

test_that("okx_trading_pairs returns symbol for specific market", {
  skip_if_offline()
  sym <- okx_trading_pairs(market = "BTC-USDT")
  expect_type(sym, "character")
  expect_equal(sym, "BTC-USDT")
})

test_that("fetch_okx returns OHLCV data frame", {
  skip_if_offline()
  result <- fetch_okx("BTC-USDT", count = 10)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
  expect_true(nrow(result) > 0)
})

test_that("fetch_okx_range returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time())
  from <- to - 3600
  result <- fetch_okx_range("BTC-USDT", from = from, to = to, unit = "min")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
})
