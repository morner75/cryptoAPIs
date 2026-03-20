test_that("upbit_trading_pairs returns expected structure", {
  skip_if_offline()
  result <- upbit_trading_pairs()
  expect_s3_class(result, "data.frame")
  expect_named(result, c("exchange", "asset", "quote", "symbol", "market"))
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$exchange), "upbit")
})

test_that("upbit_trading_pairs filters by market returns NULL for non-exact match", {
  skip_if_offline()
  result <- upbit_trading_pairs(market = "INVALID-KRW")
  expect_null(result)
})

test_that("upbit_trading_pairs returns symbol string for specific market", {
  skip_if_offline()
  sym <- upbit_trading_pairs(market = "BTC-KRW")
  expect_type(sym, "character")
  expect_equal(sym, "KRW-BTC")
})

test_that("fetch_upbit returns OHLCV tibble", {
  skip_if_offline()
  result <- fetch_upbit("BTC-KRW", count = 5)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
  expect_equal(nrow(result), 5)
})

test_that("fetch_upbit_range returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time())
  from <- to - 3600
  result <- fetch_upbit_range("BTC-KRW", from = from, to = to, unit = "min")
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(all(result$time_kst >= from))
  expect_true(all(result$time_kst <= to))
})

test_that("get_upbit_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_upbit_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, c("market", "timestamp", "ask_price", "bid_price",
                          "ask_size", "bid_size", "total_ask_size",
                          "total_bid_size", "level"))
  expect_equal(nrow(result), 30)
  expect_equal(unique(result$market), "BTC-KRW")
  expect_s3_class(result$timestamp, "POSIXct")
})

test_that("get_upbit_orderbook respects count parameter", {
  skip_if_offline()
  result <- get_upbit_orderbook("BTC-KRW", count = 5)
  expect_equal(nrow(result), 5)
})

test_that("get_upbit_orderbook is sorted by ask_price ascending", {
  skip_if_offline()
  result <- get_upbit_orderbook("BTC-KRW")
  expect_true(all(diff(result$ask_price) >= 0))
})

test_that("get_upbit_orderbook returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_upbit_orderbook("INVALID"), "업비트")
  expect_null(result)
})
