test_that("get_upbit_trades returns expected structure", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 300  # 5분
  result <- get_upbit_trades("BTC-KRW", from = from, to = to)
  expect_s3_class(result, "data.frame")
  expect_named(result, c("time_kst", "trade_price", "volume", "ask_bid", "sequential_id"))
  expect_true(nrow(result) > 0)
  expect_s3_class(result$time_kst, "POSIXct")
  expect_type(result$trade_price, "double")
  expect_type(result$volume, "double")
  expect_true(all(result$ask_bid %in% c("ASK", "BID")))
})

test_that("get_upbit_trades returns data within range", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 300
  result <- get_upbit_trades("BTC-KRW", from = from, to = to)
  expect_true(all(result$time_kst >= from))
  expect_true(all(result$time_kst <= to))
})

test_that("get_upbit_trades is sorted by time_kst ascending", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 300
  result <- get_upbit_trades("BTC-KRW", from = from, to = to)
  expect_true(all(diff(as.numeric(result$time_kst)) >= 0))
})

test_that("get_upbit_trades has no duplicate sequential_id", {
  skip_if_offline()
  to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
  from <- to - 300
  result <- get_upbit_trades("BTC-KRW", from = from, to = to)
  expect_equal(nrow(result), length(unique(result$sequential_id)))
})

test_that("get_upbit_trades returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_upbit_trades("INVALID-KRW", from = Sys.time() - 60,
                                             to = Sys.time()), "업비트")
  expect_null(result)
})
