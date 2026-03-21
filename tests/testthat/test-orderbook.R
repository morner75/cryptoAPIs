expected_cols <- c("market", "timestamp", "ask_price", "bid_price",
                   "ask_size", "bid_size", "total_ask_size", "total_bid_size", "level")

# ── Upbit ──────────────────────────────────────────────────────────────────────

test_that("get_upbit_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_upbit_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
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
  expect_message(result <- get_upbit_orderbook("INVALID-KRW"), "업비트")
  expect_null(result)
})

# ── Bithumb ────────────────────────────────────────────────────────────────────

test_that("get_bithumb_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_bithumb_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_equal(nrow(result), 30)
  expect_equal(unique(result$market), "BTC-KRW")
  expect_s3_class(result$timestamp, "POSIXct")
})

test_that("get_bithumb_orderbook respects count parameter", {
  skip_if_offline()
  result <- get_bithumb_orderbook("BTC-KRW", count = 5)
  expect_equal(nrow(result), 5)
})

test_that("get_bithumb_orderbook is sorted by ask_price ascending", {
  skip_if_offline()
  result <- get_bithumb_orderbook("BTC-KRW")
  expect_true(all(diff(result$ask_price) >= 0))
})

test_that("get_bithumb_orderbook returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_bithumb_orderbook("INVALID-KRW"), "빗썸")
  expect_null(result)
})

# ── Coinone ────────────────────────────────────────────────────────────────────

test_that("get_coinone_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_coinone_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$market), "BTC-KRW")
  expect_s3_class(result$timestamp, "POSIXct")
  expect_true(all(result$level == 0L))
})

test_that("get_coinone_orderbook respects count parameter", {
  skip_if_offline()
  result <- get_coinone_orderbook("BTC-KRW", count = 5)
  expect_lte(nrow(result), 5)
})

test_that("get_coinone_orderbook is sorted by ask_price ascending", {
  skip_if_offline()
  result <- get_coinone_orderbook("BTC-KRW")
  expect_true(all(diff(result$ask_price) >= 0))
})

test_that("get_coinone_orderbook returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_coinone_orderbook("INVALID-KRW"), "코인원")
  expect_null(result)
})

# ── Korbit ─────────────────────────────────────────────────────────────────────

test_that("get_korbit_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_korbit_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$market), "BTC-KRW")
  expect_s3_class(result$timestamp, "POSIXct")
  expect_true(all(result$level == 0L))
})

test_that("get_korbit_orderbook respects count parameter", {
  skip_if_offline()
  result <- get_korbit_orderbook("BTC-KRW", count = 5)
  expect_lte(nrow(result), 5)
})

test_that("get_korbit_orderbook is sorted by ask_price ascending", {
  skip_if_offline()
  result <- get_korbit_orderbook("BTC-KRW")
  expect_true(all(diff(result$ask_price) >= 0))
})

test_that("get_korbit_orderbook returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_korbit_orderbook("INVALID-KRW"), "코빗")
  expect_null(result)
})

# ── GOPAX ──────────────────────────────────────────────────────────────────────

test_that("get_gopax_orderbook returns expected structure", {
  skip_if_offline()
  result <- get_gopax_orderbook("BTC-KRW")
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_true(nrow(result) > 0)
  expect_equal(unique(result$market), "BTC-KRW")
  expect_s3_class(result$timestamp, "POSIXct")
  expect_true(all(result$level == 0L))
})

test_that("get_gopax_orderbook respects count parameter", {
  skip_if_offline()
  result <- get_gopax_orderbook("BTC-KRW", count = 5)
  expect_lte(nrow(result), 5)
})

test_that("get_gopax_orderbook is sorted by ask_price ascending", {
  skip_if_offline()
  result <- get_gopax_orderbook("BTC-KRW")
  expect_true(all(diff(result$ask_price) >= 0))
})

test_that("get_gopax_orderbook returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_gopax_orderbook("INVALID-KRW"), "고팍스")
  expect_null(result)
})
