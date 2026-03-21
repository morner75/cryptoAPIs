expected_cols <- c("market", "time_kst", "trade_price", "opening_price",
                   "high_price", "low_price", "prev_closing_price",
                   "change", "signed_change_rate",
                   "acc_trade_volume_24h", "acc_trade_price_24h")

check_prices <- function(result, market, nrow = 1,
                         na_cols   = character(0),
                         na_change = FALSE) {
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_equal(nrow(result), nrow)
  expect_equal(result$market, market)
  expect_s3_class(result$time_kst, "POSIXct")
  expect_equal(attr(result$time_kst, "tzone"), "Asia/Seoul")
  numeric_cols <- c("trade_price", "opening_price", "high_price", "low_price",
                    "prev_closing_price", "signed_change_rate",
                    "acc_trade_volume_24h", "acc_trade_price_24h")
  for (col in setdiff(numeric_cols, na_cols)) {
    expect_true(is.numeric(result[[col]]),     label = paste(col, "is numeric"))
    expect_true(all(!is.na(result[[col]])),    label = paste(col, "is not NA"))
    expect_true(all(result[[col]] >= 0),       label = paste(col, "is non-negative"))
  }
  for (col in na_cols) {
    expect_true(all(is.na(result[[col]])), label = paste(col, "is NA"))
  }
  if (!na_change) {
    expect_true(all(result$change %in% c("RISE", "FALL", "EVEN")),
                label = "change is valid")
  } else {
    expect_true(all(is.na(result$change)), label = "change is NA")
  }
  expect_true(all(result$trade_price > 0), label = "trade_price is positive")
}

# ── Upbit ──────────────────────────────────────────────────────────────────────

test_that("get_upbit_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_upbit_prices("BTC-KRW")
  check_prices(result, "BTC-KRW")
})

test_that("get_upbit_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_upbit_prices(c("BTC-KRW", "ETH-KRW"))
  check_prices(result, c("BTC-KRW", "ETH-KRW"), nrow = 2)
})

test_that("get_upbit_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_upbit_prices("INVALID-KRW"), "\uc5c5\ube44\ud2b8")
  expect_null(result)
})

# ── Bithumb ────────────────────────────────────────────────────────────────────

test_that("get_bithumb_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_bithumb_prices("BTC-KRW")
  check_prices(result, "BTC-KRW")
})

test_that("get_bithumb_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_bithumb_prices(c("BTC-KRW", "ETH-KRW"))
  check_prices(result, c("BTC-KRW", "ETH-KRW"), nrow = 2)
})

test_that("get_bithumb_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_bithumb_prices("INVALID-KRW"), "\ube57\uc378")
  expect_null(result)
})

# ── Coinone ────────────────────────────────────────────────────────────────────

test_that("get_coinone_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_coinone_prices("BTC-KRW")
  check_prices(result, "BTC-KRW")
})

test_that("get_coinone_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_coinone_prices(c("BTC-KRW", "ETH-KRW"))
  check_prices(result, c("BTC-KRW", "ETH-KRW"), nrow = 2)
})

test_that("get_coinone_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_coinone_prices("INVALID-KRW"), "\ucf54\uc778\uc6d0")
  expect_null(result)
})

# ── Korbit ─────────────────────────────────────────────────────────────────────

test_that("get_korbit_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_korbit_prices("BTC-KRW")
  check_prices(result, "BTC-KRW")
})

test_that("get_korbit_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_korbit_prices(c("BTC-KRW", "ETH-KRW"))
  check_prices(result, c("BTC-KRW", "ETH-KRW"), nrow = 2)
})

test_that("get_korbit_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_korbit_prices("INVALID-KRW"), "\ucf54\ube57")
  expect_null(result)
})

# ── GOPAX ──────────────────────────────────────────────────────────────────────

test_that("get_gopax_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_gopax_prices("BTC-KRW")
  check_prices(result, "BTC-KRW",
               na_cols   = c("prev_closing_price", "signed_change_rate"),
               na_change = TRUE)
})

test_that("get_gopax_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_gopax_prices(c("BTC-KRW", "ETH-KRW"))
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_equal(nrow(result), 2)
  expect_equal(result$market, c("BTC-KRW", "ETH-KRW"))
})

test_that("get_gopax_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_gopax_prices("INVALID-KRW"), "\uace0\ud30d\uc2a4")
  expect_null(result)
})

# ── Binance ────────────────────────────────────────────────────────────────────

test_that("get_binance_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_binance_prices("BTC-USDT")
  check_prices(result, "BTC-USDT")
})

test_that("get_binance_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_binance_prices(c("BTC-USDT", "ETH-USDT"))
  check_prices(result, c("BTC-USDT", "ETH-USDT"), nrow = 2)
})

test_that("get_binance_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_binance_prices("INVALID-USDT"), "\ubc14\uc774\ub0b8\uc2a4")
  expect_null(result)
})

# ── Coinbase ───────────────────────────────────────────────────────────────────

test_that("get_coinbase_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_coinbase_prices("BTC-USD")
  check_prices(result, "BTC-USD",
               na_cols = c("prev_closing_price", "acc_trade_price_24h"))
})

test_that("get_coinbase_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_coinbase_prices(c("BTC-USD", "ETH-USD"))
  expect_s3_class(result, "data.frame")
  expect_named(result, expected_cols)
  expect_equal(nrow(result), 2)
  expect_equal(result$market, c("BTC-USD", "ETH-USD"))
})

test_that("get_coinbase_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_coinbase_prices("INVALID-USD"), "\ucf54\uc778\ubca0\uc774\uc2a4")
  expect_null(result)
})

# ── OKX ────────────────────────────────────────────────────────────────────────

test_that("get_okx_prices returns expected structure for single market", {
  skip_if_offline()
  result <- get_okx_prices("BTC-USDT")
  check_prices(result, "BTC-USDT")
})

test_that("get_okx_prices returns multiple rows for multiple markets", {
  skip_if_offline()
  result <- get_okx_prices(c("BTC-USDT", "ETH-USDT"))
  check_prices(result, c("BTC-USDT", "ETH-USDT"), nrow = 2)
})

test_that("get_okx_prices returns NULL for invalid market", {
  skip_if_offline()
  expect_message(result <- get_okx_prices("INVALID-USDT"), "OKX")
  expect_null(result)
})
