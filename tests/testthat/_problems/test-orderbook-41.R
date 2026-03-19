# Extracted from test-orderbook.R:41

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# prequel ----------------------------------------------------------------------
expected_cols <- c("market", "timestamp", "ask_price", "bid_price",
                   "ask_size", "bid_size", "total_ask_size", "total_bid_size", "level")

# test -------------------------------------------------------------------------
skip_if_offline()
result <- get_bithumb_orderbook("BTC-KRW")
expect_s3_class(result, "data.frame")
expect_named(result, expected_cols)
expect_equal(nrow(result), 30)
