# Extracted from test-orderbook.R:73

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# prequel ----------------------------------------------------------------------
expected_cols <- c("market", "timestamp", "ask_price", "bid_price",
                   "ask_size", "bid_size", "total_ask_size", "total_bid_size", "level")

# test -------------------------------------------------------------------------
skip_if_offline()
result <- get_coinone_orderbook("BTC-KRW")
expect_s3_class(result, "data.frame")
expect_named(result, expected_cols)
expect_true(nrow(result) > 0)
expect_equal(unique(result$market), "BTC-KRW")
expect_s3_class(result$timestamp, "POSIXct")
