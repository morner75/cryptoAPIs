# Extracted from test-upbit.R:52

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- get_upbit_orderbook("BTC-KRW")
expect_s3_class(result, "data.frame")
expect_named(result, c("market", "timestamp", "ask_price", "bid_price",
                          "ask_size", "bid_size", "total_ask_size",
                          "total_bid_size", "level"))
expect_equal(nrow(result), 30)
expect_equal(unique(result$market), "BTC-KRW")
expect_s3_class(result$timestamp, "POSIXct")
