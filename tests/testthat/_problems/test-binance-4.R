# Extracted from test-binance.R:4

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- binance_trading_pairs()
expect_s3_class(result, "data.frame")
