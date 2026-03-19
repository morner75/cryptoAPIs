# Extracted from test-upbit.R:58

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- get_upbit_orderbook("BTC-KRW", count = 5)
expect_equal(nrow(result), 5)
