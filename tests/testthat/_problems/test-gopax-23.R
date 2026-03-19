# Extracted from test-gopax.R:23

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- fetch_gopax("BTC-KRW", count = 10)
expect_s3_class(result, "data.frame")
expect_named(result, c("time_kst", "opening_price", "high_price",
                          "low_price", "trade_price", "volume"))
expect_true(nrow(result) > 0)
