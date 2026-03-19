# Extracted from test-gopax.R:20

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- fetch_gopax("BTC-KRW", count = 10)
expect_s3_class(result, "data.frame")
