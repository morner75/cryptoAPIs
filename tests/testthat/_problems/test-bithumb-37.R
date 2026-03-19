# Extracted from test-bithumb.R:37

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
result <- get_bithumb_alarm(quote = "KRW")
