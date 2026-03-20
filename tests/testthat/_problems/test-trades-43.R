# Extracted from test-trades.R:43

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
expect_message(result <- get_upbit_trades("INVALID-KRW", from = Sys.time() - 60,
                                             to = Sys.time()), "업비트")
