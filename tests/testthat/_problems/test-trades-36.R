# Extracted from test-trades.R:36

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "cryptoAPIs", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
skip_if_offline()
to   <- as.POSIXct(Sys.time(), tz = "Asia/Seoul")
from <- to - 300
result <- get_upbit_trades("BTC-KRW", from = from, to = to)
