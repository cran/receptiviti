skip_if(Sys.getenv("RECEPTIVITI_KEY") == "", "no API key")

test_that("listing works", {
  expect_error(receptiviti_frameworks(url = ""))
  framworks <- receptiviti_frameworks()
  expect_true("liwc15" %in% framworks)
})
