test_that("failures works", {
  expect_error(receptiviti_status(key = ""), "specify your key")
  expect_error(
    receptiviti_status(key = 123, secret = ""),
    "specify your secret"
  )
  expect_error(
    receptiviti_status("localhost", key = 123, secret = 123),
    "url does not appear to be valid"
  )
})

skip_if(Sys.getenv("RECEPTIVITI_KEY") == "", "no API key")

test_that("http failures works", {
  expect_null(receptiviti_status(
    "http://localhost:0/not_served",
    key = 123,
    secret = 123
  ))
  invalid_message <- capture.output(
    receptiviti_status("example.com", key = 123, secret = 123),
    type = "message"
  )[2]
  skip_if(invalid_message == "Message: 500", "not checking real responses")
  expect_identical(invalid_message, "Message: 404")
  expect_true(grepl(
    "Message: 401 (1411): ",
    capture.output(
      receptiviti_status(key = 123, secret = 123),
      type = "message"
    )[2],
    fixed = TRUE
  ))
})

test_that("success works", {
  message <- capture.output(
    receptiviti_status(include_headers = TRUE),
    type = "message"
  )
  expect_identical(message[1], "Status: OK")
  expect_true(grepl("200", message[4], fixed = TRUE))
})
