test_that("invalid inputs are caught", {
  expect_error(
    receptiviti_norming(
      name = "INVALID",
      key = "123",
      secret = "123"
    ),
    "`name` can only include"
  )
})

skip_if(Sys.getenv("RECEPTIVITI_KEY") == "", "no API key")

test_that("listing works", {
  custom_norms <- receptiviti_norming()
  expect_true("custom/test" %in% custom_norms$name)

  custom_norms <- receptiviti_norming(name_only = TRUE)
  expect_true("custom/test" %in% custom_norms)
})

test_that("retrieving a status works", {
  expect_identical(receptiviti_norming("test")$name, "custom/test")
})

test_that("updating works", {
  norming_context <- "short_text"
  receptiviti_norming(norming_context, delete = TRUE)
  receptiviti_norming(norming_context, options = list(min_word_count = 1))
  expect_error(
    receptiviti(
      "a text to score",
      version = "v2",
      custom_context = norming_context
    ),
    "is not complete"
  )
  updated <- receptiviti_norming(norming_context, "new text to add")
  final_status <- receptiviti_norming(norming_context)
  expect_true(final_status$status == "completed")
  base_request <- receptiviti("a new text to add", version = "v2")
  self_normed_request <- receptiviti(
    "a new text to add",
    version = "v2",
    custom_context = norming_context
  )
  expect_false(identical(base_request, self_normed_request))
})
