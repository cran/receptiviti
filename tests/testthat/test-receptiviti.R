options(stringsAsFactors = FALSE)
key <- Sys.getenv("RECEPTIVITI_KEY")
secret <- Sys.getenv("RECEPTIVITI_SECRET")
text <- "a text to score"
temp <- normalizePath(tempdir(), "/")
temp_cache <- paste0(temp, "/temp_cache")

test_that("invalid inputs are caught", {
  expect_error(
    receptiviti(
      text,
      url = "http://localhost:0/not_served",
      key = 123,
      secret = 123
    ),
    "URL is unreachable",
    fixed = TRUE
  )
  expect_error(
    receptiviti(text, version = "1", key = 123, secret = 123),
    "invalid version: 1",
    fixed = TRUE
  )
  expect_error(
    receptiviti(text, endpoint = "framework/v1", key = 123, secret = 123),
    "invalid endpoint: v1",
    fixed = TRUE
  )
  expect_error(receptiviti(), "enter text as the first argument", fixed = TRUE)
  expect_error(receptiviti("", key = ""), "specify your key", fixed = TRUE)
  expect_error(
    receptiviti("", key = 123, secret = 123),
    "401 (1411): ",
    fixed = TRUE
  )
  expect_error(
    receptiviti("", key = 123, secret = ""),
    "specify your secret",
    fixed = TRUE
  )
  expect_error(
    receptiviti(matrix(0, 2, 2)),
    "text has dimensions, but no text_column column",
    fixed = TRUE
  )
  expect_error(
    receptiviti("", id = 1:2),
    "id is not the same length as text",
    fixed = TRUE
  )
  expect_error(
    receptiviti(c("", ""), id = c(1, 1)),
    "id contains duplicate values",
    fixed = TRUE
  )
  expect_error(
    receptiviti(NA, text_as_paths = TRUE),
    "NAs are not allowed in text when being treated as file paths",
    fixed = TRUE
  )
  expect_error(
    receptiviti("", text_as_paths = TRUE),
    "not all of the files in text exist",
    fixed = TRUE
  )
})

test_that("errors given existing results", {
  file <- tempfile(fileext = ".csv")
  write.csv(matrix(1), file, row.names = FALSE)
  expect_error(
    receptiviti(output = file),
    "output file already exists",
    fixed = TRUE
  )
})

skip_if(key == "", "no API key")
Sys.setenv(RECEPTIVITI_KEY = key, RECEPTIVITI_SECRET = secret)
output <- paste0(tempdir(), "/single_text.csv")

test_that("default cache works", {
  receptiviti(text, cache = TRUE)
  expect_identical(
    receptiviti(text, cache = TRUE, make_request = FALSE)$summary.word_count,
    4L
  )
})

test_that("invalid texts are caught", {
  expect_error(
    receptiviti(paste(rep(" ", 1e7), collapse = "")),
    "one of your texts is over the individual size limit",
    fixed = TRUE
  )
  expect_error(receptiviti(NA), "no valid texts to process", fixed = TRUE)
})

test_that("make_request works", {
  expect_error(
    receptiviti(text, request_cache = FALSE, make_request = FALSE),
    "make_request is FALSE, but there are texts with no cached results",
    fixed = TRUE
  )
})

test_that("a single text works", {
  score <- receptiviti(
    text,
    output,
    overwrite = TRUE,
    cache = temp_cache,
    clear_cache = TRUE
  )
  expect_equal(
    score[, c(
      "social_dynamics.clout",
      "disc_dimensions.bold_assertive_outgoing"
    )],
    data.frame(
      social_dynamics.clout = 63.646087,
      disc_dimensions.bold_assertive_outgoing = 68.137361
    )
  )
  expect_true(file.exists(output))
  expect_equal(read.csv(output), score)
})

test_that("v2 works", {
  score <- receptiviti(text, version = "v2")
  expect_equal(
    score[, c(
      "social_dynamics.clout",
      "disc_dimensions.bold_assertive_outgoing"
    )],
    data.frame(
      social_dynamics.clout = 45.1339937,
      disc_dimensions.bold_assertive_outgoing = 54.6356911
    )
  )
})

test_that("context works", {
  score <- receptiviti(text, version = "v2", context = "spoken")
  expect_true(score$drives.power > .05)
})

test_that("api arguments work", {
  txt <- "the whole feeling in my mind now is one of joy and thankfulness"
  sparse <- receptiviti(
    txt,
    frameworks = "sallee",
    api_args = list(sallee_mode = "sparse")
  )
  expect_identical(sparse$sallee_mode, "sparse")
  default <- receptiviti(txt, frameworks = "sallee")
  expect_identical(default$sallee_mode, NULL)
  expect_true(sparse$sentiment != default$sentiment)
  expect_error(
    receptiviti(
      txt,
      version = "v2",
      custom_context = "not_created"
    ),
    "not on record"
  )
})

test_that("framework selection works", {
  receptiviti(text, cache = temp_cache, collect_results = FALSE)
  score <- receptiviti(text, cache = temp_cache, make_request = FALSE)
  expect_equal(
    receptiviti(
      text,
      frameworks = c("summary", "liwc"),
      framework_prefix = TRUE,
      cache = temp_cache
    ),
    score[, grep("^(?:text_|summary|liwc)", colnames(score))]
  )
  options(receptiviti.frameworks = "summary")
  expect_equal(
    receptiviti(text, framework_prefix = TRUE, cache = temp_cache),
    score[, grep("^(?:text_|summary)", colnames(score))]
  )
  options(receptiviti.frameworks = "all")
  expect_warning(
    receptiviti(text, frameworks = "x", cache = temp_cache),
    "frameworks did not match any columns -- returning all",
    fixed = TRUE
  )
  expect_error(
    receptiviti(text, frameworks = "x", version = "v2"),
    "not available to your account: x",
    fixed = TRUE
  )
})

test_that("framework prefix removal works", {
  score <- receptiviti(text, cache = temp_cache)
  colnames(score) <- sub("^.+\\.", "", colnames(score))
  expect_equal(
    receptiviti(text, cache = temp_cache, framework_prefix = FALSE),
    score
  )
})

test_that("as_list works", {
  score_list <- receptiviti(matrix(text), as_list = TRUE)
  expect_identical(
    score_list$personality,
    receptiviti(text, cache = temp_cache, frameworks = "personality")
  )
})

test_that("compression works", {
  compressed_output <- tempfile(fileext = ".csv")
  scores <- receptiviti(
    text,
    compressed_output,
    cache = temp_cache,
    compress = TRUE
  )
  expect_true(file.exists(paste0(compressed_output, ".xz")))
})

test_that("NAs and empty texts are handled, and IDs align", {
  id <- paste0("id", rnorm(5))
  score <- receptiviti(
    c("", text, NA, text, NA),
    id = id
  )
  expect_identical(score$id, id)
  expect_identical(score$summary.word_count, c(NA, 4L, NA, 4L, NA))
})

test_that("repeated texts works", {
  texts <- rep(text, 2000)
  scores <- receptiviti(texts, return_text = TRUE)
  expect_identical(texts, scores$text)
  expect_true(all(scores[1, -(1:2)] == scores[2000, -(1:2)]))
})

test_that("later invalid inputs are caught", {
  expect_error(
    receptiviti(" ", text_column = ""),
    "text_column is specified, but text has no columns",
    fixed = TRUE
  )
  expect_error(
    receptiviti(" ", id_column = ""),
    "id_column is specified, but text has no columns",
    fixed = TRUE
  )
  expect_error(
    receptiviti(matrix(0, 2), text_column = ""),
    "text_column not found in text",
    fixed = TRUE
  )
  expect_error(
    receptiviti(matrix(0, 2), id_column = ""),
    "id_column not found in text",
    fixed = TRUE
  )
})

skip_if(!grepl("R_LIBS", getwd(), fixed = TRUE), "not making bigger requests")

words <- vapply(
  seq_len(200),
  function(w) {
    paste0(sample(letters, sample.int(9, 1)), collapse = "")
  },
  ""
)
texts <- vapply(
  seq_len(50),
  function(d) {
    paste0(sample(words, sample.int(100, 1), TRUE), collapse = " ")
  },
  ""
)
temp_output <- tempfile(fileext = ".csv")
initial <- NULL
temp_source <- paste0(temp, "/temp_store/")
dir.create(temp_source, FALSE)
text_seq <- seq_along(texts)
files_txt <- paste0(temp_source, text_seq, ".txt")

test_that("verbose works", {
  expect_identical(
    sub(
      " \\([0-9.]+\\)",
      "",
      capture.output(
        initial <<- receptiviti(
          texts,
          temp_output,
          cache = temp_cache,
          clear_cache = TRUE,
          verbose = TRUE,
          overwrite = TRUE
        ),
        type = "message"
      )
    ),
    c(
      "pinging API",
      "preparing text",
      "prepared text in 1 bundle",
      "processing bundle sequentially",
      "done retrieving",
      "defragmenting cache",
      "preparing output",
      paste("writing results to file:", temp_output),
      "done"
    )
  )
})

test_that("cache updating and acceptable alternates are handled", {
  expect_identical(
    sub(
      " \\([0-9.]+\\)",
      "",
      capture.output(
        receptiviti(
          data.frame(
            text = as.factor(paste0(
              sample(words, sample.int(100, 1), TRUE),
              collapse = " "
            ))
          ),
          text_column = "text",
          verbose = TRUE,
          cache = temp_cache,
          retry_limit = FALSE,
          bundle_size = FALSE
        ),
        type = "message"
      )
    ),
    c(
      "pinging API",
      "preparing text",
      "prepared text in 1 bundle",
      "processing bundle sequentially",
      "done retrieving",
      "defragmenting cache",
      "preparing output",
      "done"
    )
  )
})

test_that("return is consistent between sources", {
  # from request cache
  expect_equal(receptiviti(texts), initial)

  # from main cache
  expect_equal(
    receptiviti(texts, cache = temp_cache, request_cache = FALSE),
    initial
  )
})

test_that("parallelization methods are consistent", {
  # sequential
  expect_equal(
    receptiviti(texts, cache = temp_cache, bundle_size = 25, cores = 1),
    initial
  )

  # parallel
  expect_equal(
    receptiviti(texts, cache = temp_cache, bundle_size = 10),
    initial
  )

  # parallel from disc
  expect_equal(
    receptiviti(texts, cache = temp_cache, bundle_size = 10, in_memory = FALSE),
    initial
  )

  # sequential future
  expect_equal(
    receptiviti(texts, cache = temp_cache, bundle_size = 25, use_future = TRUE),
    initial
  )
})

test_that("reading from files works", {
  file_txt <- paste0(temp, "/texts.txt")
  file_csv <- paste0(temp, "/texts.csv")
  writeLines(texts, file_txt)
  csv_data <- data.frame(id = text_seq, raw_text = texts)
  arrow::write_csv_arrow(csv_data, file_csv)
  files_csv <- paste0(temp_source, text_seq, ".csv")
  for (i in text_seq) {
    writeLines(texts[i], files_txt[i])
    arrow::write_csv_arrow(
      data.frame(id = i, raw_text = texts[i]),
      files_csv[i]
    )
  }
  writeLines(texts[1], paste0(temp_source, "0.txt"))

  txt_directory <- receptiviti(
    temp_source,
    cache = temp_cache,
    bundle_size = 25
  )
  expect_identical(
    receptiviti(dir = temp_source, cache = temp_cache),
    txt_directory
  )
  initial <- cbind(id = files_txt, initial)
  expect_true(all(initial$text_hash %in% txt_directory$text_hash))
  expect_false(anyDuplicated(txt_directory$text_hash) == 0)
  txt_directory <- txt_directory[-1, ]
  txt_directory <- txt_directory[!duplicated(txt_directory$text_hash), ]
  rownames(txt_directory) <- txt_directory$text_hash
  txt_directory <- txt_directory[initial$text_hash, ]
  rownames(txt_directory) <- NULL
  expect_equal(txt_directory, initial)

  expect_error(
    receptiviti(temp_source, file_type = "csv", cache = temp_cache),
    "text appears to point to csv files, but text_column was not specified",
    fixed = TRUE
  )
  arrow::write_csv_arrow(
    data.frame(raw_text = NA),
    paste0(temp_source, "!.csv")
  )
  csv_directory <- receptiviti(
    temp_source,
    text_column = "raw_text",
    file_type = "csv",
    cache = temp_cache,
    bundle_size = 25,
    collapse_lines = TRUE
  )
  expect_true(all(initial$text_hash %in% csv_directory$text_hash))
  csv_directory <- csv_directory[!is.na(csv_directory$text_hash), ]
  rownames(csv_directory) <- csv_directory$text_hash
  csv_directory <- csv_directory[initial$text_hash, ]
  rownames(csv_directory) <- NULL
  initial$id <- files_csv
  expect_equal(csv_directory, initial)

  expect_equal(
    receptiviti(file_txt, cache = temp_cache, in_memory = FALSE)[, -1],
    initial[, -1]
  )
  expect_equal(
    receptiviti(files = file_txt, cache = temp_cache)[, -1],
    initial[, -1]
  )
  expect_equal(
    receptiviti(file_txt, collapse_lines = TRUE, cache = temp_cache)[, -1],
    receptiviti(paste(texts, collapse = " "), cache = temp_cache)
  )
  expect_error(receptiviti(file_csv, cache = temp_cache))
  expect_equal(
    receptiviti(file_csv, text_column = "raw_text", cache = temp_cache)[, -1],
    initial[, -1]
  )

  alt_id <- receptiviti(
    file_csv,
    text_column = "raw_text",
    id_column = "id",
    cache = temp_cache
  )
  expect_identical(
    alt_id,
    receptiviti(
      csv_data,
      text_column = "raw_text",
      id = "id",
      cache = temp_cache
    )
  )
  expect_identical(
    alt_id,
    receptiviti(csv_data$raw_text, id = csv_data$id, cache = temp_cache)
  )

  first_res <- receptiviti(texts[1], cache = temp_cache)
  writeBin(
    iconv(texts[1], "utf-8", "utf-16", toRaw = TRUE)[[1]],
    file_txt,
    useBytes = TRUE
  )
  expect_error(
    suppressWarnings(receptiviti(file_txt, encoding = "utf-8")),
    "no texts were found"
  )
  expect_equal(receptiviti(file_txt)[, -1], first_res)

  writeBin(
    iconv(
      paste0("raw_text\n", texts[1], "\n"),
      "utf-8",
      "utf-16",
      toRaw = TRUE
    )[[1]],
    file_csv,
    useBytes = TRUE
  )
  expect_error(
    suppressWarnings(receptiviti(
      file_csv,
      text_column = "raw_text",
      encoding = "utf-8"
    )),
    "failed to read in"
  )
  expect_equal(receptiviti(file_csv, text_column = "raw_text")[, -1], first_res)
})

test_that("rate limit is handled", {
  texts <- vapply(
    seq_len(50),
    function(d) {
      paste0(sample(words, 5, TRUE), collapse = " ")
    },
    ""
  )
  expect_error(
    receptiviti(texts, bundle_size = 1, request_cache = FALSE, retry_limit = 0),
    "Rate limit exceeded"
  )
  expect_identical(
    receptiviti(
      texts,
      bundle_size = 1,
      request_cache = FALSE
    )$summary.word_count,
    rep(5L, 50)
  )
})

url <- Sys.getenv("RECEPTIVITI_URL_TEST")
skip_if(is.null(receptiviti_status(url)), "test API is not reachable")

key <- Sys.getenv("RECEPTIVITI_KEY_TEST")
secret <- Sys.getenv("RECEPTIVITI_SECRET_TEST")

test_that("spliting oversized bundles works", {
  unlink(list.files(temp_source, "txt", full.names = TRUE), TRUE)
  for (i in seq_along(texts)) {
    writeLines(texts[i], files_txt[i])
  }
  arg_hash <- digest::digest(
    jsonlite::toJSON(
      list(
        url = paste0(url, "v1/framework/bulk"),
        key = key,
        secret = secret
      ),
      auto_unbox = TRUE
    ),
    serialize = FALSE
  )
  log <- capture.output(
    res <- receptiviti(
      temp_source,
      url = url,
      key = key,
      secret = secret,
      bundle_byte_limit = 1e4,
      verbose = TRUE
    ),
    type = "message"
  )
  expect_true(any(grepl("bundles", log, fixed = TRUE)))
  expect_identical(
    res$text_hash,
    unname(vapply(
      list.files(temp_source, "txt", full.names = TRUE),
      function(f) {
        unname(digest::digest(
          paste0(arg_hash, texts[files_txt == f]),
          serialize = FALSE
        ))
      },
      ""
    ))
  )
})

test_that("different versions and endpoints are handled", {
  res <- receptiviti(
    "a text to score",
    url = paste0(url, "v2/analyze"),
    key = key,
    secret = secret
  )
  expect_true(nrow(res) == 1)
})
