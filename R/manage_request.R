manage_request <- function(
  text = NULL,
  id = NULL,
  text_column = NULL,
  id_column = NULL,
  files = NULL,
  dir = NULL,
  file_type = "txt",
  encoding = NULL,
  context = "written",
  api_args = getOption("receptiviti.api_args", list()),
  bundle_size = 1000,
  bundle_byte_limit = 75e5,
  collapse_lines = FALSE,
  retry_limit = 50,
  clear_scratch_cache = TRUE,
  request_cache = TRUE,
  cores = detectCores() - 1,
  collect_results = TRUE,
  use_future = FALSE,
  in_memory = TRUE,
  verbose = FALSE,
  make_request = TRUE,
  text_as_paths = FALSE,
  cache = Sys.getenv("RECEPTIVITI_CACHE"),
  cache_overwrite = FALSE,
  cache_format = Sys.getenv("RECEPTIVITI_CACHE_FORMAT", "parquet"),
  key = Sys.getenv("RECEPTIVITI_KEY"),
  secret = Sys.getenv("RECEPTIVITI_SECRET"),
  url = Sys.getenv("RECEPTIVITI_URL"),
  version = Sys.getenv("RECEPTIVITI_VERSION"),
  endpoint = Sys.getenv("RECEPTIVITI_ENDPOINT"),
  to_norming = FALSE
) {
  # check input
  if (use_future && !requireNamespace("future.apply", quietly = TRUE)) {
    stop("install the `future.apply` package to use future", call. = FALSE)
  }
  st <- proc.time()[[3]]
  text_as_dir <- FALSE
  if (is.null(text)) {
    if (!is.null(dir)) {
      if (!dir.exists(dir)) stop("entered dir does not exist", call. = FALSE)
      text <- dir
      text_as_dir <- TRUE
    } else if (!is.null(files)) {
      text <- files
      text_as_paths <- TRUE
    } else {
      stop(
        "enter text as the first argument, or use files or dir",
        call. = FALSE
      )
    }
  }
  if (text_as_paths) {
    if (anyNA(text))
      stop(
        "NAs are not allowed in text when being treated as file paths",
        call. = FALSE
      )
    if (!all(file.exists(text)))
      stop("not all of the files in text exist", call. = FALSE)
  }
  read_in <- FALSE
  handle_encoding <- function(file) {
    if (is.null(encoding)) {
      con <- gzfile(file, "rb")
      on.exit(close(con))
      unlist(stringi::stri_enc_detect(readBin(con, "raw", file.size(file)))[[
        1
      ]])[[1]]
    } else {
      encoding
    }
  }
  if (
    text_as_dir ||
      text_as_paths ||
      (is.character(text) && !anyNA(text) && all(nchar(text) < 500))
  ) {
    if (text_as_dir || length(text) == 1 && dir.exists(text)) {
      if (verbose)
        message(
          "reading in texts from directory: ",
          text,
          " (",
          round(proc.time()[[3]] - st, 4),
          ")"
        )
      text_as_paths <- TRUE
      text <- normalizePath(
        list.files(text, file_type, full.names = TRUE),
        "/",
        FALSE
      )
    }
    if (text_as_paths || all(file.exists(text))) {
      text_as_paths <- collapse_lines
      if (!collapse_lines) {
        if (verbose)
          message(
            "reading in texts from file list (",
            round(proc.time()[[3]] - st, 4),
            ")"
          )
        if (is.null(id_column))
          names(text) <- if (length(id) != length(text)) text else id
        if (all(grepl("\\.csv", text, TRUE))) {
          if (is.null(text_column))
            stop(
              "text appears to point to csv files, but text_column was not specified",
              call. = FALSE
            )
          read_in <- TRUE
          text <- unlist(lapply(text, function(f) {
            d <- tryCatch(
              {
                enc <- handle_encoding(f)
                con <- gzfile(f, encoding = enc)
                arrow::read_csv_arrow(
                  con,
                  read_options = arrow::CsvReadOptions$create(
                    encoding = enc
                  ),
                  col_select = c(text_column, id_column)
                )
              },
              error = function(e) NULL
            )
            if (is.null(d)) stop("failed to read in file ", f, call. = FALSE)
            if (!is.null(id_column) && id_column %in% colnames(d)) {
              structure(
                d[, text_column, drop = TRUE],
                names = d[, id_column, drop = TRUE]
              )
            } else {
              d[, text_column, drop = TRUE]
            }
          }))
        } else {
          text <- unlist(lapply(text, function(f) {
            tryCatch(
              {
                con <- gzfile(f, encoding = handle_encoding(f))
                on.exit(close(con))
                d <- readLines(con, warn = FALSE, skipNul = TRUE)
                d[d != ""]
              },
              error = function(e)
                stop("failed to read in file ", f, call. = FALSE)
            )
          }))
        }
        id <- names(text)
      }
    } else if (
      length(text) == 1 &&
        dirname(text) != "." &&
        dir.exists(dirname(dirname(text)))
    ) {
      stop("text appears to be a directory, but it does not exist")
    }
    if (text_as_paths && is.null(id)) {
      id <- text
      if (anyDuplicated(id))
        id <- names(unlist(lapply(
          split(id, factor(id, unique(id))),
          seq_along
        )))
    }
  }
  if (is.null(dim(text))) {
    if (!read_in) {
      if (!text_as_paths && !is.null(text_column))
        stop("text_column is specified, but text has no columns", call. = FALSE)
      if (!is.null(id_column))
        stop("id_column is specified, but text has no columns", call. = FALSE)
    }
  } else {
    if (length(id) == 1 && id %in% colnames(text)) id_column <- id
    if (!is.null(id_column)) {
      if (id_column %in% colnames(text)) {
        id <- text[, id_column, drop = TRUE]
      } else {
        stop("id_column not found in text", call. = FALSE)
      }
    }
    if (!is.null(text_column)) {
      if (text_column %in% colnames(text)) {
        text <- text[, text_column, drop = TRUE]
      } else {
        if (!text_as_paths) stop("text_column not found in text", call. = FALSE)
      }
    }
    if (!is.null(dim(text))) {
      if (ncol(text) == 1) {
        text <- text[, 1, drop = TRUE]
      } else {
        stop("text has dimensions, but no text_column column", call. = FALSE)
      }
    }
  }
  if (!is.character(text)) text <- as.character(text)
  if (!length(text))
    stop("no texts were found after resolving the text argument")
  if (length(id) && !is.character(id)) id <- as.character(id)
  provided_id <- FALSE
  if (length(id)) {
    if (length(id) != length(text))
      stop("id is not the same length as text", call. = FALSE)
    if (anyDuplicated(id)) stop("id contains duplicate values", call. = FALSE)
    provided_id <- TRUE
  } else {
    id <- paste0("t", seq_along(text))
  }
  if (!is.numeric(retry_limit)) retry_limit <- 0
  if (to_norming) {
    version <- "v2"
    endpoint <- "norming"
    full_url <- url
    request_cache <- FALSE
  } else {
    url_parts <- unlist(strsplit(
      regmatches(
        url,
        gregexpr("/[Vv]\\d+(?:/[^/]+)?", url)
      )[[1]],
      "/",
      fixed = TRUE
    ))
    if (version == "")
      version <- if (length(url_parts) > 1) url_parts[[2]] else "v1"
    version <- tolower(version)
    if (version == "" || !grepl("^v\\d+$", version)) {
      stop("invalid version: ", version, call. = FALSE)
    }
    if (endpoint == "") {
      endpoint <- if (length(url_parts) > 2) {
        url_parts[[3]]
      } else {
        if (tolower(version) == "v1") "framework" else "analyze"
      }
    }
    endpoint <- sub("^.*/", "", tolower(endpoint))
    if (endpoint == "" || grepl("[^a-z]", endpoint)) {
      stop("invalid endpoint: ", endpoint, call. = FALSE)
    }
    url <- paste0(sub("/+[Vv]\\d+(/.*)?$|/+$", "", url), "/", version, "/")
    full_url <- paste0(
      url,
      endpoint,
      if (version == "v1") "/bulk" else paste0("/", context)
    )
    if (!is.list(api_args)) api_args <- as.list(api_args)
    if (
      version != "v1" &&
        "context" %in% api_args &&
        "custom_context" %in% api_args
    ) {
      stop(
        "only one of `context` or `custom_context may be specified",
        call. = FALSE
      )
    }
    if (version != "v1" && length(api_args)) {
      full_url <- paste0(
        full_url,
        "?",
        paste0(names(api_args), "=", unlist(api_args), collapse = "&")
      )
    }
  }
  args_hash <- digest::digest(
    jsonlite::toJSON(
      c(
        api_args,
        url = full_url,
        key = key,
        secret = secret
      ),
      auto_unbox = TRUE
    ),
    serialize = FALSE
  )

  # ping API
  if (make_request) {
    if (verbose) message("pinging API (", round(proc.time()[[3]] - st, 4), ")")
    ping <- receptiviti_status(url, key, secret, verbose = FALSE)
    if (is.null(ping)) stop("URL is unreachable", call. = FALSE)
    if (ping$status_code != 200) stop(ping$status_message, call. = FALSE)
  }

  # prepare text
  if (verbose) message("preparing text (", round(proc.time()[[3]] - st, 4), ")")
  data <- data.frame(text = text, id = id, stringsAsFactors = FALSE)
  text <- data[!is.na(data$text) & data$text != "" & !duplicated(data$text), ]
  if (!nrow(text)) stop("no valid texts to process", call. = FALSE)
  if (!is.numeric(bundle_size)) bundle_size <- 1000
  n_texts <- nrow(text)
  n <- ceiling(n_texts / min(1000, max(1, bundle_size)))
  bundles <- split(text, sort(rep_len(seq_len(n), nrow(text))))
  size_fun <- if (text_as_paths) function(b) sum(file.size(b$text)) else
    object.size
  for (i in rev(seq_along(bundles))) {
    size <- size_fun(bundles[[i]])
    if (size > bundle_byte_limit) {
      sizes <- vapply(
        seq_len(nrow(bundles[[i]])),
        function(r) as.numeric(size_fun(bundles[[i]][r, ])),
        0
      )
      if (any(sizes > bundle_byte_limit)) {
        stop(
          "one of your texts is over the individual size limit (",
          bundle_byte_limit / 1024e3,
          " MB)",
          call. = FALSE
        )
      }
      bins <- rep(1, length(sizes))
      bin_size <- 0
      bi <- 1
      for (ti in seq_along(bins)) {
        bin_size <- bin_size + sizes[ti]
        if (bin_size > bundle_byte_limit) {
          bin_size <- sizes[ti]
          bi <- bi + 1
        }
        bins[ti] <- bi
      }
      bundles <- c(
        bundles[-i],
        unname(split(bundles[[i]], paste0(i, ".", bins)))
      )
    }
  }
  n_bundles <- length(bundles)
  bundle_ref <- if (n_bundles == 1) "bundle" else "bundles"
  if (verbose)
    message(
      "prepared text in ",
      n_bundles,
      " ",
      bundle_ref,
      " (",
      round(proc.time()[[3]] - st, 4),
      ")"
    )

  auth <- paste0(key, ":", secret)
  if (is.null(in_memory) && (use_future || cores > 1) && n_bundles > cores)
    in_memory <- FALSE
  request_scratch <- NULL
  if (!in_memory) {
    if (verbose)
      message(
        "writing ",
        bundle_ref,
        " to disc (",
        round(proc.time()[[3]] - st, 4),
        ")"
      )
    request_scratch <- paste0(tempdir(), "/receptiviti_request_scratch/")
    dir.create(request_scratch, FALSE)
    if (clear_scratch_cache) on.exit(unlink(request_scratch, recursive = TRUE))
    bundles <- vapply(
      bundles,
      function(b) {
        scratch_bundle <- paste0(request_scratch, digest::digest(b), ".rds")
        if (!file.exists(scratch_bundle))
          saveRDS(b, scratch_bundle, compress = FALSE)
        scratch_bundle
      },
      "",
      USE.NAMES = FALSE
    )
  }

  doprocess <- function(bundles, cores, future) {
    env <- parent.frame()
    if (future) {
      eval(
        expression(future.apply::future_lapply(bundles, process)),
        envir = env
      )
    } else {
      cl <- parallel::makeCluster(cores)
      parallel::clusterExport(cl, ls(envir = env), env)
      on.exit(parallel::stopCluster(cl))
      (if (length(bundles) > cores * 2) parallel::parLapplyLB else
        parallel::parLapply)(cl, bundles, process)
    }
  }

  request <- function(body, body_hash, bin, ids, attempt = retry_limit) {
    temp_file <- paste0(tempdir(), "/", body_hash, ".json")
    if (!request_cache) unlink(temp_file)
    res <- NULL
    if (!file.exists(temp_file)) {
      if (make_request) {
        handler <- tryCatch(
          curl::new_handle(httpauth = 1, userpwd = auth, copypostfields = body),
          error = function(e) e$message
        )
        if (is.character(handler)) {
          stop(
            if (grepl("libcurl", handler, fixed = TRUE)) {
              "libcurl encountered an error; try setting the bundle_byte_limit argument to a smaller value"
            } else {
              paste("failed to create handler:", handler)
            },
            call. = FALSE
          )
        }
        if (to_norming) curl::handle_setopt(handler, customrequest = "PATCH")
        res <- curl::curl_fetch_disk(full_url, temp_file, handler)
      } else {
        stop(
          "make_request is FALSE, but there are texts with no cached results",
          call. = FALSE
        )
      }
    }
    result <- if (file.exists(temp_file)) {
      if (
        is.null(res$type) || grepl("application/json", res$type, fixed = TRUE)
      ) {
        tryCatch(
          jsonlite::read_json(temp_file, simplifyVector = TRUE),
          error = function(e) list(message = "invalid response format")
        )
      } else {
        list(message = "invalid response format")
      }
    } else {
      list(message = rawToChar(res$content))
    }
    valid_result <- if (to_norming) {
      !is.null(result$submitted)
    } else {
      !is.null(result$results) || is.null(result$message)
    }
    if (valid_result) {
      if (!is.null(result$results)) result <- result$results
      if ("error" %in% names(result)) {
        if (!is.list(result$error)) {
          warning("bundle ", body_hash, " failed: ", result$error)
        } else if (is.list(result$error)) {
          warning(
            "bundle ",
            body_hash,
            " failed: ",
            if (!is.null(result$error$code))
              paste0("(", result$error$code, ") ") else NULL,
            result$error$message
          )
        } else {
          su <- !is.na(result$error$code)
          errors <- if (is.data.frame(result)) {
            result[su & !duplicated(result$error$code), "error"]
          } else {
            result$error
          }
          warning(
            if (sum(su) > 1) "some texts were invalid: " else
              "a text was invalid: ",
            paste(
              do.call(
                paste0,
                data.frame(
                  "(",
                  errors$code,
                  ") ",
                  errors$message,
                  stringsAsFactors = FALSE
                )
              ),
              collapse = "; "
            ),
            call. = FALSE
          )
        }
      }
      if (to_norming) {
        cbind(body_hash = body_hash, as.data.frame(result))
      } else {
        unpack <- function(d) {
          if (is.list(d)) as.data.frame(lapply(d, unpack), optional = TRUE) else
            d
        }
        result <- unpack(result[
          !names(result) %in% c("response_id", "language", "version", "error")
        ])
        if (!is.null(result) && nrow(result)) {
          if (colnames(result)[[1]] == "request_id") {
            colnames(result)[[1]] <- "text_hash"
          }
          cbind(id = ids, bin = bin, result)
        }
      }
    } else {
      unlink(temp_file)
      if (length(result$message) == 1 && substr(result$message, 1, 1) == "{") {
        result <- jsonlite::fromJSON(result$message)
      }
      if (
        attempt > 0 &&
          (length(result$code) == 1 && result$code == 1420) ||
          (length(result$message) == 1 &&
            result$message == "invalid response format")
      ) {
        wait_time <- as.numeric(regmatches(
          result$message,
          regexec("[0-9]+(?:\\.[0-9]+)?", result$message)
        ))
        Sys.sleep(if (is.na(wait_time)) 1 else wait_time / 1e3)
        request(body, body_hash, bin, ids, attempt - 1)
      } else {
        message <- if (is.null(res$status_code)) 200 else res$status_code
        if (length(result$code))
          message <- paste0(message, " (", result$code, "): ", result$message)
        if (length(result$error)) message <- paste0(message, ": ", result$error)
        stop(message, call. = FALSE)
      }
    }
  }

  process <- function(bundle) {
    opts <- getOption("stringsAsFactors")
    options("stringsAsFactors" = FALSE)
    on.exit(options("stringsAsFactors" = opts))
    if (is.character(bundle)) bundle <- readRDS(bundle)
    text <- bundle$text
    bin <- NULL
    if (text_as_paths) {
      if (all(grepl("\\.csv", text, TRUE))) {
        if (is.null(text_column))
          stop(
            "files appear to be csv, but no text_column was specified",
            call. = FALSE
          )
        text <- vapply(
          text,
          function(f) {
            tryCatch(
              paste(
                arrow::read_csv_arrow(
                  f,
                  read_options = arrow::CsvReadOptions$create(
                    encoding = handle_encoding(f)
                  ),
                  col_select = dplyr::all_of(text_column)
                )[[1]],
                collapse = " "
              ),
              error = function(e)
                stop("failed to read in file ", f, call. = FALSE)
            )
          },
          ""
        )
      } else {
        text <- vapply(
          text,
          function(f) {
            tryCatch(
              {
                con <- file(f, encoding = handle_encoding(f))
                on.exit(close(con))
                paste(
                  readLines(con, warn = FALSE, skipNul = TRUE),
                  collapse = " "
                )
              },
              error = function(e)
                stop("failed to read in file ", f, call. = FALSE)
            )
          },
          ""
        )
      }
    }
    bundle$hashes <- paste0(vapply(
      paste0(args_hash, text),
      digest::digest,
      "",
      serialize = FALSE
    ))
    if (to_norming) {
      body <- jsonlite::toJSON(
        lapply(
          seq_along(text),
          function(i) list(text = text[[i]], request_id = bundle$hashes[[i]])
        ),
        auto_unbox = TRUE
      )
      res <- request(
        body,
        digest::digest(body, serialize = FALSE),
        initial,
        bundle$id
      )
      prog(amount = nrow(bundle))
    } else {
      initial <- paste0("h", substr(bundle$hashes, 1, 1))
      set <- !is.na(text) &
        text != "" &
        text != "logical(0)" &
        !duplicated(bundle$hashes)
      res_cached <- cached_cols <- res_fresh <- NULL
      nres <- ncached <- 0
      check_cache <- !cache_overwrite &&
        (cache != "" && length(list.dirs(cache)))
      if (check_cache) {
        db <- arrow::open_dataset(
          cache,
          partitioning = arrow::schema(bin = arrow::string()),
          format = cache_format
        )
        cached_cols <- colnames(db)
        cached <- if (!is.null(db$schema$GetFieldByName("text_hash"))) {
          text_hash <- NULL
          su <- dplyr::filter(
            db,
            bin %in% unique(initial),
            text_hash %in% bundle$hashes
          )
          tryCatch(
            dplyr::compute(
              if (collect_results) su else dplyr::select(su, text_hash)
            ),
            error = function(e) matrix(integer(), 0)
          )
        } else {
          matrix(integer(), 0)
        }
        ncached <- nrow(cached)
        if (ncached) {
          cached <- as.data.frame(cached$to_data_frame())
          if (anyDuplicated(cached$text_hash))
            cached <- cached[!duplicated(cached$text_hash), ]
          rownames(cached) <- cached$text_hash
          cached_set <- which(bundle$hashes %in% cached$text_hash)
          set[cached_set] <- FALSE
          if (collect_results) {
            res_cached <- cbind(
              id = bundle$id[cached_set],
              cached[bundle$hashes[cached_set], ]
            )
          }
        }
      }
      valid_options <- names(api_args)
      if (any(set)) {
        set <- which(set)
        make_bundle <- if (version == "v1") {
          function(i) {
            c(
              api_args,
              list(content = text[[i]], request_id = bundle$hashes[[i]])
            )
          }
        } else {
          function(i) {
            list(text = text[[i]], request_id = bundle$hashes[[i]])
          }
        }
        body <- jsonlite::toJSON(
          unname(lapply(set, make_bundle)),
          auto_unbox = TRUE
        )
        body_hash <- digest::digest(body, serialize = FALSE)
        res_fresh <- request(body, body_hash, initial[set], bundle$id[set])
        valid_options <- valid_options[valid_options %in% colnames(res_fresh)]
        if (length(valid_options)) {
          res_fresh <- res_fresh[,
            !colnames(res_fresh) %in% valid_options,
            drop = FALSE
          ]
        }
        if (ncached && !all(cached_cols %in% colnames(res_fresh))) {
          res_cached <- NULL
          ncached <- 0
          body <- jsonlite::toJSON(
            lapply(cached_set, make_bundle),
            auto_unbox = TRUE
          )
          res_fresh <- rbind(
            res_fresh,
            request(
              body,
              digest::digest(body, serialize = FALSE),
              initial[cached_set],
              bundle$id[cached_set]
            )
          )
        }
        nres <- nrow(res_fresh)
        if (cache != "" && nres) {
          writer <- if (cache_format == "parquet") arrow::write_parquet else
            arrow::write_feather
          cols <- vapply(
            res_fresh[,
              !(colnames(res_fresh) %in% c("id", "bin", names(api_args)))
            ],
            is.character,
            TRUE
          )
          schema <- list()
          for (v in names(cols)) {
            schema[[v]] <- if (cols[[v]]) {
              arrow::string()
            } else if (
              v %in% c("summary.word_count", "summary.sentence_count")
            ) {
              if (anyNA(res_fresh[[v]]))
                res_fresh[[v]][is.na(res_fresh[[v]])] <- NA_integer_
              arrow::int32()
            } else {
              if (anyNA(res_fresh[[v]]))
                res_fresh[[v]][is.na(res_fresh[[v]])] <- NA_real_
              arrow::float64()
            }
          }
          schema <- arrow::schema(schema)
          for (part_bin in unique(res_fresh$bin)) {
            part <- res_fresh[res_fresh$bin == part_bin, ]
            part$id <- NULL
            part$bin <- NULL
            bin_dir <- paste0(cache, "/bin=", part_bin, "/")
            dir.create(bin_dir, FALSE, TRUE)
            writer(
              arrow::as_arrow_table(part, schema = schema),
              paste0(bin_dir, "fragment-", body_hash, "-0.", cache_format)
            )
          }
        }
      }
      if (collect_results) {
        res <- rbind(res_cached, res_fresh)
        if (length(valid_options))
          for (n in valid_options) res[[n]] <- api_args[[n]]
        missing_ids <- !bundle$id %in% res$id
        if (any(missing_ids)) {
          varnames <- colnames(res)[colnames(res) != "id"]
          res <- rbind(
            res,
            cbind(
              id = bundle$id[missing_ids],
              as.data.frame(matrix(
                NA,
                sum(missing_ids),
                length(varnames),
                dimnames = list(NULL, varnames)
              ))
            )
          )
          res$text_hash <- structure(bundle$hashes, names = bundle$id)[res$id]
        }
      }
      prog(amount = nres + ncached)
    }
    if (collect_results) res else NULL
  }

  # make request(s)
  cores <- if (is.numeric(cores)) max(1, min(n_bundles, cores)) else 1
  prog <- progressor(n_texts)
  results <- if (use_future || cores > 1) {
    call_env <- new.env(parent = globalenv())
    environment(doprocess) <- call_env
    environment(request) <- call_env
    environment(process) <- call_env
    for (name in c(
      "doprocess",
      "request",
      "process",
      "text_column",
      "prog",
      "make_request",
      "full_url",
      "cache",
      "cache_overwrite",
      "use_future",
      "cores",
      "bundles",
      "cache_format",
      "request_cache",
      "auth",
      "version",
      "to_norming",
      "text_as_paths",
      "retry_limit",
      "api_args",
      "args_hash",
      "encoding",
      "handle_encoding",
      "collect_results"
    )) {
      call_env[[name]] <- get(name)
    }
    if (verbose) {
      message(
        "processing ",
        bundle_ref,
        " using ",
        if (use_future) "future backend" else paste(cores, "cores"),
        " (",
        round(proc.time()[[3]] - st, 4),
        ")"
      )
    }
    eval(expression(doprocess(bundles, cores, use_future)), envir = call_env)
  } else {
    if (verbose)
      message(
        "processing ",
        bundle_ref,
        " sequentially (",
        round(proc.time()[[3]] - st, 4),
        ")"
      )
    lapply(bundles, process)
  }
  if (verbose)
    message("done retrieving (", round(proc.time()[[3]] - st, 4), ")")
  if (collect_results) {
    final_res <- do.call(rbind, results)
    list(data = data, final_res = final_res, provided_id = provided_id)
  } else {
    NULL
  }
}
