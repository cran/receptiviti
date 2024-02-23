.onLoad <- function(lib, pkg) {
  if (Sys.getenv("RECEPTIVITI_URL") == "") Sys.setenv(RECEPTIVITI_URL = "https://api.receptiviti.com/")
}

#' Receptiviti API
#'
#' The main function to access the \href{https://www.receptiviti.com}{Receptiviti} API.
#'
#' @param text A character vector with text to be processed, path to a directory containing files, or a vector of file paths.
#' If a single path to a directory, each file is collapsed to a single text. If a path to a file or files,
#' each line or row is treated as a separate text, unless \code{collapse_lines} is \code{TRUE} (in which case,
#' files will be read in as part of bundles at processing time, as is always the case when a directory).
#' Use \code{files} to more reliably enter files, or \code{dir} to more reliably specify a directory.
#' @param output Path to a \code{.csv} file to write results to. If this already exists, set \code{overwrite} to \code{TRUE}
#' to overwrite it.
#' @param id Vector of unique IDs the same length as \code{text}, to be included in the results.
#' @param text_column,id_column Column name of text/id, if \code{text} is a matrix-like object, or a path to a csv file.
#' @param files A list of file paths, as alternate entry to \code{text}.
#' @param dir A directory to search for files in, as alternate entry to \code{text}.
#' @param file_type File extension to search for, if \code{text} is the path to a directory containing files to be read in.
#' @param encoding Encoding of file(s) to be read in. If not specified, this will be detected, which can fail,
#' resulting in mis-encoded characters; for best (and fasted) results, specify encoding.
#' @param return_text Logical; if \code{TRUE}, \code{text} is included as the first column of the result.
#' @param api_args A list of additional arguments to pass to the API (e.g., \code{list(sallee_mode = "sparse")}). Defaults to the
#' \code{receptiviti.api_args} option.
#' @param frameworks A vector of frameworks to include results from. Texts are always scored with all available framework --
#' this just specifies what to return. Defaults to \code{all}, to return all scored frameworks. Can be set by the
#' \code{receptiviti.frameworks} option (e.g., \code{options(receptiviti.frameworks = c("liwc", "sallee"))}).
#' @param framework_prefix Logical; if \code{FALSE}, will remove the framework prefix from column names, which may result in duplicates.
#' If this is not specified, and 1 framework is selected, or \code{as_list} is \code{TRUE}, will default to remove prefixes.
#' @param as_list Logical; if \code{TRUE}, returns a list with frameworks in separate entries.
#' @param bundle_size Number of texts to include in each request; between 1 and 1,000.
#' @param bundle_byte_limit Memory limit (in bytes) of each bundle, under \code{1e7} (10 MB, which is the API's limit).
#' May need to be lower than the API's limit, depending on the system's requesting library.
#' @param collapse_lines Logical; if \code{TRUE}, and \code{text} contains paths to files, each file is treated as a single text.
#' @param retry_limit Maximum number of times each request can be retried after hitting a rate limit.
#' @param overwrite Logical; if \code{TRUE}, will overwrite an existing \code{output} file.
#' @param compress Logical; if \code{TRUE}, will save as an \code{xz}-compressed file.
#' @param make_request Logical; if \code{FALSE}, a request is not made. This could be useful if you want to be sure and
#' load from one of the caches, but aren't sure that all results exist there; it will error out if it encounters
#' texts it has no other source for.
#' @param text_as_paths Logical; if \code{TRUE}, ensures \code{text} is treated as a vector of file paths. Otherwise, this will be
#' determined if there are no \code{NA}s in \code{text} and every entry is under 500 characters long.
#' @param cache Path to a directory in which to save unique results for reuse; defaults to
#' \code{Sys.getenv(}\code{"RECEPTIVITI_CACHE")}. See the Cache section for details.
#' @param cache_overwrite Logical; if \code{TRUE}, will write results to the cache without reading from it. This could be used
#' if you want fresh results to be cached without clearing the cache.
#' @param cache_format Format of the cache database; see \code{\link[arrow]{FileFormat}}.
#' Defaults to \code{Sys.getenv(}\code{"RECEPTIVITI_CACHE_FORMAT")}.
#' @param clear_cache Logical; if \code{TRUE}, will clear any existing files in the cache. Use \code{cache_overwrite} if
#' you want fresh results without clearing or disabling the cache. Use \code{cache = FALSE} to disable the cache.
#' @param request_cache Logical; if \code{FALSE}, will always make a fresh request, rather than using the response
#' from a previous identical request.
#' @param cores Number of CPU cores to split bundles across, if there are multiple bundles. See the Parallelization section.
#' @param use_future Logical; if \code{TRUE}, uses a \code{future} back-end to process bundles, in which case,
#' parallelization can be controlled with the \code{\link[future]{plan}} function (e.g., \code{plan("multisession")}
#' to use multiple cores); this is required to see progress bars when using multiple cores. See the Parallelization section.
#' @param in_memory Logical; if \code{FALSE}, will write bundles to temporary files, and only load them as they are being requested.
#' @param clear_scratch_cache Logical; if \code{FALSE}, will preserve the bundles written when \code{in_memory} is \code{TRUE}, after
#' the request has been made.
#' @param verbose Logical; if \code{TRUE}, will show status messages.
#' @param key API Key; defaults to \code{Sys.getenv("RECEPTIVITI_KEY")}.
#' @param secret API Secret; defaults to \code{Sys.getenv("RECEPTIVITI_SECRET")}.
#' @param url API URL; defaults to \code{Sys.getenv("RECEPTIVITI_URL")}, which defaults to
#' \code{"https://api.receptiviti.com/"}.
#' @param version API version; defaults to \code{Sys.getenv("RECEPTIVITI_VERSION")}, which defaults to
#' \code{"v1"}.
#' @param endpoint API endpoint (path name after the version); defaults to \code{Sys.getenv("RECEPTIVITI_ENDPOINT")},
#' which defaults to \code{"framework"}.
#' @param include_headers Logical; if \code{TRUE}, \code{receptiviti_status}'s verbose message will include
#' the HTTP headers.
#'
#' @returns A \code{data.frame} with columns for \code{text} (if \code{return_text} is \code{TRUE}; the originally entered text),
#' \code{id} (if one was provided), \code{text_hash} (the MD5 hash of the text), a column each for relevant entries in \code{api_args},
#' and scores from each included framework (e.g., \code{summary.word_count} and \code{liwc.i}). If \code{as_list} is \code{TRUE},
#' returns a list with a named entry containing such a \code{data.frame} for each framework.
#'
#' @section Cache:
#' If the \code{cache} argument is specified, results for unique texts are saved in an
#' \href{https://arrow.apache.org}{Arrow} database in the cache location
#' (\code{Sys.getenv(}\code{"RECEPTIVITI_CACHE")}), and are retrieved with subsequent requests.
#' This ensures that the exact same texts are not re-sent to the API.
#' This does, however, add some processing time and disc space usage.
#'
#' If \code{cache} is \code{TRUE}, a default directory (\code{receptiviti_cache}) will be looked for
#' in the system's temporary directory (which is usually the parent of \code{tempdir()}).
#' If this does not exist, you will be asked if it should be created.
#'
#' The primary cache is checked when each bundle is processed, and existing results are loaded at
#' that time. When processing many bundles in parallel, and many results have been cached,
#' this can cause the system to freeze and potentially crash.
#' To avoid this, limit the number of cores, or disable parallel processing.
#'
#' The \code{cache_format} arguments (or the \code{RECEPTIVITI_CACHE_FORMAT} environment variable) can be used to adjust the format of the cache.
#'
#' You can use the cache independently with \code{open_database(Sys.getenv("RECEPTIVITI_CACHE"))}.
#'
#' You can also set the \code{clear_cache} argument to \code{TRUE} to clear the cache before it is used again, which may be useful
#' if the cache has gotten big, or you know new results will be returned. Even if a cached result exists, it will be
#' reprocessed if it does not have all of the variables of new results, but this depends on there being at least 1 uncached
#' result. If, for instance, you add a framework to your account and want to reprocess a previously processed set of texts,
#' you would need to first clear the cache.
#'
#' Either way, duplicated texts within the same call will only be sent once.
#'
#' The \code{request_cache} argument controls a more temporary cache of each bundle request. This is cleared when the
#' R session ends. You might want to set this to \code{FALSE} if a new framework becomes available on your account
#' and you want to process a set of text you already processed in the current R session without restarting.
#'
#' Another temporary cache is made when \code{in_memory} is \code{FALSE}, which is the default when processing
#' in parallel (when \code{cores} is over \code{1} or \code{use_future} is \code{TRUE}). This contains
#' a file for each unique bundle, which is read in as needed by the parallel workers.
#'
#' @section Parallelization:
#' \code{text}s are split into bundles based on the \code{bundle_size} argument. Each bundle represents
#' a single request to the API, which is why they are limited to 1000 texts and a total size of 10 MB.
#' When there is more than one bundle and either \code{cores} is greater than 1 or \code{use_future} is \code{TRUE} (and you've
#' externally specified a \code{\link[future]{plan}}), bundles are processed by multiple cores.
#'
#' If you have texts spread across multiple files, they can be most efficiently processed in parallel
#' if each file contains a single text (potentially collapsed from multiple lines). If files contain
#' multiple texts (i.e., \code{collapse_lines = FALSE}), then texts need to be read in before bundling
#' in order to ensure bundles are under the length limit.
#'
#' Whether processing in serial or parallel, progress bars can be specified externally with
#' \code{\link[progressr]{handlers}}; see examples.
#' @examples
#' \dontrun{
#'
#' # check that the API is available, and your credentials work
#' receptiviti_status()
#'
#' # score a single text
#' single <- receptiviti("a text to score")
#'
#' # score multiple texts, and write results to a file
#' multi <- receptiviti(c("first text to score", "second text"), "filename.csv")
#'
#' # score many texts in separate files
#' ## defaults to look for .txt files
#' file_results <- receptiviti(dir = "./path/to/txt_folder")
#'
#' ## could be .csv
#' file_results <- receptiviti(
#'   dir = "./path/to/csv_folder",
#'   text_column = "text", file_type = "csv"
#' )
#'
#' # score many texts from a file, with a progress bar
#' ## set up cores and progress bar (only necessary if you want the progress bar)
#' future::plan("multisession")
#' progressr::handlers(global = TRUE)
#' progressr::handlers("progress")
#'
#' ## make request
#' results <- receptiviti(
#'   "./path/to/largefile.csv",
#'   text_column = "text", use_future = TRUE
#' )
#' }
#' @importFrom curl new_handle curl_fetch_memory curl_fetch_disk
#' @importFrom jsonlite toJSON fromJSON read_json
#' @importFrom utils object.size
#' @importFrom digest digest
#' @importFrom parallel detectCores makeCluster clusterExport parLapplyLB parLapply stopCluster
#' @importFrom progressr progressor
#' @importFrom stringi stri_enc_detect
#' @export

receptiviti <- function(text, output = NULL, id = NULL, text_column = NULL, id_column = NULL, files = NULL, dir = NULL,
                        file_type = "txt", encoding = NULL, return_text = FALSE, api_args = getOption("receptiviti.api_args", list()),
                        frameworks = getOption("receptiviti.frameworks", "all"), framework_prefix = TRUE, as_list = FALSE,
                        bundle_size = 1000, bundle_byte_limit = 75e5, collapse_lines = FALSE, retry_limit = 50, clear_cache = FALSE,
                        clear_scratch_cache = TRUE, request_cache = TRUE, cores = detectCores() - 1, use_future = FALSE,
                        in_memory = TRUE, verbose = FALSE, overwrite = FALSE, compress = FALSE, make_request = TRUE,
                        text_as_paths = FALSE, cache = Sys.getenv("RECEPTIVITI_CACHE"), cache_overwrite = FALSE,
                        cache_format = Sys.getenv("RECEPTIVITI_CACHE_FORMAT", "parquet"), key = Sys.getenv("RECEPTIVITI_KEY"),
                        secret = Sys.getenv("RECEPTIVITI_SECRET"), url = Sys.getenv("RECEPTIVITI_URL"),
                        version = Sys.getenv("RECEPTIVITI_VERSION"), endpoint = Sys.getenv("RECEPTIVITI_ENDPOINT")) {
  # check input
  final_res <- text_hash <- bin <- NULL
  if (!is.null(output)) {
    if (!file.exists(output) && file.exists(paste0(output, ".xz"))) output <- paste0(output, ".xz")
    if (!overwrite && file.exists(output)) stop("output file already exists; use overwrite = TRUE to overwrite it", call. = FALSE)
  }
  if (isTRUE(cache)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("install the `arrow` package to enable the cache", call. = FALSE)
    }
    temp <- dirname(tempdir())
    if (basename(temp) == "working_dir") temp <- dirname(dirname(temp))
    cache <- paste0(temp, "/receptiviti_cache")
    if (!dir.exists(cache)) {
      if (interactive() && !isFALSE(getOption("receptiviti.cache_prompt")) &&
        grepl("^(?:[Yy1]|$)", readline("Do you want to establish a default cache? [Y/n] "))) {
        dir.create(cache, FALSE)
      } else {
        options(receptiviti.cache_prompt = FALSE)
        cache <- FALSE
      }
    }
  }
  if (use_future && !requireNamespace("future.apply", quietly = TRUE)) {
    stop("install the `future.apply` package to use future", call. = FALSE)
  }
  st <- proc.time()[[3]]
  text_as_dir <- FALSE
  if (missing(text)) {
    if (!is.null(dir)) {
      if (!dir.exists(dir)) stop("entered dir does not exist", call. = FALSE)
      text <- dir
      text_as_dir <- TRUE
    } else if (!is.null(files)) {
      text <- files
      text_as_paths <- TRUE
    } else {
      stop("enter text as the first argument, or use files or dir", call. = FALSE)
    }
  }
  if (text_as_paths) {
    if (anyNA(text)) stop("NAs are not allowed in text when being treated as file paths", call. = FALSE)
    if (!all(file.exists(text))) stop("not all of the files in text exist", call. = FALSE)
  }
  read_in <- FALSE
  handle_encoding <- function(file) {
    if (is.null(encoding)) {
      unlist(stringi::stri_enc_detect(readBin(file, "raw", 200))[[1]])[[1]]
    } else {
      encoding
    }
  }
  if (text_as_dir || text_as_paths || (is.character(text) && !anyNA(text) && all(nchar(text) < 500))) {
    if (text_as_dir || length(text) == 1 && dir.exists(text)) {
      if (verbose) message("reading in texts from directory: ", text, " (", round(proc.time()[[3]] - st, 4), ")")
      text_as_paths <- TRUE
      text <- normalizePath(list.files(text, file_type, full.names = TRUE), "/", FALSE)
    }
    if (text_as_paths || all(file.exists(text))) {
      text_as_paths <- collapse_lines
      if (!collapse_lines) {
        if (verbose) message("reading in texts from file list (", round(proc.time()[[3]] - st, 4), ")")
        if (missing(id_column)) names(text) <- if (length(id) != length(text)) text else id
        if (all(grepl("\\.csv", text, TRUE))) {
          if (is.null(text_column)) stop("text appears to point to csv files, but text_column was not specified", call. = FALSE)
          read_in <- TRUE
          text <- unlist(lapply(text, function(f) {
            d <- tryCatch(arrow::read_csv_arrow(f, read_options = arrow::CsvReadOptions$create(
              encoding = handle_encoding(f)
            ), col_select = c(text_column, id_column)), error = function(e) NULL)
            if (is.null(d)) stop("failed to read in file ", f, call. = FALSE)
            if (!is.null(id_column) && id_column %in% colnames(d)) {
              structure(d[, text_column, drop = TRUE], names = d[, id_column, drop = TRUE])
            } else {
              d[, text_column, drop = TRUE]
            }
          }))
        } else {
          text <- unlist(lapply(text, function(f) {
            tryCatch(
              {
                con <- file(f, encoding = handle_encoding(f))
                on.exit(close(con))
                d <- readLines(con, warn = FALSE, skipNul = TRUE)
                d[d != ""]
              },
              error = function(e) stop("failed to read in file ", f, call. = FALSE)
            )
          }))
        }
        id <- names(text)
      }
    } else if (length(text) == 1 && dirname(text) != "." && dir.exists(dirname(dirname(text)))) {
      stop("text appears to be a directory, but it does not exist")
    }
    if (text_as_paths && missing(id)) {
      id <- text
      if (anyDuplicated(id)) id <- names(unlist(lapply(split(id, factor(id, unique(id))), seq_along)))
    }
  }
  if (is.null(dim(text))) {
    if (!read_in) {
      if (!text_as_paths && !is.null(text_column)) stop("text_column is specified, but text has no columns", call. = FALSE)
      if (!is.null(id_column)) stop("id_column is specified, but text has no columns", call. = FALSE)
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
  if (!length(text)) stop("no texts were found after resolving the text argument")
  if (length(id) && !is.character(id)) id <- as.character(id)
  provided_id <- FALSE
  if (length(id)) {
    if (length(id) != length(text)) stop("id is not the same length as text", call. = FALSE)
    if (anyDuplicated(id)) stop("id contains duplicate values", call. = FALSE)
    provided_id <- TRUE
  } else {
    id <- paste0("t", seq_along(text))
  }
  if (!is.numeric(retry_limit)) retry_limit <- 0
  url_parts <- unlist(strsplit(regmatches(
    url, gregexpr("/[Vv]\\d+(?:/[^/]+)?", url)
  )[[1]], "/", fixed = TRUE))
  if (version == "") version <- if (length(url_parts) > 1) url_parts[[2]] else "v1"
  if (endpoint == "") {
    endpoint <- if (length(url_parts) > 2) {
      url_parts[[3]]
    } else {
      if (tolower(version) == "v1") "framework" else "taxonomies"
    }
  }
  url <- paste0(sub("/+[Vv]\\d+(/.*)?$|/+$", "", url), "/", version, "/")
  full_url <- paste0(url, endpoint, "/bulk")
  if (!is.list(api_args)) api_args <- as.list(api_args)
  args_hash <- digest::digest(jsonlite::toJSON(c(
    api_args,
    url = full_url, key = key, secret = secret
  ), auto_unbox = TRUE), serialize = FALSE)

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
  size_fun <- if (text_as_paths) function(b) sum(file.size(b$text)) else object.size
  for (i in rev(seq_along(bundles))) {
    size <- size_fun(bundles[[i]])
    if (size > bundle_byte_limit) {
      sizes <- vapply(seq_len(nrow(bundles[[i]])), function(r) as.numeric(size_fun(bundles[[i]][r, ])), 0)
      if (any(sizes > bundle_byte_limit)) {
        stop(
          "one of your texts is over the individual size limit (", bundle_byte_limit / 1e6, " MB)",
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
      bundles <- c(bundles[-i], unname(split(bundles[[i]], paste0(i, ".", bins))))
    }
  }
  n_bundles <- length(bundles)
  bundle_ref <- if (n_bundles == 1) "bundle" else "bundles"
  if (verbose) message("prepared text in ", n_bundles, " ", bundle_ref, " (", round(proc.time()[[3]] - st, 4), ")")

  # prepare cache
  if (is.character(cache) && cache != "") {
    temp <- normalizePath(cache, "/", FALSE)
    cache <- TRUE
    if (clear_cache) unlink(temp, recursive = TRUE)
    dir.create(temp, FALSE)
  } else {
    temp <- NULL
    cache <- FALSE
  }

  check_cache <- cache && !cache_overwrite
  auth <- paste0(key, ":", secret)
  if (missing(in_memory) && (use_future || cores > 1) && n_bundles > cores) in_memory <- FALSE
  request_scratch <- NULL
  if (!in_memory) {
    if (verbose) message("writing ", bundle_ref, " to disc (", round(proc.time()[[3]] - st, 4), ")")
    request_scratch <- paste0(tempdir(), "/receptiviti_request_scratch/")
    dir.create(request_scratch, FALSE)
    if (clear_scratch_cache) on.exit(unlink(request_scratch, recursive = TRUE))
    bundles <- vapply(bundles, function(b) {
      scratch_bundle <- paste0(request_scratch, digest::digest(b), ".rds")
      if (!file.exists(scratch_bundle)) saveRDS(b, scratch_bundle, compress = FALSE)
      scratch_bundle
    }, "", USE.NAMES = FALSE)
  }

  doprocess <- function(bundles, cores, future) {
    env <- parent.frame()
    if (future) {
      eval(expression(future.apply::future_lapply(bundles, process)), envir = env)
    } else {
      cl <- parallel::makeCluster(cores)
      parallel::clusterExport(cl, ls(envir = env), env)
      on.exit(parallel::stopCluster(cl))
      (if (length(bundles) > cores * 2) parallel::parLapplyLB else parallel::parLapply)(cl, bundles, process)
    }
  }

  request <- function(body, bin, ids, attempt = retry_limit) {
    unpack <- function(d) {
      if (is.list(d)) as.data.frame(lapply(d, unpack), optional = TRUE) else d
    }
    json <- jsonlite::toJSON(unname(body), auto_unbox = TRUE)
    temp_file <- paste0(tempdir(), "/", digest::digest(json, serialize = FALSE), ".json")
    if (!request_cache) unlink(temp_file)
    res <- NULL
    if (!file.exists(temp_file)) {
      if (make_request) {
        handler <- tryCatch(
          curl::new_handle(httpauth = 1, userpwd = auth, copypostfields = json),
          error = function(e) e$message
        )
        if (is.character(handler)) {
          stop(if (grepl("libcurl", handler, fixed = TRUE)) {
            "libcurl encountered an error; try setting the bundle_byte_limit argument to a smaller value"
          } else {
            paste("failed to create handler:", handler)
          }, call. = FALSE)
        }
        res <- curl::curl_fetch_disk(full_url, temp_file, handler)
      } else {
        stop("make_request is FALSE, but there are texts with no cached results", call. = FALSE)
      }
    }
    result <- if (file.exists(temp_file)) {
      if (is.null(res$type) || grepl("application/json", res$type, fixed = TRUE)) {
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
    if (!is.null(result$results) || is.null(result$message)) {
      if (!is.null(result$results)) result <- result$results
      if ("error" %in% names(result)) {
        su <- !is.na(result$error$code)
        errors <- result[su & !duplicated(result$error$code), "error"]
        warning(
          if (sum(su) > 1) "some texts were invalid: " else "a text was invalid: ",
          paste(
            do.call(paste0, data.frame("(", errors$code, ") ", errors$message, stringsAsFactors = FALSE)),
            collapse = "; "
          ),
          call. = FALSE
        )
      }
      result <- unpack(result[!names(result) %in% c("response_id", "language", "version", "error")])
      if (!is.null(result) && nrow(result)) {
        if (colnames(result)[1] == "request_id") {
          colnames(result)[1] <- "text_hash"
        } else {
          result <- cbind(text_hash = vapply(body, "[[", "", "request_id"), result)
        }
        cbind(id = ids, bin = bin, result)
      }
    } else {
      unlink(temp_file)
      if (length(result$message) == 1 && substr(result$message, 1, 1) == "{") {
        result <- jsonlite::fromJSON(result$message)
      }
      if (attempt > 0 && (length(result$code) == 1 && result$code == 1420) || (
        length(result$message) == 1 && result$message == "invalid response format"
      )) {
        wait_time <- as.numeric(regmatches(result$message, regexec("[0-9]+(?:\\.[0-9]+)?", result$message)))
        Sys.sleep(if (is.na(wait_time)) 1 else wait_time / 1e3)
        request(body, bin, ids, attempt - 1)
      } else {
        stop(paste0(if (length(result$code)) {
          paste0(
            if (is.null(res$status_code)) 200 else res$status_code, " (", result$code, "): "
          )
        }, result$message), call. = FALSE)
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
        if (is.null(text_column)) stop("files appear to be csv, but no text_column was specified", call. = FALSE)
        text <- vapply(text, function(f) {
          tryCatch(
            paste(arrow::read_csv_arrow(f, read_options = arrow::CsvReadOptions$create(
              encoding = handle_encoding(f)
            ), col_select = dplyr::all_of(text_column))[[1]], collapse = " "),
            error = function(e) stop("failed to read in file ", f, call. = FALSE)
          )
        }, "")
      } else {
        text <- vapply(text, function(f) {
          tryCatch(
            {
              con <- file(f, encoding = handle_encoding(f))
              on.exit(close(con))
              paste(readLines(con, warn = FALSE, skipNul = TRUE), collapse = " ")
            },
            error = function(e) stop("failed to read in file ", f, call. = FALSE)
          )
        }, "")
      }
    }
    bundle$hashes <- paste0(vapply(paste0(args_hash, text), digest::digest, "", serialize = FALSE))
    initial <- paste0("h", substr(bundle$hashes, 1, 1))
    set <- !is.na(text) & text != "" & text != "logical(0)" & !duplicated(bundle$hashes)
    res_cached <- res_fresh <- NULL
    if (check_cache && dir.exists(paste0(temp, "/bin=h"))) {
      db <- arrow::open_dataset(temp, partitioning = arrow::schema(bin = arrow::string()), format = cache_format)
      cached <- if (!is.null(db$schema$GetFieldByName("text_hash"))) {
        tryCatch(
          dplyr::compute(dplyr::filter(db, bin %in% unique(initial), text_hash %in% bundle$hashes)),
          error = function(e) matrix(integer(), 0)
        )
      } else {
        matrix(integer(), 0)
      }
      if (nrow(cached)) {
        cached <- as.data.frame(cached$to_data_frame())
        if (anyDuplicated(cached$text_hash)) cached <- cached[!duplicated(cached$text_hash), ]
        rownames(cached) <- cached$text_hash
        cached_set <- which(bundle$hashes %in% cached$text_hash)
        set[cached_set] <- FALSE
        res_cached <- cbind(id = bundle[cached_set, "id"], cached[bundle[cached_set, "hashes"], ])
      }
    }
    valid_options <- names(api_args)
    if (any(set)) {
      set <- which(set)
      res_fresh <- request(lapply(
        set, function(i) c(api_args, list(content = text[[i]], request_id = bundle[i, "hashes"]))
      ), initial[set], bundle[set, "id"])
      valid_options <- valid_options[valid_options %in% colnames(res_fresh)]
      if (length(valid_options)) {
        res_fresh <- res_fresh[, !colnames(res_fresh) %in% valid_options, drop = FALSE]
      }
      if (check_cache && !is.null(res_cached) && !all(colnames(res_cached) %in% colnames(res_fresh))) {
        res_cached <- NULL
        res_fresh <- rbind(
          res_fresh,
          request(lapply(
            cached_set, function(i) c(api_args, list(content = text[[i]], request_id = bundle[i, "hashes"]))
          ), initial[cached_set], bundle[cached_set, "id"])
        )
      }
    }
    res <- rbind(res_cached, res_fresh)
    if (length(valid_options)) for (n in valid_options) res[[n]] <- api_args[[n]]
    missing_ids <- !bundle$id %in% res$id
    if (any(missing_ids)) {
      varnames <- colnames(res)[colnames(res) != "id"]
      res <- rbind(res, cbind(
        id = bundle[missing_ids, "id"],
        as.data.frame(matrix(NA, sum(missing_ids), length(varnames), dimnames = list(NULL, varnames)))
      ))
      res$text_hash <- structure(bundle$hashes, names = bundle$id)[res$id]
    }
    prog(amount = nrow(res))
    res
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
      "doprocess", "request", "process", "text_column", "prog", "make_request", "check_cache", "full_url",
      "temp", "use_future", "cores", "bundles", "cache_format", "request_cache", "auth",
      "text_as_paths", "retry_limit", "api_args", "args_hash", "encoding", "handle_encoding"
    )) {
      call_env[[name]] <- get(name)
    }
    if (verbose) {
      message(
        "processing ", bundle_ref, " using ", if (use_future) "future backend" else paste(cores, "cores"),
        " (", round(proc.time()[[3]] - st, 4), ")"
      )
    }
    eval(expression(doprocess(bundles, cores, use_future)), envir = call_env)
  } else {
    if (verbose) message("processing ", bundle_ref, " sequentially (", round(proc.time()[[3]] - st, 4), ")")
    lapply(bundles, process)
  }
  if (verbose) message("done retrieving; preparing final results (", round(proc.time()[[3]] - st, 4), ")")
  final_res <- do.call(rbind, results)
  if (length(api_args)) {
    su <- !names(api_args) %in% colnames(final_res)
    if (any(su)) warning("unrecognized api_args: ", paste(names(api_args)[su], collapse = ", "), call. = FALSE)
  }

  # update cache
  if (!is.null(temp)) {
    if (verbose) message("checking cache (", round(proc.time()[[3]] - st, 4), ")")
    initialized <- dir.exists(paste0(temp, "/bin=h"))
    if (initialized) {
      db <- arrow::open_dataset(temp, partitioning = arrow::schema(bin = arrow::string()), format = cache_format)
      if (db$num_cols != (ncol(final_res) - 1)) {
        if (verbose) message("clearing existing cache since columns did not align (", round(proc.time()[[3]] - st, 4), ")")
        dir.create(temp, FALSE)
        initialized <- FALSE
      }
    }
    exclude <- c("id", names(api_args))
    if (!initialized) {
      su <- !colnames(final_res) %in% exclude
      if (sum(su) > 2) {
        initial <- final_res[1, su]
        initial$text_hash <- ""
        initial$bin <- "h"
        initial[, !colnames(initial) %in% c(
          "summary.word_count", "summary.sentence_count"
        ) & !vapply(initial, is.character, TRUE)] <- .1
        initial <- rbind(initial, final_res[, colnames(initial)])
        if (verbose) {
          message(
            "initializing cache with ", nrow(final_res), " result",
            if (nrow(final_res) > 1) "s", " (", round(proc.time()[[3]] - st, 4), ")"
          )
        }
        arrow::write_dataset(initial, temp, partitioning = "bin", format = cache_format)
      }
    } else {
      fresh <- final_res[
        !duplicated(final_res$text_hash), !colnames(final_res) %in% names(api_args),
        drop = FALSE
      ]
      cached <- dplyr::filter(db, bin %in% unique(fresh$bin), text_hash %in% fresh$text_hash)
      if (!any(dim(cached) == 0) || nrow(cached) != nrow(fresh)) {
        uncached_hashes <- if (nrow(cached)) {
          !fresh$text_hash %in% dplyr::collect(dplyr::select(cached, text_hash))[[1]]
        } else {
          rep(TRUE, nrow(fresh))
        }
        if (any(uncached_hashes)) {
          if (verbose) {
            message(
              "updating cache with ", sum(uncached_hashes), " result",
              if (sum(uncached_hashes) > 1) "s", " (", round(proc.time()[[3]] - st, 4), ")"
            )
          }
          arrow::write_dataset(
            fresh[uncached_hashes, !colnames(fresh) %in% exclude], temp,
            partitioning = "bin", format = cache_format
          )
        }
      }
    }
  }

  # prepare final results
  if (verbose) message("preparing output (", round(proc.time()[[3]] - st, 4), ")")
  rownames(final_res) <- final_res$id
  rownames(data) <- data$id
  data$text_hash <- structure(final_res$text_hash, names = data[final_res$id, "text"])[data$text]
  final_res <- cbind(
    data[, c(if (return_text) "text", if (provided_id) "id", "text_hash"), drop = FALSE],
    final_res[
      structure(final_res$id, names = final_res$text_hash)[data$text_hash],
      !colnames(final_res) %in% c("id", "bin", "text_hash", "custom"),
      drop = FALSE
    ]
  )
  row.names(final_res) <- NULL
  if (!is.null(output)) {
    if (!grepl("\\.csv", output, TRUE)) output <- paste0(output, ".csv")
    if (compress && !grepl(".xz", output, fixed = TRUE)) output <- paste0(output, ".xz")
    if (grepl(".xz", output, fixed = TRUE)) compress <- TRUE
    if (verbose) message("writing results to file: ", output, " (", round(proc.time()[[3]] - st, 4), ")")
    dir.create(dirname(output), FALSE, TRUE)
    if (overwrite) unlink(output)
    if (compress) output <- xzfile(output)
    arrow::write_csv_arrow(final_res, file = output)
  }

  if (is.character(frameworks) && frameworks[1] != "all") {
    if (verbose) message("selecting frameworks (", round(proc.time()[[3]] - st, 4), ")")
    vars <- colnames(final_res)
    sel <- grepl(paste0("^(?:", paste(tolower(frameworks), collapse = "|"), ")"), vars)
    if (any(sel)) {
      if (missing(framework_prefix) && (length(frameworks) == 1 && frameworks != "all")) framework_prefix <- FALSE
      sel <- unique(c("text", "id", "text_hash", names(api_args), vars[sel]))
      sel <- sel[sel %in% vars]
      final_res <- final_res[, sel]
    } else {
      warning("frameworks did not match any columns -- returning all", call. = FALSE)
    }
  }
  if (as_list) {
    if (missing(framework_prefix)) framework_prefix <- FALSE
    inall <- c("text", "id", "text_hash", names(api_args))
    cols <- colnames(final_res)
    inall <- inall[inall %in% cols]
    pre <- sub("\\..*$", "", cols)
    pre <- unique(pre[!pre %in% inall])
    final_res <- lapply(structure(pre, names = pre), function(f) {
      res <- final_res[, c(inall, grep(paste0("^", f), cols, value = TRUE))]
      if (!framework_prefix) colnames(res) <- sub("^.+\\.", "", colnames(res))
      res
    })
  } else if (!framework_prefix) colnames(final_res) <- sub("^.+\\.", "", colnames(final_res))
  if (verbose) message("done (", round(proc.time()[[3]] - st, 4), ")")
  invisible(final_res)
}
