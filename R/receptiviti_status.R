#' @rdname receptiviti
#' @export

receptiviti_status <- function(
  url = Sys.getenv("RECEPTIVITI_URL"),
  key = Sys.getenv("RECEPTIVITI_KEY"),
  secret = Sys.getenv("RECEPTIVITI_SECRET"),
  verbose = TRUE,
  include_headers = FALSE
) {
  params <- handle_request_params(url, key, secret)
  ping <- tryCatch(
    curl_fetch_memory(paste0(params$url, "/v1/ping"), params$handler),
    error = function(e) NULL
  )
  if (is.null(ping)) {
    if (verbose) message("Status: ERROR\nMessage: URL is unreachable")
    invisible(return())
  }
  ping$content <- list(message = rawToChar(ping$content))
  if (substr(ping$content, 1, 1) == "{")
    ping$content <- fromJSON(ping$content$message)
  ok <- ping$status_code == 200 && !length(ping$content$code)
  ping$status_message <- if (ok) {
    ping$content$pong
  } else {
    paste0(
      if (length(ping$content$code))
        paste0(ping$status_code, " (", ping$content$code, "): "),
      if (
        nchar(ping$content$message) > 500 ||
          grepl("<", ping$content$message, fixed = TRUE)
      ) {
        ping$status_code
      } else {
        ping$content$message
      }
    )
  }
  if (verbose) {
    message(
      "Status: ",
      if (ok) "OK" else "ERROR",
      "\nMessage: ",
      ping$status_message
    )
    if (include_headers) {
      ping$headers <- strsplit(
        rawToChar(ping$headers),
        "[\r\n]+",
        perl = TRUE
      )[[1]]
      json <- regexec("\\{.+\\}", ping$headers)
      for (i in seq_along(json)) {
        if (json[[i]] != -1) {
          regmatches(ping$headers[[i]], json[[i]]) <- paste(
            " ",
            strsplit(
              toJSON(
                fromJSON(regmatches(ping$headers[[i]], json[[i]])),
                auto_unbox = TRUE,
                pretty = TRUE
              ),
              "\n"
            )[[1]],
            collapse = "\n"
          )
        }
      }
      message(paste0("\n", paste(" ", ping$headers, collapse = "\n")))
    }
  }
  invisible(ping)
}

handle_request_params <- function(url, key, secret) {
  if (key == "") {
    stop(
      "specify your key, or set it to the RECEPTIVITI_KEY environment variable",
      call. = FALSE
    )
  }
  if (secret == "") {
    stop(
      "specify your secret, or set it to the RECEPTIVITI_SECRET environment variable",
      call. = FALSE
    )
  }
  url <- paste0(
    if (!grepl("http", tolower(url), fixed = TRUE)) "https://",
    sub("/+[Vv]\\d+(?:/.*)?$|/+$", "", url)
  )
  if (!grepl("^https?://[^.]+[.:][^.]", url, TRUE)) {
    stop(
      "url does not appear to be valid: ",
      url,
      call. = FALSE
    )
  }
  list(
    url = url,
    handler = new_handle(httpauth = 1, userpwd = paste0(key, ":", secret))
  )
}
