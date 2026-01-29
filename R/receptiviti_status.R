#' @rdname receptiviti
#' @export

receptiviti_status <- function(
  url = Sys.getenv("RECEPTIVITI_URL"),
  key = Sys.getenv("RECEPTIVITI_KEY"),
  secret = Sys.getenv("RECEPTIVITI_SECRET"),
  version = Sys.getenv("RECEPTIVITI_VERSION"),
  verbose = TRUE,
  include_headers = FALSE
) {
  params <- handle_request_params(url, key, secret)
  url_parts <- parse_url(url, version)
  ping <- tryCatch(
    curl_fetch_memory(
      paste0(params$url, "/", url_parts$version, "/ping"),
      params$handler
    ),
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

parse_url <- function(url, version = "", endpoint = "") {
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
  list(
    url = sub("/+[Vv]\\d+(/.*)?$|/+$", "", url),
    version = version,
    endpoint = endpoint
  )
}
