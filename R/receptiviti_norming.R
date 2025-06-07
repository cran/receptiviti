#' View or Establish Custom Norming Contexts
#'
#' Custom norming contexts can be used to process later texts by specifying the
#' \code{custom_context} API argument in the \code{receptiviti} function (e.g.,
#' \code{receptiviti("text to score", version = "v2",
#' options = list(custom_context = "norm_name"))},
#' where \code{norm_name} is the name you set here).
#'
#' @param name Name of a new norming context, to be established from the provided \code{text}.
#' Not providing a name will list the previously created contexts.
#' @param text Text to be processed and used as the custom norming context.
#' Not providing text will return the status of the named norming context.
#' @param options Options to set for the norming context (e.g.,
#' \code{list(min_word_count = 350,} \code{max_punctuation = .25)}).
#' @param delete Logical; If \code{TRUE}, will request to remove the \code{name} context.
#' @param name_only Logical; If \code{TRUE}, will return a character vector of names
#' only, including those of build-in contexts.
#' @param id,text_column,id_column,files,dir,file_type,collapse_lines,encoding Additional
#' arguments used to handle \code{text}; same as those in \code{\link{receptiviti}}.
#' @param bundle_size,bundle_byte_limit,retry_limit,clear_scratch_cache,use_future,in_memory
#' Additional arguments used to manage the requests; same as those in
#' \code{\link{receptiviti}}.
#' @param key,secret,url Request arguments; same as those in \code{\link{receptiviti}}.
#' @param verbose Logical; if \code{TRUE}, will show status messages.
#' @returns Nothing if \code{delete} if \code{TRUE}.
#' Otherwise, if \code{name} is not specified, a character vector containing names of each
#' available norming context (built-in and custom).
#' If \code{text} is not specified, the status of the
#' named context in a \code{list}. If \code{text}s are provided, a \code{list}:
#' \itemize{
#'    \item \code{initial_status}: Initial status of the context.
#'    \item \code{first_pass}: Response after texts are sent the first time, or
#'      \code{NULL} if the initial status is \code{pass_two}.
#'    \item \code{second_pass}: Response after texts are sent the second time.
#' }
#' @examples
#' \dontrun{
#'
#' # get status of all existing custom norming contexts
#' contexts <- receptiviti_norming(name_only = TRUE)
#'
#' # create or get the status of a single custom norming context
#' status <- receptiviti_norming("new_context")
#'
#' # send texts to establish the context
#'
#' ## these texts can be specified just like
#' ## texts in the main receptiviti function
#'
#' ## such as directly
#' full_status <- receptiviti_norming("new_context", c(
#'   "a text to set the norm",
#'   "another text part of the new context"
#' ))
#'
#' ## or from a file
#' full_status <- receptiviti_norming(
#'   "new_context", "./path/to/text.csv",
#'   text_column = "texts"
#' )
#'
#' ## or from multiple files in a directory
#' full_status <- receptiviti_norming(
#'   "new_context",
#'   dir = "./path/to/txt_files"
#' )
#' }
#' @export

receptiviti_norming <- function(
  name = NULL,
  text = NULL,
  options = list(),
  delete = FALSE,
  name_only = FALSE,
  id = NULL,
  text_column = NULL,
  id_column = NULL,
  files = NULL,
  dir = NULL,
  file_type = "txt",
  collapse_lines = FALSE,
  encoding = NULL,
  bundle_size = 1000,
  bundle_byte_limit = 75e5,
  retry_limit = 50,
  clear_scratch_cache = TRUE,
  use_future = FALSE,
  in_memory = TRUE,
  url = Sys.getenv("RECEPTIVITI_URL"),
  key = Sys.getenv("RECEPTIVITI_KEY"),
  secret = Sys.getenv("RECEPTIVITI_SECRET"),
  verbose = TRUE
) {
  params <- handle_request_params(url, key, secret)
  if (name_only) {
    req <- curl::curl_fetch_memory(
      paste0(params$url, "/v2/norming"),
      params$handler
    )
    if (req$status_code != 200) {
      stop(
        "failed to make norming list request: ",
        req$status_code,
        call. = FALSE
      )
    }
    norms <- jsonlite::fromJSON(rawToChar(req$content))
    if (verbose) {
      if (length(norms)) {
        message(
          "available norming context(s): ",
          paste(sub("custom/", "", norms, fixed = TRUE), collapse = ", ")
        )
      } else {
        message("no custom norming contexts found")
      }
    }
    return(norms)
  }

  baseurl <- paste0(params$url, "/v2/norming/custom/")
  if (!is.null(name) && grepl("[^a-z0-9_.-]", name)) {
    stop(
      "`name` can only include lowercase letters, numbers, hyphens, underscores, or periods",
      call. = FALSE
    )
  }

  # list current contexts
  req <- curl::curl_fetch_memory(baseurl, params$handler)
  if (req$status_code != 200) {
    stop(
      "failed to make norming list request: ",
      req$status_code,
      call. = FALSE
    )
  }
  norms <- jsonlite::fromJSON(rawToChar(req$content))
  if (length(norms)) {
    if (verbose && is.null(name)) {
      message(
        "custom norming context(s) found: ",
        paste(sub("custom/", "", norms$name, fixed = TRUE), collapse = ", ")
      )
    }
  } else {
    if (verbose && is.null(name)) message("no custom norming contexts found")
    norms <- NULL
  }
  if (is.null(name)) {
    return(norms)
  }

  context_id <- paste0("custom/", name)
  if (context_id %in% norms$name) {
    if (delete) {
      curl::handle_setopt(params$handler, customrequest = "DELETE")
      req <- curl::curl_fetch_memory(paste0(baseurl, name), params$handler)
      if (req$status_code != 200) {
        message <- list(error = rawToChar(req$content))
        if (substr(message$error, 1, 1) == "{")
          message$error <- jsonlite::fromJSON(message$error)
        stop(
          "failed to delete custom norming context: ",
          message$error,
          call. = FALSE
        )
      }
      return(invisible(NULL))
    }
    status <- as.list(norms[norms$name == context_id, ])
    if (length(options)) {
      warning(
        "context ",
        name,
        " already exists, so options do not apply",
        call. = FALSE
      )
    }
  } else if (!delete) {
    # establish a new context if needed
    if (verbose) message("requesting creation of custom context ", name)
    curl::handle_setopt(
      params$handler,
      copypostfields = jsonlite::toJSON(
        c(name = name, options),
        auto_unbox = TRUE
      )
    )
    req <- curl::curl_fetch_memory(baseurl, params$handler)
    if (req$status_code != 200) {
      message <- list(error = rawToChar(req$content))
      if (substr(message$error, 1, 1) == "{")
        message$error <- jsonlite::fromJSON(message$error)
      stop(
        "failed to make norming creation request: ",
        message$error,
        call. = FALSE
      )
    }
    status <- jsonlite::fromJSON(rawToChar(req$content))
    for (option in names(options)) {
      if (!is.null(status[[option]]) && status[[option]] != options[[option]]) {
        warning(
          "set option ",
          option,
          " does not match the requested value",
          call. = FALSE
        )
      }
    }
  }
  if (delete) {
    message("context ", name, " does not exist")
    return(invisible(NULL))
  }
  if (verbose) {
    message(
      "status of ",
      name,
      ": ",
      jsonlite::toJSON(status, pretty = TRUE, auto_unbox = TRUE)
    )
  }
  if (is.null(text)) {
    return(status)
  }
  if (status$status != "created") {
    warning("status is not `created`, so cannot send text", call. = FALSE)
    return(invisible(list(
      initial_status = status,
      first_pass = NULL,
      second_pass = NULL
    )))
  }
  if (verbose) message("sending first-pass samples for ", name)
  first_pass <- manage_request(
    text,
    id = id,
    text_column = text_column,
    id_column = id_column,
    files = files,
    dir = dir,
    file_type = file_type,
    collapse_lines = collapse_lines,
    encoding = encoding,
    bundle_size = bundle_size,
    bundle_byte_limit = bundle_byte_limit,
    retry_limit = retry_limit,
    clear_scratch_cache = clear_scratch_cache,
    cores = 1,
    use_future = use_future,
    in_memory = in_memory,
    url = paste0(baseurl, name, "/one"),
    key = key,
    secret = secret,
    verbose = verbose,
    to_norming = TRUE
  )$final_res
  second_pass <- NULL
  if (
    !is.null(first_pass$analyzed_samples) &&
      all(first_pass$analyzed_samples == 0)
  ) {
    warning(
      "no texts were successfully analyzed in the first pass, so second pass was skipped",
      call. = FALSE
    )
  } else {
    if (verbose) message("sending second-pass samples for ", name)
    second_pass <- manage_request(
      text,
      id = id,
      text_column = text_column,
      id_column = id_column,
      files = files,
      dir = dir,
      file_type = file_type,
      collapse_lines = collapse_lines,
      encoding = encoding,
      bundle_size = bundle_size,
      bundle_byte_limit = bundle_byte_limit,
      retry_limit = retry_limit,
      clear_scratch_cache = clear_scratch_cache,
      cores = 1,
      use_future = use_future,
      in_memory = in_memory,
      url = paste0(baseurl, name, "/two"),
      key = key,
      secret = secret,
      verbose = verbose,
      to_norming = TRUE
    )$final_res
  }
  if (
    !is.null(second_pass$analyzed_samples) &&
      all(second_pass$analyzed_samples == 0)
  ) {
    warning(
      "no texts were successfully analyzed in the second pass",
      call. = FALSE
    )
  }
  invisible(list(
    initial_status = status,
    first_pass = first_pass,
    second_pass = second_pass
  ))
}
