#' List Available Frameworks
#'
#' Retrieve the list of frameworks available to your account.
#' @param url,key,secret Request arguments; same as those in \code{\link{receptiviti}}.
#' @returns A character vector containing the names of frameworks available to your account.
#' @examples
#' \dontrun{
#'
#' # see which frameworks are available to your account
#' frameworks <- receptiviti_frameworks()
#' }
#' @export

receptiviti_frameworks <- function(
  url = Sys.getenv("RECEPTIVITI_URL"),
  key = Sys.getenv("RECEPTIVITI_KEY"),
  secret = Sys.getenv("RECEPTIVITI_SECRET")
) {
  params <- handle_request_params(url, key, secret)
  req <- curl::curl_fetch_memory(
    paste0(params$url, "/v2/frameworks"),
    params$handler
  )
  if (req$status_code == 200) {
    return(jsonlite::fromJSON(rawToChar(req$content)))
  }
  content <- list(message = rawToChar(req$content))
  if (substr(content$message, 1, 1) == "{")
    content <- jsonlite::fromJSON(content$message)
  stop("failed to retrieve frameworks list: ", content$message, call. = FALSE)
}
