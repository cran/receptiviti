# receptiviti 0.1.9

### Features
* Adds `collect_results` option for large requests.
* Adds framework checking and listing functionality.
* Adds custom norming context creation functionality.
* Adds support for V2 of the API.

### Improvements
* Improves cache performance.
* Adds support for compressed files.
* Validates `version` and `endpoint` arguments.

### Bug Fixes
* Avoids failure on total bundle failure.
* Avoids overwriting existing cache results within overlapping bins on update.

# receptiviti 0.1.8

### Improvements
* Makes encoding detection more reliable.

# receptiviti 0.1.7

### Improvements
* Suggests rather than imports optional packages (`arrow` and `dplyr` for the cache, and `future.apply` for use with Future).

# receptiviti 0.1.6

### Improvements
* Adds `encoding` argument; improves handling of non-UTF-8 files.

### Bug Fixes
* Fixes `collapse_line` when reading files from a directory.
* Fixes version and endpoint extraction from URL.

# receptiviti 0.1.5

### Features
* Supports custom API versions and endpoints.

### Improvements
* Disables cache by default.
* Adds `files` and `dir` arguments for clearer input.
* Returns file names as IDs when `text_as_paths` is `TRUE`.
* Reworks text hashing for improved cache handling.

### Bug Fixes
* Avoids unnecessary cache rewrites.
* Fixes partial cache updating.

# receptiviti 0.1.4

### Features
* Supports additional API argument.

### Improvements
* Standardizes option name format (`receptiviti_frameworks` changed to `receptiviti.frameworks`).
* Makes the request cache sensitive to URL and credentials, to make it easier to make different requests with the same text.

### Bug Fixes
* Cleans up cached malformed responses.
* Avoids an unhandled body-size-related issue with libcurl.

# receptiviti 0.1.3

### Improvements
* An ID column can be specified with `id`, alternative to `id_column`.

# receptiviti 0.1.2

### Bug Fixes
* Avoids establishing the default cache in non-interactive sessions.

# receptiviti 0.1.0

First release.