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
