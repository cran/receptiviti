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
