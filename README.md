# Receptiviti
An R package to process text with the [Receptiviti](https://www.receptiviti.com) API.

A Python package is also available at [Receptiviti/receptiviti-python](https://receptiviti.github.io/receptiviti-python/).

## Installation
Download R from [r-project.org](https://www.r-project.org), then install the package from an R console:

Release ([version 0.1.7](https://cran.r-project.org/package=receptiviti))
```R
install.packages("receptiviti")
```
Development (version 0.1.8)
```R
# install.packages("remotes")
remotes::install_github("Receptiviti/receptiviti-r")
```

And load the package:
```R
library(receptiviti)
```

## Features

- Makes requests to the Receptiviti API, working around size and rate limitations.
- Avoids sending invalid or identical texts, or repeating requests in the same session.
- Optionally builds up a local database of returned results to avoid making repeat requests in the longer-term.

## Examples

```R
# score a single text
single <- receptiviti("a text to score")

# score multiple texts, and write results to a file
multi <- receptiviti(c("first text to score", "second text"), "filename.csv")

# score texts in separate files
## defaults to look for .txt files
file_results <- receptiviti(dir = "./path/to/txt_folder")

## could be .csv
file_results <- receptiviti(
  dir = "./path/to/csv_folder",
  text_column = "text", file_type = "csv"
)

# score texts in a single file
results <- receptiviti("./path/to/file.csv", text_column = "text")
```

## API Access
To access the API, you will need to load your key and secret, as found on your [dashboard](https://dashboard.receptiviti.com).

You can enter these as arguments in each function call, but by default they will be looked for in these environment variables:
```sh
RECEPTIVITI_KEY="32lettersandnumbers"
RECEPTIVITI_SECRET="56LettersAndNumbers"
```

You can store these in your R environment file permanently:
```R
# opens ~/.Renviron; after editing, save and restart R
usethis::edit_r_environ()
```

Or set them temporarily:
```R
Sys.setenv(
  RECEPTIVITI_KEY = "32lettersandnumbers",
  RECEPTIVITI_SECRET = "56LettersAndNumbers"
)
```
