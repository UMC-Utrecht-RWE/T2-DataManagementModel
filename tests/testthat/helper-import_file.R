import_file <- function(path, cols = NULL, cols_new = NULL, exprs = NULL,
                        date_cols = NULL) {
  if (!file.exists(path)) {
    warning("csv file is not found")
    return(NULL)
  } else {
    # Import csv all in character. This is because estimation of the format by
    # fread may give unexpected behavior.
    # Now you have a good fixed starting point.
    if (!is.null(cols)) {
      loaded_file <- fread(path,
        stringsAsFactors = FALSE, select = cols,
        na.strings = c("", NA), colClasses = c("character"),
        encoding = "UTF-8"
      )
    } else {
      loaded_file <- fread(path,
        stringsAsFactors = FALSE, na.strings = c("", NA),
        colClasses = c("character"),
        encoding = "UTF-8"
      )
    }

    if (nrow(loaded_file) == 0) {
      warning("0 rows in imported csv")
    }

    # Rename
    if (!is.null(cols) && !is.null(cols_new)) {
      setnames(loaded_file, eval(cols), eval(cols_new))
    }

    # Add correction for spaces to prevent misinterpretation of NA's and
    # avoiding leading and tailing spaced.
    invisible(lapply(colnames(loaded_file), function(x) {
      loaded_file <- loaded_file[
        , eval(x) := trimws(get(x), "b", whitespace = "[\\h\\v]")
      ]
    }))
    loaded_file[loaded_file == ""] <- NA

    # Set specified columns in data.colls to date format
    if (!is.null(date_cols)) {
      lapply(date_cols, function(x) {
        loaded_file[
          , eval(x) := as.Date(get(x), "%Y%m%d")
        ]
      })
    }

    # Apply the subsetting with the specified exprs
    if (!is.null(exprs)) {
      rem1 <- nrow(loaded_file)
      loaded_file <- loaded_file[eval(exprs), ]
      rem2 <- nrow(loaded_file)
      message(paste0(
        rem2, " selected rows from ", rem1,
        " rows after evaluation of exprs"
      ))
    }
    return(data.table::as.data.table(loaded_file))
  }
}
