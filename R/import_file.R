#' Import and Format CSV Data
#'
#' This function imports CSV files and applies basic formatting, such as
#' handling dates, handling missing values, and removing leading and trailing
#' spaces. It also allows for column selection and subsetting based on specified
#' criteria.
#'
#' @param path Path to the CSV file.
#' @param cols A vector of strings indicating which columns to import. Optional.
#' @param cols_new A vector of strings indicating the new column names.
#'   Optional; must have the same length as cols.
#' @param exprs A data.table expression (within expression()) for subsetting
#'   rows. Optional.
#' @param date_cols Vector of strings indicating variables with 'YYYYMMDD'
#'   format to be converted to date format. Optional.
#'
#' @return A data.table data frame with formatted columns.
#' @examples
#' \dontrun{
#' # Example usage of import_file
#' path <- "path/to/your/file.csv"
#' data <- import_file(path,
#'   cols = c("col1", "col2"),
#'   cols_new = c("new_col1", "new_col2"), exprs = expression(col1 > 0),
#'   date_cols = c("date_col")
#' )
#' }
#'
#' @export
import_file <- function(path, cols = NULL, cols_new = NULL, exprs = NULL,
                        date_cols = NULL) {
  if (!file.exists(path)) {
    warning("csv file is not found")
    return(NULL)
  } else {
    # Import csv all in character. This is because estimation of the format by
    # fread may give unexpected behavior. Now you have a good fixed starting point
    if (!is.null(cols)) {
      loaded_file <- fread(path,
        stringsAsFactors = F, select = cols,
        na.strings = c("", NA), colClasses = c("character")
      )
    } else {
      loaded_file <- fread(path,
        stringsAsFactors = F, na.strings = c("", NA),
        colClasses = c("character")
      )
    }

    if (nrow(loaded_file) == 0) {
      warning("0 rows in imported csv")
    }

    # Rename
    if (!is.null(cols) & !is.null(cols_new)) {
      setnames(loaded_file, eval(cols), eval(cols_new))
    }

    # Add correction for spaces to prevent misinterpretation of NA's and
    # avoiding leading and tailing spaced.
    invisible(lapply(colnames(loaded_file), function(x) {
      loaded_file <- loaded_file[, eval(x) :=
        trimws(get(x), "b", whitespace = "[\\h\\v]")]
    }))
    loaded_file[loaded_file == ""] <- NA

    # Set specified columns in data.colls to date format
    if (!is.null(date_cols)) {
      lapply(date_cols, function(x) {
        loaded_file[, eval(x) :=
          as.Date(get(x), "%Y%m%d")]
      })
    }

    # Apply the subsetting with the specified exprs
    if (!is.null(exprs)) {
      rem1 <- nrow(loaded_file)
      loaded_file <- loaded_file[eval(exprs), ]
      rem2 <- nrow(loaded_file)
      print(paste0(
        rem2, " selected rows from ", rem1,
        " rows after evaluation of exprs"
      ))
    }
    loaded_file_dt <- data.table::as.data.table(loaded_file)
    return(loaded_file_dt)
  }
}
