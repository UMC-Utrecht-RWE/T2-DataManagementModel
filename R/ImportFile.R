#' Import and Format CSV Data
#'
#' This function imports CSV files and applies basic formatting, such as handling dates, handling missing values,
#' and removing leading and trailing spaces. It also allows for column selection and subsetting based on specified criteria.
#'
#' @param path Path to the CSV file.
#' @param colls A vector of strings indicating which columns to import. Optional.
#' @param colls.new A vector of strings indicating the new column names. Optional; must have the same length as colls.
#' @param exprss A data.table expression (within expression()) for subsetting rows. Optional.
#' @param date.colls Vector of strings indicating variables with 'YYYYMMDD' format to be converted to date format. Optional.
#'
#' @return A data.table data frame with formatted columns.
#'
#' @author Roel Elbers, Albert Cid Royo
#'
#' @import data.table
#'
#' @examples
#' \dontrun{
#' # Example usage of ImportFile
#' path <- "path/to/your/file.csv"
#' data <- ImportFile(path, colls = c("col1", "col2"), colls.new = c("new_col1", "new_col2"), exprss = expression(col1 > 0), date.colls = c("date_col"))
#' }
#'
#' @export
#' @keywords data
#' @name RWEDataManagementModel
#' @docType package
#'

ImportFile <- function(path, colls = NULL, colls.new = NULL, exprss = NULL, date.colls = NULL) {
  if (!file.exists(path)) {
    warning("csv file is not found")
    return(NULL)
  } else {
    # Import csv all in character. This is because estimation of the format by fread may give unexpected behavior.Now you have a good fixed starting point
    if (!is.null(colls)) {
      TEMP <- fread(path, stringsAsFactors = F, select = colls, na.strings = c("", NA), colClasses = c("character"))
    } else {
      TEMP <- fread(path, stringsAsFactors = F, na.strings = c("", NA), colClasses = c("character"))
    }

    if (nrow(TEMP) == 0) {
      warning("0 rows in imported csv")
    }

    # Rename
    if (!is.null(colls) & !is.null(colls.new)) {
      setnames(TEMP, eval(colls), eval(colls.new))
    }

    # Add correction for spaces to prevent misinterpretation of NA's and avoiding leading and tailing spaced.
    invisible(lapply(colnames(TEMP), function(x) TEMP <- TEMP[, eval(x) := trimws(get(x), "b", whitespace = "[\\h\\v]")]))
    TEMP[TEMP == ""] <- NA

    # Set specified columns in data.colls to date format
    if (!is.null(date.colls)) {
      lapply(date.colls, function(x) TEMP[, eval(x) := as.Date(get(x), "%Y%m%d")])
    }

    # Apply the subsetting with the specified exprss
    if (!is.null(exprss)) {
      rem1 <- nrow(TEMP)
      TEMP <- TEMP[eval(exprss), ]
      rem2 <- nrow(TEMP)
      print(paste0(rem2, " selected rows from ", rem1, " rows after evaluation of exprss"))
    }


    return(as.data.table(TEMP))
  }
}
