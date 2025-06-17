#' Ensure an object is a data.table.
#' It used the function data.table::as.data.table. This is very
#' permissive. Almost anything that can be converted to a data.table
#' by this function. Caution is advised.
#'
#' @param obj The object to check and convert.
#' @param error_message Custom error message if conversion fails.
#' @return A data.table object.
#' @keywords internal
ensure_data_table <- function(
  obj,
  error_message = "Input must be convertible to a data.table"
) {
  if (!data.table::is.data.table(obj)) {
    if (is.null(obj)) {
      stop("Input is NULL.")
    }
    # Attempt to convert the object to a data.table
    tryCatch(
      {
        obj <- data.table::as.data.table(obj)
      },
      error = function(e) {
        stop(error_message, call. = FALSE)
      }
    )
  }
  return(obj)
}