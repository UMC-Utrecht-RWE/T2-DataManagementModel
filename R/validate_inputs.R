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
  obj
}


# Validate chracter types for required columns
validate_column_type <- function(data, col_name, allowed_types = c("character")) {
  col <- data[[col_name]]
  is_valid <- any(sapply(allowed_types, function(type) {
    switch(type,
           "character" = is.character(col),
           "factor" = is.factor(col),
           FALSE)
  }))
  if (!is_valid) {
    stop(paste(col_name, "must be", paste(allowed_types, collapse = " or ")))
  }
}