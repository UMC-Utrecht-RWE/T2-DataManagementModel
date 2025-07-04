#' Get values from the origin database based on specified search_scheme
#'
#' This function retrieves values from the origin database for specified
#' search_scheme and tables.
#'
#' @param cases_dt Data table containing information about cases.
#' Must be a data.table with at least the search_scheme "ROWID" and "ori_table".
#' @param db_connection Database connection object.
#' Must be an active DuckDB connection.
#' @param search_scheme A list specifying the search_scheme to retrieve
# for each unique ori_table. Each element name must correspond to a table
# in the database, and each value must be a single column name as character.
#'
#' @return A data table with the collected values.
#'
#' @examples
#' \dontrun{
#' get_origin_value(
#'   cases_dt,
#'   db_connection,
#'   list("Table1" = "Column1", "Table2" = "Column2")
#' )}
#'
#' @export
get_origin_value <- function(
  cases_dt,
  db_connection,
  search_scheme = NULL
) {
  # Input validation
  # Check if cases_dt is a data.table
  cases_dt <- T2.DMM:::ensure_data_table(
    cases_dt,
    error_message = "[get_origin_value] 'cases_dt' must be a data.table"
  )

  # Check if required search_scheme exist in cases_dt
  required_cols <- c("ROWID", "ori_table")
  missing_cols <- required_cols[!required_cols %in% names(cases_dt)]
  if (length(missing_cols) > 0) {
    stop(sprintf(
      "[get_origin_value] 'cases_dt' is missing required search_scheme: %s",
      paste(missing_cols, collapse = ", ")
    ))
  }

  # Check if db_connection is valid
  if (
    !inherits(db_connection, "duckdb_connection") ||
      !duckdb::dbIsValid(db_connection)
  ) {
    stop("[get_origin_value] 'db_connection' must be a valid DuckDB connection")
  }

  # Check if search_scheme parameter is provided and valid
  if (is.null(search_scheme)) {
    stop("[get_origin_value] search_scheme need to be defined")
  }

  if (!is.list(search_scheme) || length(search_scheme) == 0) {
    stop("[get_origin_value] 'search_scheme' must be a non-empty list")
  }

  if (!all(sapply(search_scheme, is.character)) ||
        !all(sapply(search_scheme, length) == 1)) {
    stop(paste0(
      "[get_origin_value] Each element in 'search_scheme'",
      " must be a single character string"
    ))
  }

  # Check if cases_dt is empty
  if (nrow(cases_dt) == 0) {
    warning("[get_origin_value] 'cases_dt' is empty, returning empty result")
    return(data.table::data.table(
      ori_table = character(0),
      ROWID = integer(0),
      Value = character(0)
    ))
  }

  # Extract unique ori_tables from the cases data table
  ori_tables <- unique(cases_dt[, ori_table])

  # Write cases data table to a temporary table in the database
  tryCatch(
    {
      DBI::dbWriteTable(
        db_connection, "cases_tmp", cases_dt,
        overwrite = TRUE, temp = TRUE
      )
    },
    error = function(e) {
      stop(sprintf(
        "[get_origin_value] Failed to write temporary table: %s",
        e$message
      ))
    }
  )

  # Initialize an empty list to store updated values
  updated_values <- list()

  # Loop through each unique ori_table that exists in search_scheme list
  # valid_tables <- ori_tables[ori_tables %in% names(search_scheme)]

  for (i in seq_along(search_scheme)) {
    column <- search_scheme[[i]]
    ori_table <- names(search_scheme[i])

    # Verify table exists in database
    table_exists <- tryCatch(
      {
        DBI::dbExistsTable(db_connection, ori_table)
      },
      error = function(e) {
        warning(sprintf(
          "[get_origin_value] Error checking if table '%s' exists: %s",
          ori_table, e$message
        ))
        FALSE
      }
    )

    if (!table_exists) {
      warning(sprintf(
        "[get_origin_value] Table '%s' does not exist in the database",
        ori_table
      ))
      next
    }

    # Verify column exists in the table
    if (!column %in% DBI::dbListFields(db_connection, ori_table)) {
      warning(sprintf(
        "[get_origin_value] Column '%s' does not exist in the '%s' table",
        column, ori_table
      ))
      next
    }

    # Query the database to get values based on the
    # specified column and ori_table
    query <- paste0(
      "SELECT t2.ori_table, t1.ROWID, '",
      column,
      "' AS column_origin, t1.",
      column, " AS value FROM ",
      ori_table, " t1",
      " INNER JOIN (SELECT * FROM cases_tmp WHERE ori_table = '",
      ori_table, "') t2 ON t1.ROWID = t2.ROWID"
    )

    rs <- tryCatch(
      {
        data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
      },
      error = function(e) {
        warning(sprintf(
          "[get_origin_value] Query failed for table '%s': %s",
          ori_table, e$message
        ))
        NULL
      }
    )

    if (!is.null(rs) && nrow(rs) > 0) {
      # Rename the column in the result set
      # data.table::setnames(rs, column, "value")

      # Combine the result set with the updated values list
      updated_values <- data.table::rbindlist(
        list(updated_values, rs), use.names = TRUE, fill = TRUE
      )

      # Remove the result set from memory
      rm(rs)
    }
  }

  # Remove the temporary table from the database
  tryCatch(
    {
      DBI::dbRemoveTable(db_connection, "cases_tmp")
    },
    error = function(e) {
      warning(sprintf(
        "[get_origin_value] Failed to remove temporary table: %s",
        e$message
      ))
    }
  )

  # Return unique values or empty data.table if no results
  if (length(updated_values) > 0) {
    unique_updated_values <- unique(updated_values)
    return(unique_updated_values)
  } else {
    return(data.table::data.table(
      ori_table = character(0),
      ROWID = integer(0),
      Value = character(0)
    ))
  }
}
