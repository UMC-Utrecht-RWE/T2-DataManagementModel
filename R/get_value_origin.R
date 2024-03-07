#' Get values from the origin database based on specified columns
#'
#' This function retrieves values from the origin database for specified columns
#' and tables.
#'
#' @param cases_dt Data table containing information about cases.
#' @param db_connection Database connection object.
#' @param columns A list specifying the columns to retrieve for each unique
#'   ori_table.
#'
#' @return A data table with the collected values.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' get_value_origin(cases_dt, db_connection,
#'                  list('Table1' = 'Column1', 'Table2' = 'Column2'))
#' }
#'
#'@export
get_value_origin <- function(cases_dt, db_connection, columns = NULL) {
  # Write cases data table to a temporary table in the database
  DBI::dbWriteTable(db_connection, "cases_tmp", cases_dt, overwrite = TRUE, temp = TRUE)
  
  # Extract unique ori_tables from the cases data table
  ori_tables <- unique(cases_dt[, "ori_table"])
  
  # Initialize an empty list to store updated values
  updated_values <- list()
  
  # Check if columns are specified
  if (is.null(columns)) {
    stop("[get_value_origin] Columns need to be defined")
  }
  
  # Loop through each unique ori_table
  for (ori_table in ori_tables) {
    column <- columns[[ori_table]]
    
    # Query the database to get values based on the specified column and ori_table
    query <- paste0(
      "SELECT t2.ori_table, t1.ROWID, ", column,
      " FROM ", ori_table, " t1",
      " INNER JOIN cases_tmp t2 ON t1.ROWID = t2.ROWID"
    )
    rs <- data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
    
    # Rename the column in the result set
    data.table::setnames(rs, column, "Value")
    
    # Combine the result set with the updated values list
    updated_values <- data.table::rbindlist(list(updated_values, rs), use.names = TRUE)
    
    # Remove the result set from memory
    rm(rs)
  }
  
  # Remove the temporary table from the database
  DBI::dbRemoveTable(db_connection, "cases_tmp")
  
  # Return unique values
  unique_updated_values <- unique(updated_values)
  return(unique_updated_values)
}
