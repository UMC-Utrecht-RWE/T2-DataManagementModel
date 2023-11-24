#' Get values from the origin database based on specified columns
#'
#' This function retrieves values from the origin database for specified columns and tables.
#'
#' @param cases_dt Data table containing information about cases.
#' @param dbConnection Database connection object.
#' @param columns A list specifying the columns to retrieve for each unique Ori_Table.
#'
#' @return A data table with the collected values.
#'
#' @export
#'
#' @examples
#' # Example usage:
#' # GetValueOriginDatabase(cases_dt, dbConnection, list('Table1' = 'Column1', 'Table2' = 'Column2'))
#'
#' @import data.table
#' @import DBI
#'
GetValueOriginDatabase <- function(cases_dt, dbConnection, columns = NULL) {
  # Write cases data table to a temporary table in the database
  dbWriteTable(dbConnection, "cases_tmp", cases_dt, overwrite = TRUE, temp = TRUE)

  # Extract unique Ori_Tables from the cases data table
  OriTables <- unique(cases_dt[, "Ori_Table"])

  # Initialize an empty list to store updated values
  updated_values <- list()

  # Check if columns are specified
  if (is.null(columns)) {
    stop(paste0("[GetValueOriginDatabase] Columns need to be defined"))
  }

  # Loop through each unique Ori_Table
  for (Ori_table in OriTables) {
    column <- columns[[Ori_table]]

    # Query the database to get values based on the specified column and Ori_Table
    rs <- as.data.table(dbGetQuery(dbConnection, paste0(
      "SELECT t2.Ori_Table, t1.ROWID, ", column,
      " FROM ", Ori_table, " t1
                            INNER JOIN
                            cases_tmp t2
                            ON t1.ROWID = t2.ROWID"
    )))

    # Rename the column in the result set
    setnames(rs, column, "Value")

    # Combine the result set with the updated values list
    updated_values <- rbindlist(list(updated_values, rs), use.names = TRUE)

    # Remove the result set from memory
    rm(rs)
  }

  # Remove the temporary table from the database
  dbRemoveTable(dbConnection, "cases_tmp")

  # Return unique values
  return(unique(updated_values))
}
