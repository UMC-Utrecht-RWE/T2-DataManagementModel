#' Get Row Counts for Tables in a Database
#'
#' This function retrieves the row counts for all tables in a SQLite database.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#'
#' @return A data frame with two columns: 'name' (table name) and 'row_count' (number of rows in each table).
#'
#' @author Albert Cid Royo
#'
#' @importFrom DBI dbListTables dbGetQuery
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' db_connection <- dbConnect(RSQLite::SQLite(), "your_database.db")
#' GetRowsTablesDataBase(db_connection)
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

GetRowsTablesDataBase <- function(db_connection) {
  # Retrieve the names of all tables in the database
  tables <- dbListTables(db_connection)

  # Construct the SQLite query to get the row counts for all tables
  query <- paste0("SELECT '", tables[1], "' AS name, (SELECT COUNT(1) FROM ", tables[1], ") AS row_count")

  for (i in 2:length(tables)) {
    query <- paste0(query, " UNION ALL SELECT '", tables[i], "' AS name, (SELECT COUNT(1) FROM ", tables[i], ") AS row_count")
  }

  # Execute the query and retrieve the result
  result <- dbGetQuery(db_connection, query)
  return(result)
}
