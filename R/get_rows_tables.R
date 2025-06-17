#' Get Row Counts for Tables in a Database
#'
#' This function retrieves the row counts for all tables in a SQLite database.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param verbose Logical. If `TRUE`, prints intermediate queries
#' and table names. Default is `FALSE`.
#'
#' @return A data frame with two columns: 'name' (table name) and 'row_count'
#' (number of rows in each table).
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' db_connection <- DBI::dbConnect(duckdb::duckdb(), "your_database.db")
#' get_rows_tables(db_connection)
#' }
#'
#' @export
get_rows_tables <- function(db_connection, verbose = TRUE) {
  # Get the list of tables
  message("Getting tables from the database")
  tables <- DBI::dbListTables(db_connection)

  if (length(tables) == 0) {
    stop("No tables found in the database.")
  }
  if (verbose) {
    message(glue::glue("Tables found: {paste(tables, collapse = ', ')}"))
  }

  # Construct the query to get row counts
  queries <- lapply(tables, function(table) {
    sprintf(
      "SELECT '%s' AS name, (SELECT COUNT(1) FROM %s) AS row_count",
      table, table
    )
  })

  # Combine all queries using UNION ALL
  full_query <- paste(queries, collapse = " UNION ALL ")

  if (verbose) {
    message(glue::glue("Generated query: {full_query}"))
  }

  # Execute the query and retrieve the result
  return(DBI::dbGetQuery(db_connection, full_query))
}
