#' Get Row Counts for Tables in a Database
#'
#' This function retrieves row counts for tables in a DuckDB database.
#'
#' @param db_connection Database connection object (DBIConnection).
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
get_rows_tables <- function(db_connection) {
  message(
    "Retrieving table names from DuckDB for connection of class: ",
    class(db_connection)
  )

  tryCatch(
    {
      tables <- DBI::dbGetQuery(
        db_connection,
        paste(
          "SELECT table_catalog, table_schema, table_name",
          "FROM information_schema.tables",
          "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')",
          "ORDER BY table_catalog, table_schema, table_name"
        )
      )
    },
    error = function(e) {
      stop(
        "Error retrieving table names from the database. ",
        "Please check the database connection.\n",
        "Error message: ", e$message
      )
    }
  )

  #################
  # input validation
  #################
  # Check for empty or invalid table names
  if (nrow(tables) == 0) {
    stop("No tables found in the database.")
  }

  row_counts <- lapply(seq_len(nrow(tables)), function(i) {
    quoted_name <- DBI::dbQuoteIdentifier(
      db_connection,
      unlist(tables[i, c("table_catalog", "table_schema", "table_name")])
    )
    qualified_name <- paste(as.character(quoted_name), collapse = ".")
    query <- paste0("SELECT COUNT(1) AS row_count FROM ", qualified_name)

    tryCatch(
      {
        data.frame(
          name = tables$table_name[i],
          row_count = DBI::dbGetQuery(db_connection, query)$row_count[1]
        )
      },
      error = function(e) {
        stop(
          "Error executing query. Problematic query: ",
          query,
          "\nError message: ",
          e$message
        )
      }
    )
  })

  result <- do.call(rbind, row_counts)
  rownames(result) <- NULL
  result
}
