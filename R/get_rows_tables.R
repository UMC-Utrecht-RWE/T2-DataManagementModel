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
get_rows_tables <- function(db_connection) {
  # Retrieve the names of all tables in the database
  print(glue::glue("Retrieving dbListTables from {db_connection}"))

  tryCatch(
    {
      tables <- DBI::dbListTables(db_connection)
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
  if (length(tables) == 0) {
    stop("No tables found in the database.")
  }

  invalid_tables <- tables[tables == "" | is.na(tables)]
  if (length(invalid_tables) > 0) {
    warning(
      "The following tables have invalid names and will be skipped: ",
      paste(invalid_tables, collapse = ", ")
    )
    tables <- tables[!(tables == "" | is.na(tables))]
  }

  # Handle the case where no valid tables remain
  if (length(tables) == 0) {
    stop("No valid tables found in the database after filtering.")
  }

  ###############
  # Query formation
  ###############
  # Construct the SQLite query to get the row counts for all tables
  query <- paste0(
    "SELECT '", tables[1], "' AS name, (SELECT COUNT(1) FROM ",
    tables[1], ") AS row_count"
  )

  for (i in 2:length(tables)) {
    query <- paste0(
      query, " UNION ALL SELECT '", tables[i],
      "' AS name, (SELECT COUNT(1) FROM ", tables[i],
      ") AS row_count"
    )
  }

  ###############
  # Execute the query and retrieve the result
  ###############
  tryCatch(
    {
      return(DBI::dbGetQuery(db_connection, query))
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
}
