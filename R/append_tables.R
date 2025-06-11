#' Database Appending Function
#'
#' Functions for appending tables into a new table in a database.
#'
#' @param db connection to the database
#' @param tables vector of table present in the db to append
#' @param name new name of the resulting table
#' @param dt_coll name of the column that represents dates
#' @param colls which columns to append
#' @param return if TRUE, the function returns the resulting data frame
#' @param sqlite_temp if TRUE, the SQL table created is temporary
#'
#' @keywords internal
append_tables <- function(
  db,
  tables,
  name,
  dt_coll = "date",
  colls = "*",
  return = TRUE,
  sqlite_temp = FALSE
) {
  # Check for missing tables in the database
  lists_tables_available <- DBI::dbListTables(db)
  missing <- tables[!tables %in% lists_tables_available]
  if (length(missing) > 0) {
    stop(paste0(paste0(missing, collapse = " "), " not in database"))
  }

  tables <- tables[tables %in% lists_tables_available]

  # Build the SQL query for appending tables
  if (length(tables) > 1) {
    query <- paste0(
      "SELECT DISTINCT ", colls, " FROM ", tables[1],
      paste0(
        paste0(
          " UNION SELECT DISTINCT ", colls,
          " FROM ", tables[2:length(tables)]
        ),
        collapse = " "
      )
    )
  } else if (length(tables) == 1) {
    query <- paste0("SELECT DISTINCT ", colls, " FROM ", tables[1])
  }

  # Set up the query statement based on whether it's a temporary table or not
  if (sqlite_temp == TRUE) {
    query_statement <- paste0("CREATE TEMP TABLE ", name, " AS ", query)
  }
  if (sqlite_temp == FALSE) {
    query_statement <- paste0("CREATE TABLE ", name, " AS ", query)
  }
  # Check if tables have zero cases and log a message
  for (i in tables) {
    if (DBI::dbGetQuery(db, paste0("SELECT count(*) FROM ", i)) == 0) {
      message(paste0(
        "0 cases in ",
        i,
        " for the Appended TABLE ",
        name
      ))
    }
  }

  # Execute the query statement and return result as a data.table if needed
  p <- DBI::dbSendStatement(db, query_statement)
  DBI::dbClearResult(p)
  if (return == TRUE) {
    results <- data.table::as.data.table(
      DBI::dbGetQuery(db, paste0("SELECT * FROM ", name))
    )
    results[, eval(dt_coll) := as.Date(get(dt_coll), origin = "1970-01-01")]
    return(data.table::as.data.table((results)))
  }
}
