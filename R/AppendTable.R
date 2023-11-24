#' Database Appending and Joining Functions
#'
#' Functions for appending and joining tables in a database.
#'
#' @author Albert Cid Royo
#'
#' @docType package
#'
#' @import data.table DBI
#'
#' @examples
#' \dontrun{
#' # Example usage of the functions
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#'
# Function to append tables from a list of concepts in a database
AppendTable <- function(DB, TABLES, NAME, DT.coll = "Date", colls = "*") {
  # Check for missing tables in the database
  MISSING <- TABLES[!TABLES %in% dbListTables(DB)]
  if (length(MISSING) > 0) print(paste0(paste0(MISSING, collapse = " "), " not in database"))

  if (any(TABLES %in% dbListTables(DB))) {
    TABLES <- TABLES[TABLES %in% dbListTables(DB)]

    # Build the SQL query for appending tables
    if (length(TABLES) > 1) {
      Query <- paste0("SELECT DISTINCT ", colls, " FROM ", TABLES[1], paste0(paste0(" UNION SELECT ", colls, " FROM ", TABLES[2:length(TABLES)]), collapse = " "))
    }
    if (length(TABLES) == 1) {
      Query <- paste0("SELECT DISTINCT ", colls, " FROM ", TABLES[1])
    }

    # Check if tables have zero cases and log a message
    for (i in TABLES) {
      if (dbGetQuery(DB, paste0("SELECT count(*) FROM ", i)) == 0) log_print(paste0("0 cases in ", i, " for the Appended TABLE", NAME))
    }

    # Execute the query and return the result as a data.table
    resultTable <- as.data.table(dbGetQuery(DB, Query))[, eval(DT.coll) := as.Date(get(DT.coll), origin = "1970-01-01")]
    return(resultTable)
  } else {
    stop(paste0("0 tables for ", NAME, " in database"))
  }
}


# Function to append tables within a list that leads to a new table
AppendTables2 <- function(DB, TABLES, NAME, DT.coll = "date", colls = "*", return = TRUE, sqlite_TEMP = FALSE) {
  # Check for missing tables in the database
  MISSING <- TABLES[!TABLES %in% dbListTables(DB)]
  if (length(MISSING) > 0) print(paste0(paste0(MISSING, collapse = " "), " not in database"))

  if (any(TABLES %in% dbListTables(DB))) {
    TABLES <- TABLES[TABLES %in% dbListTables(DB)]

    # Build the SQL query for appending tables
    if (length(TABLES) > 1) {
      Query <- paste0("SELECT DISTINCT ", colls, " FROM ", TABLES[1], paste0(paste0(" UNION SELECT DISTINCT ", colls, " FROM ", TABLES[2:length(TABLES)]), collapse = " "))
    } else if (length(TABLES) == 1) {
      Query <- paste0("SELECT DISTINCT ", colls, " FROM ", TABLES[1])
    }

    # Set up the query statement based on whether it's a temporary table or not
    if (sqlite_TEMP == TRUE) {
      QueryStatement <- paste0("CREATE TEMP TABLE ", NAME, " AS ", Query)
    }
    if (sqlite_TEMP == FALSE) {
      QueryStatement <- paste0("CREATE TABLE ", NAME, " AS ", Query)
    }

    # Check if tables have zero cases and log a message
    for (i in TABLES) {
      if (dbGetQuery(DB, paste0("SELECT count(*) FROM ", i)) == 0) log_print(paste0("0 cases in ", i, " for the Appended TABLE", NAME))
    }

    # Execute the query statement and return the result as a data.table if needed
    p <- dbSendStatement(DB, QueryStatement)
    dbClearResult(p)
    if (return == TRUE) {
      results <- as.data.table(dbGetQuery(DB, paste0("SELECT * FROM ", NAME)))[, eval(DT.coll) := as.Date(get(DT.coll), origin = "1970-01-01")]
      return(results)
    }
  } else {
    log_print(paste0("0 tables for ", NAME, " in database"))
  }
}
