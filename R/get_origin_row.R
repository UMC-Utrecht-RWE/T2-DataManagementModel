#' Get Records from Origin CDM Tables by Unique Identifier
#'
#' This function retrieves records from Common Data Model (CDM) tables in a database
#' that match provided unique identifiers. The function assumes that unique identifiers
#' are formatted as 'table_name-row_id'.
#'
#' @param db_connection Database connection object (DBIConnection).
#' @param unique_id_dt A data.frame or data.table containing unique identifiers.
#' @param unique_id_name Column name in unique_id_dt containing the identifiers. Default is "ID".
#' @param separator_id Character that separates table name from row ID in the unique identifier. Default is "-".
#'
#' @return A named list where each element contains the records from a CDM table
#'         matching the provided identifiers. List names correspond to table names.
#'
#' @details The function splits each unique identifier into a table name and row ID 
#'          using the specified separator. It then queries each unique table to retrieve 
#'          the corresponding records. Tables without matching records return empty lists.
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' library(data.table)
#' 
#' # Create a database connection
#' db_connection <- dbConnect(RSQLite::SQLite(), ":memory:")
#' 
#' # Create and populate a sample table
#' dbExecute(db_connection, "CREATE TABLE EVENTS (ID TEXT, value TEXT)")
#' dbExecute(db_connection, "INSERT INTO EVENTS VALUES ('EVENTS-1', 'value1')")
#' dbExecute(db_connection, "INSERT INTO EVENTS VALUES ('EVENTS-2', 'value2')")
#' 
#' # Create unique identifiers
#' cases <- data.table(ID = c("EVENTS-1", "EVENTS-2"))
#' 
#' # Retrieve records
#' results <- get_origin_row(
#'   db_connection = db_connection,
#'   unique_id_dt = cases,
#'   separator_id = "-"
#' )
#' 
#' # Clean up
#' dbDisconnect(db_connection)
#' }
#'
#' @export
get_origin_row <- function(db_connection, unique_id_dt,
                           unique_id_name = "ID", separator_id = "-") {
  unique_id_dt <- data.table::as.data.table(unique_id_dt)
  return_values <- list()
  
  # Check if the specified unique identifier exists in unique_id_dt
  if (!unique_id_name %in% names(unique_id_dt)) {
    message(paste0(
      "[get_origin_row] The unique identifier '",
      unique_id_name, "' does not exist in the unique_id_dt"
    ))
    return(return_values)
  }
  
  # Split the unique identifier into cdm_table and rowid
  unique_id_dt[, c("cdm_table", "rowid") :=
                 data.table::tstrsplit(get(unique_id_name),
                                       separator_id,
                                       fixed = TRUE
                 )]
  
  # Check if separator_id is present in the unique identifier
  if (is.null(unique_id_dt$rowid) || all(is.na(unique_id_dt$rowid))) {
    message(paste0(
      "[get_origin_row] The separator '", separator_id,
      "' does not exist in the unique identifiers"
    ))
    return(return_values)
  }
  
  # Loop through unique cdm_tables
  for (table in unique(unique_id_dt$cdm_table)) {
    # Skip if table is NA
    if (is.na(table)) {
      next
    }
    
    # Verify table exists in database
    table_exists <- DBI::dbExistsTable(db_connection, table)
    if (!table_exists) {
      message(paste0("[get_origin_row] Table '", table, "' does not exist in the database"))
      return_values[[table]] <- data.table::data.table()
      next()
    }
    
    col_names_db <- DBI::dbListFields(db_connection, table)
    
    # Check if unique_id_name exists in the table
    if (!unique_id_name %in% col_names_db) {
      message(paste0(
        "[get_origin_row] Column '", unique_id_name,
        "' does not exist in table '", table, "'"
      ))
      return_values[[table]] <- data.table::data.table()
      next()
    }
    
    # Get IDs for this table
    table_ids <- unique_id_dt[cdm_table == table, get(unique_id_name)]
    
    if (length(table_ids) == 0) {
      return_values[[table]] <- data.table::data.table()
      next()
    }
    
    # Format IDs properly for SQL query
    formatted_ids <- paste0("'", table_ids, "'")
    
    # Retrieve records from the table based on the unique identifier
    query <- paste0(
      "SELECT * FROM ", DBI::SQL(table), " WHERE ", DBI::SQL(unique_id_name), " IN (",
      paste0(formatted_ids, collapse = ","),
      ")"
    )
    
    result <- tryCatch({
      data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
    }, error = function(e) {
      message(paste0("[get_origin_row] Error querying table '", table, "': ", e$message))
      data.table::data.table()
    })
    
    return_values[[table]] <- result
  }
  
  return(return_values)
}