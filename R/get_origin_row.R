#' Get Records from Origin CDM Tables by Unique Identifier
#'
#' This function retrieves records from Common Data Model (CDM)
#'  tables in a database that match provided unique identifiers.
#' The function assumes that unique identifiers are formatted
#' as 'table_name-row_id'.
#'
#' @param db_connection Database connection object (DBIConnection).
#' @param ids A data.table containing unique identifiers.
#'
#' @return A named list where each element contains the records from a CDM table
#' matching the provided identifiers. List names correspond to table names.
#'
#' @details The function splits each unique identifier into a table name and
#' row ID using the specified separator. It then queries each unique table to
#' retrieve the corresponding records. Tables without matching records return
#' empty lists.
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' library(data.table)
#'
#' # Create a database connection
#' db_connection <- dbConnect(duckdb::duckdb(), ":memory:")
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
#'   ids = cases
#' )
#'
#' # Clean up
#' dbDisconnect(db_connection)
#' }
#'
#' @export
get_origin_row <- function(db_connection, ids) {
  
  # 1. Validate inputs and ensure data.table
  # Using requireNamespace check for internal safety
  ids <- ensure_data_table(ids)
  unique_identifiers <- c('ori_table', 'unique_id')
  return_values <- list()
  
  if (nrow(ids) == 0) {
    message("[get_origin_row] The data.table ids is empty")
    return(return_values)
  }
  
  # Check if required columns exist in the input 'ids'
  if (any(!unique_identifiers %in% names(ids))) {
    missing_id <- unique_identifiers[!unique_identifiers %in% names(ids)]
    message(paste0("[get_origin_row] Missing columns in 'ids': ", paste(missing_id, collapse = ", ")))
    return(return_values)
  }
  
  # 2. Loop through unique tables
  tables_to_query <- unique(ids$ori_table)
  
  for (table in tables_to_query) {
    # Skip if table is NA
    if (is.na(table)) next
    
    # Verify table exists in database
    if (!DBI::dbExistsTable(db_connection, table)) {
      message(paste0("[get_origin_row] Table '", table, "' does not exist in the database"))
      return_values[[table]] <- data.table::data.table()
      next
    }
    
    # FIX: Get actual column names from the database
    col_names_db <- DBI::dbListFields(db_connection, table)
    
    # FIX: Check if the DATABASE table has the required columns
    if (any(!unique_identifiers %in% col_names_db)) {
      missing_cols <- unique_identifiers[!unique_identifiers %in% col_names_db]
      message(paste0("[get_origin_row] The unique identifier '", 
                     paste(missing_cols, collapse = ", "), "' does not exist in the ", table))
      return_values[[table]] <- data.table::data.table()
      next
    }
    
    # Get IDs for this table
    table_ids <- ids[ori_table == table, unique_id]
    if (length(table_ids) == 0) {
      return_values[[table]] <- data.table::data.table()
      next
    }
    
    # 3. Execution with Error Handling
    # Note: Using SQL() to handle table names and quoting IDs for safety
    formatted_ids <- paste0("'", gsub("'", "''", table_ids), "'", collapse = ",")
    query <- paste0("SELECT * FROM ", DBI::dbQuoteIdentifier(db_connection, table), 
                    " WHERE unique_id IN (", formatted_ids, ")")
    
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