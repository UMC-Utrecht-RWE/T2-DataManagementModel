#' Get Unique Code List from Database
#'
#' This function retrieves unique code lists from a database based on the
#' specified columns.
#'
#' @param db_connection A database connection object.
#' @param column_specs A list of lists specifying the columns to be used for
#'   distinct, along with their new names. Each inner list should contain two
#'   elements: "column_name" and "new_name".
#'
#' @return A data.table with distinct values based on the specified columns.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' # Define the column information list
#' column_info_list <- list(
#'   list(column_name = "event_code", alias_name = "code"),
#'   list(column_name = "event_record_vocabulary", alias_name = "coding_system"),
#'   list(column_name = "event_free_text", alias_name = "code")
#' )
#' }
#'
#' # Replace 'your_db_connection' with your actual database connection
#' db_connection_origin <- dbConnect(RSQLite::SQLite(), dbname = "your_database.db")
#'
#' # Call the function with the database connection and column information list
#' result_list <- get_unique_codelist(db_connection_origin, column_info_list)
#'
#' # Close the database connection
#' dbDisconnect(db_connection_origin)
#'
#' # Access individual results from the list
#' unique_codelist_event_code <- result_list[[1]]
#' unique_codelist_free_text <- result_list[[2]]
#' }
#'
#' @export
get_unique_codelist <- function(db_connection, column_info_list) {
  # Initialize an empty list to store the individual queries
  queries <- list()
  
  # Iterate through the column information list
  for (col_info in column_info_list) {
    # Extract column details
    column_name <- col_info$column_name
    alias_name <- col_info$alias_name
    
    select_clause <- paste(
      sprintf("SELECT %s AS %s", spec$column_name, spec$new_name),
      collapse = ", "
    )
    
    # Generate the GROUP BY clause
    group_by_clause <- paste(
      sprintf("GROUP BY %s", spec$column_name),
      collapse = ", "
    )
    
    # Construct the SQL query
    sql_query <- paste(select_clause, group_by_clause)
    
    # Add the query to the list
    queries <- c(queries, list(sql_query))
  }
  
  # Initialize an empty list to store the results
  results <- list()
  
  # Execute each query and store the result in the results list
  for (query in queries) {
    result <- data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
    results <- c(results, list(result))
  }
  
  # Return the list of results
  return(results)
}
