#' Get Unique Code List from Database
#'
#' This function retrieves unique code lists from a database based on the
#' specified columns.
#'
#' @param db_connection A database connection object.
#' @param column_info_list A list of lists specifying the columns to be used for
#'   distinct, along with their new names. Each inner list should contain two
#'   elements: "column_name" and "new_name".
#' @param tb_name name of the CDM table
#'
#' @return A data.table with distinct values based on the specified columns.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' # Define the column information list
#' column_info_list <- list(
#'   list(column_name = "event_code", alias_name = "code"),
#'   list(column_name = "event_record_vocabulary",
#' alias_name = "coding_system"),
#'   list(column_name = "event_free_text", alias_name = "code")
#' )
#' column_info_list <- list(
#'   list(
#'     column_name = c("event_code", "event_record_vocabulary"),
#'     alias_name = c("code", "coding_system")
#'   ),
#'   list(
#'     column_name = c("event_free_text"),
#'     alias_name = c("code")
#'   )
#' )
#'
#' # Replace 'your_db_connection' with your actual database connection
#' db_connection_origin <- DBI::dbConnect(duckdb::duckdb(), dbname = "your_database.db")
#'
#' # Call the function with the database connection and column information list
#' result_list <- get_unique_codelist(db_connection_origin, column_info_list)
#'
#' # Close the database connection
#' DBI::dbDisconnect(db_connection_origin)
#'
#' # Access individual results from the list
#' unique_codelist_event_code <- result_list[[1]]
#' unique_codelist_free_text <- result_list[[2]]
#' }
#'
#' @keywords internal
get_unique_codelist <- function(db_connection, column_info_list, tb_name) {
  # Initialize an empty list to store the individual queries
  queries <- list()

  # Iterate through the column information list
  for (col_info in column_info_list) {
    # Extract column details
    column_name <- col_info$column_name
    alias_name <- col_info$alias_name

    # take EVENTS table for example, switch to function argument later
    select_clause <-
      sprintf(
        "SELECT %s, count(*) AS COUNT FROM %s",
        paste(sprintf("%s AS %s", column_name, alias_name), collapse = ", "),
        tb_name
      )

    # Generate the GROUP BY clause
    group_by_clause <-
      sprintf("GROUP BY %s", paste(column_name, collapse = ", "))

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
  results
}
