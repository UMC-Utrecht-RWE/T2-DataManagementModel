#' Get Unique Code List from Database
#'
#' This function retrieves unique code lists from a database based on the
#' specified columns.
#'
#' @param db_connection A database connection object.
#' @param column_info_list A list of lists specifying the columns to be used for
#'   distinct, along with their new names. Each inner list should contain two
#'   elements: "source_column" and "alias_name".
#' @param tb_name Name of the CDM table.
#'
#' @return A list of data.tables with distinct values based
#' on the specified columns.
#' @export
get_unique_codelist <- function(db_connection, column_info_list, tb_name) {

  # 1. Structure Validation
  if (!is.list(column_info_list) || length(column_info_list) == 0) {
    stop("Input 'column_info_list' must be a non-empty list.")
  }

  for (i in seq_along(column_info_list)) {
    item <- column_info_list[[i]]

    # Check for required names
    required_names <- c("source_column", "alias_name")
    if (!all(required_names %in% names(item))) {
      stop(
        sprintf(
          "Element '%s' is missing columns: 'source_column' and/or 'alias_name'.",
          i
        )
      )
    }

    # Ensure source and alias have the same length for mapping
    if (length(item$source_column) != length(item$alias_name)) {
      stop(
        sprintf(
          "In %s, 'source_column' and 'alias_name' must have the same length.",
          i
        )
      )
    }
  }

  # 2. Query Generation
  queries <- list()

  for (col_info in column_info_list) {
    # Renamed from column_name to source_column
    source_column <- col_info$source_column
    alias_name <- col_info$alias_name

    # Construct SELECT clause with mapping
    select_clause <- sprintf(
      "SELECT %s, count(*) AS COUNT FROM %s",
      paste(sprintf("%s AS %s", source_column, alias_name), collapse = ", "),
      tb_name
    )

    # Generate the GROUP BY clause using source columns
    group_by_clause <- sprintf(
      "GROUP BY %s", paste(source_column, collapse = ", ")
    )

    # Construct the SQL query
    sql_query <- paste(select_clause, group_by_clause)
    queries <- c(queries, list(sql_query))
  }

  # 3. Execution
  results <- list()

  for (query in queries) {
    # Using tryCatch is often safer for DB operations
    result <- tryCatch({
      data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
    }, error = function(e) {
      message(sprintf("Error executing query: %s", query))
      stop(e)
    })
    results <- c(results, list(result))
  }

  results
}