#' Create Indexes for Tables in a Database
#'
#' This function creates indexes for tables in a database according to the
#' provided specifications.
#'
#' @param db_conn A database connection object.
#' @param specs A list containing the table names as keys and lists of column
#' names for indexing as values.
#'
#' @examples
#' \dontrun{
#' # Example usage of create_indexes
#' db_conn <- dbConnect(duckdb::duckdb(), ":memory:")
#' specs <- list(
#'   persons = list(
#'      "1" = "person_id", "2" = c("person_id", "sex_at_instance_creation")),
#'   medicines = list(
#'      "1" = "person_id", "2" = c("person_id", "date_dispensing"))
#' )
#' create_indexes(db_conn, specs)
#' }
#' @keywords internal
create_indexes <- function(db_conn, specs) {
  # Loop through the table names in the specs list
  for (table_name in names(specs)) {
    # Get the table's list of index column names
    table <- specs[[table_name]]

    # Loop through the index names within the table
    for (index_name in names(table)) {
      # Get the column names for the current index
      index <- table[[index_name]]

      # Concatenate the index elements with commas
      index_elements <- paste(index, collapse = ", ")

      # Send the CREATE INDEX statement to the database
      p <- DBI::dbSendStatement(db_conn, paste0(
        "CREATE INDEX ", table_name, "_index_", index_name,
        " ON ", table_name, "(", index_elements, ")"
      ))
      DBI::dbClearResult(p)
    }
  }
}
