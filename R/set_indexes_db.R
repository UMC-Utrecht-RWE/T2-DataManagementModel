#' Set Indexes for Tables in a Database
#'
#' This function creates indexes for tables in a database according to the 
#' provided specifications.
#'
#' @author Albert Cid Royo
#'
#' @param db_conn A database connection object.
#' @param specs A list containing the table names as keys and lists of column 
#' names for indexing as values.
#'
#' @examples
#' \dontrun{
#' # Example usage of set_indexes_db
#' db_conn <- dbConnect(RSQLite::SQLite(), ":memory:")
#' specs <- list(
#'   persons = list("1" = "person_id", "2" = c("person_id", "sex_at_intance_creation")),
#'   medicines = list("1" = "person_id", "2" = c("person_id", "date_dispensing"))
#' )
#' set_indexes_db(db_conn, specs)
#' }
#'
#' @importFrom DBI dbSendStatement
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @rdname set_indexes_db
#'
set_indexes_db <- function(db_conn, specs) {
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
