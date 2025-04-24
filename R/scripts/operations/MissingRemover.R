#' MissingRemover Class
#'
#' @description
#' The `MissingRemover` class is a subclass of the `DatabaseOperation` class.
#' It is responsible for identifying and removing rows with missing or invalid
#' values from specific columns in the database tables.
#'
#' @details
#' This class iterates through the tables and columns specified in the
#' `list_colums_clean` configuration of the `db_loader` object. For each table,
#' it deletes rows where the specified columns contain `NULL`,
#' empty strings (`''`), or the string `'NA'`.
#'
#' @field classname A string representing the name of the class.
#' Default is `"MissingRemover"`.
#'
#' @method run
#' @param db_loader An instance of the `DatabaseLoader` class. This object
#' provides the database connection (`db`), the list of tables and columns
#' to clean (`list_colums_clean`), and other configuration details required
#' for the operation.
#' @return None.
#'
#' @examples
#' # Example usage:
#' loader <- DatabaseLoader$new()
#' missing_remover <- MissingRemover$new()
#' missing_remover$run(loader)
#'
#' @importFrom DBI dbListTables dbSendStatement
#' @importFrom duckdb dbGetRowsAffected dbClearResult
#' @importFrom glue glue
#' @keywords internal
MissingRemover <- R6::R6Class("MissingRemover", #nolint
  inherit = DatabaseOperation,
  public = list(
    classname = "MissingRemover",
    run = function(db_loader) {
      print(glue::glue("Removing missing values from tables."))
      tables_available <- DBI::dbListTables(db_loader$db)

      for (index in seq_along(db_loader$config$list_colums_clean)) {
        table_to_clean <- names(db_loader$config$list_colums_clean[index])

        if (table_to_clean %in% tables_available) {
          print(glue::glue(
            "Deleting rows with missing values: {table_to_clean}"
          ))

          lapply(db_loader$config$list_colums_clean[index], function(x) {
            query <- paste0(
              "DELETE FROM ", table_to_clean,
              " WHERE ", x, " IS NULL OR ", x, " = '' OR ", x, " = 'NA'"
            )

            result <- tryCatch({
              affected <- DBI::dbExecute(db_loader$db, query)
              if (affected == 0) {
                message(glue::glue("No missing rows in {table_to_clean}.{x}"))
              } else {
                message(glue::glue("Deleted rows from {table_to_clean}.{x}"))
              }
            }, error = function(e) {
              warning(glue::glue(
                "Failed to clean {table_to_clean}.{x}: {e$message}"
              ))
            })
          })
        } else {
          print(glue::glue("Table {table_to_clean} does not exist"))
        }
      }
      print(glue::glue("Missing values removed."))
    }
  )
)
