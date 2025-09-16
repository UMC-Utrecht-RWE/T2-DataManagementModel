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
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Executes the missing value removal process.
#'     \itemize{
#'       \item `db_loader`: A `DatabaseLoader` object provides db connection,
#'       the list of tables and columns to clean, and other configuration
#'     }
#'   }
#' }
#'
#' @field classname A string representing the name of the class.
#' Default is `"MissingRemover"`.
#'
#' @examples
#' \dontrun{
#' loader <- DatabaseLoader$new()
#' missing_remover <- MissingRemover$new()
#' missing_remover$run(loader)
#' }
#'
#' @importFrom DBI dbListTables dbSendStatement
#' @importFrom duckdb dbGetRowsAffected dbClearResult
#' @importFrom glue glue
#' @docType class
#' @keywords internal
MissingRemover <- R6::R6Class("MissingRemover", # nolint
  inherit = T2.DMM:::DatabaseOperation,
  public = list(
    classname = "MissingRemover",
    #' @description
    #' Executes the missing value removal process.
    #' @param db_loader A `DatabaseLoader` object provides database connection,
    #' the list of tables and columns to clean, and other configuration details.
    run = function(db_loader) {
      message(glue::glue("Removing missing values from tables."))
      cols_to_remove_miss <- db_loader$config$missing_remover$columns
      to_view_bool <- db_loader$config$missing_remover$to_view
      pipeline_extension_name <- db_loader$config$missing_remover$pipeline_extension
      viex_extension_name <- db_loader$config$missing_remover$view_extension
      clean_missing_values(con = db_loader$db, 
                                    list_columns_clean = cols_to_remove_miss, 
                                    to_view = to_view_bool,
                                     pipeline_extension = pipeline_extension_name, 
                                     view_extension = viex_extension_name)
      message(glue::glue("Missing values removed."))
    }
  )
)
