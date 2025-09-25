#' DuplicateRemover Class
#'
#' @description
#' The `DuplicateRemover` class is a subclass of the `DatabaseOperation` class.
#' It is responsible for identifying and removing duplicate records from the
#' database tables specified in the `db_loader` object.
#'
#' @details
#' This class uses the `delete_duplicates_origin` function from the `T2.DMM`
#' package to remove duplicates. The duplicates are removed based on the
#' schema provided by the `db_loader` object. Optionally, the deleted records
#' can be saved to an intermediate file for auditing or debugging purposes.
#'
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Executes the duplicate removal process.
#'     \itemize{
#'       \item `db_loader`: A `DatabaseLoader` object provides db connection,
#'       table names, and configuration details required for the operation.
#'     }
#'   }
#' }
#'
#' @docType class
#' @keywords internal
DuplicateRemover <- R6::R6Class("DuplicateRemover", # nolint
  inherit = T2.DMM:::DatabaseOperation,
  public = list(
    #' @field classname A string representing the name of the class.
    #' Default is `"DuplicateRemover"`.
    classname = "DuplicateRemover",
    #' @description
    #' Executes the duplicate removal process.
    #' @param db_loader A `DatabaseLoader` object provides database connection,
    #' table names, and other configuration details required for the operation.
    run = function(db_loader) {
      message(glue::glue("Removing duplicates from tables."))

      # Retrieve distinct column info from config or default to '*'
      distinct_config <- db_loader$config$duplicate_remover$cdm_tables_columns
      table_names <- db_loader$config$cdm_tables_names

      scheme <- setNames(
        lapply(table_names, function(tbl) {
          if (!is.null(distinct_config[[tbl]])) {
            # Use the distinct columns defined in the config for that table
            distinct_config[[tbl]]
          } else {
            "*" # for all columns not defined
          }
        }),
        table_names
      )

      T2.DMM:::delete_duplicates_origin(
        db_connection = db_loader$db,
        scheme = scheme,
        # DuplicateRemover unique properties
        save_deleted = db_loader$config$duplicate_remover$save_deleted,
        save_path = db_loader$config$duplicate_remover$save_path,
        add_postfix = db_loader$config$duplicate_remover$add_postfix,
        schema_name = db_loader$config$duplicate_remover$schema_name,
        to_view = db_loader$config$duplicate_remover$to_view,
        pipeline_extension =
          db_loader$config$duplicate_remover$pipeline_extension
      )
      message(glue::glue("Duplicates removed."))
    }
  )
)
