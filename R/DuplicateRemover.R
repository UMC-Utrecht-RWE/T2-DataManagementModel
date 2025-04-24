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
#' @field classname A string representing the name of the class.
#' Default is `"DuplicateRemover"`.
#'
#' @method run
#' @param db_loader An instance of the `DatabaseLoader` class. This object
#' provides the database connection (`db`), table names (`cdm_tables_names`),
#' and other configuration details required for the operation.
#' @return None.
#'
#' @examples
#' # Example usage:
#' loader <- DatabaseLoader$new()
#' duplicate_remover <- DuplicateRemover$new()
#' duplicate_remover$run(loader)
#'
#' @importFrom T2.DMM delete_duplicates_origin
#' @keywords internal
#' @export
DuplicateRemover <- R6::R6Class("DuplicateRemover", #nolint
  inherit = T2.DMM::DatabaseOperation,
  public = list(
    classname = "DuplicateRemover",
    run = function(db_loader) {
      print(glue::glue("Removing duplicates from tables."))
      scheme <- setNames(rep("*", length(db_loader$config$cdm_tables_names)),
                         db_loader$config$cdm_tables_names)
      T2.DMM::delete_duplicates_origin(
        db_connection = db_loader$db,
        scheme = scheme,
        save_deleted = TRUE,
        save_path = "intermediate_data_file" #TODO add in config file
      )
      print(glue::glue("Duplicates removed."))
    }
  )
)