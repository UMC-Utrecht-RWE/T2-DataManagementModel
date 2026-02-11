#' UniqueIdGenerator Class
#'
#' @description
#' The `UniqueIdGenerator` class is a subclass of the `DatabaseOperation` class.
#' It is responsible for generating unique identifiers for the records in the
#' specified database tables.
#'
#' @details
#' This class uses the `create_unique_id` function from the `T2.DMM` package to
#' generate unique IDs for the tables specified in the `cdm_tables_names`
#' configuration of the `db_loader` object. The unique IDs are generated
#' based on the instance name provided in the `db_loader` object.
#'
#' @section Fields:
#' \describe{
#'   \item{`classname`}{
#'     A string representing the name of the class.
#'     Default is `"UniqueIdGenerator"`.
#'   }
#' }
#'
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Executes the unique ID generation process.
#'     \itemize{
#'       \item `db_loader`: A `DatabaseLoader` object that provides the
#'        database connection, the list of tables to process, and the
#'        instance name used for generating unique IDs.
#'     }
#'   }
#' }
#'
#' @examples
#' \dontrun{
#' loader <- DatabaseLoader$new()
#' unique_id_generator <- UniqueIdGenerator$new()
#' unique_id_generator$run(loader)
#' }
#'
#' @docType class
#' @keywords internal
UniqueIdGenerator <- R6::R6Class("UniqueIdGenerator", # nolint
  inherit = T2.DMM:::DatabaseOperation,
  public = list(
    #' @field classname A string representing the name of the class.
    classname = "UniqueIdGenerator",
    #' @description
    #' Executes the unique ID generation process.
    #' @param db_loader A `DatabaseLoader` object provides database connection.
    run = function(db_loader) {
      T2.DMM:::create_unique_id(
        db_connection = db_loader$db,
        cdm_tables_names = db_loader$config$cdm_tables_names,
        # UniqueIdGenerator unique properties
        extension_name = db_loader$config$unique_id_generator$instance_name,
        schema_name = db_loader$config$unique_id_generator$schema_name,
        to_view = db_loader$config$unique_id_generator$to_view,
        pipeline_extension =
          db_loader$config$unique_id_generator$pipeline_extension
      )
      message("Unique IDs created.")
    }
  )
)
