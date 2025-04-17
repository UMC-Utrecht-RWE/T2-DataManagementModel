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
#' @method run
#' @param db_loader An instance of the `DatabaseLoader` class.
#' This object provides the database connection (`db_con`), the list of tables
#' to process (`cdm_tables_names`), and the instance name (`instance_name`)
#' used for generating unique IDs.
#' @return None.
#'
#' @examples
#' # Example usage:
#' loader <- DatabaseLoader$new()
#' unique_id_generator <- UniqueIdGenerator$new()
#' unique_id_generator$run(loader)
#'
#' @importFrom T2.DMM create_unique_id
source("R/scripts/operations/DatabaseOperation.R")
UniqueIdGenerator <- R6::R6Class("UniqueIdGenerator", #nolint
  inherit = DatabaseOperation,
  public = list(
    run = function(db_loader) {
      T2.DMM::create_unique_id(
        db_loader$db,
        db_loader$config$cdm_tables_names,
        db_loader$config$instance_name
      )
      print("Unique IDs created.")
    }
  )
)