#' DatabaseOperation Class
#' @description
#' This is a subclass of the DatabaseOperation class.
#' It is responsible for removing duplicates from the database.
source("R/scripts/operations/DatabaseOperation.R")
DuplicateRemover <- R6::R6Class("DuplicateRemover", #nolint
  inherit = DatabaseOperation,
  public = list(
    run = function(db_loader) {
      scheme <- setNames(rep("*", length(db_loader$cdm_tables_names)),
                         db_loader$cdm_tables_names)
      delete_duplicates_origin(
        db_connection = db_loader$db_con,
        scheme = scheme,
        save_deleted = TRUE,
        save_path = "intermediate_data_file"
      )
      print("Duplicates removed from tables.")
    }
  )
)