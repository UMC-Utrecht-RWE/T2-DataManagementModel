source("R/scripts/operations/DatabaseOperation.R")
UniqueIdGenerator <- R6::R6Class("UniqueIdGenerator", #nolint
  inherit = DatabaseOperation,
  public = list(
    run = function(db_loader) {
      create_unique_id(db_loader$db_con,
                       db_loader$cdm_tables_names,
                       db_loader$instance_name)
      print("Unique IDs created.")
    }
  )
)