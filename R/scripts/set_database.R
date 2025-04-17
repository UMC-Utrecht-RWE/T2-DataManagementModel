DatabaseLoader <- R6::R6Class("DatabaseLoader", #nolint
  public = list(
    # Public attributes
    db_path = NULL,
    config_path = NULL,
    cdm_metadata = NULL,
    # interanl attributes
    config = NULL,
    metadata = NULL,
    db = NULL,

    initialize = function(
        db_path = "somewhere/d2.db",
        config_path = "configuration/set_db.json",
        cdm_metadata = "somewhere/CDM_metadata.rds") {
      # Initialize the class with the provided parameters
      self$db_path <- db_path
      self$config <- jsonlite::fromJSON(config_path)
      self$metadata <- data.table::as.data.table(
        base::readRDS(cdm_metadata)
      )
      tryCatch({
        self$db <-  duckdb::dbConnect(duckdb::duckdb(), self$db_path)
      },
      error = function(e) {
        print(paste("Error connecting to database:", e))
      })
    },

    set_database = function() {
      print("Setting up the database")
      tryCatch({
        T2.DMM::load_db(
          db_connection = self$db,
          csv_path_dir = self$db_path,
          cdm_metadata = self$metadata,
          cdm_tables_names = self$config$cdm_tables_names
        )
      },
      error = function(e) {
        print(paste("Error loading database:", e))
      })
    },

    run_db_ops = function(ops = NULL) {
      if (is.null(ops)) {
        ops <- private$get_all_operations()
      }
      for (op in ops) {
        print(glue::glue("Running class: {op$classname}"))
        op$run(self)
      }
      # DBI::dbDisconnect(self$db)
    }
  ),

  private = list(
    clean_files = function(dir) {
      files_to_remove <- Sys.glob(dir)
      if (length(files_to_remove) > 0) {
        print(paste("Removing files in:", dir))
        unlink(files_to_remove, recursive = TRUE)
      } else {
        print(paste("No files to remove in:", dir))
      }
    },

    get_all_operations = function() {
      print(glue::glue(
        "Getting all operations from folder: {self$config$operations_path}"
      ))
      ops <- list()
      op_files <- list.files(
        self$config$operations_path, pattern = "\\.R$", full.names = TRUE
      )

      # Ensure operations are loaded in the order specified in the configuration
      ordered_operations <- names(self$config$operations)

      for (operation in ordered_operations) {
        operation_file <- op_files[
          grepl(paste0("^", operation, "\\.R$"), basename(op_files))
        ]
        if (length(operation_file) == 1) {
          source(operation_file)
          class_obj <- get(operation, envir = .GlobalEnv)

          # Check if class inherits from DatabaseOperation and is enabled
          # in the configuration file.
          inherits_from_dbop <- !is.null(class_obj$inherit) &&
            class_obj$inherit == "DatabaseOperation"
          is_enabled <- isTRUE(self$config$operations[[class_obj$classname]])

          if (inherits_from_dbop && is_enabled) {
            print(glue::glue(("Loading operation: {class_obj$classname}")))
            ops[[length(ops) + 1]] <- class_obj$new()
          } else {
            print(glue::glue("Skipped operation: {class_obj$classname}"))
          }
        } else {
          print(
            glue::glue("Operation file not found or ambiguous for: {operation}")
          )
        }
      }
      ops
    }
  )
)



loader <- DatabaseLoader$new()
loader$set_database()
# print(loader$db)
loader$run_db_ops()