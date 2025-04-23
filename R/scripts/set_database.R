#' DatabaseLoader Class
#'
#' @description
#' The `DatabaseLoader` class manages the database connection,
#' loading configuration and metadata, and executing database operations in a
#' specified order.
#'
#' @details
#' This class provides methods to initialize a database connection, load the
#' database with required data, and execute a series of operations defined in
#' external R class scripts.
#' The operations are executed in the order specified in the configuration file.
#' The class also includes utility methods for cleaning up temporary files
#' and retrieving all operations from a specified folder.
#'
#' @field db_path A string representing the path to the database file.
#' @field config_path A string representing the path to the configuration file.
#' @field cdm_metadata A string representing the path to the metadata file.
#' @field config A list containing the configuration from `config_path` file.
#' @field metadata A `data.table` object containing the metadata loaded.
#' @field db A database connection object created using the `duckdb` package.
#'
#' @method initialize
#' @param db_path A string representing the path to the database file.
#' @param config_path String representing the path to configuration file JSON.
#' @param cdm_metadata A string representing the path to the metadata RDS file
#' @return None. Initializes the class and sets up the database connection.
#'
#' @method set_database
#' @description Loads the database with the required data using the
#' configuration and metadata provided during initialization.
#' @return None. Prints messages to the console during the process.
#'
#' @method run_db_ops
#' @param ops A list of operation objects to execute. If `NULL`, the operations
#' are retrieved using the `get_all_operations` method.
#' @description Executes a series of database operations in the order specified
#' in the configuration file. Each operation is an R6 class that inherits from
#' `DatabaseOperation`.
#' @return None. Prints messages to the console during the process.
#'
#' @examples
#' # Example usage:
#' loader <- DatabaseLoader$new(
#'   db_path = "path/to/database.db",
#'   config_path = "path/to/set_db.json",
#'   cdm_metadata = "path/to/CDM_metadata.rds"
#' )
#' loader$set_database()
#' loader$run_db_ops()
#'
#' @importFrom jsonlite fromJSON
#' @importFrom data.table as.data.table
#' @importFrom duckdb dbConnect dbDisconnect
#' @importFrom DBI dbDisconnect
#' @importFrom glue glue
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
        db_path = NULL,
        config_path = NULL,
        cdm_metadata = NULL) {
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
      # Loading files into the origin database
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
      DBI::dbDisconnect(self$db)
      gc()
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
