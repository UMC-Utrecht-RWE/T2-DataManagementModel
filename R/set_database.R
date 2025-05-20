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
#' @field data_instance A string representing the path to the data instance.
#' @field config_path A string representing the path to the configuration file.
#' @field cdm_metadata A string representing the path to the metadata file.
#' @field config A list containing the configuration from `config_path` file.
#' @field metadata A `data.table` object containing the metadata loaded.
#' @field db A database connection object created using the `duckdb` package.
#'
#' @section Methods:
#' \describe{
#'   \item{`initialize(db_path, config_path, cdm_metadata)`}{
#'     Initializes the class with the provided parameters.
#'     \itemize{
#'       \item `db_path`: A string representing the path to the database file.
#'       \item `config_path`: A string representing the path to configuration.
#'       \item `cdm_metadata`: A string representing the path to the metadata.
#'     }
#'   }
#'   \item{`set_database()`}{
#'     Loads the database with the required data.
#'   }
#'   \item{`run_db_ops(ops)`}{
#'     Executes a series of database operations in the order of config file.
#'     \itemize{
#'       \item `ops`: A list of operation objects to execute. If `NULL, they
#'       are retrieved using the `get_all_operations` method.
#'     }
#'   }
#' }
#'
#' @examples
#' \dontrun{
#' loader <- DatabaseLoader$new(
#'   db_path = "path/to/database.db",
#'   config_path = "path/to/set_db.json",
#'   cdm_metadata = "path/to/CDM_metadata.rds"
#' )
#' loader$set_database()
#' loader$run_db_ops()
#' }
#'
#' @importFrom jsonlite fromJSON
#' @importFrom data.table as.data.table
#' @importFrom duckdb dbConnect dbDisconnect
#' @importFrom DBI dbDisconnect
#' @importFrom glue glue
#' @docType class
#' @export
DatabaseLoader <- R6::R6Class("DatabaseLoader", # nolint
  public = list(
    # Public attributes
    db_path = NULL,
    data_instance = NULL,
    config_path = NULL,
    cdm_metadata = NULL,

    # Internal attributes
    config = NULL,
    metadata = NULL,
    db = NULL,

    #' @description
    #' Initializes the class with the provided parameters.
    #' @param db_path A string representing the path to the database file.
    #' @param data_instance A string representing the path to the data instance.
    #' @param config_path A string representing the path to configuration file.
    #' @param cdm_metadata A string representing the path to the metadata file.
    initialize = function(db_path = NULL,
                          data_instance = NULL,
                          config_path = NULL,
                          cdm_metadata = NULL) {
      self$db_path <- db_path
      self$data_instance <- data_instance
      self$config <- jsonlite::fromJSON(config_path)
      self$metadata <- data.table::as.data.table(
        base::readRDS(cdm_metadata)
      )
      tryCatch(
        {
          self$db <- duckdb::dbConnect(duckdb::duckdb(), self$db_path)
        },
        error = function(e) {
          print(paste("Error connecting to database:", e))
        }
      )
    },
    #' @description
    #' Loads the database with the required data using config and metadata.
    set_database = function() {
      print("Setting up the database")
      tryCatch(
        {
          T2.DMM::load_db(
            db_connection = self$db,
            data_instance_path = self$data_instance,
            cdm_metadata = self$metadata,
            cdm_tables_names = self$config$cdm_tables_names,
            extension_name = self$config$extension_name
          )
        },
        error = function(e) {
          print(paste("Error loading database:", e))
        }
      )
    },
    #' @description
    #' Executes a series of database operations in order specified in config.
    #' @param ops A list of operation objects to execute. If `NULL`, operations
    #' are retrieved using the `get_all_operations` method.
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
      ordered_operations <- names(self$config$operations)
      ops <- list() # Initialize an empty list to store operation objects

      for (operation in ordered_operations) {
        # Check if the operation is enabled in the JSON config
        is_enabled <- isTRUE(self$config$operations[[operation]])
        if (!is_enabled) {
          next # Skip this operation if it's not enabled
        }

        # Attempt to retrieve the class object from the T2.DMM package
        class_obj <- tryCatch(
          get(operation, envir = asNamespace("T2.DMM")),
          error = function(e) NULL
        )

        # Check if the class object inherits from "T2.DMM:::DatabaseOperation"
        inherits_from_dbop <- !is.null(class_obj) &&
          !is.null(class_obj$inherit) &&
          class_obj$inherit == "T2.DMM:::DatabaseOperation"

        if (inherits_from_dbop) {
          print(glue::glue("Loading operation: {operation}"))
          ops[[length(ops) + 1]] <- class_obj$new()
        } else {
          print(glue::glue("{operation} not a valid DatabaseOperation class."))
        }
      }

      ops
    }
  )
)


# loader <- T2.DMM::DatabaseLoader$new(
#   db_path = "/Users/mcinelli/repos/RSV-1026/somewhere/d2.duckdb",
#   data_instance = NULL,
#   config_path = "configuration/set_db.json",
#   cdm_metadata = "/Users/mcinelli/repos/RSV-1026/somewhere/CDM_metadata.rds"
# )

# loader$run_db_ops()
