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
      print(glue::glue(
        "Getting all operations from folder: {self$config$operations_path}"
      ))
      ops <- list()
      op_files <- list.files(
        self$config$operations_path,
        pattern = "\\.R$", full.names = TRUE
      )

      ordered_operations <- names(self$config$operations)

      for (operation in ordered_operations) {
        operation_file <- op_files[
          grepl(paste0("^", operation, "\\.R$"), basename(op_files))
        ]

        if (length(operation_file) == 1) {
          source(operation_file)
          class_obj <- get(operation, envir = .GlobalEnv)

          inherits_from_dbop <- !is.null(class_obj$inherit) &&
            class_obj$inherit == "T2.DMM:::DatabaseOperation"
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
