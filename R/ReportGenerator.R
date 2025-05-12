#' ReportGenerator Class
#'
#' @description
#' The `ReportGenerator` class is an abstract base class that provides the
#' general workflow for generating reports from a database. It is intended to
#' be subclassed by specific report writers (e.g., `FSTReportGenerator`,
#' `CSVReportGenerator`) that implement logic for saving data in
#' a particular format.
#'
#' @details
#' This class retrieves table row counts using the `get_rows_tables()` function
#' from the `T2.DMM` package. It then delegates the saving of this data to a
#' dynamically selected subclass, based on the output file extension
#' (e.g., `.fst`, `.csv`) specified in the `report_name` field of
#' the `db_loader` configuration.
#'
#' The dynamic subclass selection is handled by a private method,
#' `find_saving_class()`, which scans all `.R` files in the `R/` directory,
#' sources them, and checks for subclasses that declare support for
#' the target file extension.
#'
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Executes the report generation process, including dynamic format
#'    detection and saving via subclass.
#'   }
#'   \item{`write_report(data, db_loader)`}{
#'     Abstract method to be implemented by subclasses; throws
#'     an error by default.
#'   }
#' }
#'
#' @field classname A string representing the name of the class.
#' Default is `"ReportGenerator"`.
#'
#' @examples
#' \dontrun{
#' loader <- DatabaseLoader$new()
#' report_generator <- ReportGenerator$new()
#' report_generator$run(loader) # Delegates to the correct subclass
#' }
#'
#' @importFrom glue glue
#' @importFrom tools file_ext
#' @keywords internal
ReportGenerator <- R6::R6Class("ReportGenerator", # nolint
  inherit = T2.DMM:::DatabaseOperation,
  public = list(
    classname = "ReportGenerator",
    #' @description
    #' Executes the report generation process.
    #' @param db_loader A `DatabaseLoader` object provides database connection,
    #' the instance name, and the configuration details for saving the report.
    run = function(db_loader) {
      print(
        glue::glue("Generating reports for: {db_loader$config$instance_name}")
      )

      count_rows_origin <- T2.DMM:::get_rows_tables(db_loader$db)
      self$write_report(count_rows_origin, db_loader)

      print("Reports generated.")
    },

    #' @description
    #' Placeholder for report writing logic. Should be overridden in subclass.
    #' @param data A data frame containing the data to be saved.
    #' @param db_loader A `DatabaseLoader` object that provides the
    #' configuration details for saving the report.
    write_report = function(data, db_loader) {
      stop("write_report() must be implemented in a subclass.")
    }
  ),
  private = list(
    find_saving_class = function(data, db_loader) {
      report_name <- db_loader$config$report$report_name
      ext <- tools::file_ext(report_name)
      r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE)

      for (file in r_files) {
        source(file, local = TRUE) # Avoid global namespace pollution
      }

      # Get all R6 classes currently in the environment
      classes <- mget(ls(), envir = .GlobalEnv, ifnotfound = NA)
      r6_classes <- Filter(
        function(x) {
          inherits(x, "R6ClassGenerator") &&
            "ReportGenerator" %in% names(x$inherit)
        },
        classes
      )

      for (class_obj in r6_classes) {
        instance <- class_obj$new()
        if (!is.null(instance$supported_ext) &&
          ext %in% instance$supported_ext()) {
          return(instance$write_report(data, db_loader))
        }
      }

      stop(glue::glue("No ReportGenerator subclass found for extension: {ext}"))
    }
  )
)
