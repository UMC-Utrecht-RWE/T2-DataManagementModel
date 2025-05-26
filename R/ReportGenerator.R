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
      count_rows_origin <- T2.DMM:::get_rows_tables(db_loader$db)
      self$write_report(count_rows_origin, db_loader)

      count_rows_origin
    },

    #' @description
    #' Dispatches to the subclass to write the report based on extension.
    #' @param data A data frame containing the data to be saved.
    #' @param db_loader A `DatabaseLoader` object.
    write_report = function(data, db_loader) {
      subclass_instance <- private$find_saving_class(db_loader)
      message(
        glue::glue("Using subclass: {subclass_instance$classname}")
      )
      subclass_instance$write_report(data, db_loader)
    }
  ),

  private = list(
    find_saving_class = function(db_loader) {
      report_name <- db_loader$config$report_generator$report_name
      message(
        glue::glue("Generating reports for: {report_name}")
      )

      if (is.null(report_name) || !nzchar(report_name)) {
        stop("The report_name in report_generator is missing or empty.")
      }
      ext <- tools::file_ext(report_name)
      if (is.na(ext) || !nzchar(ext)) {
        stop(
          glue::glue(
            "Invalid or missing file extension in report_name: {report_name}"
          )
        )
      }

      pkg_env <- asNamespace("T2.DMM")
      objs <- mget(ls(pkg_env), envir = pkg_env)

      # Filter for R6 classes in the package environment
      r6_classes <- Filter(function(x) {
        inherits(x, "R6ClassGenerator")
      }, objs)
      # Print the names of R6 classes found in the package
      class_names <- sapply(r6_classes, function(cls) cls$classname)
      formatted_class_names <- paste(class_names, collapse = ", ")
      message(glue::glue(
        "R6 classes in T2.DMM package: {formatted_class_names}"
      ))

      # Load all R6 classes from the package environment
      subclasses <- Filter(function(cls) {
        inherits(cls$get_inherit(), "R6ClassGenerator") &&
          cls$get_inherit()$classname == "ReportGenerator"
      }, r6_classes)
      # Print the names of subclasses of ReportGenerator
      subclass_names <- sapply(subclasses, function(cls) cls$classname)
      formatted_subclass_names <- paste(subclass_names, collapse = ", ")
      message(glue::glue(
        "Subclasses of ReportGenerator: {formatted_subclass_names}"
      ))

      for (cls in subclasses) {
        instance <- NULL
        supported <- character(0)

        tryCatch({
          instance <- cls$new()
          if (is.function(instance$supported_ext)) {

            supported <- instance$supported_ext()
            message(glue::glue(
              "Extensions for {cls$classname}: {
              paste(supported, collapse = ', ')}"
            ))
            # Ensure result is a clean character vector
            if (!is.character(supported)) {
              supported <- character(0)
            }
          }
        }, error = function(e) {
          warning(glue::glue("Skipping class {cls$classname}: {e$message}"))
        })

        # Ensure supported does not contain NA values
        supported <- supported[!is.na(supported)]
        if (length(supported) > 0 && ext %in% supported) {
          message(glue::glue(
            "Found subclass {cls$classname} for extension: {report_name}"
          ))
          return(instance)
        }
      }

      stop(glue::glue("No ReportGenerator subclass found for extension: {ext}"))
    }
  )
)
