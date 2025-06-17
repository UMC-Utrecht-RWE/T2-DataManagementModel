#' FSTReportGenerator Class
#'
#' @description
#' The `FSTReportGenerator` class is a specialized subclass of `ReportGenerator`
#' designed to generate and save tabular reports in the `.fst` file format.
#' It uses the `fst::write_fst()` function to persist data efficiently.
#'
#' This class is automatically selected when the report extension is `.fst`,
#' as determined by the `ReportGenerator`'s internal logic. The generated report
#' is saved in the directory and with the filename specified in the
#' `report` configuration section of the provided `DatabaseLoader` object.
#'
#' @details
#' The main method `write_report()` takes the output of a table row count query
#' and writes it to a `.fst` file. The class also exposes a `supported_ext()`
#' method, which declares the supported file extension(s) for dynamic dispatch.
#'
#' @section Methods:
#' \describe{
#'   \item{`write_report(data, db_loader)`}{
#'     Writes the provided `data` to a `.fst`.
#'   }
#'   \item{`supported_ext()`}{
#'     Returns a character vector indicating file extensions (here, `"fst"`).
#'   }
#' }
#'
#' @field classname A string identifying the class name.
#' Default is `"FSTReportGenerator"`.
#'
#' @importFrom fst write_fst
#' @keywords internal
FSTReportGenerator <- R6::R6Class("FSTReportGenerator", # nolint
  inherit = T2.DMM:::ReportGenerator,
  public = list(
    classname = "FSTReportGenerator",

    #' @description
    #' Writes the provided data to a `fst` file.
    #' @param data A data frame containing the data to be saved.
    #' @param db_loader A `DatabaseLoader` object that provides the
    #' configuration details for saving the report.
    write_report = function(data, db_loader) {
      dir_save <- file.path(
        db_loader$config$report_generator$report_path,
        db_loader$config$report_generator$report_name
      )
      if (!dir.exists(dirname(dir_save))) {
        stop(glue::glue("The directory {dirname(dir_save)} does not exist."))
      }
      fst::write_fst(data, dir_save)
      message(glue::glue("Report saved successfully."))
    },

    #' @description
    #' Returns a character vector indicating file extensions (here, `"fst"`).
    supported_ext = function() {
      c("fst")
    }
  )
)
