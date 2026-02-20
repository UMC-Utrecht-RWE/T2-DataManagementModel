#' CSVReportGenerator Class
#'
#' @description
#' The `CSVReportGenerator` class is a specialized subclass of `ReportGenerator`
#' designed to generate and save tabular reports in the `.csv` file format.
#' It uses the `csv::write_csv()` function to persist data efficiently.
#'
#' This class is automatically selected when the report extension is `.csv`,
#' as determined by the `ReportGenerator`'s internal logic. The generated report
#' is saved in the directory and with the filename specified in the
#' `report` configuration section of the provided `DatabaseLoader` object.
#'
#' @details
#' The main method `write_report()` takes the output of a table row count query
#' and writes it to a `.csv` file. The class also exposes a `supported_ext()`
#' method, which declares the supported file extension(s) for dynamic dispatch.
#'
#' @section Methods:
#' \describe{
#'   \item{`write_report(data, db_loader)`}{
#'     Writes the provided `data` to a `.csv`.
#'   }
#'   \item{`supported_ext()`}{
#'     Returns a character vector indicating file extensions (here, `"csv"`).
#'   }
#' }
#'
#' @field classname A string identifying the class name.
#' Default is `"CSVReportGenerator"`.
#'
#' @importFrom readr write_csv
#' @import R6
#' @keywords internal
CSVReportGenerator <- R6::R6Class("CSVReportGenerator", # nolint
  inherit = T2.DMM:::ReportGenerator,
  public = list(
    classname = "CSVReportGenerator",

    #' @description
    #' Writes the provided data to a `.csv` file.
    #' @param data A data frame containing the data to be saved.
    #' @param db_loader A `DatabaseLoader` object that provides the
    #' configuration details for saving the report.
    write_report = function(data, db_loader) {
      report_path <- db_loader$config$report_generator$report_path
      dir_save <- file.path(
        report_path,
        db_loader$config$report_generator$report_name
      )
      message(glue::glue("Saving report to {report_path}"))
      if (!dir.exists(dirname(dir_save))) {
        stop(glue::glue("The directory {dirname(dir_save)} does not exist."))
      }
      readr::write_csv(data, dir_save)
      message(glue::glue("Report saved successfully."))
    },

    #' @description
    #' Returns a character vector indicating file extensions (here, `"csv"`).
    supported_ext = function() {
      c("csv")
    }
  )
)
