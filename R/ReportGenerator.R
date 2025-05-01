#' ReportGenerator Class
#'
#' @description
#' The `ReportGenerator` class is a subclass of the `DatabaseOperation` class.
#' It is responsible for generating reports based on the data in the database.
#'
#' @details
#' This class generates a report containing the row counts for all tables in the
#' database. The row counts are retrieved using the `get_rows_tables` function
#' from the `T2.DMM` package. The report is saved as a `.fst` file in the
#' directory specified in the `report` configuration of the `db_loader` object.
#' The database connection is closed after the report is generated.
#'
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Executes the report generation process.
#'     \itemize{
#'       \item `db_loader`: A `DatabaseLoader` object provides db connection,
#'       the instance name, and the configuration details for saving the report.
#'     }
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
#' report_generator$run(loader)
#' }
#'
#' @importFrom fst write_fst
#' @importFrom glue glue
#' @importFrom duckdb dbDisconnect
#' @docType class
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

      count_rows_origin <- T2.DMM::get_rows_tables(db_loader$db)
      dir_save_count_row <- file.path(
        db_loader$config$report$report_path,
        db_loader$config$report$report_name
      )
      fst::write_fst(count_rows_origin, dir_save_count_row)

      print("Reports generated.")
    }
  )
)
