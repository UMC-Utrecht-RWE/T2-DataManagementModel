#' DatabaseOperation Class
#'
#' @description
#' This class is responsible for managing database operations.
#' It provides methods to run database operations and check if they are enabled.
#'
#' @section Methods:
#' \describe{
#'   \item{`run(db_loader)`}{
#'     Abstract method. Should be implemented by subclass.
#'   }
#'   \item{`is_enabled()`}{
#'     Returns TRUE if the operation is enabled. Can be overridden.
#'   }
#' }
#'
#' @param db_loader A `DatabaseLoader` object or equivalent, used by `run()`.
#'
#' @keywords internal
#' @export
DatabaseOperation <- R6::R6Class("DatabaseOperation", #nolint
  public = list(
    run = function(db_loader) {
      stop("This method should be implemented in subclass")
    },
    is_enabled = function() {
      TRUE
    }
  )
)