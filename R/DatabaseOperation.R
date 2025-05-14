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
#'     \itemize{
#'       \item `db_loader`: A `DatabaseLoader` object to execute the operation.
#'     }
#'   }
#'   \item{`is_enabled()`}{
#'     Method to check if the operation is enabled.
#'     Returns `TRUE` by default but can be overridden.
#'   }
#' }
#'
#' @docType class
#' @keywords internal
DatabaseOperation <- R6::R6Class("DatabaseOperation", # nolint
  public = list(
    #' @description
    #' Abstract method to execute the database operation.
    #' Should be implemented by subclasses.
    #' @param db_loader A `DatabaseLoader` object.
    run = function(db_loader) {
      stop("This method should be implemented in subclass")
    },
    #' @description
    #' Method to check if the operation is enabled.
    #' Returns TRUE by default but can be overridden.
    #' @return Logical value indicating if the operation is enabled.
    is_enabled = function() {
      TRUE
    }
  )
)
