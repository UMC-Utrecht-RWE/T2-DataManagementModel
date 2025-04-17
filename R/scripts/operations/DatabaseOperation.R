#' DatabaseOperation Class
#' @description
#' This class is responsible for managing database operations.
#' It provides methods to run database operations and check if they are enabled.
#' It is an abstract class and should be extended by other classes.
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