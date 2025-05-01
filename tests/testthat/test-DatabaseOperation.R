unlink("temp", recursive = TRUE) ## Create a temporary folder where to test

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

# test-database-operation.R
testthat::test_that("DatabaseOperation abstract methods behave as expected", {
  # Instantiate the abstract class
  db_op <- T2.DMM:::DatabaseOperation$new()

  testthat::expect_error(
    db_op$run(NULL),
    "This method should be implemented in subclass"
  )

  testthat::expect_true(db_op$is_enabled())
})

testthat::test_that("Subclass of DatabaseOperation can override run()", {
  # Define dummy subclass inline
  DummyOp <- R6::R6Class("DummyOp", # nolint
    inherit = T2.DMM:::DatabaseOperation,
    public = list(
      run = function(db_loader) {
        "success"
      }
    )
  )

  dummy <- DummyOp$new()

  testthat::expect_equal(dummy$run(NULL), "success")
  testthat::expect_true(dummy$is_enabled())
  testthat::expect_s3_class(dummy, "DummyOp")
})

## We conclude by exiting the file
setwd("../")
unlink("temp", recursive = TRUE)
