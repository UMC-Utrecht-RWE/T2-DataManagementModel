# Test file for the ensure_data_table function
testthat::test_that("ensure_data_table converts a data.frame to data.table", {
  df <- data.frame(a = 1:3, b = letters[1:3])
  dt <- ensure_data_table(df)

  testthat::expect_true(data.table::is.data.table(dt))
  testthat::expect_equal(nrow(dt), 3)
  testthat::expect_equal(ncol(dt), 2)
})

testthat::test_that("ensure_data_table does not modify existing data.table", {
  dt <- data.table::data.table(a = 1:3, b = letters[1:3])
  result <- ensure_data_table(dt)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_identical(result, dt)
})

testthat::test_that("ensure_data_table throws an error for NULL input", {
  testthat::expect_error(
    ensure_data_table(NULL),
    "Input is NULL."
  )
})

testthat::test_that("ensure_data_table handles error", {
  model <- lm(mpg ~ cyl + hp, data = mtcars)

  testthat::expect_error(
    ensure_data_table(model)
  )
})

testthat::test_that("ensure_data_table handles custom error messages", {
  model <- lm(mpg ~ cyl + hp, data = mtcars)

  testthat::expect_error(
    ensure_data_table(model, error_message = "Custom error message"),
    "Custom error message"
  )
})
