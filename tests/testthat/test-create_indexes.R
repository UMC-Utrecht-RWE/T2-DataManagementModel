# tests for filepath: R/test_create_indexes.R

testthat::test_that("create_indexes creates single-column indexes correctly", {
  # Setup: Create an in-memory database and a test table
  db_conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(db_conn, "CREATE TABLE persons (person_id INTEGER, name TEXT)")

  # Define specifications for single-column indexes
  specs <- list(
    persons = list(
      "1" = "person_id"
    )
  )

  # Call the function
  create_indexes(db_conn, specs)

  # Verify the index exists
  indexes <- DBI::dbGetQuery(
    db_conn, "SELECT * FROM sqlite_master WHERE type = 'index'"
  )
  testthat::expect_true(any(grepl("persons_index_1", indexes$name)))

  # Teardown
  DBI::dbDisconnect(db_conn)
})

testthat::test_that("create_indexes creates multi-column indexes correctly", {
  # Setup: Create an in-memory database and a test table
  db_conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(
    db_conn, "CREATE TABLE medicines (person_id INTEGER, date_dispensing DATE)"
  )

  # Define specifications for multi-column indexes
  specs <- list(
    medicines = list(
      "1" = c("person_id", "date_dispensing")
    )
  )

  # Call the function
  create_indexes(db_conn, specs)

  # Verify the index exists
  indexes <- DBI::dbGetQuery(
    db_conn, "SELECT * FROM sqlite_master WHERE type = 'index'"
  )
  testthat::expect_true(any(grepl("medicines_index_1", indexes$name)))

  # Teardown
  DBI::dbDisconnect(db_conn)
})


testthat::test_that("create_indexes handles empty specifications", {
  # Setup: Create an in-memory database
  db_conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Define empty specifications
  specs <- list()

  # Call the function
  create_indexes(db_conn, specs)

  # Verify no indexes are created
  indexes <- DBI::dbGetQuery(
    db_conn, "SELECT * FROM sqlite_master WHERE type = 'index'"
  )
  testthat::expect_equal(nrow(indexes), 0)

  # Teardown
  DBI::dbDisconnect(db_conn)
})

testthat::test_that("create_indexes handles invalid table names.", {
  # Setup: Create an in-memory database
  db_conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Define specifications with an invalid table name
  specs <- list(
    non_existent_table = list(
      "1" = "non_existent_column"
    )
  )

  # Call the function and expect an error
  testthat::expect_error(create_indexes(db_conn, specs))

  # Teardown
  DBI::dbDisconnect(db_conn)
})