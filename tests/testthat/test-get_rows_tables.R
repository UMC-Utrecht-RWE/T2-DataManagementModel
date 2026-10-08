testthat::test_that("Returns row counts for DuckDB tables in schemas", {
  db_con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  withr::defer(DBI::dbDisconnect(db_con, shutdown = TRUE))
  DBI::dbExecute(db_con, "CREATE SCHEMA CDM")
  DBI::dbExecute(db_con, "CREATE TABLE CDM.PERSONS (id INTEGER)")
  DBI::dbExecute(db_con, "INSERT INTO CDM.PERSONS VALUES (1), (2)")
  DBI::dbExecute(db_con, "CREATE TABLE CDM.VACCINES (id INTEGER)")
  DBI::dbExecute(db_con, "INSERT INTO CDM.VACCINES VALUES (1), (2), (3)")

  result <- suppressMessages(get_rows_tables(db_con))

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_named(result, c("name", "row_count"))
  testthat::expect_setequal(result$name, c("PERSONS", "VACCINES"))
  testthat::expect_equal(
    result$row_count[match(c("PERSONS", "VACCINES"), result$name)],
    c(2, 3)
  )
})

testthat::test_that("get_rows_tables handles database with no tables", {
  # We create a fresh connection without calling the loader helper
  empty_con <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))
  withr::defer(DBI::dbDisconnect(empty_con, shutdown = TRUE))

  testthat::expect_error(
    get_rows_tables(empty_con),
    "No tables found in the database"
  )
})

testthat::test_that("get_rows_tables catches connection errors", {
  # Create and immediately close a connection to trigger the first tryCatch
  con <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  testthat::expect_error(
    get_rows_tables(con),
    "Error retrieving table names from the database. "
  )
})

testthat::test_that("get_rows_tables handles table names requiring quotes", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "CREATE SCHEMA CDM")
  DBI::dbExecute(con, 'CREATE TABLE CDM."bad name" (id INTEGER)')
  DBI::dbExecute(con, 'INSERT INTO CDM."bad name" VALUES (1)')

  result <- suppressMessages(get_rows_tables(con))
  testthat::expect_equal(result$name, "bad name")
  testthat::expect_equal(result$row_count, 1)
})
