testthat::test_that("Returns correct row counts from loaded CSVs", {
  # Setup using your custom helper
  # suppressMessages handles the "Retrieving dbListTables..."
  # we do this to have a clean output
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con, shutdown = TRUE))

  # Execute function
  result <- get_rows_tables(db_con)

  # 1. Check structure
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_named(result, c("name", "row_count"))

  # 2. Check that the tables we requested in create_loaded_test_db are present
  testthat::expect_setequal(result$name, c("PERSONS", "VACCINES"))

  # 3. Validation against raw files
  # Assuming these files exist in your test directory as per your initial prompt
  vx1 <- import_file(testthat::test_path("dbtest", "VACCINES.csv"))
  vx2 <- import_file(testthat::test_path("dbtest", "VACCINES2.csv"))
  expected_vaccine_total <- nrow(vx1) + nrow(vx2)

  actual_vaccine_total <- result$row_count[result$name == "VACCINES"]
  testthat::expect_equal(actual_vaccine_total, expected_vaccine_total)
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

testthat::test_that("get_rows_tables reports query execution errors", {
  con <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  # This name is valid when quoted, but get_rows_tables() interpolates it
  # unquoted into SQL, which makes dbGetQuery() fail in the second tryCatch.
  DBI::dbExecute(con, 'CREATE TABLE "bad name" (id INTEGER)')
  DBI::dbExecute(con, 'INSERT INTO "bad name" VALUES (1)')

  testthat::expect_error(
    get_rows_tables(con),
    "Error executing query\\. Problematic query:"
  )
})
