test_that("Checking that number of rows match with original csvs", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  # Check if the number of rows in the database matches the original CSV files
  count_rows_origin <- get_rows_tables(db_con)
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  expect_equal(
    count_rows_origin[count_rows_origin$"name" %in% "VACCINES", "row_count"],
    nrow(vx1) + nrow(vx2)
  )
  expect_equal(unique(count_rows_origin$name), c("PERSONS", "VACCINES"))

  # Expect an error when calling the function
  dbname <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  expect_error(
    get_rows_tables(con),
    "No tables found in the database."
  )
})
