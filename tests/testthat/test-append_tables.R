
testthat::test_that("append_tables handles missing tables correctly", {
  # Create a temporary SQLite database
  db <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))

  # Create a table in the database
  DBI::dbWriteTable(db, "table1", data.frame(id = 1:5, date = Sys.Date() + 1:5))

  # Test with a missing table
  testthat::expect_error(
    append_tables(db, c("table1", "missing_table"), "result_table"),
    "missing_table not in database"
  )

  DBI::dbDisconnect(db)
})

testthat::test_that("append_tables appends tables correctly", {
  # Create a temporary SQLite database
  db <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))

  # Create tables in the database
  DBI::dbWriteTable(db, "table1", data.frame(id = 1:5, date = Sys.Date() + 1:5))
  DBI::dbWriteTable(
    db, "table2", data.frame(id = 6:10, date = Sys.Date() + 6:10)
  )

  # Append tables
  result <- append_tables(
    db, c("table1", "table2"), "result_table", return = TRUE
  )

  # Check the result
  testthat::expect_equal(nrow(result), 10)
  testthat::expect_true(all(c("id", "date") %in% colnames(result)))

  DBI::dbDisconnect(db)
})

testthat::test_that("append_tables raise message if appends empthy tables", {
  # Create a temporary SQLite database
  db <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))

  # Create tables in the database
  DBI::dbExecute(db, "
    CREATE TABLE table1 (
      id INTEGER,
      name TEXT,
      date DATE,
      created_at TIMESTAMP
    )
  ")

  # Append tables
  testthat::expect_message(
    append_tables(
      db, "table1", "result_table", return = TRUE
    ),
    "0 cases in table1 for the Appended TABLE result_table"
  )

  DBI::dbDisconnect(db)
})

testthat::test_that("append_tables creates temporary tables correctly", {
  # Create a temporary SQLite database
  db <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))

  # Create a table in the database
  DBI::dbWriteTable(db, "table1", data.frame(id = 1:5, date = Sys.Date() + 1:5))

  # Create a temporary table
  append_tables(db, "table1", "temp_table", sqlite_temp = TRUE, return = FALSE)

  # Check if the temporary table exists
  tables <- DBI::dbListTables(db)
  testthat::expect_true("temp_table" %in% tables)

  DBI::dbDisconnect(db)
})


testthat::test_that("append_tables returns correct data when return = TRUE", {
  # Create a temporary SQLite database
  db <- DBI::dbConnect(duckdb::duckdb(), tempfile(fileext = ".duckdb"))

  # Create a table in the database
  DBI::dbWriteTable(db, "table1", data.frame(id = 1:5, date = Sys.Date() + 1:5))

  # Append the table and return the result
  result <- append_tables(db, "table1", "result_table", return = TRUE)

  # Check the returned data
  testthat::expect_equal(nrow(result), 5)
  testthat::expect_true(all(c("id", "date") %in% colnames(result)))

  DBI::dbDisconnect(db)
})
