# Test suite for get_origin_value function
test_that("get_origin_value returns correct values", {
  # Create a temporary DuckDB connection for testing
  db_connection <- DBI::dbConnect(duckdb::duckdb())

  # Setup test tables
  DBI::dbExecute(
    db_connection,
    paste0(
      "CREATE TABLE cdm_table1 (ori_table TEXT, ROWID ",
      "INTEGER, Column1 TEXT, Column2 TEXT)"
    )
  )
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE cdm_table2 (ori_table TEXT, ROWID INTEGER, Column2 TEXT)"
  )

  # Insert test data
  DBI::dbExecute(
    db_connection,
    paste0(
      "INSERT INTO cdm_table1 VALUES ('cdm_table1',1, 'Value1','Value3'), ",
      "('cdm_table1',2, 'Value2','Value4')"
    )
  )
  DBI::dbExecute(
    db_connection,
    paste0(
      "INSERT INTO cdm_table2 VALUES ('cdm_table2',1, 100), ",
      "('cdm_table2',2, 200), ('cdm_table2',3, 300)"
    )
  )

  # Create test cases data table
  cases_dt <- data.table::data.table(
    ROWID = c(1, 2, 3),
    ori_table = c("cdm_table1", "cdm_table1", "cdm_table2")
  )

  # Test with valid columns parameter
  search_scheme <- list(
    "cdm_table1" = "Column1", "cdm_table1" = "Column2", "cdm_table2" = "Column2"
  )
  result <- get_origin_value(
    cases_dt, db_connection,
    search_scheme = search_scheme
  )

  # Check result structure
  expect_s3_class(result, "data.table")
  expect_equal(ncol(result), 4)
  expect_equal(names(result), c("ori_table", "ROWID", "column_origin", "value"))

  # Check result content
  expect_equal(nrow(result), 5)

  # Find records for Table1
  table1_records <- result[ori_table == "cdm_table1"]
  expect_equal(nrow(table1_records), 4)
  expect_equal(
    table1_records[ROWID == 1 & column_origin == "Column1", value], "Value1"
  )
  expect_equal(
    table1_records[ROWID == 2 & column_origin == "Column1", value], "Value2"
  )
  expect_equal(
    table1_records[ROWID == 1 & column_origin == "Column2", value], "Value3"
  )
  expect_equal(
    table1_records[ROWID == 2 & column_origin == "Column2", value], "Value4"
  )

  # Find records for Table2
  table2_records <- result[ori_table == "cdm_table2"]
  expect_equal(nrow(table2_records), 1)
  expect_equal(
    table2_records[ROWID == 3 & column_origin == "Column2", value], "300"
  )

  # Clean up
  DBI::dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_origin_value handles multiple tables correctly", {
  # Create a temporary DuckDB connection for testing
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())

  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE TableA (ori_table TEXT, ROWID INTEGER, ColumnA TEXT)"
  )
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE TableB (ori_table TEXT, ROWID INTEGER, ColumnB INTEGER)"
  )
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE TableC (ori_table TEXT,ROWID INTEGER, ColumnC BOOLEAN)"
  )

  # Insert test data
  DBI::dbExecute(
    db_connection,
    paste0(
      "INSERT INTO TableA VALUES ('TableA',1, 'A1'), ",
      "('TableA',2, 'A2'), ('TableA',3, 'A3')"
    )
  )
  DBI::dbExecute(
    db_connection,
    paste0(
      "INSERT INTO TableB VALUES ('TableB',1, 10), ",
      "('TableB',2, 20), ('TableB',3, 30)"
    )
  )
  DBI::dbExecute(
    db_connection,
    paste0(
      "INSERT INTO TableC VALUES ('TableC',1, TRUE), ",
      "('TableC',2, FALSE), ('TableC',3, TRUE)"
    )
  )

  # Create test cases data table with multiple tables
  cases_dt <- data.table::data.table(
    ROWID = c(1, 2, 3, 2, 3),
    ori_table = c("TableA", "TableA", "TableB", "TableB", "TableC")
  )

  # Test with columns for all tables
  search_scheme <- list(
    "TableA" = "ColumnA", "TableB" = "ColumnB", "TableC" = "ColumnC"
  )
  result <- get_origin_value(
    cases_dt, db_connection,
    search_scheme = search_scheme
  )

  # Check overall result
  expect_equal(nrow(result), 5)

  # Check if results contain correct values for each table
  expect_equal(sort(result[ori_table == "TableA", value]), c("A1", "A2"))
  expect_equal(sort(result[ori_table == "TableB", value]), c("20", "30"))
  expect_equal(result[ori_table == "TableC", value], "TRUE")

  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
  gc()
})


test_that("get_origin_value throws error with NULL columns", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb())

  # Create a simple test cases data table
  cases_dt <- data.table(ROWID = 1, ori_table = "Table1")

  # Test with NULL columns
  expect_error(
    get_origin_value(cases_dt, db_connection, search_scheme = NULL),
    "\\[get_origin_value\\] search_scheme need to be defined"
  )

  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_origin_value handles empty cases data table", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb())

  # Create empty test cases data table
  cases_dt <- data.table::data.table(
    ROWID = integer(0), ori_table = character(0)
  )

  # Setup a test table
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE EmptyTest (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )

  # Test with valid columns parameter but empty cases
  columns <- list("EmptyTest" = "Column1")

  result <- suppressWarnings(
    get_origin_value(cases_dt, db_connection, search_scheme = columns)
  )
  # Check the result is an empty data table with correct structure
  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 0)

  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_origin_value handles invented column names not in columns list", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())

  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )

  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )

  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )

  # Only include Table1 in columns
  search_scheme <- list("Table1" = "Column1", "Table1" = "InventedColumn")

  # This should run without error, but only return results for Table1
  expect_warning(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    paste0(
      "\\[get_origin_value\\] Column 'InventedColumn' does not exist ",
      "in the 'Table1' table"
    ) # No warning expected
  )

  # Check result only includes Table1
  expect_equal(nrow(result), 1)
  expect_equal(result$ori_table, "Table1")

  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})


test_that("get_origin_value handles empty scheme", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )
  
  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )
  
  search_scheme <- list()
  
  # This should run without error, but only return results for Table1
  expect_error(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    "\\[get_origin_value\\] 'search_scheme' must be a non-empty list"
  )
  
  # Close connection for testing
  dbDisconnect(db_connection, shutdown = TRUE)
  
})


test_that("get_origin_value handleswrong multiple column in list", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )
  
  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )
  
  # Testing scheme correct definition
  search_scheme <- list("Table1" = c("Column1","Column2"))
  
  # This should run without error, but only return results for Table1
  expect_error(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    "\\[get_origin_value\\] Each element in 'search_scheme' must be a single character string"
  )
  
  # Close connection for testing
  dbDisconnect(db_connection, shutdown = TRUE)
  
})


test_that("get_origin_value handles closed connection", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )
  
  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )
  
  # Testing scheme correct definition
  search_scheme <- list("Table1" = c("Column1","Column2"))
  
  # Close connection for testing
  dbDisconnect(db_connection, shutdown = TRUE)
  
  # This should run without error, but only return results for Table1
  expect_error(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    "\\[get_origin_value\\] 'db_connection' must be a valid DuckDB connection"
  )
  
})

test_that("get_origin_value handles missing table", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )
  
  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )
  
  # Testing if tables from scheme exist in database
  search_scheme <- list("Table2" = c("Column1"))
  
  # This should run without error, but only return results for Table1
  expect_warning(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    "\\[get_origin_value\\] Table 'Table2' does not exist in the database"
  )
  
  
  # Close connection for testing
  dbDisconnect(db_connection, shutdown = TRUE)
  
})


test_that("get_origin_value handles missing requiered column in cases_dt", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(
    db_connection,
    "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)"
  )
  
  # Insert test data
  DBI::dbExecute(
    db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')"
  )
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1)
  )
  
  # Testing if tables from scheme exist in database
  search_scheme <- list("Table1" = c("A"))
  
  # This should run without error, but only return results for Table1
  expect_error(
    result <- get_origin_value(cases_dt, db_connection, search_scheme),
    "\\[get_origin_value\\] 'cases_dt' is missing required search_scheme: ori_table"
  )
  
  
  # Close connection for testing
  dbDisconnect(db_connection, shutdown = TRUE)
  
})