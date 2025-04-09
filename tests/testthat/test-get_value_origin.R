library(testthat)
library(data.table)
library(DBI)
library(duckdb)

# Source the function file - you'll need to adjust this path
# source("path/to/your/function.R")

# Test suite for get_value_origin function
test_that("get_value_origin returns correct values", {
  # Create a temporary DuckDB connection for testing
  db_connection <- DBI::dbConnect(duckdb::duckdb())
  
  # Setup test tables
  DBI::dbExecute(db_connection, "CREATE TABLE cdm_table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT, Column2 TEXT)")
  DBI::dbExecute(db_connection, "CREATE TABLE cdm_table2 (ori_table TEXT, ROWID INTEGER, Column2 TEXT)")
  
  # Insert test data
  DBI::dbExecute(db_connection, "INSERT INTO cdm_table1 VALUES ('cdm_table1',1, 'Value1','Value3'), ('cdm_table1',2, 'Value2','Value4')")
  DBI::dbExecute(db_connection, "INSERT INTO cdm_table2 VALUES ('cdm_table2',1, 100), ('cdm_table2',2, 200), ('cdm_table2',3, 300)")
  
  # Create test cases data table
  cases_dt <- data.table::data.table(
    ROWID = c(1, 2, 3),
    ori_table = c("cdm_table1", "cdm_table1", "cdm_table2")
  )
  
  # Test with valid columns parameter
  search_scheme <- list("cdm_table1" = "Column1", "cdm_table1" = "Column2", "cdm_table2" = "Column2")
  result <- get_value_origin(cases_dt, db_connection, search_scheme = search_scheme)
  
  # Check result structure
  expect_s3_class(result, "data.table")
  expect_equal(ncol(result), 4)
  expect_equal(names(result), c("ori_table", "ROWID", "column_origin","value"))
  
  # Check result content
  expect_equal(nrow(result), 5)
  
  # Find records for Table1
  table1_records <- result[ori_table == "cdm_table1"]
  expect_equal(nrow(table1_records), 4)
  expect_equal(table1_records[ROWID == 1 & column_origin == "Column1" , value], "Value1")
  expect_equal(table1_records[ROWID == 2 & column_origin == "Column1", value], "Value2")
  expect_equal(table1_records[ROWID == 1 & column_origin == "Column2", value], "Value3")
  expect_equal(table1_records[ROWID == 2 & column_origin == "Column2", value], "Value4")
  
  # Find records for Table2
  table2_records <- result[ori_table == "cdm_table2"]
  expect_equal(nrow(table2_records), 1)
  expect_equal(table2_records[ROWID == 3 & column_origin == "Column2", value], '300')
  
  # Clean up
  DBI::dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_value_origin handles multiple tables correctly", {
  # Create a temporary DuckDB connection for testing
  db_connection <-  DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(db_connection, "CREATE TABLE TableA (ori_table TEXT, ROWID INTEGER, ColumnA TEXT)")
  DBI::dbExecute(db_connection, "CREATE TABLE TableB (ori_table TEXT, ROWID INTEGER, ColumnB INTEGER)")
  DBI::dbExecute(db_connection, "CREATE TABLE TableC (ori_table TEXT,ROWID INTEGER, ColumnC BOOLEAN)")
  
  # Insert test data
  DBI::dbExecute(db_connection, "INSERT INTO TableA VALUES ('TableA',1, 'A1'), ('TableA',2, 'A2'), ('TableA',3, 'A3')")
  DBI::dbExecute(db_connection, "INSERT INTO TableB VALUES ('TableB',1, 10), ('TableB',2, 20), ('TableB',3, 30)")
  DBI::dbExecute(db_connection, "INSERT INTO TableC VALUES ('TableC',1, TRUE), ('TableC',2, FALSE), ('TableC',3, TRUE)")
  
  # Create test cases data table with multiple tables
  cases_dt <- data.table::data.table(
    ROWID = c(1, 2, 3, 2, 3),
    ori_table = c("TableA", "TableA", "TableB", "TableB", "TableC")
  )
  
  # Test with columns for all tables
  search_scheme <- list("TableA" = "ColumnA", "TableB" = "ColumnB", "TableC" = "ColumnC")
  result <- get_value_origin(cases_dt, db_connection, search_scheme = search_scheme)
  
  # Check overall result
  expect_equal(nrow(result), 5)
  
  # Check if results contain correct values for each table
  expect_equal(sort(result[ori_table == "TableA", value]), c("A1", "A2"))
  expect_equal(sort(result[ori_table == "TableB", value]), c('20', '30'))  
  expect_equal(result[ori_table == "TableC", value], 'TRUE')
  
  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
  gc()
})


test_that("get_value_origin throws error with NULL columns", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb())
  
  # Create a simple test cases data table
  cases_dt <- data.table(ROWID = 1, ori_table = "Table1")
  
  # Test with NULL columns
  expect_error(
    get_value_origin(cases_dt, db_connection, search_scheme = NULL),
    "\\[get_value_origin\\] search_scheme need to be defined"
  )
  
  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_value_origin handles empty cases data table", {
  # Create a temporary DuckDB connection
  db_connection <- DBI::dbConnect(duckdb::duckdb())
  
  # Create empty test cases data table
  cases_dt <- data.table::data.table(ROWID = integer(0), ori_table = character(0))
  
  # Setup a test table
  DBI::dbExecute(db_connection, "CREATE TABLE EmptyTest (ori_table TEXT, ROWID INTEGER, Column1 TEXT)")
  
  # Test with valid columns parameter but empty cases
  columns <- list("EmptyTest" = "Column1")
  
  result <- suppressWarnings(get_value_origin(cases_dt, db_connection, search_scheme = columns))
  # Check the result is an empty data table with correct structure
  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 0)
  
  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})

test_that("get_value_origin handles table not in columns list", {
  # Create a temporary DuckDB connection
  db_connection <-  DBI::dbConnect(duckdb::duckdb(), dir = temp())
  
  # Setup test tables
  DBI::dbExecute(db_connection, "CREATE TABLE Table1 (ori_table TEXT, ROWID INTEGER, Column1 TEXT)")
  
  # Insert test data
  DBI::dbExecute(db_connection, "INSERT INTO Table1 VALUES ('Table1',1, 'Value1')")
  
  # Create test cases data table including a table not in columns list
  cases_dt <- data.table::data.table(
    ROWID = c(1),
    ori_table = c("Table1")
  )
  
  # Only include Table1 in columns
  search_scheme <- list("Table1" = "Column1", "Table1" = "InventedColumn")
  
  # This should run without error, but only return results for Table1
  expect_warning(
    result <- get_value_origin(cases_dt, db_connection, search_scheme),
    "\\[get_value_origin\\] Column 'InventedColumn' does not exist in the 'Table1' table"  # No warning expecte
  )
 
  # Check result only includes Table1
  expect_equal(nrow(result), 1)
  expect_equal(result$ori_table, "Table1")
  
  # Clean up
  dbDisconnect(db_connection, shutdown = TRUE)
})