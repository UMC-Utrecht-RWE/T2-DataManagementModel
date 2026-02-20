library(testthat)
library(data.table)
library(DBI)
library(duckdb)

setup_comprehensive_db <- function() {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  
  # Standard table
  dbExecute(con, "CREATE TABLE EVENTS (unique_id VARCHAR, ori_table VARCHAR, val TEXT)")
  dbExecute(con, "INSERT INTO EVENTS VALUES ('1','EVENTS', 'A')")
  
  # Malformed table
  dbExecute(con, "CREATE TABLE MALFORMED (wrong_id VARCHAR, ori_table VARCHAR)")
  
  # Broken View: Force a CAST error (String to Integer) to trigger tryCatch
  dbExecute(con, "CREATE TABLE CRASH_DATA (unique_id VARCHAR, ori_table VARCHAR, bad_val VARCHAR)")
  dbExecute(con, "INSERT INTO CRASH_DATA VALUES ('1', 'CRASH_DATA', 'NOT_A_NUMBER')")
  dbExecute(con, "CREATE VIEW BROKEN_VIEW AS SELECT unique_id, ori_table, CAST(bad_val AS INTEGER) AS crash FROM CRASH_DATA")
  
  return(con)
}

test_that("get_origin_row handles SQL execution errors via tryCatch", {
  con <- setup_comprehensive_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  ids <- data.table(unique_id = "1", ori_table = "BROKEN_VIEW")
  
  # 1. Check message
  expect_message(get_origin_row(con, ids), "Error querying table 'BROKEN_VIEW'")
  
  # 2. Check result is empty data.table
  res <- suppressMessages(get_origin_row(con, ids))
  expect_equal(nrow(res$BROKEN_VIEW), 0)
})

test_that("get_origin_row validates database columns correctly", {
  con <- setup_comprehensive_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  ids <- data.table(unique_id = "1", ori_table = "MALFORMED")
  
  expect_message(get_origin_row(con, ids), "unique_id' does not exist in the MALFORMED")
})

test_that("get_origin_row validates input data structures", {
  con <- setup_comprehensive_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # Test for missing columns in the INPUT data table
  # Note: The error message changed slightly in the function fix, so update test here
  expect_message(
    get_origin_row(con, data.table(ID = "1")), 
    "Missing columns in 'ids': ori_table, unique_id"
  )
})