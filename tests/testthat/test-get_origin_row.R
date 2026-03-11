setup_comprehensive_db <- function() {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  
  # Standard table
  DBI::dbExecute(con, "CREATE TABLE EVENTS (unique_id VARCHAR, ori_table VARCHAR, val TEXT)")
  DBI::dbExecute(con, "INSERT INTO EVENTS VALUES ('1','EVENTS', 'A')")

  # Malformed table
  DBI::dbExecute(con, "CREATE TABLE MALFORMED (wrong_id VARCHAR, ori_table VARCHAR)")
  
  # Broken View: Force a CAST error (String to Integer) to trigger tryCatch
  DBI::dbExecute(con, "CREATE TABLE CRASH_DATA (unique_id VARCHAR, ori_table VARCHAR, bad_val VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO CRASH_DATA VALUES ('1', 'CRASH_DATA', 'NOT_A_NUMBER')")
  DBI::dbExecute(con, "CREATE VIEW BROKEN_VIEW AS SELECT unique_id, ori_table, CAST(bad_val AS INTEGER) AS crash FROM CRASH_DATA")
  
  return(con)
}

testthat::test_that("get_origin_row handles SQL execution errors via tryCatch", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  ids <- data.table::data.table(unique_id = "1", ori_table = "BROKEN_VIEW")
  
  # 1. Check message
  testthat::expect_message(get_origin_row(con, ids), "Error querying table 'BROKEN_VIEW'")

  # 2. Check result is empty data.table
  res <- suppressMessages(get_origin_row(con, ids))
  testthat::expect_equal(nrow(res$BROKEN_VIEW), 0)
})

testthat::test_that("get_origin_row validates database columns correctly", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  ids <- data.table::data.table(unique_id = "1", ori_table = "MALFORMED")
  
  testthat::expect_message(get_origin_row(con, ids), "unique_id' does not exist in the MALFORMED")
})

testthat::test_that("get_origin_row validates input data structures", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Test for missing columns in the INPUT data table
  # Note: The error message changed slightly in the function fix, so update test here
  testthat::expect_message(
    get_origin_row(con, data.table::data.table(ID = "1")),
    "Missing columns in 'ids': ori_table, unique_id"
  )
})