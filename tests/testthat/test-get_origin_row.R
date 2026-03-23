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

test_that("get_origin_row handles complex multi-table scenarios", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # A mix of: 1 valid, 1 non-existent, 1 missing column (MALFORMED), and 1 NA
  ids <- data.table::data.table(
    unique_id = c("1", "99", "1", "2"), 
    ori_table = c("EVENTS", "EVENTS", "NON_EXISTENT", NA)
  )
  
  res <- suppressMessages(get_origin_row(con, ids))
  
  # EVENTS should return 1 row (id '1'), even though we asked for '99' too
  testthat::expect_equal(nrow(res$EVENTS), 1)
  # NON_EXISTENT should be an empty data.table
  testthat::expect_true(data.table::is.data.table(res$NON_EXISTENT))
  testthat::expect_equal(nrow(res$NON_EXISTENT), 0)
})

test_that("get_origin_row handles special characters", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Insert a row with a single quote in the ID
  special_id <- "O'Brian-123"
  DBI::dbExecute(con, sprintf("INSERT INTO EVENTS VALUES ('%s', 'EVENTS', 'Special')", gsub("'", "''", special_id)))
  
  ids <- data.table::data.table(unique_id = special_id, ori_table = "EVENTS")
  
  # This tests the gsub("'", "''", table_ids) logic in your function
  res <- get_origin_row(con, ids)
  testthat::expect_equal(res$EVENTS$unique_id, special_id)
})

testthat::test_that("get_origin_row returns empty list for empty input", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Testing the 'nrow(ids) == 0' early return
  testthat::expect_message(res <- get_origin_row(con, data.table::data.table()), "ids is empty")
  testthat::expect_type(res, "list")
  testthat::expect_length(res, 0)
})

testthat::test_that("get_origin_row handles duplicate input IDs gracefully", {
  con <- setup_comprehensive_db()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Requesting the same ID twice
  ids <- data.table::data.table(unique_id = c("1", "1"), ori_table = c("EVENTS", "EVENTS"))
  
  res <- get_origin_row(con, ids)
  # Depending on your SQL logic, this usually returns the row once 
  # because of the 'WHERE unique_id IN (...)' clause
  testthat::expect_equal(nrow(res$EVENTS), 1)
})