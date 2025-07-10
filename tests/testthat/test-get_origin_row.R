# Helper function to set up test database
setup_test_db <- function() {
  # Create an in-memory DuckDB database
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create test tables
  DBI::dbExecute(
    con,
    "CREATE TABLE EVENTS (ori_id VARCHAR, event_name VARCHAR, event_date DATE)"
  )
  DBI::dbExecute(
    con, "CREATE TABLE PATIENTS (ori_id VARCHAR, name VARCHAR, age INTEGER)"
  )

  # Insert test data
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-1', 'admission', '2023-01-01')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-2', 'discharge', '2023-01-05')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-3', 'test', '2023-01-03')"
  )

  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('PATIENTS-101', 'John Doe', 45)"
  )
  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('PATIENTS-102', 'Jane Smith', 32)"
  )
  con
}
# Helper function to set up test database
setup_test_db_custom_id <- function() {
  # Create an in-memory DuckDB database
  con <- DBI::dbConnect(duckdb::duckdb(), dir = tempdir())

  # Create test tables
  DBI::dbExecute(
    con,
    paste0(
      "CREATE TABLE EVENTS (CUSTOM_ID VARCHAR, ",
      "event_name VARCHAR, event_date DATE)"
    )
  )
  DBI::dbExecute(
    con, "CREATE TABLE PATIENTS (CUSTOM_ID VARCHAR, name VARCHAR, age INTEGER)"
  )

  # Insert test data
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-1', 'admission', '2023-01-01')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-2', 'discharge', '2023-01-05')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('EVENTS-3', 'test', '2023-01-03')"
  )

  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('PATIENTS-101', 'John Doe', 45)"
  )
  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('PATIENTS-102', 'Jane Smith', 32)"
  )

  con
}
# Tests
test_that("get_origin_row correctly retrieves data from a single table", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with single table, multiple IDs
  ids <- data.table(ori_id = c("EVENTS-1", "EVENTS-2"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, "EVENTS")
  expect_equal(nrow(result$EVENTS), 2)
  expect_equal(result$EVENTS$ori_id, c("EVENTS-1", "EVENTS-2"))
  expect_equal(result$EVENTS$event_name, c("admission", "discharge"))
})

test_that("get_origin_row correctly retrieves data from multiple tables", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with multiple tables
  ids <- data.table(ori_id = c("EVENTS-1", "PATIENTS-101"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, c("EVENTS", "PATIENTS"))
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(nrow(result$PATIENTS), 1)
  expect_equal(result$EVENTS$ori_id, "EVENTS-1")
  expect_equal(result$PATIENTS$ori_id, "PATIENTS-101")
})

test_that("get_origin_row handles non-existent IDs gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with non-existent ori_id
  ids <- data.table(ori_id = c("EVENTS-999", "EVENTS-1"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(result$EVENTS$ori_id, "EVENTS-1")
})

test_that("get_origin_row handles non-existent tables gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with non-existent table
  ids <- data.table(ori_id = c("NONEXISTENT-1", "EVENTS-1"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, c("NONEXISTENT", "EVENTS"))
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(nrow(result$NONEXISTENT), 0)
})

test_that("get_origin_row works with custom unique ori_id column name", {
  # Setup
  con <- setup_test_db_custom_id()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Create data with custom ori_id column
  ids <- data.table(CUSTOM_ID = c("EVENTS-1", "EVENTS-2"))
  result <- get_origin_row(con, ids, id_name = "CUSTOM_ID")

  # Expectations
  expect_equal(nrow(result$EVENTS), 2)
  expect_equal(result$EVENTS$CUSTOM_ID, c("EVENTS-1", "EVENTS-2"))
})

test_that("get_origin_row works with custom separator", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # First add data with different separator
  dbExecute(
    con,
    "INSERT INTO EVENTS VALUES ('EVENTS_4', 'follow-up', '2023-01-10')"
  )

  # Test with underscore separator
  ids <- data.table(ori_id = c("EVENTS_4"))
  result <- get_origin_row(con, ids, separator_id = "_")

  # Expectations
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(result$EVENTS$ori_id, "EVENTS_4")
})

test_that(
  "get_origin_row returns empty list when ori_id column does not exist",
  {
    # Setup
    con <- setup_test_db()
    on.exit(dbDisconnect(con, shutdown = TRUE))

    # Test with non-existent ori_id column
    ids <- data.table(WRONG_COLUMN = c("EVENTS-1"))
    result <- get_origin_row(con, ids)

    # Expectations
    expect_type(result, "list")
    expect_length(result, 0)
  }
)

test_that("get_origin_row handles data frames by converting to data.table", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with data frame instead of data.table
  ids <- data.frame(ori_id = c("EVENTS-1", "EVENTS-2"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_equal(nrow(result$EVENTS), 2)
})

test_that("get_origin_row handles malformed IDs gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with malformed IDs (missing separator)
  ids <- data.table(ori_id = c("EVENTS1", "EVENTS-2"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(result$EVENTS$ori_id, "EVENTS-2")
})

test_that("get_origin_row handles empty input gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with empty data.table
  ids <- data.table(ori_id = character(0))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_length(result, 0)
})

test_that("get_origin_row works reports wrong separator", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with underscore separator
  ids <- data.table(ori_id = c("EVENTS-1"))
  separator_id <- "_"
  expect_message(
    get_origin_row(con, ids, separator_id = "_"),
                fixed = TRUE,
               "[get_origin_row] The separator '_' does not exist in the unique identifiers"
               
   )

})

test_that("get_origin_row works reports wrong separator version 2", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # Test with underscore separator
  ids <- data.table(ori_id = c("EVENTS-1", "EVENTS_2"))
  separator_id <- "_"
  expect_message(
    get_origin_row(con, ids, separator_id = "_"),
    fixed = TRUE,
    "[get_origin_row] Some cases' unique identifier do not contain the separator '_'"
  )
  
})

test_that(
  "get_origin_row returns handles wrong ori_id input value",
  {
    # Setup
    con <- setup_test_db()
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    # Test with underscore separator
    ids <- data.table(INVENTEDNAME = c("EVENTS-1"))
    expect_message(
      get_origin_row(con, ids, id_name = 'INVENTEDNAME'),
      fixed = TRUE,
      "[get_origin_row] Column 'INVENTEDNAME' does not exist in table 'EVENTS'"
      
    )
  }
)

test_that(
  "get_origin_row returns empty when cases do not exist",
  {
    # Setup
    con <- setup_test_db()
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    # Test with underscore separator
    ids <- data.table(ori_id = c("EVENTS-9999"))

    result <- get_origin_row(con, ids)
    # Expectations
    expect_equal(nrow(result$EVENTS), 0)
    
  }
)