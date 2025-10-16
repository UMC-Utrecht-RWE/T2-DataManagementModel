# Helper function to set up test database
setup_test_db <- function() {
  # Create an in-memory DuckDB database
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create test tables
  DBI::dbExecute(
    con,
    "CREATE TABLE EVENTS (unique_id VARCHAR, ori_table VARCHAR, event_name VARCHAR, event_date DATE)"
  )
  DBI::dbExecute(
    con, "CREATE TABLE PATIENTS (unique_id VARCHAR, ori_table VARCHAR, name VARCHAR, age INTEGER)"
  )

  # Insert test data
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('1','EVENTS', 'admission', '2023-01-01')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('2','EVENTS', 'discharge', '2023-01-05')"
  )
  DBI::dbExecute(
    con, "INSERT INTO EVENTS VALUES ('3','EVENTS', 'test', '2023-01-03')"
  )

  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('101','PATIENTS', 'John Doe', 45)"
  )
  DBI::dbExecute(
    con, "INSERT INTO PATIENTS VALUES ('102','PATIENTS', 'Jane Smith', 32)"
  )
  con
}
# Tests
test_that("get_origin_row correctly retrieves data from a single table", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with single table, multiple IDs
  ids <- data.table(unique_id = c("1", "2"),
                    ori_table = c("EVENTS", "EVENTS"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, "EVENTS")
  expect_equal(nrow(result$EVENTS), 2)
  expect_equal(result$EVENTS$unique_id, c("1", "2"))
  expect_equal(result$EVENTS$ori_table, c("EVENTS", "EVENTS"))
  expect_equal(result$EVENTS$event_name, c("admission", "discharge"))
})

test_that("get_origin_row correctly retrieves data from multiple tables", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with multiple tables
  ids <- data.table(unique_id = c("1", "101"), ori_table = c("EVENTS","PATIENTS"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, c("EVENTS", "PATIENTS"))
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(nrow(result$PATIENTS), 1)
  expect_equal(result$EVENTS$unique_id, "1")
  expect_equal(result$PATIENTS$unique_id, "101")
})

test_that("get_origin_row handles non-existent IDs gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with non-existent ori_id
  ids <- data.table(unique_id = c("999", "1"), ori_table = c("EVENTS","EVENTS"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(result$EVENTS$unique_id, "1")
  expect_equal(result$EVENTS$ori_table, "EVENTS")
})

test_that("get_origin_row handles non-existent tables gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with non-existent table
  ids <- data.table(unique_id = c("NONEXISTENT", "1"), ori_table = c("NONEXISTENT","EVENTS"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_named(result, c("NONEXISTENT", "EVENTS"))
  expect_equal(nrow(result$EVENTS), 1)
  expect_equal(nrow(result$NONEXISTENT), 0)
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
  ids <- data.frame(unique_id = c("1", "2"), ori_table = c("EVENTS", "EVENTS"))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_equal(nrow(result$EVENTS), 2)
})

test_that("get_origin_row handles empty input gracefully", {
  # Setup
  con <- setup_test_db()
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Test with empty data.table
  ids <- data.table(unique_id = character(0), ori_table = character(0))
  result <- get_origin_row(con, ids)

  # Expectations
  expect_type(result, "list")
  expect_length(result, 0)
})


test_that(
  "get_origin_row returns handles wrong ori_id input value",
  {
    # Setup
    con <- setup_test_db()
    on.exit(dbDisconnect(con, shutdown = TRUE))

    # Test with underscore separator
    ids <- data.table(INVENTEDNAME = c("1", "2"),
                      ori_table = c("EVENTS", "EVENTS"))
    expect_message(
      get_origin_row(con, ids),
      fixed = TRUE,
      "[get_origin_row] The unique identifier 'unique_id' does not exist in the ids"

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
    ids <- data.table(unique_id = "9999", ori_table = "EVENTS")

    result <- get_origin_row(con, ids)
    # Expectations
    expect_equal(nrow(result$EVENTS), 0)

  }
)