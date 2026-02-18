test_that("initialize_concept_table respects existing tables", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # --- 1. SETUP: Create an initial table ---
  dbExecute(con, "CREATE TABLE concept_table (id INTEGER)")
  
  # --- 2. TEST: Attempt to initialize again ---
  # It should NOT throw an error, but it should NOT overwrite the table.
  # We check the 'message' to confirm it skipped.
  expect_message(
    initialize_concept_table(con, type_table = "table"),
    "already exists"
  )
  
  # Verify the original structure is still there (our dummy 'id' column)
  # instead of the full schema the function would have created.
  cols <- dbListFields(con, "concept_table")
  expect_equal(cols, "id")
  expect_false("unique_id" %in% cols)
})

test_that("initialize_concept_table basic functionality and validation", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # Test creation of table when it doesn't exist
  expect_no_error(initialize_concept_table(con, type_table = "table"))
  expect_true(dbExistsTable(con, "concept_table"))
  
  # Test validation: Missing path for view
  # Create a new connection to clear the previous 'concept_table'
  con2 <- dbConnect(duckdb::duckdb(), ":memory:")
  expect_error(
    initialize_concept_table(con2, type_table = "view", path_parquets = NULL),
    "valid 'path_parquets' must be provided"
  )
})