test_that("initialize_concept_table handles overwriting and schema", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # 1. Test creation with 'id_set' (default)
  initialize_concept_table(con, type_table = "table", add_id_set = TRUE)
  cols <- dbListFields(con, "concept_table")
  expect_true("id_set" %in% cols)
  expect_true("unique_id" %in% cols)
  
  # 2. Test overwrite = TRUE
  # Create a dummy table with different columns
  dbExecute(con, "DROP TABLE concept_table")
  dbExecute(con, "CREATE TABLE concept_table (old_col INTEGER)")
  
  expect_message(
    initialize_concept_table(con, type_table = "table", overwrite = TRUE, add_id_set = FALSE),
    "Dropping table"
  )
  
  new_cols <- dbListFields(con, "concept_table")
  expect_false("old_col" %in% new_cols)
  expect_false("id_set" %in% new_cols) # Checked that add_id_set = FALSE worked
  expect_true("unique_id" %in% new_cols)
})

test_that("initialize_concept_table handles view creation and invalid inputs", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  # 1. Test Invalid type_table
  expect_error(
    initialize_concept_table(con, type_table = "invalid_type"),
    "must be either 'view' or 'table'"
  )
  
  # 2. Test View Creation (Mocking a Parquet environment)
  # We create a temporary directory and a dummy parquet file so DuckDB doesn't error on scan
  tmp_dir <- tempdir()
  tmp_path <- file.path(tmp_dir, "test.parquet")
  arrow::write_parquet(data.frame(
    unique_id = "a", ori_table = "b", person_id = "c", 
    concept_id = "d", value = "e", date = as.Date("2023-01-01"), id_set = "f"
  ), tmp_path)
  
  expect_no_error(
    initialize_concept_table(con, type_table = "view", path_parquets = tmp_dir, partition = FALSE)
  )
  
  # Verify it is a view and not a table
  # In DuckDB, views appear in information_schema
  res <- dbGetQuery(con, "SELECT table_type FROM information_schema.tables WHERE table_name = 'concept_table'")
  expect_equal(res$table_type, "VIEW")
  
  unlink(tmp_path)
})