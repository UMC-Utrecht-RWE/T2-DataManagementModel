test_that("import_dap_specific_codelist correctly processes and imports data", {
  
  # 1. SETUP: Create a mock RDS file
  tmp_rds <- tempfile(fileext = ".rds")
  mock_data <- data.table(
    code = c("a1", "b2", "c3"),
    description = c("apple", "banana", "cherry"),
    Comment = c("BOTH", "ONLY_ONE", "BOTH") # Note: "b2" should be filtered out
  )
  saveRDS(mock_data, tmp_rds)
  
  # 2. SETUP: Create an in-memory database connection
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  
  # Define parameters
  target_table <- "test_codelist"
  target_cols <- c("code", "description")
  
  # 3. EXECUTE
  import_dap_specific_codelist(
    codelist_path = tmp_rds,
    codelist_name_db = target_table,
    db_connection = con,
    columns = target_cols
  )
  
  # 4. VERIFY
  # Fetch the data back from the DB
  result_db <- dbReadTable(con, target_table)
  
  # Test A: Check row count (Should be 2 because "ONLY_ONE" was filtered out)
  expect_equal(nrow(result_db), 2)
  
  # Test B: Check Uppercase conversion
  expect_equal(result_db$code, c("A1", "C3"))
  expect_equal(result_db$description, c("APPLE", "CHERRY"))
  
  # Test C: Check that ONLY requested columns exist
  expect_setequal(colnames(result_db), target_cols)
  
  # 5. CLEANUP
  dbDisconnect(con, shutdown = TRUE)
  unlink(tmp_rds)
})

test_that("import_dap_specific_codelist throws error on missing file", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  
  expect_error(
    import_dap_specific_codelist("non_existent_file.rds", "table", con, c("col1")),
    "File not found."
  )
  
  dbDisconnect(con, shutdown = TRUE)
})