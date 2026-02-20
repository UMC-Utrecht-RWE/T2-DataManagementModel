test_that("create_unique_id: columns are added correctly in overwrite mode", {
  db_con <- create_loaded_test_db() # Assuming this sets up PERSONS/VACCINES
  withr::defer(DBI::dbDisconnect(db_con))
  
  # Test basic overwrite (to_view = FALSE is default/explicit)
  create_unique_id(db_con, cdm_tables_names = "PERSONS", to_view = FALSE)
  
  persons_db <- DBI::dbReadTable(db_con, "PERSONS")
  
  # Check columns exist
  expect_contains(names(persons_db), c("unique_id", "ori_table"))
  # Check data integrity: ori_table column should match the table name
  expect_equal(unique(persons_db$ori_table), "PERSONS")
  # Check uniqueness: every row should have a distinct UUID
  expect_equal(nrow(persons_db), length(unique(persons_db$unique_id)))
})

test_that("create_unique_id: creates views when to_view is TRUE", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  # Note: This test assumes T2.DMM:::add_view is accessible or mocked
  # If add_view creates a table/view named [table][pipeline_extension]
  table_name <- "VACCINES"
  pipe_ext <- "_T2DMM"
  view_ext <- "_view_1"
  
  create_unique_id(
    db_con, 
    cdm_tables_names = table_name, 
    to_view = TRUE, 
    pipeline_extension = pipe_ext
  )
  
  view_name <- paste0(table_name, pipe_ext, view_ext)
  
  # Check if the view/table exists in the DB
  expect_true(view_name %in% DBI::dbListTables(db_con))
  
  # Check content of the view
  view_data <- DBI::dbReadTable(db_con, view_name)
  expect_contains(names(view_data), c("unique_id", "ori_table"))
  expect_equal(unique(view_data$ori_table), table_name)
})

test_that("create_unique_id: handles extension_name correctly", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  # Create a table with a suffix manually to simulate multi-instance
  DBI::dbExecute(db_con, "CREATE TABLE PERSONS_CDM1 AS SELECT * FROM PERSONS")
  
  create_unique_id(db_con, cdm_tables_names = "PERSONS", extension_name = "_CDM1")
  
  res <- DBI::dbReadTable(db_con, "PERSONS_CDM1")
  expect_contains(names(res), "unique_id")
  expect_equal(unique(res$ori_table), "PERSONS_CDM1")
})

test_that("create_unique_id: handles non-existent tables gracefully", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  # Should not crash, but should emit a message
  expect_message(
    create_unique_id(db_con, cdm_tables_names = "NON_EXISTENT_TABLE"),
    "Can not create unique IDs"
  )
})

test_that("create_unique_id: custom schema support", {
  db_con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(db_con))
  
  # Create a custom schema and table
  DBI::dbExecute(db_con, "CREATE SCHEMA test_schema")
  DBI::dbExecute(db_con, "CREATE TABLE test_schema.DATA AS SELECT 1 AS val")
  
  create_unique_id(
    db_con, 
    cdm_tables_names = "DATA", 
    schema_name = "test_schema"
  )
  
  # Check if the table in the schema was updated
  res <- DBI::dbGetQuery(db_con, "SELECT * FROM test_schema.DATA")
  expect_contains(names(res), "unique_id")
})