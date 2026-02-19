
test_that("apply_codelist performs input validation", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")

  # Test invalid data table
  expect_error(apply_codelist(con, data.frame(a=1),
                              materialize = "in_database"), "must be a data.table")

  # Test empty data table
  expect_error(apply_codelist(con, data.table(),
                              materialize = "in_database"), "codelist is empty")

  # Test missing columns
  incomplete_dt <- data.table(concept_id = 1)
  expect_error(apply_codelist(con, incomplete_dt,
                              materialize = "in_database"), "Missing required columns")
  
  valid_cols <- c(
    "cdm_table_name", "cdm_column", "concept_id", "code",
    "order_index", "keep_value_column_name", "keep_date_column_name"
  )
  
  # Helper to create a valid base object
  get_valid_dt <- function() {
    dt <- data.table(matrix(nrow = 1, ncol = length(valid_cols)))
    setnames(dt, valid_cols)
    dt[, order_index := 1L] # Must be numeric
    return(dt)
  }
  
  bad_type_dt <- get_valid_dt()
  bad_type_dt[, order_index := "not_a_number"]
  expect_error(
    apply_codelist(con, bad_type_dt, materialize = "in_database"), 
    "'order_index' must be numeric/integer."
  )
  
  # --- 4. Materialization & Path Logic ---
  expect_error(
    apply_codelist(con, get_valid_dt(), materialize = "invalid_string"), 
    "must be either 'in_parquet' or 'in_database'"
  )
  
  expect_error(
    apply_codelist(con, get_valid_dt(), materialize = "in_parquet", path_parquets = NULL), 
    "'path_parquets' must be provided"
  )
  
  # --- 5. Warnings ---
  warnings <- capture_warnings(
    apply_codelist(con, get_valid_dt(), materialize = "in_database", path_parquets = "ignored/path")
  )
  
  expect_length(warnings, 2)
  
  expect_true(any(grepl("path_parquets' is ignored", warnings)))
  expect_true(any(grepl("requiered tables do not exist", warnings)))

  
  dbDisconnect(con, shutdown = TRUE)
})

test_that("apply_codelist executes hierarchical SQL flow", {

  db_path <- tempfile(fileext = ".duckdb")
  # 1. SETUP: Create temporary DB and mock CDM table
  loader <- suppressMessages(DatabaseLoader$new(
    db_path = db_path,
    data_instance = "dbtest",
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.rds"
    ),
    config_path = Sys.getenv("APPLYCODELIST_CONFIG_PATH"))
  )
  suppressMessages(loader$set_database())
  suppressMessages(loader$run_db_ops())

  # 2. SETUP: Create a hierarchical codelist
  # We have one family with a parent (order 1) and a child (order 2)
  test_codelist <- data.table(
    id_set = c(1, 1),                 # Added: Wrangling function always adds this
    concept_id = c("covid_test", "covid_test"),
    cdm_table_name = "EVENTS",
    cdm_column = c("event_code", "event_record_vocabulary"),
    code = c("U07.1", "ICD10CM"),
    keep_value_column_name = c("event_code","event_code"),
    keep_date_column_name = c("start_date_record","start_date_record"),
    order_index = c(1, 2)             # L suffix ensures Integer type
  )

  con <- dbConnect(duckdb(),db_path)
  list_tables <- dbListTables(con)
  expect_contains(list_tables,"PERSONS")
  # 3. EXECUTE: We wrap in capture_messages to check the flow
  msgs <- capture_messages(apply_codelist(db_con = con,
                                          test_codelist,
                                          materialize = "in_database"))

  # 4. VERIFY: Check if the logic branched correctly
  expect_match(msgs[2], "Applying parent scheme")
  expect_match(msgs[3], "Processing child with cdm_column=event_record_vocabulary")

  # Verify that the temporary table 'codelist' was created in the DB during the process
  # (Note: In a real run, it might be dropped, but we check logic flow via messages)
  expect_true(any(grepl("Order index: 2", msgs)))

  #Verifying that the only identificable case was identified
  concept_table <- dbReadTable(con,"concept_table")
  expect_equal(nrow(concept_table),1)
  expect_equal(concept_table$person_id,"21110000001")

  dbDisconnect(con, shutdown = TRUE)

  con <- dbConnect(duckdb(),db_path)
  list_tables <- dbListTables(con)
  expect_contains(list_tables,"PERSONS")

  temp_parquet_path <- tempdir()

  # 3. EXECUTE: We wrap in capture_messages to check the flow
  msgs <- capture_messages(apply_codelist(db_con = con,
                                          test_codelist,
                                          materialize = "in_parquet",
                                          path_parquets = temp_parquet_path))

  # 4. VERIFY: Check if the logic branched correctly
  expect_match(msgs[2], "Applying parent scheme")
  expect_match(msgs[3], "Processing child with cdm_column=event_record_vocabulary")

  # Verify that the temporary table 'codelist' was created in the DB during the process
  # (Note: In a real run, it might be dropped, but we check logic flow via messages)
  expect_true(any(grepl("Order index: 2", msgs)))

  #Verifying that the only identificable case was identified
  concept_table <- dbReadTable(con,"concept_table")
  expect_equal(nrow(concept_table),1)
  expect_equal(concept_table$person_id,"21110000001")

  dbDisconnect(con, shutdown = TRUE)

})

