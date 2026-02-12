# 
# test_that("apply_codelist performs input validation", {
#   con <- dbConnect(duckdb::duckdb(), ":memory:")
#   
#   # Test invalid data table
#   expect_error(apply_codelist(con, data.frame(a=1)), "must be a data.table")
#   
#   # Test empty data table
#   expect_error(apply_codelist(con, data.table()), "codelist is empty")
#   
#   # Test missing columns
#   incomplete_dt <- data.table(concept_id = 1)
#   expect_error(apply_codelist(con, incomplete_dt), "Missing required columns")
#   
#   dbDisconnect(con, shutdown = TRUE)
# })
# 
# test_that("apply_codelist executes hierarchical SQL flow", {
# 
#   # Define dummy variables that the function expects in its environment
#   dir_sqlqueries_t2 <- "."
#   # 1. SETUP: Create temporary DB and mock CDM table
#   con <- suppressMessages(create_loader_from_file())
#   
#   
#   # 2. SETUP: Create a hierarchical codelist
#   # We have one family with a parent (order 1) and a child (order 2)
#   test_codelist <- data.table(
#     id_set = c(1, 1),                 # Added: Wrangling function always adds this
#     concept_id = c("covid_test", "covid_test"),
#     cdm_table_name = "EVENTS",
#     cdm_column = c("event_code", "event_record_vocabulary"),
#     code = c("U07.1", "ICD10CM"),
#     keep_value_column_name = c("event_code","event_code"),
#     keep_date_column_name = c("event_date","event_date"),
#     order_index = c(1, 2)             # L suffix ensures Integer type
#   )
#   
#   # 3. EXECUTE: We wrap in capture_messages to check the flow
#   msgs <- capture_messages(apply_codelist(con, test_codelist))
#   
#   # 4. VERIFY: Check if the logic branched correctly
#   expect_match(msgs[2], "Applying parent scheme")
#   expect_match(msgs[3], "Processing child with cdm_column=diag_code")
#   
#   # Verify that the temporary table 'codelist' was created in the DB during the process
#   # (Note: In a real run, it might be dropped, but we check logic flow via messages)
#   expect_true(any(grepl("Order index: 2", msgs)))
#   
#   dbDisconnect(con, shutdown = TRUE)
# })
# 
# test_that("apply_codelist handles multiple families separately", {
#   con <- dbConnect(duckdb::duckdb(), ":memory:")
#   
#   # Two different families (different cdm_table or keep_columns)
#   multi_family_codelist <- data.table(
#     cdm_table_name = c("table_A", "table_B"),
#     cdm_column = c("col1", "col2"),
#     concept_id = c("C1", "C2"),
#     code = c("X", "Y"),
#     order_index = c(1L, 1L),
#     keep_value_column_name = c("v1", "v2"),
#     keep_date_column_name = c("d1", "d2")
#   )
#   
#   msgs <- capture_messages(apply_codelist(con, multi_family_codelist))
#   
#   # Should see two "Processing family" messages
#   family_msgs <- grep("Processing family", msgs, value = TRUE)
#   expect_equal(length(family_msgs), 2)
#   
#   dbDisconnect(con, shutdown = TRUE)
# })