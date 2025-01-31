dbname <- tempfile(fileext = ".duckdb")
d2_db_conn <- DBI::dbConnect(duckdb::duckdb(), dbname)

# concept_table
ctname <- tempfile(fileext = ".duckdb")
concepts_db_conn <- DBI::dbConnect(duckdb::duckdb(), ctname)


test_that("Check column names consistency", {
  dap_specific_concept_map <- data.frame(
    concept_id = "WEIGHT_BIRTH",
    dap_name = "CPRD",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    column_name_1 = "mo_meaning",
    column_name_2 = NA,
    column_name_3 = NA,
    expected_value_1 = "weight",
    expected_value_2 = NA,
    expeted_value_3 = NA, # wrong name here!
    keep_value_column_name_1 = "mo_source_value",
    keep_unit_column_name = NA,
    keep_date_column_name = "mo_date",
    IR1 = NA,
    Comments = NA,
    dap_spec_id = "dap_spec_id-1")
  
  expect_error(
    create_dap_specific_concept(codelist=dap_specific_concept_map,
                                data_db= d2_db_conn,
                                name_attachment = dbname, 
                                save_db= concepts_db_conn, 
                                date_col_filter = "1900-01-01",
                                add_meaning = TRUE)
  )
})

DBI::dbDisconnect(d2_db_conn)
DBI::dbDisconnect(concepts_db_conn)
