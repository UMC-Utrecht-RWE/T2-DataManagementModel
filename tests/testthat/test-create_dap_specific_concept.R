# save db to temp connection
mo <- readRDS("dbtest/MEDICAL_OBSERVATIONS.rds")
dbname <-  tempfile(fileext = ".duckdb")
d2_db_conn <- DBI::dbConnect(duckdb::duckdb(), dbname)
DBI::dbWriteTable(d2_db_conn, "MEDICAL_OBSERVATIONS", mo)

# concept_table
ctname <- tempfile(fileext = ".duckdb")
concepts_db_conn <- DBI::dbConnect(duckdb::duckdb(), ctname)
# Creation of concepts_table within the database
DBI::dbExecute(concepts_db_conn, "CREATE TABLE concept_table (
    ori_id TEXT,     -- Original ID in CDM_database
    ori_table TEXT,   -- Original CDM table
    ROWID INTEGER,  -- Row ID in CDM_database
    person_id TEXT,    
    code TEXT,
    coding_system TEXT,     
    value TEXT,
    concept_id TEXT,     
    date DATE,
    meaning TEXT
);")

# Attach CDM origin database to pre-matching database
attachName <- "d2_db_conn"
DBI::dbExecute(concepts_db_conn, paste0("ATTACH DATABASE '", dbname, "' AS ", attachName))

test_that("Check column names consistency", {
  dap_specific_concept_map <- data.table(
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
    keep_value_column_name = "mo_source_value",
    keep_unit_column_name = NA,
    keep_date_column_name = "mo_date",
    IR1 = NA,
    Comments = NA,
    dap_spec_id = "dap_spec_id-1")
  
  expect_error(
    create_dap_specific_concept(codelist=dap_specific_concept_map,
                                data_db= d2_db_conn,
                                name_attachment = attachName, 
                                save_db= concepts_db_conn, 
                                date_col_filter = "1900-01-01",
                                add_meaning = TRUE)
  )
})


test_that("retrieve mo concepts", {
  dap_specific_concept_map <- data.table(
    concept_id = "WEIGHT_BIRTH",
    dap_name = "CPRD",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    column_name_1 = "mo_meaning",
    column_name_2 = NA,
    column_name_3 = NA,
    expected_value_1 = "weight",
    expected_value_2 = NA,
    expected_value_3 = NA, 
    keep_value_column_name = "mo_source_value",
    keep_unit_column_name = NA,
    keep_date_column_name = "mo_date",
    IR1 = NA,
    Comments = NA,
    dap_spec_id = "dap_spec_id-1")
  
  create_dap_specific_concept(codelist=dap_specific_concept_map,
                              data_db= d2_db_conn,
                              name_attachment = attachName, 
                              save_db= concepts_db_conn, 
                              date_col_filter = "1900-01-01",
                              add_meaning = TRUE)
  mo_concept_table <- 
    DBI::dbReadTable(concepts_db_conn,"concept_table")
  expect_equal(nrow(mo_concept_table), 39)
  
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
  
})

test_that("handle NA values in codelist", {
  dap_specific_concept_map <- data.table(
    concept_id = "WEIGHT_BIRTH",
    dap_name = "CPRD",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    column_name_1 = "mo_meaning",
    column_name_2 = NA,
    column_name_3 = NA,
    expected_value_1 = "weight",
    expected_value_2 = NA,
    expected_value_3 = NA, 
    keep_value_column_name = "mo_source_value",
    keep_unit_column_name = NA,
    # no keep_date_column_name
    IR1 = NA,
    Comments = NA,
    dap_spec_id = "dap_spec_id-1")
  
  create_dap_specific_concept(codelist=dap_specific_concept_map,
                              data_db= d2_db_conn,
                              name_attachment = attachName, 
                              save_db= concepts_db_conn, 
                              date_col_filter = "1900-01-01",
                              add_meaning = TRUE)
  mo_concept_table <- 
    DBI::dbReadTable(concepts_db_conn,"concept_table")
  # date column would be NA because dap_specific_concept_map has no 'keep_date_column_name'
  expect_true(all(is.na(mo_concept_table$date)))
  
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
  
})

DBI::dbDisconnect(d2_db_conn)
DBI::dbDisconnect(concepts_db_conn)
