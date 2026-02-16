# create a good input example
good_input <- data.table(
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
  dap_spec_id = "dap_spec_id-1"
)

# Attach CDM origin database to pre-matching database
attach_name <- "d2_db_conn"

db_setup_test <- function(good_input) {
  # save db to temp connection
  mo <- readRDS("dbtest/MEDICAL_OBSERVATIONS.rds")
  dbname <-  tempfile(fileext = ".duckdb")
  d2_db_conn <- DBI::dbConnect(duckdb::duckdb(), dbname)
  DBI::dbWriteTable(d2_db_conn, "MEDICAL_OBSERVATIONS", mo, overwrite = TRUE)
  T2.DMM:::create_unique_id(
    d2_db_conn,
    cdm_tables_names = "MEDICAL_OBSERVATIONS",
    to_view = FALSE
  )
  DBI::dbDisconnect(d2_db_conn)

  # concept_table
  ctname <- tempfile(fileext = ".duckdb")
  concepts_db_conn <- DBI::dbConnect(duckdb::duckdb(), ctname)
  # Creation of concepts_table within the database
  DBI::dbExecute(concepts_db_conn, "CREATE TABLE concept_table (
    ori_table TEXT,   -- Original CDM table
    unique_id UUID,
    person_id TEXT,
    code TEXT,
    coding_system TEXT,
    value TEXT,
    concept_id TEXT,
    date DATE,
    meaning TEXT
);")

  DBI::dbExecute(
    concepts_db_conn,
    paste0("ATTACH DATABASE '", dbname, "' AS ", attach_name)
  )
  concepts_db_conn
}

testthat::test_that("Check column names consistency", {
  dap_specific_concept_map <- data.table::data.table(
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
    dap_spec_id = "dap_spec_id-1"
  )

  testthat::expect_error(
    create_dap_specific_concept(
      codelist = dap_specific_concept_map,
      name_attachment = attach_name,
      save_db = concepts_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE
    )
  )
})


testthat::test_that("retrieve mo concepts", {
  concepts_db_conn <- db_setup_test()
  create_dap_specific_concept(
    codelist = good_input,
    name_attachment = attach_name,
    save_db = concepts_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )
  mo_concept_table <-
    DBI::dbReadTable(concepts_db_conn, "concept_table")
  testthat::expect_equal(nrow(mo_concept_table), 39)

  DBI::dbExecute(
    concepts_db_conn,
    "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
})

# testthat::test_that("handle NA values in codelist", {
#   concepts_db_conn <- db_setup_test()
#   codelist_wo_date <- good_input %>% dplyr::select(-keep_date_column_name)
#
#   create_dap_specific_concept(codelist= codelist_wo_date,
#                               name_attachment = attach_name,
#                               save_db= concepts_db_conn,
#                               date_col_filter = "1900-01-01",
#                               add_meaning = TRUE)
#   mo_concept_table <-
#     DBI::dbReadTable(concepts_db_conn,"concept_table")
#   # date column would be NA because dap_specific_concept_map has no "keep_date_column_name"
#   expect_true(all(is.na(mo_concept_table$date)))
#
#   DBI::dbExecute(concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec")
#   rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
#   DBI::dbClearResult(rs)
#
#   codelist_wo_date <- good_input %>% dplyr::mutate(keep_date_column_name = NA)
#
#   create_dap_specific_concept(codelist= codelist_wo_date,
#                               name_attachment = attach_name,
#                               save_db= concepts_db_conn,
#                               date_col_filter = "1900-01-01",
#                               add_meaning = TRUE)
#   mo_concept_table <-
#     DBI::dbReadTable(concepts_db_conn,"concept_table")
#   # date column would be NA because dap_specific_concept_map has no "keep_date_column_name"
#   expect_true(all(is.na(mo_concept_table$date)))
#
#   DBI::dbExecute(concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec")
#   rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
#   DBI::dbClearResult(rs)
#   DBI::dbDisconnect(concepts_db_conn)
# })

testthat::test_that("empty codelist", {
  concepts_db_conn <- db_setup_test()
  codelist <- data.frame(test = c())
  testthat::expect_error(
    create_dap_specific_concept(
      codelist,
      name_attachment = attach_name,
      save_db = concepts_db_conn
    ),
    "Codelist does not contain any data."
  )
  DBI::dbDisconnect(concepts_db_conn)
})

testthat::test_that("created MEDICAL_OBSERVATIONS_EDITED", {
  concepts_db_conn <- db_setup_test()
  DBI::dbWriteTable(
    concepts_db_conn,
    "MEDICAL_OBSERVATIONS_EDITED",
    data.frame(test = 1),
    overwrite = TRUE
  )
  create_dap_specific_concept(
    codelist=good_input,
    name_attachment = attach_name,
    save_db= concepts_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )
  testthat::expect_true(
    "MEDICAL_OBSERVATIONS_EDITED" %in% DBI::dbListTables(concepts_db_conn)
  )
  DBI::dbExecute(
    concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
  DBI::dbDisconnect(concepts_db_conn)
})

testthat::test_that("reference non-existent column", {
  concepts_db_conn <- db_setup_test()
  codelist <- good_input |> dplyr::mutate(
    column_name_1 = "something",
    expected_value_1 = "anything"
  )
  testthat::expect_error(
    create_dap_specific_concept(
      codelist = codelist,
      name_attachment = attach_name,
      save_db= concepts_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE
    ),
    # "Binder Error: Column "something" referenced"
  )
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
  DBI::dbDisconnect(concepts_db_conn)
})

testthat::test_that("NA keep_value_column_name", {
  concepts_db_conn <- db_setup_test()
  codelist <- good_input |> dplyr::mutate(keep_value_column_name = NA)
  create_dap_specific_concept(
    codelist = codelist,
    name_attachment = attach_name,
    save_db = concepts_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )
  DBI::dbExecute(
    concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )
  mo_concept_table <-
    DBI::dbReadTable(concepts_db_conn, "concept_table")
  testthat::expect_true(all(mo_concept_table$value == "true"))

  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)

  codelist <- good_input |> dplyr::select(-keep_value_column_name)
  create_dap_specific_concept(
    codelist = codelist,
    name_attachment = attach_name,
    save_db = concepts_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )
  mo_concept_table <-
    DBI::dbReadTable(concepts_db_conn, "concept_table")
  testthat::expect_true(all(mo_concept_table$value == "true"))

  DBI::dbExecute(
    concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )
  rs <- DBI::dbSendQuery(concepts_db_conn, "DELETE FROM concept_table")
  DBI::dbClearResult(rs)
  DBI::dbDisconnect(concepts_db_conn)
})


testthat::test_that("skip extracting meaning column", {
  concepts_db_conn <- db_setup_test()
  rs <- DBI::dbSendQuery(
    concepts_db_conn, "ALTER TABLE concept_table\nDROP COLUMN meaning"
  )

  create_dap_specific_concept(
    codelist = good_input,
    name_attachment = attach_name,
    save_db = concepts_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = FALSE
  )
  mo_concept_table <-
    DBI::dbReadTable(concepts_db_conn, "concept_table")
  expect_true(all(is.na(mo_concept_table$meaning)))
  DBI::dbExecute(
    concepts_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )
  DBI::dbDisconnect(concepts_db_conn)
})
