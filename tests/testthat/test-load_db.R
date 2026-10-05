testthat::test_that("load_db runs end-to-end with all possible combinations of inputs", {
  source_dir <- local_dbtest_copy()
  for (cdb in c("views", "tables")) { # nolint
    dbname <- tempfile("test.duckdb")
    con <- DBI::dbConnect(duckdb::duckdb(), dbname)
    cat(paste("Testing load_db pipeline with
                create_db_as = ", cdb, "\n"))
    testthat::expect_output( # nolint
      load_db(
        con = con,
        data_model = "ConcePTION",
        cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.json",
        format_source_files = "csv",
        folder_path_to_source_files = source_dir,
        create_db_as = cdb,
        tables_in_cdm = c("EVENTS", "MEDICINES", "MEDICAL_OBSERVATIONS")
      ),
      regexp = "Hooray! Script finished running!"
    )
    dbDisconnect(con)
    rm(con)
    gc()
  }
})
