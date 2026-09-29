testthat::test_that("load_db runs end-to-end with 
                    all possible combinations of inputs", {
  for (tp in c("yes", "no")) {
    for (cdb in c("views", "tables")) {
      dbname <- tempfile("ConcePTION.duckdb")
      con <- DBI::dbConnect(duckdb::duckdb(), dbname)
      cat(paste("Testing load_db pipeline with through_parquet = ", tp, " 
                and create_db_as = ", cdb, "\n"))

      if (tp == "no" && cdb == "views") {
        testthat::expect_output(
          load_db(
            con = con,
            data_model = "ConcePTION",
            excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
            format_source_files = "csv",
            folder_path_to_source_files = "dbtest/",
            through_parquet = tp,
            create_db_as = cdb,
            tables_in_cdm = c("EVENTS", "MEDICINES", "MEDICAL_OBSERVATIONS")
          ),
          regexp = "Invalid parameter combination"
        )
      } else {
        testthat::expect_output(
          load_db(
            con = con,
            data_model = "ConcePTION",
            excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
            format_source_files = "csv",
            folder_path_to_source_files = "dbtest/",
            through_parquet = tp,
            create_db_as = cdb,
            tables_in_cdm = c("EVENTS", "MEDICINES", "MEDICAL_OBSERVATIONS")
          ),
          regexp = "Hooray! Script finished running!"
        )
      }
    }
  }
})
