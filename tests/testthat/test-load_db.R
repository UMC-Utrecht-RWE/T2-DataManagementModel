testthat::test_that("load_db runs end-to-end with all possible combinations of inputs", {
  source_dir <- local_dbtest_copy()
  for (cdb in c("views", "tables")) { # nolint
    dbname <- tempfile("test.duckdb")
    con <- DBI::dbConnect(duckdb::duckdb(), dbname)
    message(paste("Testing load_db pipeline with
                create_db_as = ", cdb, "\n"))
    testthat::expect_message( # nolint
      load_db(
        con = con,
        data_model = "ConcePTION",
        cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.yaml",
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

testthat::test_that("load_db extracts tables from a database connection via parquet", {
  # A DuckDB connection stands in for PostgreSQL
  pg <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(pg, shutdown = TRUE))
  DBI::dbExecute(pg, paste0(
    "CREATE TABLE events AS SELECT * FROM read_csv_auto('",
    testthat::test_path("dbtest", "EVENTS.csv"), "', ALL_VARCHAR = TRUE)"
  ))
  n_events <- DBI::dbGetQuery(pg, "SELECT count(*) AS n FROM events")$n

  source_dir <- withr::local_tempdir()
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  testthat::expect_message(
    load_db(
      con = con,
      data_model = "ConcePTION",
      cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.yaml",
      folder_path_to_source_files = source_dir,
      create_db_as = "tables",
      tables_in_cdm = c("EVENTS"),
      pg_con = pg,
      pg_schema = "main"
    ),
    regexp = "Hooray! Script finished running!"
  )

  testthat::expect_length(list.files(source_dir, pattern = "\\.parquet$"), 0)
  testthat::expect_true(length(list.files(
    file.path(source_dir, "intermediate_parquet"),
    pattern = "^EVENTS.*\\.parquet$"
  )) > 0)
  n_loaded <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM ConcePTION.EVENTS")$n
  testthat::expect_equal(n_loaded, n_events)
})
