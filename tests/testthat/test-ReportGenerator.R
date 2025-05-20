testthat::test_that(
  "ReportGenerator calls generate_report with expected arguments",
  {
    temp_dir <- withr::local_tempdir()
    setwd(temp_dir)

    testthat::expect_true(
      ReportGenerator$inherit == "T2.DMM:::DatabaseOperation"
    )

    generator <- T2.DMM:::ReportGenerator$new()
    testthat::expect_s3_class(generator, "ReportGenerator")

    # # dbname <- tempfile(fileext = ".duckdb")
    # # con <- DBI::dbConnect(duckdb::duckdb(), dbname)
    # source("/Users/mcinelli/repos/T2-DataManagementModel/tests/testthat/setup.R")

    # loader <- T2.DMM::DatabaseLoader$new(
    #   db_path = "/Users/mcinelli/repos/RSV-1026/somewhere/d2.duckdb",
    #   data_instance = "/Users/mcinelli/repos/RSV-1026/somewhere",
    #   config_path = Sys.getenv("CONFIG_PATH"),
    #   cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
    # )

    # invisible(T2.DMM:::FSTReportGenerator)
    # generator$run(loader)
  }
)
