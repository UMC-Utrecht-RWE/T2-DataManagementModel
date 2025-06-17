testthat::test_that(
  "ReportGenerator calls generate_report with expected arguments",
  {
    testthat::expect_true(
      T2.DMM:::ReportGenerator$inherit == "T2.DMM:::DatabaseOperation"
    )

    reporter <- T2.DMM:::ReportGenerator$new()
    testthat::expect_s3_class(reporter, "ReportGenerator")

    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$set_database()

    testthat::expect_error(
      reporter$run(loader),
      NA # means expect no error
    )

    # Test if the file is created
    testthat::expect_true(
      file.exists("count_rows_origin.fst")
    )
    # Clean up the generated file
    if (file.exists("count_rows_origin.fst")) {
      file.remove("count_rows_origin.fst")
    }

  }
)

testthat::test_that(
  "ReportGenerator with report_name without extension throws an error",
  {
    reporter <- T2.DMM:::ReportGenerator$new()

    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$config$report_generator$report_name <- "count_rows_origin"
    loader$set_database()
    testthat::expect_error(reporter$run(loader),
      "Invalid or missing file extension in report_name: count_rows_origin"
    )
  }
)

testthat::test_that(
  "ReportGenerator with report_name missing throws an error",
  {
    reporter <- T2.DMM:::ReportGenerator$new()

    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$config$report_generator$report_name <- ""
    loader$set_database()
    testthat::expect_error(reporter$run(loader),
      "The report_name in report_generator is missing or empty."
    )
  }
)

testthat::test_that(
  "ReportGenerator with a not-implemented extension throws an error",
  {
    reporter <- T2.DMM:::ReportGenerator$new()

    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$config$report_generator$report_name <- "count_rows_origin.txt"
    loader$set_database()
    testthat::expect_error(reporter$run(loader),
      "No ReportGenerator subclass found for extension: txt"
    )
  }
)
