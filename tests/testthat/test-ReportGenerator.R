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
