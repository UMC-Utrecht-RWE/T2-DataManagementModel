testthat::test_that(
  "ReportGenerator calls generate_report with expected arguments",
  {
    temp_dir <- withr::local_tempdir()
    setwd(temp_dir)
    testthat::expect_true(
      ReportGenerator$inherit == "T2.DMM::DatabaseOperation"
    )

    generator <- ReportGenerator$new()
    testthat::expect_s3_class(generator, "ReportGenerator")
    testthat::expect_s3_class(generator, "DatabaseOperation")
  }
)
