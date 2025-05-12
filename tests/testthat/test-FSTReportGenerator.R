testthat::test_that("FSTReportGenerator creates a valid .fst file", {
  # Create the generator
  fst_report_generator <- FSTReportGenerator$new()

  # Confirm inheritance
  testthat::expect_s3_class(fst_report_generator, "FSTReportGenerator")
  testthat::expect_s3_class(fst_report_generator, "ReportGenerator")

  # Define fake data and target save path
  test_data <- data.frame(
    id = 1:5,
    name = letters[1:5],
    stringsAsFactors = FALSE
  )

  # Use a temp directory for safe, isolated testing
  report_path <- withr::local_tempdir()
  report_name <- "test_output.fst"

  # Call write_report
  fst_report_generator$write_report(
    data = test_data,
    db_loader = list(
      config = list(
        report = list(
          report_path = report_path,
          report_name = report_name
        )
      )
    )
  )

  # Verify that file exists
  full_path <- file.path(report_path, report_name)
  testthat::expect_true(file.exists(full_path))

  # Verify content integrity
  result <- fst::read_fst(full_path)
  testthat::expect_equal(result, test_data)
})
