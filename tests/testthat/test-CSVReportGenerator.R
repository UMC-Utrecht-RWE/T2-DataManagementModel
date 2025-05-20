testthat::test_that("CSVReportGenerator creates a valid .csv file", {
  csv_report_generator <- CSVReportGenerator$new()

  # Confirm inheritance
  testthat::expect_s3_class(csv_report_generator, "CSVReportGenerator")
  testthat::expect_s3_class(csv_report_generator, "ReportGenerator")

  # Fake data
  test_data <- data.frame(
    id = 1:5,
    name = letters[1:5],
    stringsAsFactors = FALSE
  )

  # Use temp directory
  report_path <- withr::local_tempdir()
  report_name <- "test_output.csv"
  full_path <- file.path(report_path, report_name)

  # Call write_report
  csv_report_generator$write_report(
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

  # File exists
  testthat::expect_true(file.exists(full_path))

  # Read result and normalize class
  result <- as.data.frame(readr::read_csv(full_path, col_types = readr::cols()))
  testthat::expect_equal(result, test_data)
})
