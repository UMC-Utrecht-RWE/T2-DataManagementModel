testthat::test_that("FSTReportGenerator creates a valid .fst file", {

  temp_dir <- withr::local_tempdir()
  setwd(temp_dir)

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
  report_path <- getwd()
  report_name <- "test_output.fst"

  # Call write_report
  fst_report_generator$write_report(
    data = test_data,
    db_loader = list(
      config = list(
        report_generator = list(
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

  # remove the test file after the test
  file.remove(full_path)
})

testthat::test_that("report_path is missing", {
  fst_report_generator <- FSTReportGenerator$new()
  # The error should indicate that the directory does not exist
  expect_error(
    fst_report_generator$write_report(
      data = data.frame(),
      db_loader = list(
        config = list(
          report_generator = list(
            report_path = "non_existent_directory",
            report_name = "test_output.fst"
          )
        )
      )
    ),
    glue::glue(
      "The directory non_existent_directory does not exist."
    )
  )
})