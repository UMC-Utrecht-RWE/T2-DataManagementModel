unlink("temp", recursive = TRUE)

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

testthat::test_that(
  "ReportGenerator calls generate_report with expected arguments", {
    testthat::expect_true(ReportGenerator$inherit == "DatabaseOperation")

    generator <- ReportGenerator$new()
    testthat::expect_s3_class(generator, "ReportGenerator")
    testthat::expect_s3_class(generator, "DatabaseOperation")
  }
)

setwd("../")
unlink("temp", recursive = TRUE)