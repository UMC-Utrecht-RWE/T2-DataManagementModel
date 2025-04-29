library(dplyr)
library(testthat)
library(data.table)
source("R/create_dap_specific_codelist.R")

unique_codelist <- data.table::fread("tests/testthat/data/unique_codelist.csv")
study_codelist  <- data.table::fread("tests/testthat/data/study_codelist.csv")


# Testing the input
## values of create_dap_specific_codelist
testthat::test_that(
  "create_dap_specific_codelist throws an error for non-data.table inputs", {
    testthat::expect_error(
      create_dap_specific_codelist(
        unique_codelist = "not a data.table",
        study_codelist = study_codelist
      ),
      "unique_codelist must be a data.table"
    )
    testthat::expect_error(
      create_dap_specific_codelist(
        unique_codelist = unique_codelist,
        study_codelist = "not a data.table"
      ),
      "study_codelist must be a data.table"
    )
  }
)

testthat::test_that(
  "create_dap_specific_codelist errors for start_with_cols format", {
    testthat::expect_error(
      create_dap_specific_codelist(
        unique_codelist, study_codelist, start_with_cols = 123
      ),
      "start_with_cols must be a character vector"
    )
  }
)

# Testing the output
testthat::test_that(
  "create_dap_specific_codelist returns a data.table when inputs are correct", {
    result <- create_dap_specific_codelist(unique_codelist, study_codelist)
    testthat::expect_s3_class(result, "data.table")
  }
)
