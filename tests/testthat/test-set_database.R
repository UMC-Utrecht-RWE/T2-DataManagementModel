unlink("temp", recursive = TRUE) ## Create a temporary folder where to test

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

testthat::test_that("DatabaseLoader initializes with environment variables", {
  loader <- DatabaseLoader$new(
    db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
    config_path = Sys.getenv("CONFIG_PATH"),
    cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
  )

  testthat::expect_s3_class(loader, "DatabaseLoader")
  testthat::expect_true(DBI::dbIsValid(loader$db))
  testthat::expect_true(!is.null(loader$config))
  testthat::expect_s3_class(loader$metadata, "data.table")

  DBI::dbDisconnect(loader$db)
})

testthat::test_that("DatabaseLoader runs set_database() without error", {
  loader <- DatabaseLoader$new(
    db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
    config_path = Sys.getenv("CONFIG_PATH"),
    cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
  )

  # We'll just check that it doesn't throw
  testthat::expect_error(
    loader$set_database(),
    NA # means expect no error
  )

  DBI::dbDisconnect(loader$db)
})

testthat::test_that("DatabaseLoader runs enabled operations in config", {
  loader <- DatabaseLoader$new(
    db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
    config_path = Sys.getenv("CONFIG_PATH"),
    cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
  )

  # Suppress output and check no error is raised
  testthat::expect_error(
    suppressMessages(loader$run_db_ops()),
    NA
  )

  testthat::expect_false(DBI::dbIsValid(loader$db))
})

setwd("../")
unlink("temp", recursive = TRUE)
