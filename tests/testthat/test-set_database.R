testthat::test_that("DatabaseLoader initializes with environment variables", {
  loader <- create_database_loader(config_path = "CONFIG_SET_DB")

  testthat::expect_s3_class(loader, "DatabaseLoader")
  testthat::expect_true(DBI::dbIsValid(loader$db))
  testthat::expect_true(!is.null(loader$config))
  testthat::expect_s3_class(loader$metadata, "data.table")

  DBI::dbDisconnect(loader$db)
})

testthat::test_that("DatabaseLoader forwards configuration to load_db", {
  loader <- create_database_loader(config_path = "CONFIG_SET_DB")
  withr::defer(DBI::dbDisconnect(loader$db))
  load_db_args <- NULL

  testthat::local_mocked_bindings(
    load_db = function(...) {
      load_db_args <<- list(...)
    },
    .package = "T2.DMM"
  )

  loader$set_database()

  testthat::expect_identical(load_db_args$con, loader$db)
  testthat::expect_identical(load_db_args$data_model, loader$config$data_model)
  testthat::expect_identical(load_db_args$cdm_schema, loader$config$cdm_schema)
  testthat::expect_identical(
    load_db_args$format_source_files,
    loader$config$file_format
  )
  testthat::expect_identical(
    load_db_args$folder_path_to_source_files,
    loader$data_instance
  )
  testthat::expect_identical(
    load_db_args$through_parquet,
    loader$config$load_db_through_parquet
  )
  testthat::expect_identical(
    load_db_args$create_db_as,
    loader$config$create_db_as
  )
  testthat::expect_identical(
    load_db_args$tables_in_cdm,
    loader$config$cdm_tables_names
  )
})

testthat::test_that("With imported cdm_metadata", {
  testthat::expect_error(
    loader <- create_loader_from_file("CONFIG_SET_DB"),
    NA # means expect no error
  )
})

testthat::test_that("With imported cdm_metadata", {
  testthat::expect_error(
    loader <- create_loader_from_wrong_file("CONFIG_SET_DB"),
    "cdm_metadata not valid data.table object."
  )
})

testthat::test_that("set_database reports errors from load_db", {
  loader <- create_loader_from_file("CONFIG_SET_DB")
  withr::defer(DBI::dbDisconnect(loader$db))

  testthat::local_mocked_bindings(
    load_db = function(...) {
      stop("load failure")
    },
    .package = "T2.DMM"
  )

  testthat::expect_message(
    loader$set_database(),
    "Error loading database:"
  )
})

testthat::test_that("Running run_db_ops", {
  loader <- create_database_loader(config_path = "CONFIG_SET_DB")
  loader$set_database()
  testthat::expect_no_error(
    loader$run_db_ops()
  )
})

testthat::test_that("Absent operation", {
  loader <- create_database_loader("CONFIG_ABSENT")
  loader$set_database()
  testthat::expect_no_error(
    loader$run_db_ops()
  )
})
