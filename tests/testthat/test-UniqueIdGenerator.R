testthat::test_that(
  "UniqueIdGenerator calls create_unique_id with expected arguments",
  {
    temp_dir <- withr::local_tempdir()
    setwd(temp_dir)
    testthat::expect_true(
      UniqueIdGenerator$inherit == "T2.DMM:::DatabaseOperation"
    )

    generator <- T2.DMM:::UniqueIdGenerator$new()
    testthat::expect_s3_class(generator, "UniqueIdGenerator")

    loader <- create_database_loader(config_path = "CONFIG_SET_DB")

    generator$run(loader)
  }
)
