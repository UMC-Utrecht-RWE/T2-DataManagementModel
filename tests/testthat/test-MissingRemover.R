testthat::test_that(
  "MissingRemover calls delete_missing_origin with expected arguments",
  {
    testthat::expect_true(
      MissingRemover$inherit == "T2.DMM:::DatabaseOperation"
    )

    remover <- T2.DMM:::MissingRemover$new()
    testthat::expect_s3_class(remover, "MissingRemover")

    loader <- create_database_loader(config_path = "CONFIG_SET_DB")

    remover$run(loader)
  }
)
