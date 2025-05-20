testthat::test_that(
  "DuplicateRemover calls delete_duplicates_origin with expected arguments",
  {
    loader <- create_database_loader(config_path = "CONFIG_SET_DB")

    testthat::expect_true(
      DuplicateRemover$inherit == "T2.DMM:::DatabaseOperation"
    )

    remover <- T2.DMM:::DuplicateRemover$new()
    testthat::expect_error(
      remover$run(loader),
      NA # means expect no error
    )

    # To check if table are removed check function test delete_duplicates_origin

    DBI::dbDisconnect(loader$db)
  }
)
