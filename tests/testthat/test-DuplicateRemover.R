testthat::test_that(
  "DuplicateRemover calls delete_duplicates_origin with expected arguments",
  {
    temp_dir <- withr::local_tempdir()
    setwd(temp_dir)

    testthat::expect_true(
      DuplicateRemover$inherit == "T2.DMM:::DatabaseOperation"
    )
    remover <- T2.DMM:::DuplicateRemover$new()

    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$set_database()

    testthat::expect_error(
      remover$run(loader),
      NA # means expect no error
    )

    # To check if table are removed check function test delete_duplicates_origin
  }
)
