testthat::test_that(
  "DuplicateRemover calls delete_duplicates_origin with expected arguments",
  {
    temp_dir <- withr::local_tempdir()
    setwd(temp_dir)
    testthat::expect_true(
      DuplicateRemover$inherit == "T2.DMM::DatabaseOperation"
    )

    remover <- DuplicateRemover$new()
    testthat::expect_s3_class(remover, "DuplicateRemover")
    testthat::expect_s3_class(remover, "DatabaseOperation")

    loader <- DatabaseLoader$new(
      db_path = "",
      data_instance = "dbtest",
      config_path = Sys.getenv("CONFIG_PATH"),
      cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
    )

    remover$run(loader)
  }
)
