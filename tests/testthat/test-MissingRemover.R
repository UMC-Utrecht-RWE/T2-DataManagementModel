testthat::test_that(
  "MissingRemover calls delete_missing_origin with expected arguments",
  {
    testthat::expect_true(
      MissingRemover$inherit == "T2.DMM::DatabaseOperation"
    )

    remover <- MissingRemover$new()
    testthat::expect_s3_class(remover, "MissingRemover")
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
