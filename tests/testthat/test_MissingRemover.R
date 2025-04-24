unlink("temp", recursive = TRUE)

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

testthat::test_that(
  "MissingRemover calls delete_missing_origin with expected arguments",
  {
    testthat::expect_true(MissingRemover$inherit == "DatabaseOperation")

    remover <- MissingRemover$new()
    testthat::expect_s3_class(remover, "MissingRemover")
    testthat::expect_s3_class(remover, "DatabaseOperation")

    loader <- DatabaseLoader$new(
      db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
      config_path = Sys.getenv("CONFIG_PATH"),
      cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
    )

    remover$run(loader)
  }
)

setwd("../")
unlink("temp", recursive = TRUE)
