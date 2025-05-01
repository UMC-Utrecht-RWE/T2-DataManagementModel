unlink("temp", recursive = TRUE) ## Create a temporary folder where to test

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

testthat::test_that(
  "DuplicateRemover calls delete_duplicates_origin with expected arguments",
  {
    testthat::expect_true(
      DuplicateRemover$inherit == "T2.DMM:::DatabaseOperation"
    )

    remover <- T2.DMM:::DuplicateRemover$new()

    loader <- DatabaseLoader$new(
      db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
      config_path = Sys.getenv("CONFIG_PATH"),
      cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
    )

    remover$run(loader)
  }
)

## We conclude by exiting the file
setwd("../")
unlink("temp", recursive = TRUE)
