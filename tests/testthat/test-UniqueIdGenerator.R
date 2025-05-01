unlink("temp", recursive = TRUE)

if (!file.exists("temp")) {
  dir.create("temp")
}
setwd("temp")

testthat::test_that(
  "UniqueIdGenerator calls create_unique_id with expected arguments",
  {
    testthat::expect_true(
      UniqueIdGenerator$inherit == "T2.DMM:::DatabaseOperation"
    )

    generator <- T2.DMM:::UniqueIdGenerator$new()
    testthat::expect_s3_class(generator, "UniqueIdGenerator")

    loader <- DatabaseLoader$new(
      db_path = Sys.getenv("SYNTHETIC_DB_PATH"),
      config_path = Sys.getenv("CONFIG_PATH"),
      cdm_metadata = Sys.getenv("SHARED_METADATA_PATH")
    )

    generator$run(loader)
  }
)

setwd("../")
unlink("temp", recursive = TRUE)
