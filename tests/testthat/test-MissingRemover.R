testthat::test_that(
  "MissingRemover calls delete_missing_origin with expected arguments",
  {
    loader <- create_database_loader(config_path = "CONFIG_PATH")
    loader$set_database()

    testthat::expect_true(
      T2.DMM:::MissingRemover$inherit == "T2.DMM:::DatabaseOperation"
    )

    remover <- T2.DMM:::MissingRemover$new()
    testthat::expect_s3_class(remover, "MissingRemover")

    testthat::expect_error(
      remover$run(loader),
      NA # means expect no error
    )

    person_db <- DBI::dbReadTable(loader$db, "PERSONS")
    testthat::expect_true(
      nrow(person_db) == 13,
    )
    vaccines_db <- DBI::dbReadTable(loader$db, "VACCINES")
    testthat::expect_true(
      nrow(vaccines_db) == 0,
    )
  }
)

testthat::test_that("No missing rows message", {
  loader <- create_database_loader(config_path = "CONFIG_PATH")
  loader$set_database()
  remover <- T2.DMM:::MissingRemover$new()

  loader$config$missing_remover$columns$PERSONS <- "person_id"

  testthat::expect_message(
    remover$run(loader),
    "No missing rows in PERSONS.person_id"
  )

})

testthat::test_that("Table to clean does not exist", {
  loader <- create_database_loader(config_path = "CONFIG_PATH")
  loader$set_database()
  remover <- T2.DMM:::MissingRemover$new()

  loader$config$missing_remover$columns$NON_EXISTENT <- "non_existent_column"

  testthat::expect_output(
    remover$run(loader),
    "Table NON_EXISTENT does not exist."
  )
})