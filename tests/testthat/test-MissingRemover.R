testthat::test_that(
  "MissingRemover calls delete_missing_origin with expected arguments",
  {
    loader <- create_database_loader(config_path = "CONFIG_SET_DB")
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

    person_db <- DBI::dbGetQuery(loader$db, "SELECT * FROM CDM.PERSONS")
    testthat::expect_true(
      nrow(person_db) == 13,
    )
    vaccines_db <- DBI::dbGetQuery(loader$db, "SELECT * FROM CDM.VACCINES")
    testthat::expect_true(
      nrow(vaccines_db) == 0,
    )
  }
)
