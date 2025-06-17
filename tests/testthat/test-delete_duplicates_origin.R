test_that("Checking if the function delete the duplicates cases", {
  # Load the database
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  expect_equal(nrow(vx_db), nrow(vx1) + nrow(vx2))

  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = db_con, scheme, save_deleted = FALSE
  )

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  # The first 10 rows of vx1 are duplicates of vx2
  expect_equal(nrow(vx_db), nrow(vx1))
})
