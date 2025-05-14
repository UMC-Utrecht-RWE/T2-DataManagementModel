test_that("Names and number of new columns added", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))

  create_unique_id(db_con,
    cdm_tables_names = c("PERSONS", "VACCINES"),
    extension_name = "", id_name = "ori_id",
    separator_id = "-"
  )

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  expect_contains(names(vx_db), c("ori_id", "ROWID", "ori_table"))

  persons_db <- DBI::dbReadTable(db_con, "persons")
  expect_contains(names(persons_db), c("ori_id", "ROWID", "ori_table"))
})

test_that("Checking the ROWID is the same as the number of rows of the table", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))

  create_unique_id(db_con,
    cdm_tables_names = c("PERSONS"),
    extension_name = "", id_name = "ori_id",
    separator_id = "-"
  )
  persons_db <- DBI::dbReadTable(db_con, "persons")
  max_rowID <- max(persons_db$ROWID)
  expect_equal(nrow(persons_db) - 1, max_rowID)
})

test_that("Checking the OriTable is the same as the included table", {
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))

  create_unique_id(db_con,
    cdm_tables_names = c("PERSONS"),
    extension_name = "", id_name = "ori_id",
    separator_id = "-"
  )


  persons_db <- DBI::dbReadTable(db_con, "persons")
  persons <- import_file("dbtest/PERSONS.csv")
  ori_table_name <- unique(persons_db$ori_table)
  expect_equal("PERSONS", ori_table_name)
})
